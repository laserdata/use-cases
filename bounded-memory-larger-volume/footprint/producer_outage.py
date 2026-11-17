"""Test 2 at scale: one producer's memory at a high send rate to a LaserData Cloud cluster, with
the network to the cluster cut for 60 seconds halfway.

  produce  one Laser SDK producer in background (buffered) mode, with the SDK's default buffer
           budget, sends RATE records/s (1 KiB each, default 8,000/s, the same as the first blog) for 30 minutes. Halfway,
           every connection from this VM to the cluster is dropped for 60 seconds (iptables on
           the client VM), so nothing is acknowledged and the client buffer fills. A managed
           cluster can't be paused, so the outage is made on the network instead.
  consume  WORKERS consumer processes share one consumer group and check every record; each
           writes the sequence numbers it saw to seen-<i>.bin
  verify   joins the workers' bitmaps and reports missing records, and which of them the SDK
           reported to the application as not confirmed

Usage: python producer_outage.py consume --worker 0 --workers 6 &  ...
       python producer_outage.py produce
       python producer_outage.py verify --workers 6
"""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import threading
import time

import common as c
import laser_sdk as ls

TEST = "producer_outage"
STREAM, TOPIC = "scale-outage", "events"
RATE = int(os.environ.get("SCALE_RATE", "8000"))
PARTITIONS = int(os.environ.get("SCALE_PARTITIONS", "12"))
OUTAGE_SECONDS = 60


def planned_seconds() -> int:
    return c.scaled(30 * 60)


def planned_records() -> int:
    return RATE * planned_seconds()


def cluster_ips() -> list[str]:
    """Every remote address this VM holds a TCP connection to, except SSH and Google's metadata."""
    out = subprocess.run(["ss", "-tnH", "state", "established"], capture_output=True, text=True).stdout
    ips = set()
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        local, remote = parts[-2], parts[-1]
        rip, _, rport = remote.rpartition(":")
        lport = local.rpartition(":")[2]
        rip = rip.strip("[]")
        if "22" in (rport, lport) or rip.startswith("169.254.") or rip.startswith("127."):
            continue
        ips.add(rip)
    return sorted(ips)


def iptables(action: str, ip: str) -> None:
    for chain, flag in (("OUTPUT", "-d"), ("INPUT", "-s")):
        subprocess.run(["sudo", "iptables", action, chain, flag, ip, "-j", "DROP"], check=False)


def cut_network(after_seconds: float) -> None:
    time.sleep(after_seconds)
    ips = cluster_ips()
    c.set_phase(TEST, "network-cut")
    for ip in ips:
        iptables("-I", ip)
    c.event(TEST, "network_cut", addresses=len(ips))
    time.sleep(OUTAGE_SECONDS)
    for ip in ips:
        iptables("-D", ip)
    c.set_phase(TEST, "after-restore")
    c.event(TEST, "network_restored")


async def produce() -> int:
    c.register_client("producer")
    seconds, total = planned_seconds(), planned_records()
    failures: list[dict] = []
    reported: list[int] = []

    def on_error(error) -> None:
        detail = {"error": repr(error)[:300]}
        for field in ("committed", "unconfirmed"):
            items = getattr(error, field, None) or []
            detail[field] = items if isinstance(items, int) else len(items)
            if field == "unconfirmed" and not isinstance(items, int):
                for item in items:
                    data = item if isinstance(item, (bytes, bytearray)) else getattr(item, "payload", None)
                    if data:
                        reported.append(c.seq_of(bytes(data)))
        failures.append(detail)

    laser = await c.connect()
    lag = 0.0
    try:
        producer = laser.stream(STREAM).topic(TOPIC).producer(
            partitions=PARTITIONS, create_stream=True, never_expire=True,
            background=ls.BackgroundConfig(error_callback=on_error))  # SDK default buffer budget
        await producer.init()
        c.set_phase(TEST, "steady-before")
        start = time.perf_counter()
        cutter = threading.Thread(target=cut_network, args=(seconds / 2,), daemon=True)
        cutter.start()
        sent = 0
        while sent < total:
            await producer.send_batch([c.payload(s) for s in range(sent, sent + c.BATCH)])
            sent += c.BATCH
            ahead = start + sent / RATE - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
            else:
                lag = max(lag, -ahead)
        cutter.join()
        try:
            await producer.shutdown()  # flushes the buffer
        except Exception as error:
            on_error(error)
        took = time.perf_counter() - start
    finally:
        await laser.close()
        c.unregister_client("producer")
    (c.OUT / "outage_total").write_text(str(total))
    result = {"records_sent": total, "rate_target": RATE, "partitions": PARTITIONS,
              "seconds": round(took, 1), "achieved_rate": round(total / took),
              "max_behind_schedule_s": round(lag, 1), "outage_seconds": OUTAGE_SECONDS,
              "failed_writes": len(failures), "failures": failures[:10],
              "records_reported_unconfirmed": sum(f.get("unconfirmed", 0) for f in failures),
              "producer_settings": "Laser SDK background mode, default buffer budget, block when full"}
    (c.OUT / "reported_unconfirmed.txt").write_text("\n".join(map(str, sorted(reported))))
    c.write_result(f"{TEST}-producer", result)
    print(result)
    return 0


async def consume(worker: int) -> int:
    role = f"consumer-{worker}"
    c.register_client(role)
    total = planned_records()
    tracker = c.BitTracker(total)
    marker = c.OUT / "outage_total"
    laser = await c.connect()
    try:
        topic = laser.stream(STREAM).topic(TOPIC)
        await topic.ensure(PARTITIONS)
        live = await c.consume_live(topic.consumer_group("footprint"), tracker,
                                    lambda: total if marker.exists() else None, TEST)
    finally:
        await laser.close()
        c.unregister_client(role)
    (c.OUT / f"seen-{worker}.bin").write_bytes(bytes(tracker.bits))
    result = {"worker": worker, "received_unique": tracker.unique, "duplicates": tracker.duplicates,
              "consumer_errors": live["errors"], "consumer_recreated": len(live["recreations"]),
              "gaps_over_half_second": len(live["gaps"])}
    c.write_result(f"{TEST}-{role}", result)
    print(result)
    return 0


def verify(workers: int) -> int:
    total = planned_records()
    bits = bytearray((total + 7) // 8)
    for w in range(workers):
        part = (c.OUT / f"seen-{w}.bin").read_bytes()
        for i, b in enumerate(part):
            bits[i] |= b
    missing, runs = c.missing_from_bits(bits, total)
    path = c.OUT / "reported_unconfirmed.txt"
    reported = {int(x) for x in path.read_text().split()} if path.exists() else set()
    not_reported = 0
    if missing:
        not_reported = sum(1 for s in range(total)
                           if not bits[s >> 3] & (1 << (s & 7)) and s not in reported)
    result = {"records_expected": total, "missing": missing, "missing_not_reported": not_reported,
              "missing_ranges": runs, "workers": workers}
    c.write_result(f"{TEST}-verify", result)
    print(result)
    return 0 if not_reported == 0 else 1


async def cleanup() -> None:
    laser = await c.connect()
    try:
        await laser.stream(STREAM).delete()
    finally:
        await laser.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["produce", "consume", "verify", "cleanup"])
    p.add_argument("--worker", type=int, default=0)
    p.add_argument("--workers", type=int, default=6)
    args = p.parse_args()
    if args.step == "produce":
        return asyncio.run(produce())
    if args.step == "consume":
        return asyncio.run(consume(args.worker))
    if args.step == "verify":
        return verify(args.workers)
    asyncio.run(cleanup())
    return 0


if __name__ == "__main__":
    sys.exit(main())

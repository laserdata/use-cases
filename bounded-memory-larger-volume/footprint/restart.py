"""Test 3 at scale: a graceful restart of a LaserData Cloud cluster, with data on disk and clients connected.

The problem: restarting the broker sent CPU and memory so high, for so long, that the services around
it stalled, and clients saw errors while it came back.

What this does:
  preload      writes 4 million records (1 KiB each, about 4 GiB) to an archive topic, so every node
               has real data to reopen
  produce      a Laser SDK producer keeps sending 5,000 records/s to a separate live topic
  consume      a Laser SDK consumer group keeps reading the live topic and checks every record
  orchestrate  marks the phases: steady (2 min), then waits while the restart is started from the
               LaserData Cloud console (Restart on the Iggy runtime: a rolling restart, one node at
               a time). The operator marks the click and the end with marker files (see below);
               after the end marker it records 2 more minutes, then stops the clients.

The managed nodes can't be restarted from the client VM, so the restart itself is started in the
console. Per-node server memory, CPU, role and process start time come from the console recorder
(cloud/console_recorder.js); a new process id/start time on a node is that node's restart.
The client side reports how long the producer was stalled or failing, the consumer's longest gap,
and fails if any record the producer had confirmed is missing.

Markers (touch on the client VM):  $FOOTPRINT_OUT/restart_clicked   when Restart is clicked
                                    $FOOTPRINT_OUT/restart_finished  when all nodes serve again

Usage: python restart.py preload
       python restart.py consume &  python restart.py produce &  python restart.py orchestrate
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

import common as c

TEST = "restart"
STREAM, TOPIC, ARCHIVE = "scale-restart", "events", "archive"
PARTITIONS = 8
RATE = 5_000  # the first blog's restart rate; below what one Python consumer keeps up with
PRELOAD_RATE = 15_000  # the same safe write rate as the backlog tests
STOP = c.OUT / "restart_stop"
TOTAL = c.OUT / "restart_total"
CLICKED = c.OUT / "restart_clicked"
FINISHED = c.OUT / "restart_finished"
PRELOAD = c.scaled(4_000_000)
LIVE_MAX = RATE * 3600  # upper bound on live records, for the tracker
WAIT_LIMIT = 40 * 60  # stop waiting for the restart after 40 minutes


async def preload() -> None:
    c.set_phase(TEST, "preload")
    c.register_client("preload")
    laser = await c.connect()
    try:
        # The archive is data every node must reopen on restart; live traffic uses its own topic.
        producer = laser.stream(STREAM).topic(ARCHIVE).producer(
            partitions=PARTITIONS, batch_length=c.BATCH, create_stream=True, never_expire=True)
        await producer.init()
        start = time.perf_counter()
        for first in range(0, PRELOAD, c.BATCH):
            await producer.send_batch([c.payload(s) for s in range(first, first + c.BATCH)])
            ahead = start + (first + c.BATCH) / PRELOAD_RATE - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
        await producer.shutdown()
        took = time.perf_counter() - start
    finally:
        await laser.close()
        c.unregister_client("preload")
    c.event(TEST, "preloaded", records=PRELOAD, seconds=round(took, 1))
    print(f"preloaded {PRELOAD:,} records in {took:.0f}s")


async def produce() -> int:
    """Sends at RATE, and after any failure retries the same batch until it is confirmed."""
    c.register_client("producer")
    seq = 0
    outages, stalls = [], []
    laser = await c.connect()
    try:
        producer = laser.stream(STREAM).topic(TOPIC).producer(partitions=PARTITIONS, batch_length=c.BATCH,
                                                               create_stream=True, never_expire=True)
        await producer.init()
        start, sent_since = time.perf_counter(), 0
        while not STOP.exists():
            batch = [c.payload(s) for s in range(seq, seq + c.BATCH)]
            failed_at = None
            sent_at = time.time()
            while True:
                try:
                    await producer.send_batch(batch)
                    took = time.time() - sent_at
                    if took > 0.25:  # the SDK retried inside its own timeout: count it as a stall
                        stalls.append({"from": sent_at, "to": time.time()})
                        c.event(TEST, "producer_stall", seconds=round(took, 2))
                    break
                except Exception as error:
                    if failed_at is None:
                        failed_at = time.time()
                        c.event(TEST, "producer_error", error=repr(error)[:200])
                    await asyncio.sleep(0.2)
            if failed_at is not None:
                outages.append({"from": failed_at, "to": time.time()})
                c.event(TEST, "producer_resumed", seconds=round(time.time() - failed_at, 2))
                start, sent_since = time.perf_counter(), 0  # no catch-up burst after an outage
            seq += c.BATCH
            sent_since += c.BATCH
            ahead = start + sent_since / RATE - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
        await producer.shutdown()
    finally:
        await laser.close()
        c.unregister_client("producer")
    TOTAL.write_text(str(seq))
    c.write_result(f"{TEST}-producer", {"records_confirmed": seq, "outages": outages, "stalls": stalls})
    return 0


async def consume() -> int:
    c.register_client("consumer")
    tracker = c.SeqTracker(LIVE_MAX)
    laser = await c.connect()
    try:
        topic = laser.stream(STREAM).topic(TOPIC)
        await topic.ensure(PARTITIONS)
        live = await c.consume_live(topic.consumer_group("footprint"), tracker,
                                    lambda: int(TOTAL.read_text()) if TOTAL.exists() else None, TEST)
    finally:
        await laser.close()
        c.unregister_client("consumer")
    total = int(TOTAL.read_text())
    result = {"records_confirmed": total, "received_unique": total - tracker.missing(total),
              "missing_confirmed": tracker.missing(total), "duplicates": tracker.duplicates,
              "missing_ranges": c.missing_ranges(tracker, total),
              "consumer_errors": live["errors"], "consumer_recreated": len(live["recreations"]),
              "gaps": live["gaps"]}
    c.write_result(f"{TEST}-consumer", result)
    print({k: v for k, v in result.items() if k != "gaps"})
    return 0 if result["missing_confirmed"] == 0 else 1


def orchestrate() -> int:
    for f in (STOP, TOTAL, CLICKED, FINISHED):
        f.unlink(missing_ok=True)
    c.set_phase(TEST, "steady")
    time.sleep(c.scaled(120))
    c.set_phase(TEST, "waiting-for-restart")
    c.event(TEST, "ready_for_restart")
    print("ready: start the restart in the console, then touch", CLICKED, flush=True)
    began = time.time()
    clicked = finished = None
    while time.time() - began < WAIT_LIMIT:
        if clicked is None and CLICKED.exists():
            clicked = CLICKED.stat().st_mtime
            c.set_phase(TEST, "restarting")
            c.event(TEST, "restart_clicked", at=clicked)
        if FINISHED.exists():
            finished = FINISHED.stat().st_mtime
            c.event(TEST, "restart_finished", at=finished)
            break
        time.sleep(1)
    c.set_phase(TEST, "after-restart")
    time.sleep(c.scaled(120))
    STOP.write_text("1")
    result = {"restart": "graceful, rolling, started from the LaserData Cloud console",
              "clicked": clicked, "finished": finished,
              "seconds_click_to_all_serving": round(finished - clicked, 1) if clicked and finished else None}
    c.write_result(f"{TEST}-orchestrator", result)
    print(json.dumps(result))
    return 0 if clicked and finished else 1


async def cleanup() -> None:
    laser = await c.connect()
    try:
        await laser.stream(STREAM).delete()
    finally:
        await laser.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["preload", "produce", "consume", "orchestrate", "cleanup"])
    step = p.parse_args().step
    if step == "orchestrate":
        return orchestrate()
    return asyncio.run({"preload": preload, "produce": produce, "consume": consume,
                        "cleanup": cleanup}[step]()) or 0


if __name__ == "__main__":
    sys.exit(main())

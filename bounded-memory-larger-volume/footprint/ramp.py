"""Finds a rate the cluster sustains comfortably, before any real test runs.

One Laser SDK producer sends 1 KiB records in batches of 100 to a 10-partition topic, at
2,000 records/s, then 5,000, 10,000, 20,000 and 40,000, two minutes per step. Each step
records the rate it achieved and how long sends took. The ramp stops at the first step with
an error, a send slower than 2 s, or an achieved rate below 90% of the target, and reports
the last healthy step. Afterwards the test stream is deleted.

Usage: python ramp.py [--steps 2000,5000,10000,20000,40000] [--seconds 120]
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time

import common as c

TEST = "ramp"
STREAM, TOPIC, PARTITIONS = "scale-ramp", "events", 10


async def ramp(steps: list[int], seconds: int) -> int:
    c.register_client("producer")
    laser = await c.connect()
    results, healthy = [], None
    try:
        producer = laser.stream(STREAM).topic(TOPIC).producer(
            partitions=PARTITIONS, batch_length=c.BATCH, create_stream=True)
        await producer.init()
        seq = 0
        for rate in steps:
            c.set_phase(TEST, f"rate-{rate}")
            lat, errors, sent = [], 0, 0
            start = time.perf_counter()
            while time.perf_counter() - start < seconds:
                t0 = time.perf_counter()
                try:
                    await producer.send_batch([c.payload(s) for s in range(seq, seq + c.BATCH)])
                    lat.append(time.perf_counter() - t0)
                    seq += c.BATCH
                    sent += c.BATCH
                except Exception as error:
                    errors += 1
                    print(f"  send error at {rate}/s: {error!r}"[:200])
                    if errors >= 3:
                        break
                ahead = start + sent / rate - time.perf_counter()
                if ahead > 0:
                    await asyncio.sleep(ahead)
            took = time.perf_counter() - start
            lat.sort()
            step = {"target": rate, "achieved": round(sent / took), "seconds": round(took),
                    "errors": errors,
                    "send_p50_ms": round(statistics.median(lat) * 1000, 1) if lat else None,
                    "send_p99_ms": round(lat[int(len(lat) * 0.99) - 1] * 1000, 1) if lat else None,
                    "send_max_ms": round(lat[-1] * 1000, 1) if lat else None}
            ok = errors == 0 and lat and lat[-1] < 2.0 and sent / took >= 0.9 * rate
            step["healthy"] = bool(ok)
            results.append(step)
            c.event(TEST, "step", **step)
            print(step, flush=True)
            if not ok:
                break
            healthy = rate
        await producer.shutdown()
    finally:
        try:
            await laser.stream(STREAM).delete()
        except Exception as error:
            print(f"cleanup failed: {error!r}")
        await laser.close()
        c.unregister_client("producer")
    c.write_result(TEST, {"steps": results, "last_healthy_rate": healthy})
    print(f"last healthy rate: {healthy}")
    return 0 if healthy else 1


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--steps", default="2000,5000,10000,20000,40000")
    p.add_argument("--seconds", type=int, default=120)
    args = p.parse_args()
    return asyncio.run(ramp([int(x) for x in args.steps.split(",")], args.seconds))


if __name__ == "__main__":
    sys.exit(main())

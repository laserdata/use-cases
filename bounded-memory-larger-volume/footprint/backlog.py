"""Test 1 at scale: one consumer's memory while it catches up on a large backlog on a LaserData
Cloud cluster, at 2, 10, 30, 40 and 50 partitions.

  preload  Laser SDK producer process(es) write N records at a fixed rate (1 KiB each, default 7 million),
           each its own range of sequence numbers, so the backlog is ready in minutes
  consume  one fresh process joins a consumer group with the standard Laser SDK settings and
           reads the whole backlog from the first offset; the sampler records its memory
  cleanup  deletes the test stream

Every record carries its sequence number; the run fails if any record is missing.
The tracker uses one bit per record (about 0.9 MiB for 7 million), which is part of the measured memory.

Usage: python backlog.py preload --partitions 50 [--writers 4]
       python backlog.py consume --partitions 50
       python backlog.py cleanup --partitions 50
"""
from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import sys
import time

import common as c

TEST = "backlog"


def names(partitions: int) -> tuple[str, str]:
    return f"scale-backlog-{partitions}", "events"


async def _write(partitions: int, first: int, last: int, rate: float) -> None:
    stream, topic_name = names(partitions)
    laser = await c.connect()
    try:
        producer = laser.stream(stream).topic(topic_name).producer(
            partitions=partitions, batch_length=c.BATCH, create_stream=True, never_expire=True)
        await producer.init()
        start = time.perf_counter()
        for s in range(first, last, c.BATCH):
            await producer.send_batch([c.payload(q) for q in range(s, min(s + c.BATCH, last))])
            if rate:  # paced, so the cluster gets a steady, known load instead of a burst
                ahead = start + (s + c.BATCH - first) / rate - time.perf_counter()
                if ahead > 0:
                    await asyncio.sleep(ahead)
        await producer.shutdown()
    finally:
        await laser.close()


def _writer(partitions: int, first: int, last: int, rate: float, index: int = 0) -> None:
    # The sampler records each writer's memory and CPU while the backlog is written.
    role = "producer" if index == 0 else f"producer-{index}"
    c.register_client(role)
    try:
        asyncio.run(_write(partitions, first, last, rate))
    finally:
        c.unregister_client(role)


async def _ensure(partitions: int) -> None:
    stream, topic_name = names(partitions)
    laser = await c.connect()
    try:
        producer = laser.stream(stream).topic(topic_name).producer(
            partitions=partitions, batch_length=c.BATCH, create_stream=True, never_expire=True)
        await producer.init()
        await producer.shutdown()
    finally:
        await laser.close()


def preload(partitions: int, total: int, writers: int, rate: float) -> int:
    c.set_phase(TEST, f"preload-{partitions}p")
    asyncio.run(_ensure(partitions))  # create the stream once, before the writers race
    step = -(-total // writers)
    start = time.perf_counter()
    # "spawn", not fork: a forked copy of a process that has already used the SDK deadlocks.
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_writer, args=(partitions, i * step, min(total, (i + 1) * step), rate / writers, i))
             for i in range(writers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    took = time.perf_counter() - start
    failed = sum(1 for p in procs if p.exitcode != 0)
    c.event(TEST, "preloaded", partitions=partitions, records=total, writers=writers,
            seconds=round(took, 1), records_per_second=round(total / took), failed_writers=failed)
    print(f"preloaded {total:,} records over {partitions} partitions in {took:.0f}s "
          f"({total / took:,.0f}/s, {writers} writers, {failed} failed)")
    return 1 if failed else 0


async def consume(partitions: int, total: int) -> int:
    stream, topic_name = names(partitions)
    c.register_client("consumer")
    c.set_phase(TEST, f"consume-{partitions}p")
    laser = await c.connect()
    tracker = c.BitTracker(total)
    try:
        group = laser.stream(stream).topic(topic_name).consumer_group("footprint")
        consumer = group.consumer(polling="first")  # every other setting is the SDK default
        await consumer.init()
        start = last = time.perf_counter()
        while tracker.unique < total:
            try:
                message = await consumer.next_within(60_000)
            except Exception as error:  # nothing arrived for 60 s
                print(f"stopped waiting: {error!r}")
                break
            if message is None:
                break
            tracker.add(c.seq_of(message.payload))
            last = time.perf_counter()
        took = last - start
        await consumer.shutdown()
    finally:
        await laser.close()
        c.unregister_client("consumer")
    result = {"partitions": partitions, "records": total, "received_unique": tracker.unique,
              "duplicates": tracker.duplicates, "missing": total - tracker.unique,
              "seconds": round(took, 1), "records_per_second": round(tracker.unique / took) if took else 0,
              "consumer_settings": "Laser SDK defaults, polling='first'"}
    c.write_result(f"{TEST}-{partitions}p", result)
    c.event(TEST, "consumed", **result)
    print(result)
    return 0 if result["missing"] == 0 else 1


async def cleanup(partitions: int) -> None:
    stream, _ = names(partitions)
    laser = await c.connect()
    try:
        await laser.stream(stream).delete()
    finally:
        await laser.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["preload", "consume", "cleanup"])
    p.add_argument("--partitions", type=int, required=True)
    p.add_argument("--records", type=int, default=c.scaled(7_000_000))
    p.add_argument("--writers", type=int, default=1)
    p.add_argument("--rate", type=float, default=float(os.environ.get("SCALE_RATE", "10000")),
                   help="records/s across all writers; 0 = as fast as possible")
    args = p.parse_args()
    if args.step == "preload":
        return preload(args.partitions, args.records, args.writers, args.rate)
    if args.step == "consume":
        return asyncio.run(consume(args.partitions, args.records))
    asyncio.run(cleanup(args.partitions))
    return 0


if __name__ == "__main__":
    sys.exit(main())

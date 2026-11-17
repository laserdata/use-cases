"""Connection check: writes 1,000 records to a 1-partition topic, reads them back, deletes the stream."""
import asyncio, sys
import common as c

STREAM = "scale-smoke"

async def main() -> int:
    laser = await c.connect()
    try:
        producer = laser.stream(STREAM).topic("events").producer(partitions=1, batch_length=c.BATCH, create_stream=True)
        await producer.init()
        for s in range(0, 1000, c.BATCH):
            await producer.send_batch([c.payload(q) for q in range(s, s + c.BATCH)])
        await producer.shutdown()
        print("wrote 1000")
        consumer = laser.stream(STREAM).topic("events").consumer_group("smoke").consumer(polling="first")
        await consumer.init()
        seen = set()
        while len(seen) < 1000:
            m = await consumer.next_within(30_000)
            if m is None:
                break
            seen.add(c.seq_of(m.payload))
        await consumer.shutdown()
        print(f"read {len(seen)} of 1000")
        if len(seen) != 1000:
            return 1
    finally:
        try:
            await laser.stream(STREAM).delete()
        except Exception as e:
            print(f"cleanup: {e!r}")
        await laser.close()
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

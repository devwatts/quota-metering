import asyncio
import logging
import signal
import uuid
from pathlib import Path

import asyncpg
from redis.exceptions import ResponseError

from . import settings
from .consumer import ScheduleConsumer
from .store import Operation, QuotaRouter, QuotaStore

log = logging.getLogger(__name__)
GROUP = "ledger"
# Only newly inserted events increase totals; replayed events must add nothing.
INSERT = """
WITH inserted AS (
    INSERT INTO quota_events SELECT * FROM batch
    ON CONFLICT (stream, event_id) DO NOTHING
    RETURNING stream, org, feature, period, used_delta
), totals AS (
    SELECT stream, org, feature, period, sum(used_delta)::bigint AS used
    FROM inserted GROUP BY stream, org, feature, period
)
INSERT INTO quota_totals SELECT * FROM totals
ON CONFLICT (stream, org, feature, period) DO UPDATE
SET used = quota_totals.used + EXCLUDED.used
"""


class Ledger:
    def __init__(self, store: QuotaStore, pg):
        self.store, self.pg = store, pg
        self.consumer = str(uuid.uuid4())
        self.cursor = "0-0"

    async def setup(self):
        # One active ledger reader per stream. Losing SQL also loses this lock.
        locked = await self.pg.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", self.store.stream
        )
        if not locked:
            raise RuntimeError("Another ledger worker owns this stream")
        # IF NOT EXISTS alone can race in PostgreSQL's catalogs on a fresh DB.
        async with self.pg.transaction():
            await self.pg.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended('quota-schema', 0))"
            )
            await self.pg.execute(Path(__file__).with_name("schema.sql").read_text())
        await self.pg.execute("CREATE TEMP TABLE batch (LIKE quota_events) ON COMMIT DELETE ROWS")
        try:
            await self.store.redis.xgroup_create(self.store.stream, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if not str(exc).startswith("BUSYGROUP"):
                raise

    async def read(self, block_ms=20):
        # The per-stream advisory lock lets this reader reclaim pending events immediately.
        self.cursor, messages, _ = await self.store.redis.xautoclaim(
            self.store.stream, GROUP, self.consumer, 0, self.cursor, count=500
        )
        if messages:
            return messages
        response = await self.store.redis.xreadgroup(
            GROUP, self.consumer, {self.store.stream: ">"}, count=500, block=block_ms
        )
        if response:
            messages = response[0][1]
            if len(messages) < 500:
                await asyncio.sleep(0.01)
                extra = await self.store.redis.xreadgroup(
                    GROUP, self.consumer, {self.store.stream: ">"}, count=500 - len(messages)
                )
                if extra:
                    messages.extend(extra[0][1])
        return messages

    async def commit(self, messages):
        rows = [
            (
                self.store.stream,
                event_id,
                item["org"],
                item["feature"],
                item["period"],
                item["operation"],
                item["state"],
                int(item["units"]),
                int(item["used_delta"]),
            )
            for event_id, item in messages
        ]
        async with self.pg.transaction():
            await self.pg.copy_records_to_table("batch", records=rows)
            await self.pg.execute(INSERT)

    async def acknowledge(self, messages):
        # Redis 8.2: delete only after SQL commit and all consumer groups acknowledge.
        await self.store.redis.execute_command(
            "XACKDEL",
            self.store.stream,
            GROUP,
            "ACKED",
            "IDS",
            len(messages),
            *(event_id for event_id, _ in messages),
        )


async def recover_pending(store: QuotaStore, older_than=30):
    now, _ = await store.redis.time()
    keys = await store.redis.zrangebyscore(
        store.pending, "-inf", now - older_than, start=0, num=100
    )
    consumer = ScheduleConsumer(store)
    for key in keys:
        raw = await store.redis.hgetall(key)
        if not raw:
            raise RuntimeError("Pending operation lost its receipt")
        operation = Operation.parse(raw)
        if operation.feature != "sailing-schedule":
            log.error("No recovery handler for feature %s", operation.feature)
            continue
        await consumer.complete(operation)
    return len(keys)


async def recovery_loop(store):
    while True:
        try:
            await recover_pending(store)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Reservation recovery failed; will retry")
        await asyncio.sleep(1)


async def run_shard(store):
    recovery = asyncio.create_task(recovery_loop(store))
    try:
        while True:
            pg = None
            try:
                pg = await asyncpg.connect(settings.DATABASE_URL, command_timeout=30)
                ledger = Ledger(store, pg)
                await ledger.setup()
                while True:
                    messages = await ledger.read()
                    if messages:
                        await ledger.commit(messages)
                        await ledger.acknowledge(messages)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Ledger unavailable; unacknowledged events stay in Redis")
            finally:
                if pg:
                    await pg.close(timeout=5)
            await asyncio.sleep(1)
    finally:
        recovery.cancel()
        await asyncio.gather(recovery, return_exceptions=True)


async def main():
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
    router = QuotaRouter()
    try:
        await router.ready()
        async with asyncio.TaskGroup() as tasks:
            for shard in router.shards:
                tasks.create_task(run_shard(shard))
    finally:
        await router.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass

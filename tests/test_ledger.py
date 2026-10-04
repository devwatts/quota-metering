import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest_asyncio

from quota.consumer import ScheduleConsumer
from quota.store import QuotaStore
from quota.worker import Ledger


@pytest_asyncio.fixture
async def ledger(store):
    pg = await asyncpg.connect(
        os.getenv("TEST_DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
    )
    ledger = Ledger(store, pg)
    await ledger.setup()
    yield ledger
    await pg.execute("DELETE FROM quota_events WHERE stream=$1", store.stream)
    await pg.execute("DELETE FROM quota_totals WHERE stream=$1", store.stream)
    await pg.close()


async def test_commit_replay_does_not_duplicate_ledger_and_trimming_is_safe(store, ledger):
    await ScheduleConsumer(store).execute("acme", "one", ["SGSIN-NLRTM"] * 3)
    messages = await ledger.read()
    assert len(messages) == 1
    await ledger.commit(messages)
    # Model a lost SQL commit reply / crash before Redis acknowledgement.
    await ledger.commit(messages)
    assert (
        await ledger.pg.fetchval("SELECT count(*) FROM quota_events WHERE stream=$1", store.stream)
        == 1
    )
    row = await ledger.pg.fetchrow("SELECT used FROM quota_totals WHERE stream=$1", store.stream)
    assert tuple(row) == (3,)
    await ledger.acknowledge(messages)
    assert await store.redis.xlen(store.stream) == 0
    assert (await store.redis.xpending(store.stream, "ledger"))["pending"] == 0
    retry, _ = await ScheduleConsumer(store).execute("acme", "one", ["SGSIN-NLRTM"] * 3)
    assert retry.state == "confirmed"
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 3


async def test_ledger_keeps_events_until_sql_commit_and_reclaims_after_restart(store, ledger):
    await ScheduleConsumer(store).execute("acme", "one", ["SGSIN-NLRTM"])
    messages = await ledger.read()
    assert (await store.redis.xpending(store.stream, "ledger"))["pending"] == 1
    ledger.consumer = "replacement-worker"
    reclaimed = await ledger.read()
    assert reclaimed == messages
    await ledger.commit(reclaimed)
    await ledger.acknowledge(reclaimed)
    assert await store.redis.xlen(store.stream) == 0


async def test_failed_consumer_has_no_charge_in_ledger(store, ledger):
    await ScheduleConsumer(store).execute("acme", "bad", ["UNKNOWN"])
    messages = await ledger.read()
    await ledger.commit(messages)
    row = await ledger.pg.fetchrow("SELECT used FROM quota_totals WHERE stream=$1", store.stream)
    assert tuple(row) == (0,)


async def test_two_shards_can_initialize_a_fresh_sql_schema(store):
    url = os.getenv("TEST_DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
    connections = [await asyncpg.connect(url) for _ in range(2)]
    stores = [
        QuotaStore(
            os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15"),
            f"{store.prefix}:startup-{i}",
        )
        for i in range(2)
    ]
    schema = f"startup_{uuid4().hex}"
    try:
        await connections[0].execute(f'CREATE SCHEMA "{schema}"')
        for pg in connections:
            await pg.execute(f'SET search_path TO "{schema}"')
        await asyncio.gather(
            *(Ledger(shard, pg).setup() for shard, pg in zip(stores, connections, strict=True))
        )
        for pg in connections:
            assert await pg.fetchval("SELECT to_regclass('quota_events')") is not None
            assert await pg.fetchval("SELECT to_regclass('quota_totals')") is not None
    finally:
        # Closing both sessions releases their stream locks and temporary tables.
        await asyncio.gather(*(pg.close() for pg in connections))
        cleanup = await asyncpg.connect(url)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
        await asyncio.gather(*(shard.close() for shard in stores))

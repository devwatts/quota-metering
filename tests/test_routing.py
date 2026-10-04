import asyncio
import os
from uuid import uuid4

import pytest
import pytest_asyncio

from quota.consumer import ScheduleConsumer
from quota.store import QuotaRouter


@pytest_asyncio.fixture
async def router():
    urls = os.getenv(
        "TEST_REDIS_URLS", "redis://localhost:6379/14,redis://localhost:6379/15"
    ).split(",")
    router = QuotaRouter(urls, f"routing-{uuid4().hex}")
    await router.ready()
    yield router, urls
    for shard in router.shards:
        keys = [key async for key in shard.redis.scan_iter(match=f"{router.prefix}:*")]
        if keys:
            await shard.redis.delete(*keys)
    await router.close()


async def test_organizations_keep_one_owner_and_retries_share_it(router):
    first, urls = router
    second = QuotaRouter(urls, first.prefix)
    await second.ready()
    try:
        for i in range(10):
            org = f"org-{i}"
            await first.configure(org, {"sailing-schedule": 1})
            consumers = [ScheduleConsumer(first), ScheduleConsumer(second)]
            results = await asyncio.gather(
                *(consumer.execute(org, "same", ["SGSIN-NLRTM"]) for consumer in consumers)
            )
            assert all(operation.state == "confirmed" for operation, _ in results)
            assert (await first.usage(org, "sailing-schedule"))["used"] == 1
            for shard in first.shards:
                exists = await shard.redis.exists(shard.receipt_key(org, "same"))
                assert bool(exists) == (shard is first.for_org(org))
    finally:
        await second.close()


async def test_changing_shard_count_fails_startup(router):
    current, urls = router
    changed = QuotaRouter(urls[:1], current.prefix)
    try:
        with pytest.raises(RuntimeError, match="routing changed"):
            await changed.ready()
    finally:
        await changed.close()


async def test_swapping_shard_order_fails_startup(router):
    current, urls = router
    changed = QuotaRouter(list(reversed(urls)), current.prefix)
    try:
        with pytest.raises(RuntimeError, match="routing changed"):
            await changed.ready()
    finally:
        await changed.close()

import os
import uuid

import pytest_asyncio

from quota.store import QuotaStore


@pytest_asyncio.fixture
async def store():
    store = QuotaStore(
        os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15"), prefix=f"test-{uuid.uuid4().hex}"
    )
    await store.ready()
    await store.configure("acme", {"sailing-schedule": 500, "container-tracking": 500})
    yield store
    keys = [key async for key in store.redis.scan_iter(match=f"{store.prefix}:*")]
    if keys:
        await store.redis.delete(*keys)
    await store.close()

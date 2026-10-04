import os

import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from quota.api import CONSUMER, STORE, create_app
from quota.consumer import ScheduleConsumer
from quota.store import QuotaRouter


@pytest_asyncio.fixture
async def client(store):
    router = QuotaRouter((os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15"),), store.prefix)
    await router.ready()
    await router.configure("acme", {"sailing-schedule": 500})
    app = create_app()
    app.cleanup_ctx.clear()
    app[STORE] = router
    app[CONSUMER] = ScheduleConsumer(router)
    try:
        async with TestClient(TestServer(app)) as client:
            yield client
    finally:
        await router.close()


async def test_consumer_and_usage_endpoint(client):
    response = await client.post(
        "/orgs/acme/schedules/search",
        json={"routes": ["SGSIN-NLRTM"]},
        headers={"Idempotency-Key": "one"},
    )
    assert response.status == 200
    assert (await response.json())["units"] == 1
    response = await client.get("/orgs/acme/usage/sailing-schedule")
    usage = await response.json()
    assert (usage["used"], usage["reserved"], usage["remaining"]) == (1, 0, 499)
    assert usage["resets_at"].endswith("T00:00:00+00:00")


async def test_validation_and_conflict_statuses(client):
    response = await client.post("/orgs/acme/schedules/search", json={"routes": ["SGSIN-NLRTM"]})
    assert response.status == 400
    response = await client.post(
        "/orgs/acme/schedules/search",
        json={"routes": ["SGSIN-NLRTM"]},
        headers={"Idempotency-Key": "one"},
    )
    assert response.status == 200
    response = await client.post(
        "/orgs/acme/schedules/search",
        json={"routes": ["CNSHA-USLAX"]},
        headers={"Idempotency-Key": "one"},
    )
    assert response.status == 409


async def test_batch_rejection_and_failed_search_statuses(client):
    response = await client.post(
        "/orgs/acme/schedules/search",
        json={"routes": ["SGSIN-NLRTM"] * 501},
        headers={"Idempotency-Key": "large"},
    )
    assert response.status == 429
    response = await client.post(
        "/orgs/acme/schedules/search",
        json={"routes": ["UNKNOWN"]},
        headers={"Idempotency-Key": "bad"},
    )
    assert response.status == 422

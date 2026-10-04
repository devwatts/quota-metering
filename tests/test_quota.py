import asyncio
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from quota.consumer import ScheduleConsumer, SearchFailed
from quota.store import QuotaError, QuotaStore, month_bounds
from quota.worker import recover_pending

ROUTE = "SGSIN-NLRTM"


def contenders(url, prefix, worker, count, barrier):
    async def run():
        store = QuotaStore(url, prefix)
        consumer = ScheduleConsumer(store)
        barrier.wait(timeout=30)

        async def attempt(i):
            try:
                operation, _ = await consumer.execute("acme", f"{worker}-{i}", [ROUTE] * 5)
                assert operation.state == "confirmed"
                return 5
            except QuotaError as exc:
                assert exc.code == "exhausted"
                return 0

        try:
            return sum(await asyncio.gather(*(attempt(i) for i in range(count))))
        finally:
            await store.close()

    return asyncio.run(run())


@pytest.mark.parametrize("oversubscribed", [False, True])
async def test_eight_processes_contend_for_last_500_units(store, oversubscribed):
    context = multiprocessing.get_context("spawn")
    url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
    # All eight processes cross a barrier before issuing concurrent batches.
    with context.Manager() as manager:
        barrier = manager.Barrier(8)
        with ProcessPoolExecutor(max_workers=8, mp_context=context) as pool:
            loop = asyncio.get_running_loop()
            futures = [
                loop.run_in_executor(
                    pool,
                    contenders,
                    url,
                    store.prefix,
                    worker,
                    25 if oversubscribed else (13 if worker < 4 else 12),
                    barrier,
                )
                for worker in range(8)
            ]
            assert sum(await asyncio.gather(*futures)) == 500
    usage = await store.usage("acme", "sailing-schedule")
    assert (usage["used"], usage["reserved"], usage["remaining"]) == (500, 0, 0)


async def test_duplicate_requests_charge_once(store):
    consumer = ScheduleConsumer(store)
    results = await asyncio.gather(
        *(consumer.execute("acme", "same", [ROUTE] * 10) for _ in range(50))
    )
    assert all(operation.state == "confirmed" for operation, _ in results)
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 10
    assert await store.redis.xlen(store.stream) == 1  # one final outcome


async def test_conflicting_retry_is_rejected(store):
    consumer = ScheduleConsumer(store)
    await consumer.execute("acme", "same", [ROUTE])
    for routes in ([ROUTE] * 2, ["CNSHA-USLAX"]):
        with pytest.raises(QuotaError, match="conflict"):
            await consumer.execute("acme", "same", routes)


async def test_all_or_nothing_batch(store):
    consumer = ScheduleConsumer(store)
    await consumer.execute("acme", "first", [ROUTE] * 430)
    with pytest.raises(QuotaError, match="exhausted"):
        await consumer.execute("acme", "too-big", [ROUTE] * 100)
    assert (await store.usage("acme", "sailing-schedule"))["remaining"] == 70
    assert not await store.redis.exists(store.receipt_key("acme", "too-big"))


async def test_downstream_failure_releases_entire_batch(store):
    consumer = ScheduleConsumer(store)
    operation, _ = await consumer.execute("acme", "failure", [ROUTE, "UNKNOWN"])
    assert operation.state == "released"
    retry, _ = await consumer.execute("acme", "failure", [ROUTE, "UNKNOWN"])
    assert retry == operation
    usage = await store.usage("acme", "sailing-schedule")
    assert (usage["used"], usage["reserved"], usage["remaining"]) == (0, 0, 500)


async def test_unknown_outcome_stays_reserved_until_recovery(store):
    async def unavailable(_):
        raise TimeoutError("Provider response lost")

    with pytest.raises(TimeoutError):
        await ScheduleConsumer(store, unavailable).execute("acme", "timeout", [ROUTE])
    assert (await store.usage("acme", "sailing-schedule"))["reserved"] == 1
    assert await store.redis.ttl(store.receipt_key("acme", "timeout")) == -1
    assert await recover_pending(store, older_than=0) == 1
    usage = await store.usage("acme", "sailing-schedule")
    assert (usage["used"], usage["reserved"]) == (1, 0)


async def test_lost_reservation_and_confirmation_replies(store):
    request = {"routes": [ROUTE]}
    await store.reserve("acme", "sailing-schedule", "lost", 1, request)
    # Caller did not receive the reservation reply and retries on another connection.
    consumer = ScheduleConsumer(store)
    first, _ = await consumer.execute("acme", "lost", [ROUTE])
    # Now lose the final response as well.
    second, _ = await consumer.execute("acme", "lost", [ROUTE])
    assert second == first
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 1


async def test_concurrent_failure_cannot_release_confirmed_work(store):
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def slow_failure(_):
        entered.set()
        await finish.wait()
        raise SearchFailed("Provider failed")

    failing = asyncio.create_task(
        ScheduleConsumer(store, slow_failure).execute("acme", "race", [ROUTE])
    )
    await entered.wait()
    success, _ = await ScheduleConsumer(store).execute("acme", "race", [ROUTE])
    finish.set()
    late_failure, _ = await failing
    assert late_failure == success
    assert late_failure.state == "confirmed"
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 1


async def test_success_cannot_return_after_release_wins(store):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def slow_success(_):
        entered.set()
        await finish.wait()
        return {"schedules": []}

    async def failure(_):
        raise SearchFailed("Failed before returning results")

    succeeding = asyncio.create_task(
        ScheduleConsumer(store, slow_success).execute("acme", "race", [ROUTE])
    )
    await entered.wait()
    released, _ = await ScheduleConsumer(store, failure).execute("acme", "race", [ROUTE])
    finish.set()
    late_success, _ = await succeeding
    assert late_success == released
    assert late_success.state == "released"
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 0


async def test_organizations_and_features_are_independent(store):
    await store.configure("globex", {"sailing-schedule": 7})
    await ScheduleConsumer(store).execute("acme", "one", [ROUTE])
    assert (await store.usage("globex", "sailing-schedule"))["remaining"] == 7
    assert (await store.usage("acme", "container-tracking"))["remaining"] == 500


@pytest.mark.parametrize("units", [0, -1, 1.5, True, 10001])
async def test_invalid_units_do_not_mutate_state(store, units):
    with pytest.raises(ValueError):
        await store.reserve("acme", "sailing-schedule", uuid4().hex, units, {})
    assert (await store.usage("acme", "sailing-schedule"))["remaining"] == 500


async def test_missing_configuration_fails_closed(store):
    with pytest.raises(QuotaError, match="unconfigured"):
        await store.reserve("missing", "sailing-schedule", "id", 1, {})


async def test_redis_clock_corrects_an_incorrect_application_month(store, monkeypatch):
    from quota import store as module

    class WrongClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2000, 1, 1, tzinfo=UTC)

    monkeypatch.setattr(module, "datetime", WrongClock)
    operation, _ = await ScheduleConsumer(store).execute("acme", "clock", [ROUTE])
    assert operation.period == month_bounds(datetime.now(UTC))[0]


async def test_terminal_receipt_expires_but_retries_do_not_extend_retention(store):
    await ScheduleConsumer(store).execute("acme", "retained", [ROUTE])
    key = store.receipt_key("acme", "retained")
    assert 0 < await store.redis.ttl(key) <= 3600
    await store.redis.expire(key, 60)
    await ScheduleConsumer(store).execute("acme", "retained", [ROUTE])
    assert 0 < await store.redis.ttl(key) <= 60


async def test_limit_changes_do_not_rewrite_open_period(store):
    await ScheduleConsumer(store).execute("acme", "id", [ROUTE])
    await store.configure("acme", {"sailing-schedule": 1})
    assert (await store.usage("acme", "sailing-schedule"))["limit"] == 500


async def test_maximum_supported_limit_and_units(store):
    await store.configure("large", {"sailing-schedule": 1_000_000_000})
    operation = await store.reserve("large", "sailing-schedule", "batch", 10_000, {})
    await store.settle(operation, True, {"accepted": 10_000})
    usage = await store.usage("large", "sailing-schedule")
    assert (usage["used"], usage["remaining"]) == (10_000, 999_990_000)
    with pytest.raises(ValueError):
        await store.configure("large", {"sailing-schedule": 1_000_000_001})


async def test_retry_keeps_original_month_and_settles_old_balance(store):
    operation = await store.reserve("acme", "sailing-schedule", "old", 1, {"routes": [ROUTE]})
    # Move a pending fixture into the previous period without changing the server clock.
    # The current period must still start empty; the retry must resolve the old receipt.
    prior = "2020-12"
    old_key = store.quota_key("acme", "sailing-schedule", prior)
    current_key = store.quota_key("acme", "sailing-schedule", operation.period)
    await store.redis.rename(current_key, old_key)
    receipt_key = store.receipt_key("acme", "old")
    await store.redis.hset(receipt_key, mapping={"period": prior, "balance": old_key})
    await store.redis.hset(old_key, mapping={"reset": 1609459200})
    replay, _ = await ScheduleConsumer(store).execute("acme", "old", [ROUTE])
    assert replay.period == prior
    assert int(await store.redis.hget(old_key, "used")) == 1
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 0
    await ScheduleConsumer(store).execute("acme", "new", [ROUTE])
    assert (await store.usage("acme", "sailing-schedule"))["used"] == 1


async def test_wrong_stream_type_fails_before_deducting(store):
    await store.redis.set(store.stream, "invalid")
    with pytest.raises(Exception, match="Unexpected quota key type"):
        await store.reserve("acme", "sailing-schedule", "id", 1, {})
    assert not await store.redis.exists(store.receipt_key("acme", "id"))
    assert not await store.redis.exists(
        store.quota_key("acme", "sailing-schedule", month_bounds(datetime.now(UTC))[0])
    )


@pytest.mark.parametrize(
    "instant, period, reset",
    [
        ("2024-02-29T23:59:59+00:00", "2024-02", "2024-03-01T00:00:00+00:00"),
        ("2026-12-31T23:59:59+00:00", "2026-12", "2027-01-01T00:00:00+00:00"),
        ("2026-10-01T01:00:00+05:30", "2026-09", "2026-10-01T00:00:00+00:00"),
    ],
)
def test_utc_calendar_boundaries(instant, period, reset):
    actual, _, end = month_bounds(datetime.fromisoformat(instant))
    assert actual == period
    assert datetime.fromtimestamp(end, UTC).isoformat() == reset

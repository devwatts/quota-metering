"""Scheduled HTTP arrivals; slow responses do not reduce the offered rate."""

import argparse
import asyncio
import json
import math
import multiprocessing
import time
import uuid
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import asyncpg
import uvloop

from quota import settings
from quota.store import QuotaRouter, month_bounds


def observe(histogram, milliseconds):
    histogram[math.ceil(milliseconds * 100)] += 1  # 0.01 ms upper bounds


def percentiles(histogram):
    if not histogram:
        return {}
    total = sum(histogram.values())
    result, count = {}, 0
    for bucket, size in sorted(histogram.items()):
        count += size
        for p in (50, 95, 99):
            if p not in result and count >= math.ceil(total * p / 100):
                result[p] = bucket / 100
    return {
        "samples": total,
        **{f"p{p}_ms": value for p, value in result.items()},
        "max_ms": max(histogram) / 100,
    }


def run_worker(args, worker, start_wall, run_id):
    async def run():
        histograms = {
            name: Counter() for name in ("admission", "settlement", "quota_path", "http", "arrival")
        }
        counts, units_by_org = Counter(), Counter()
        pending = set()
        max_pending = 0
        start = time.perf_counter() + start_wall - time.time()
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=256), timeout=timeout
        ) as session:

            async def request(i, scheduled):
                units = (1, 1, 1, 10, 100)[i % 5] if args.mixed else 1
                org = f"load-{run_id}-{0 if args.hot else i % args.orgs}"
                sent = time.perf_counter()
                try:
                    async with session.post(
                        f"{args.url}/orgs/{org}/schedules/search",
                        json={"routes": ["SGSIN-NLRTM"] * units},
                        headers={"Idempotency-Key": f"{run_id}-{i}"},
                    ) as response:
                        await response.read()
                        finished = time.perf_counter()
                        counts[f"http_{response.status}"] += 1
                        if response.status == 200:
                            units_by_org[org] += units
                            observe(
                                histograms["admission"],
                                float(response.headers["X-Quota-Admission-Ms"]),
                            )
                            observe(
                                histograms["quota_path"],
                                float(response.headers["X-Quota-Total-Ms"]),
                            )
                            observe(
                                histograms["settlement"],
                                float(response.headers["X-Quota-Settlement-Ms"]),
                            )
                        observe(histograms["http"], (finished - sent) * 1000)
                        observe(histograms["arrival"], (finished - scheduled) * 1000)
                except Exception as exc:  # noqa: BLE001 - every failed arrival must be counted
                    counts[f"error_{type(exc).__name__}"] += 1

            offered = int(args.rate * args.seconds)
            for i in range(worker, offered, args.workers):
                scheduled = start + i / args.rate
                delay = scheduled - time.perf_counter()
                if delay > 0:
                    # uvloop timers have millisecond resolution. Round up so an
                    # arrival is not submitted early and its latency understated.
                    await asyncio.sleep(math.ceil(delay * 1000) / 1000)
                if len(pending) >= args.max_pending:
                    # Waiting for capacity here would silently reduce the offered load.
                    counts["dropped"] += 1
                    continue
                task = asyncio.create_task(request(i, scheduled))
                pending.add(task)
                task.add_done_callback(pending.discard)
                max_pending = max(max_pending, len(pending))
            if pending:
                await asyncio.gather(*pending)
        return {
            "counts": counts,
            "units": units_by_org,
            "histograms": histograms,
            "max_pending": max_pending,
            "elapsed": time.perf_counter() - start,
        }

    return uvloop.run(run())


async def seed(args, run_id):
    router = QuotaRouter()
    await router.ready()
    features = {
        "sailing-schedule": settings.MAX_LIMIT,
        **{f"feature-{i}": settings.MAX_LIMIT for i in range(29)},
    }
    pipes = {shard: shard.redis.pipeline(transaction=False) for shard in router.shards}
    for org in range(args.orgs):
        name = f"load-{run_id}-{org}"
        shard = router.for_org(name)
        pipes[shard].hset(f"{shard.prefix}:limits:{name}", mapping=features)
        if (org + 1) % 1000 == 0:
            await asyncio.gather(*(pipe.execute() for pipe in pipes.values()))
    await asyncio.gather(*(pipe.execute() for pipe in pipes.values()))
    await router.close()


async def accounting(expected, run_id):
    router = QuotaRouter()
    period = month_bounds(datetime.now(UTC))[0]
    redis_ok = True
    for store in router.shards:
        items = [(org, units) for org, units in expected.items() if router.for_org(org) is store]
        for offset in range(0, len(items), 1000):
            batch = items[offset : offset + 1000]
            pipe = store.redis.pipeline(transaction=False)
            for org, _ in batch:
                pipe.hgetall(store.quota_key(org, "sailing-schedule", period))
            values = await pipe.execute()
            for (org, units), value in zip(batch, values, strict=True):
                balance = {
                    key: int(value[key]) for key in ("limit", "used", "reserved") if key in value
                }
                redis_ok &= (
                    balance.get("used") == units
                    and balance.get("reserved") == 0
                    and balance.get("limit", 0) >= units
                )
    pg = await asyncpg.connect(settings.DATABASE_URL)
    deadline = time.monotonic() + 60
    sql_ok = False
    try:
        while time.monotonic() < deadline:
            rows = await pg.fetch(
                "SELECT org,used FROM quota_totals WHERE stream=ANY($1::text[]) AND org LIKE $2",
                [shard.stream for shard in router.shards],
                f"load-{run_id}-%",
            )
            sql_ok = {row["org"]: row["used"] for row in rows} == expected
            if sql_ok:
                break
            await asyncio.sleep(0.1)
    finally:
        await pg.close()
        await router.close()
    return {
        "redis_units_match_successful_responses": bool(redis_ok),
        "sql_units_match_successful_responses": sql_ok,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--orgs", type=int, default=5000)
    parser.add_argument("--max-pending", type=int, default=1024, help="Per generator process")
    parser.add_argument("--hot", action="store_true")
    parser.add_argument("--mixed", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-slo", action="store_true")
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.workers, args.orgs, args.max_pending) < 1:
        parser.error("Numeric arguments must be positive")
    run_id = uuid.uuid4().hex[:12]
    asyncio.run(seed(args, run_id))
    start = time.time() + 5
    with ProcessPoolExecutor(
        max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        futures = [
            pool.submit(run_worker, args, worker, start, run_id) for worker in range(args.workers)
        ]
        workers = [future.result() for future in futures]
    counts, expected = Counter(), Counter()
    histograms = {
        name: Counter() for name in ("admission", "settlement", "quota_path", "http", "arrival")
    }
    for worker in workers:
        counts.update(worker["counts"])
        expected.update(worker["units"])
        for name, histogram in histograms.items():
            histogram.update(worker["histograms"][name])
    checks = asyncio.run(accounting(expected, run_id))
    offered = args.rate * args.seconds
    checks["all_arrivals_succeeded"] = (
        counts.get("http_200", 0) == offered and sum(counts.values()) == offered
    )
    latency = {name: percentiles(histogram) for name, histogram in histograms.items()}
    checks["admission_p99_under_10ms"] = latency["admission"].get("p99_ms", math.inf) < 10
    report = {
        "run": run_id,
        "redis_shards": len(settings.REDIS_URLS),
        "utc": datetime.now(UTC).isoformat(),
        "workload": {k: v for k, v in vars(args).items() if k not in ("output", "url")},
        "offered": offered,
        "counts": dict(counts),
        "accepted_units": sum(expected.values()),
        "latency": latency,
        "checks": checks,
        "max_pending_per_worker": [w["max_pending"] for w in workers],
        "elapsed_seconds": max(w["elapsed"] for w in workers),
        "histograms_0.01ms": histograms,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if not k.startswith("histograms")}, indent=2))
    required = [v for k, v in checks.items() if "p99" not in k or args.require_slo]
    raise SystemExit(0 if all(required) else 1)


if __name__ == "__main__":
    main()

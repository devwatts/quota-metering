import asyncio
import json
import os
from pathlib import Path

from aiohttp import web
from redis.exceptions import RedisError

from .consumer import ScheduleConsumer
from .store import QuotaError, QuotaRouter

STORE = web.AppKey("store", QuotaRouter)
CONSUMER = web.AppKey("consumer", ScheduleConsumer)


@web.middleware
async def errors(request, handler):
    try:
        return await handler(request)
    except QuotaError as exc:
        statuses = {"exhausted": 429, "conflict": 409, "unconfigured": 404}
        return web.json_response({"error": exc.code}, status=statuses.get(exc.code, 503))
    except (ValueError, TypeError) as exc:
        return web.json_response({"error": str(exc)}, status=400)
    except (RedisError, TimeoutError):
        return web.json_response(
            {"error": "Outcome unavailable; retry with the same Idempotency-Key"}, status=503
        )


async def schedules(request):
    operation_id = request.headers.get("Idempotency-Key", "")
    body = await request.json()
    if not isinstance(body, dict) or set(body) != {"routes"}:
        raise ValueError('Expected {"routes": [...]}')
    operation, timing = await request.app[CONSUMER].execute(
        request.match_info["org"], operation_id, body["routes"]
    )
    return web.json_response(
        {
            "operation_id": operation.id,
            "state": operation.state,
            "period": operation.period,
            "units": operation.units,
            **operation.response,
        },
        status=200 if operation.state == "confirmed" else 422,
        headers={
            "X-Quota-Admission-Ms": f"{timing['admission_ms']:.3f}",
            "X-Quota-Settlement-Ms": f"{timing['completion_ms']:.3f}",
            "X-Quota-Total-Ms": f"{timing['quota_path_ms']:.3f}",
        },
    )


async def usage(request):
    return web.json_response(
        await request.app[STORE].usage(request.match_info["org"], request.match_info["feature"])
    )


async def health(request):
    await asyncio.gather(*(shard.redis.ping() for shard in request.app[STORE].shards))
    return web.json_response({"status": "ok"})


async def resources(app):
    store = QuotaRouter()
    try:
        await store.ready()
        config = Path(os.getenv("QUOTA_CONFIG", "config/quotas.json"))
        if config.exists():
            for org, limits in json.loads(config.read_text()).items():
                await store.configure(org, limits)
        app[STORE], app[CONSUMER] = store, ScheduleConsumer(store)
        yield
    finally:
        await store.close()


def create_app():
    # Bound unfinished work per process; the load driver records 503s as failures.
    pending = 0

    @web.middleware
    async def capacity(request, handler):
        nonlocal pending
        if pending >= 1024:
            return web.json_response({"error": "Server busy; retry with the same key"}, status=503)
        pending += 1
        try:
            return await handler(request)
        finally:
            pending -= 1

    app = web.Application(middlewares=[errors, capacity], client_max_size=256_000)
    app.cleanup_ctx.append(resources)
    app.add_routes(
        [
            web.post("/orgs/{org}/schedules/search", schedules),
            web.get("/orgs/{org}/usage/{feature}", usage),
            web.get("/health", health),
        ]
    )
    return app


if __name__ == "__main__":
    web.run_app(create_app(), port=int(os.getenv("PORT", "8080")))

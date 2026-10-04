import time

from .store import Operation, QuotaRouter, QuotaStore

# Small, fixed dataset keeps the demo about metering. Searches have no external effects.
SAILINGS = {
    "SGSIN-NLRTM": {"service": "Asia-Europe", "transit_days": 28},
    "CNSHA-USLAX": {"service": "Transpacific", "transit_days": 16},
    "INNSA-AEJEA": {"service": "India-Gulf", "transit_days": 4},
}


class SearchFailed(Exception):
    pass


async def search(request: dict) -> dict:
    try:
        return {"schedules": [{"route": route, **SAILINGS[route]} for route in request["routes"]]}
    except KeyError as exc:
        raise SearchFailed("No schedule for one or more routes") from exc


class ScheduleConsumer:
    def __init__(self, store: QuotaStore | QuotaRouter, provider=search):
        self.store = store
        self.provider = provider

    async def complete(self, operation: Operation) -> Operation:
        if operation.state != "pending":
            return operation
        try:
            response = await self.provider(operation.request)
        except SearchFailed as exc:
            return await self.store.settle(operation, False, {"error": str(exc)})
        # Timeouts and unexpected errors leave the reservation pending. A later
        # retry/recovery repeats this read-only lookup and settles the same receipt.
        return await self.store.settle(operation, True, response)

    async def execute(self, org: str, operation_id: str, routes: list[str]):
        started = time.perf_counter()
        if (
            not isinstance(routes, list)
            or not routes
            or len(routes) > 1000
            or any(not isinstance(route, str) or len(route) > 32 for route in routes)
        ):
            raise ValueError("routes must contain 1–1000 route strings, each at most 32 characters")
        operation = await self.store.reserve(
            org, "sailing-schedule", operation_id, len(routes), {"routes": routes}
        )
        admitted = time.perf_counter()
        operation = await self.complete(operation)
        finished = time.perf_counter()
        return operation, {
            "admission_ms": (admitted - started) * 1000,
            "completion_ms": (finished - admitted) * 1000,
            "quota_path_ms": (finished - started) * 1000,
        }

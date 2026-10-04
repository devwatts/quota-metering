import asyncio
import hashlib
import json
import re
import zlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from . import settings

NAME = re.compile(r"[a-zA-Z0-9_-]{1,80}\Z")


class QuotaError(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def encode(value) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def month_bounds(now: datetime) -> tuple[str, int, int]:
    now = now.astimezone(UTC)
    return _month_bounds(now.year, now.month)


@lru_cache(maxsize=24)
def _month_bounds(year: int, month: int) -> tuple[str, int, int]:
    start = datetime(year, month, 1, tzinfo=UTC)
    end = (
        start.replace(year=start.year + 1, month=1)
        if start.month == 12
        else start.replace(month=start.month + 1)
    )
    return start.strftime("%Y-%m"), int(start.timestamp()), int(end.timestamp())


def validate_name(value: str) -> None:
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise ValueError("Identifiers must contain 1–80 letters, digits, underscores or hyphens")


@dataclass(frozen=True)
class Operation:
    id: str
    org: str
    feature: str
    units: int
    period: str
    state: str
    request: dict
    response: dict | None

    @classmethod
    def parse(cls, value: dict[str, str]) -> "Operation":
        return cls(
            id=value["id"],
            org=value["org"],
            feature=value["feature"],
            units=int(value["units"]),
            period=value["period"],
            state=value["state"],
            request=json.loads(value["request"]),
            response=json.loads(value["response"]) if value.get("response") else None,
        )


class QuotaStore:
    def __init__(self, url: str = settings.REDIS_URL, prefix: str = settings.PREFIX):
        self.prefix = prefix
        self.redis = Redis(
            connection_pool=BlockingConnectionPool.from_url(
                url,
                max_connections=32,
                timeout=5,
                socket_timeout=5,
                socket_connect_timeout=3,
                decode_responses=True,
                retry=Retry(NoBackoff(), 0),
            )
        )
        self.script = self.redis.register_script(Path(__file__).with_name("meter.lua").read_text())
        self.stream = f"{prefix}:events"
        self.pending = f"{prefix}:pending"

    async def close(self):
        await self.redis.aclose(close_connection_pool=True)

    async def ready(self):
        config = await self.redis.config_get(
            "appendonly", "appendfsync", "no-appendfsync-on-rewrite", "maxmemory-policy"
        )
        expected = {
            "appendonly": "yes",
            "appendfsync": "always",
            "no-appendfsync-on-rewrite": "no",
            "maxmemory-policy": "noeviction",
        }
        if config != expected:
            raise RuntimeError(f"Redis durability configuration does not match: {config}")
        await self.redis.script_load(Path(__file__).with_name("meter.lua").read_text())

    def quota_key(self, org, feature, period):
        return f"{self.prefix}:balance:{org}:{feature}:{period}"

    def receipt_key(self, org, operation_id):
        return f"{self.prefix}:receipt:{org}:{operation_id}"

    async def configure(self, org: str, limits: dict[str, int]):
        validate_name(org)
        for feature, limit in limits.items():
            validate_name(feature)
            if type(limit) is not int or not 0 <= limit <= settings.MAX_LIMIT:
                raise ValueError("Limits must be integers between 0 and 1,000,000,000")
        if not limits:
            raise ValueError("At least one feature limit is required")
        await self.redis.hset(f"{self.prefix}:limits:{org}", mapping=limits)

    async def _call(
        self,
        action,
        org,
        feature,
        operation_id="",
        units=0,
        request="",
        fingerprint="",
        state="",
        response="",
        period=None,
    ):
        now = datetime.now(UTC)
        for _ in range(3):
            current, start, end = month_bounds(now)
            keys = [
                self.quota_key(org, feature, period or current),
                self.receipt_key(org, operation_id),
                f"{self.prefix}:limits:{org}",
                self.stream,
                self.pending,
            ]
            reply = await self.script(
                keys=keys,
                args=[
                    action,
                    org,
                    feature,
                    operation_id,
                    units,
                    request,
                    fingerprint,
                    current,
                    start,
                    end,
                    state,
                    response,
                    settings.RECEIPT_TTL,
                ],
            )
            if reply[0] == "clock":
                now = datetime.fromtimestamp(int(reply[1]), UTC)
                continue
            if reply[0] != "ok":
                raise QuotaError(reply[0])
            return reply[1:]
        raise QuotaError("clock_changed")

    async def reserve(
        self, org: str, feature: str, operation_id: str, units: int, request: dict
    ) -> Operation:
        for name in (org, feature, operation_id):
            validate_name(name)
        if type(units) is not int or not 1 <= units <= settings.MAX_UNITS:
            raise ValueError(f"Units must be an integer between 1 and {settings.MAX_UNITS}")
        payload = encode(request)
        if len(payload.encode()) > 256_000:
            raise ValueError("Request is too large")
        fingerprint = hashlib.sha256(payload.encode()).hexdigest()
        state, period, response = await self._call(
            "reserve", org, feature, operation_id, units, payload, fingerprint
        )
        return Operation(
            operation_id,
            org,
            feature,
            units,
            period,
            state,
            request,
            json.loads(response) if response else None,
        )

    async def settle(self, operation: Operation, succeeded: bool, response: dict) -> Operation:
        state, _, stored_response = await self._call(
            "settle",
            operation.org,
            operation.feature,
            operation.id,
            state="confirmed" if succeeded else "released",
            response=encode(response),
            period=operation.period,
        )
        return replace(operation, state=state, response=json.loads(stored_response))

    async def usage(self, org: str, feature: str) -> dict:
        validate_name(org)
        validate_name(feature)
        (raw,) = await self._call("usage", org, feature)
        value = json.loads(raw)
        value["resets_at"] = datetime.fromtimestamp(value.pop("reset"), UTC).isoformat()
        value["remaining"] = value["limit"] - value["used"] - value["reserved"]
        return value


class QuotaRouter:
    """Each organization has one fixed owner; changing the shard count needs migration."""

    def __init__(self, urls=settings.REDIS_URLS, prefix=settings.PREFIX):
        self.prefix = prefix
        self.shards = tuple(QuotaStore(url, f"{prefix}:s{i}") for i, url in enumerate(urls))
        if not self.shards:
            raise ValueError("At least one Redis URL is required")

    def for_org(self, org):
        validate_name(org)
        # All workers must agree on the owner; Python's hash() varies across process starts.
        return self.shards[zlib.crc32(org.encode()) % len(self.shards)]

    async def ready(self):
        for i, shard in enumerate(self.shards):
            await shard.ready()
            identity = f"crc32-v1:{len(self.shards)}:{i}"
            key = f"{self.prefix}:routing"
            await shard.redis.set(key, identity, nx=True)
            if await shard.redis.get(key) != identity:
                raise RuntimeError(
                    "Redis routing changed; migrate existing state before changing shards"
                )

    async def close(self):
        await asyncio.gather(*(shard.close() for shard in self.shards))

    async def configure(self, org, limits):
        await self.for_org(org).configure(org, limits)

    async def reserve(self, org, feature, operation_id, units, request):
        return await self.for_org(org).reserve(org, feature, operation_id, units, request)

    async def settle(self, operation, succeeded, response):
        return await self.for_org(operation.org).settle(operation, succeeded, response)

    async def usage(self, org, feature):
        return await self.for_org(org).usage(org, feature)

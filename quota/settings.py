import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_URLS = tuple(os.getenv("REDIS_URLS", REDIS_URL).split(","))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
PREFIX = os.getenv("QUOTA_PREFIX", "quota")
RECEIPT_TTL = 60 * 60
MAX_UNITS = 10_000
MAX_LIMIT = 1_000_000_000

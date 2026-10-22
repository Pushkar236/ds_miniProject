"""PostgreSQL access: the durable system of record for events and orders."""
import asyncio
import logging

import asyncpg

from . import config

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    venue       TEXT NOT NULL,
    starts_at   TIMESTAMPTZ NOT NULL,
    capacity    INTEGER NOT NULL CHECK (capacity > 0),
    price_cents INTEGER NOT NULL CHECK (price_cents >= 0)
);

CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    event_id     INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL,
    qty          INTEGER NOT NULL CHECK (qty > 0),
    status       TEXT NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'FAILED', 'EXPIRED')),
    reason       TEXT,
    booked_by    TEXT NOT NULL,
    processed_by TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_status_created ON orders (status, created_at);
CREATE INDEX IF NOT EXISTS orders_event ON orders (event_id);
"""

# Every replica runs the migration at start-up. A transaction-scoped advisory lock makes
# that safe: concurrent CREATE TABLE IF NOT EXISTS can otherwise race on the catalog.
SCHEMA_LOCK_ID = 4242


async def connect(retries: int = 60) -> asyncpg.Pool:
    for attempt in range(1, retries + 1):
        try:
            pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=1, max_size=config.DB_POOL_SIZE)
            break
        except (OSError, asyncpg.PostgresError) as exc:
            log.warning("postgres not ready (attempt %d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(1)
    else:
        raise RuntimeError("could not connect to PostgreSQL")

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK_ID)
            await conn.execute(SCHEMA)
    return pool

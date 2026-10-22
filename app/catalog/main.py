"""Catalog service: owns events and seeds/reconciles the Redis seat inventory.

Run: uvicorn app.catalog.main:app --port 8001
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.common import bus, config, db

logging.basicConfig(level=logging.INFO, format="%(asctime)s catalog %(levelname)s %(message)s")
log = logging.getLogger("catalog")

DEMO_EVENTS = [
    ("Monsoon Beats Festival", "Pune", 500, 249900),
    ("DevSummit India 2026", "Bengaluru", 300, 149900),
    ("Midnight Stand-up Special", "Mumbai", 100, 79900),
]

state: dict = {}


class EventIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    venue: str = Field(min_length=1, max_length=120)
    capacity: int = Field(gt=0, le=1_000_000)
    price_cents: int = Field(ge=0)
    starts_in_days: int = Field(default=30, ge=0)


async def seed_and_reconcile() -> None:
    """Insert demo events once, then make sure every event has a Redis inventory counter.

    If Redis lost its data (restart without persistence), the counter is rebuilt from the
    database: capacity minus seats held by PENDING or CONFIRMED orders. SET NX means a live
    counter is never overwritten, so it is safe for several catalog replicas to run this.
    """
    pool, r = state["pool"], state["redis"]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", db.SCHEMA_LOCK_ID + 1)
            if await conn.fetchval("SELECT count(*) FROM events") == 0:
                for i, (name, venue, capacity, price) in enumerate(DEMO_EVENTS):
                    await conn.execute(
                        "INSERT INTO events (name, venue, starts_at, capacity, price_cents) VALUES ($1,$2,$3,$4,$5)",
                        name, venue, datetime.now(timezone.utc) + timedelta(days=14 + 7 * i), capacity, price,
                    )
                log.info("seeded %d demo events", len(DEMO_EVENTS))
        rows = await conn.fetch(
            """SELECT e.id, e.capacity,
                      COALESCE(SUM(o.qty) FILTER (WHERE o.status IN ('PENDING','CONFIRMED')), 0) AS held
               FROM events e LEFT JOIN orders o ON o.event_id = e.id GROUP BY e.id"""
        )
    for row in rows:
        if await r.set(config.inventory_key(row["id"]), row["capacity"] - row["held"], nx=True):
            log.info("rebuilt inventory for event %s", row["id"])


@asynccontextmanager
async def lifespan(_: FastAPI):
    state["redis"] = bus.redis_client()
    await bus.wait_for_redis(state["redis"])
    state["pool"] = await db.connect()
    await seed_and_reconcile()
    yield
    await state["pool"].close()
    await state["redis"].aclose()


app = FastAPI(title="TicketRush Catalog", lifespan=lifespan)
bus.add_instance_header(app)


async def event_rows(event_id: int | None = None) -> list[dict]:
    query = """SELECT e.*,
                      COALESCE(SUM(o.qty) FILTER (WHERE o.status = 'CONFIRMED'), 0) AS confirmed,
                      COALESCE(SUM(o.qty) FILTER (WHERE o.status = 'PENDING'), 0)   AS pending,
                      COALESCE(SUM(o.qty) FILTER (WHERE o.status = 'FAILED'), 0)    AS failed,
                      COALESCE(SUM(o.qty) FILTER (WHERE o.status = 'EXPIRED'), 0)   AS expired
               FROM events e LEFT JOIN orders o ON o.event_id = e.id
               WHERE $1::int IS NULL OR e.id = $1
               GROUP BY e.id ORDER BY e.id"""
    async with state["pool"].acquire() as conn:
        rows = [dict(r) for r in await conn.fetch(query, event_id)]
    if rows:
        remaining = await state["redis"].mget([config.inventory_key(r["id"]) for r in rows])
        for row, left in zip(rows, remaining):
            row["remaining"] = int(left) if left is not None else None
            row["starts_at"] = row["starts_at"].isoformat()
    return rows


@app.get("/events")
async def list_events():
    return await event_rows()


@app.get("/events/{event_id}")
async def get_event(event_id: int):
    rows = await event_rows(event_id)
    if not rows:
        raise HTTPException(404, "event not found")
    return rows[0]


@app.post("/events", status_code=201)
async def create_event(body: EventIn):
    async with state["pool"].acquire() as conn:
        event_id = await conn.fetchval(
            "INSERT INTO events (name, venue, starts_at, capacity, price_cents) VALUES ($1,$2,$3,$4,$5) RETURNING id",
            body.name, body.venue, datetime.now(timezone.utc) + timedelta(days=body.starts_in_days),
            body.capacity, body.price_cents,
        )
    await state["redis"].set(config.inventory_key(event_id), body.capacity, nx=True)
    await bus.publish(state["redis"], "EVENT_CREATED", event_id=event_id, name=body.name)
    return await get_event(event_id)


@app.post("/events/{event_id}/reset")
async def reset_event(event_id: int):
    """Demo helper: wipe all orders for an event and restore full capacity."""
    r = state["redis"]
    async with state["pool"].acquire() as conn:
        capacity = await conn.fetchval("SELECT capacity FROM events WHERE id = $1", event_id)
        if capacity is None:
            raise HTTPException(404, "event not found")
        order_ids = [row["id"] for row in await conn.fetch("DELETE FROM orders WHERE event_id = $1 RETURNING id", event_id)]
    async for key in r.scan_iter(f"user:{event_id}:*", count=1000):
        await r.delete(key)
    for i in range(0, len(order_ids), 500):
        await r.delete(*[config.released_key(o) for o in order_ids[i:i + 500]])
    await r.set(config.inventory_key(event_id), capacity)
    await bus.publish(r, "EVENT_RESET", event_id=event_id)
    return await get_event(event_id)


@app.get("/events/{event_id}/audit")
async def audit_event(event_id: int):
    """Consistency check: Redis remaining + seats held in PostgreSQL must equal capacity.

    Only meaningful once traffic has stopped (a booking in flight is briefly counted in
    Redis before its order row is committed).
    """
    e = await get_event(event_id)
    held = e["confirmed"] + e["pending"]
    return {
        "event_id": event_id,
        "capacity": e["capacity"],
        "redis_remaining": e["remaining"],
        "confirmed": e["confirmed"],
        "pending": e["pending"],
        "released": e["failed"] + e["expired"],
        "oversold": held > e["capacity"],
        "consistent": e["remaining"] is not None and e["remaining"] + held == e["capacity"],
    }

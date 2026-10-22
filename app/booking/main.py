"""Booking service: stateless, horizontally scalable seat reservation.

Run: uvicorn app.booking.main:app --port 8002

Request path for POST /bookings
  1. Atomically reserve seats in Redis (Lua)       -> no overselling across replicas
  2. Record a PENDING order in PostgreSQL           -> durable
  3. XADD the order to the `orders` Redis Stream    -> async hand-off to payment workers
  4. Reply 202 Accepted immediately; the client polls GET /bookings/{id}

Every replica also runs a reaper that expires unpaid reservations. No leader election is
needed: `UPDATE ... WHERE status='PENDING' RETURNING` hands each row to exactly one replica.
"""
import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.common import bus, config, db
from app.common.inventory import Inventory

logging.basicConfig(level=logging.INFO, format="%(asctime)s booking %(levelname)s %(message)s")
log = logging.getLogger("booking")

state: dict = {}


class BookingIn(BaseModel):
    event_id: int
    user_id: str = Field(min_length=1, max_length=64)
    qty: int = Field(default=1, ge=1, le=config.MAX_TICKETS_PER_USER)


async def reaper_loop() -> None:
    pool, r, inventory = state["pool"], state["redis"], state["inventory"]
    while True:
        await asyncio.sleep(config.REAPER_INTERVAL)
        try:
            async with pool.acquire() as conn:
                expired = await conn.fetch(
                    """UPDATE orders SET status = 'EXPIRED', reason = 'payment not completed in time',
                                         processed_by = $2, updated_at = now()
                       WHERE status = 'PENDING' AND created_at < now() - make_interval(secs => $1)
                       RETURNING id, event_id, user_id, qty""",
                    float(config.HOLD_SECONDS), config.INSTANCE_ID,
                )
            for row in expired:
                await inventory.release(row["id"], row["event_id"], row["user_id"], row["qty"])
                await bus.publish(r, "ORDER_EXPIRED", order_id=row["id"], event_id=row["event_id"], qty=row["qty"])
            if expired:
                log.info("expired %d stale reservations", len(expired))
        except Exception:
            log.exception("reaper iteration failed")


@asynccontextmanager
async def lifespan(_: FastAPI):
    r = bus.redis_client()
    await bus.wait_for_redis(r)
    state.update(redis=r, inventory=Inventory(r), pool=await db.connect())
    tasks = [asyncio.create_task(bus.heartbeat_loop(r, "booking")), asyncio.create_task(reaper_loop())]
    yield
    for t in tasks:
        t.cancel()
    await state["pool"].close()
    await r.aclose()


app = FastAPI(title="TicketRush Booking", lifespan=lifespan)
bus.add_instance_header(app)


@app.post("/bookings", status_code=202)
async def create_booking(body: BookingIn):
    r, inventory = state["redis"], state["inventory"]
    await r.hincrby("stats:booking", config.INSTANCE_ID, 1)

    outcome, remaining = await inventory.reserve(body.event_id, body.user_id, body.qty)
    if outcome == "UNKNOWN_EVENT":
        raise HTTPException(404, "event not found")
    if outcome == "USER_LIMIT":
        raise HTTPException(429, f"limit is {config.MAX_TICKETS_PER_USER} tickets per user per event")
    if outcome == "SOLD_OUT":
        await r.hincrby("stats:rejected", config.INSTANCE_ID, 1)
        raise HTTPException(409, "sold out")

    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    try:
        async with state["pool"].acquire() as conn:
            await conn.execute(
                "INSERT INTO orders (id, event_id, user_id, qty, status, booked_by) VALUES ($1,$2,$3,$4,'PENDING',$5)",
                order_id, body.event_id, body.user_id, body.qty, config.INSTANCE_ID,
            )
    except Exception:
        # Compensation: the database write failed, so give the seats back immediately.
        log.exception("order insert failed, releasing seats")
        await inventory.release(order_id, body.event_id, body.user_id, body.qty)
        raise HTTPException(503, "could not record order, please retry")

    # If this XADD is lost the order stays PENDING and the reaper releases it after HOLD_SECONDS.
    await r.xadd(
        config.ORDERS_STREAM,
        {"order_id": order_id, "event_id": body.event_id, "user_id": body.user_id, "qty": body.qty},
        maxlen=100_000, approximate=True,
    )
    await bus.publish(r, "ORDER_RESERVED", order_id=order_id, event_id=body.event_id, qty=body.qty, remaining=remaining)
    return {"order_id": order_id, "status": "PENDING", "remaining": remaining, "handled_by": config.INSTANCE_ID}


@app.get("/bookings/{order_id}")
async def get_booking(order_id: str):
    async with state["pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
    if row is None:
        raise HTTPException(404, "order not found")
    order = dict(row)
    order["created_at"] = order["created_at"].isoformat()
    order["updated_at"] = order["updated_at"].isoformat()
    return order

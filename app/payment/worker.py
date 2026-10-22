"""Payment worker: competing consumers on a Redis Streams consumer group.

Run: python -m app.payment.worker

* Scaling: every replica joins consumer group `payments`; Redis hands each stream entry to
  exactly one consumer, so adding replicas divides the backlog between them.
* Fault tolerance: an entry is XACKed only after it has been fully processed. If a worker
  crashes mid-payment the entry stays in the group's pending list, and another worker takes
  it over with XAUTOCLAIM once it has been idle for RECLAIM_IDLE_MS.
* Idempotency: delivery is at-least-once, so a state change happens only if the order is
  still PENDING, and seat release is guarded by a per-order marker (see inventory.py).
"""
import asyncio
import hashlib
import logging
import random

import redis.asyncio as redis

from app.common import bus, config, db
from app.common.inventory import Inventory

logging.basicConfig(level=logging.INFO, format="%(asctime)s payment %(levelname)s %(message)s")
log = logging.getLogger("payment")


class PaymentWorker:
    def __init__(self, r: redis.Redis, pool):
        self.r = r
        self.pool = pool
        self.inventory = Inventory(r)
        self.slots = asyncio.Semaphore(config.WORKER_CONCURRENCY)
        self.tasks: set[asyncio.Task] = set()

    @staticmethod
    def fraud_score(order_id: str) -> int:
        """Stand-in for CPU-bound risk scoring. Gives the CPU-based autoscaler something to measure."""
        digest = order_id.encode()
        for _ in range(config.PAYMENT_CPU_ROUNDS):
            digest = hashlib.sha256(digest).digest()
        return digest[0]

    async def charge(self, order_id: str) -> tuple[bool, str | None]:
        await asyncio.sleep(random.uniform(config.PAYMENT_MIN_MS, config.PAYMENT_MAX_MS) / 1000)
        if self.fraud_score(order_id) < 3:  # about 1% of orders
            return False, "flagged by fraud check"
        if random.random() < config.PAYMENT_FAIL_RATE:
            return False, "card declined"
        return True, None

    async def settle_stale(self, order_id: str) -> None:
        """Order is no longer PENDING: a redelivery, expired by the reaper, or deleted by a reset.

        Re-run the release in case an earlier attempt crashed before it; it is idempotent.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT event_id, user_id, qty, status FROM orders WHERE id = $1", order_id)
        if row and row["status"] in ("FAILED", "EXPIRED"):
            await self.inventory.release(order_id, row["event_id"], row["user_id"], row["qty"])

    async def process(self, fields: dict) -> None:
        order_id = fields["order_id"]
        async with self.pool.acquire() as conn:
            pending = await conn.fetchval("SELECT status = 'PENDING' FROM orders WHERE id = $1", order_id)
        if not pending:
            # Never charge for a hold that has already expired (or an order that is gone).
            await self.settle_stale(order_id)
            return

        ok, reason = await self.charge(order_id)
        status = "CONFIRMED" if ok else "FAILED"
        async with self.pool.acquire() as conn:
            # The status guard still matters: the reaper may expire the order while we charge.
            row = await conn.fetchrow(
                """UPDATE orders SET status = $2, reason = $3, processed_by = $4, updated_at = now()
                   WHERE id = $1 AND status = 'PENDING'
                   RETURNING event_id, user_id, qty""",
                order_id, status, reason, config.INSTANCE_ID,
            )
        if row is None:
            # Lost the race with the reaper. In a real system a successful charge here would be refunded.
            await self.settle_stale(order_id)
            return
        if not ok:
            await self.inventory.release(order_id, row["event_id"], row["user_id"], row["qty"])
        await self.r.hincrby("stats:payment", config.INSTANCE_ID, 1)
        await bus.publish(self.r, f"ORDER_{status}", order_id=order_id, event_id=row["event_id"], qty=row["qty"], reason=reason)

    async def handle(self, msg_id: str, fields: dict) -> None:
        try:
            await self.process(fields)
            await self.r.xack(config.ORDERS_STREAM, config.PAYMENT_GROUP, msg_id)
        except Exception:
            # No XACK: the entry stays pending and will be reclaimed and retried.
            log.exception("failed to process %s", msg_id)
        finally:
            self.slots.release()

    async def dispatch(self, msg_id: str, fields: dict | None) -> None:
        if not fields:  # entry was trimmed from the stream; nothing left to do
            await self.r.xack(config.ORDERS_STREAM, config.PAYMENT_GROUP, msg_id)
            return
        await self.slots.acquire()
        task = asyncio.create_task(self.handle(msg_id, fields))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def reclaim_loop(self) -> None:
        """Take over entries that another (probably dead) consumer received but never acknowledged."""
        while True:
            await asyncio.sleep(config.RECLAIM_IDLE_MS / 3000)
            try:
                start = "0-0"
                while True:
                    start, messages, *_ = await self.r.xautoclaim(
                        config.ORDERS_STREAM, config.PAYMENT_GROUP, config.INSTANCE_ID,
                        min_idle_time=config.RECLAIM_IDLE_MS, start_id=start, count=50,
                    )
                    for msg_id, fields in messages:
                        log.warning("reclaimed %s from a stalled consumer", msg_id)
                        await bus.publish(self.r, "ORDER_RECLAIMED", order_id=(fields or {}).get("order_id"))
                        await self.dispatch(msg_id, fields)
                    if start == "0-0":
                        break
                await self.prune_dead_consumers()
            except Exception:
                log.exception("reclaim iteration failed")

    async def prune_dead_consumers(self) -> None:
        """Remove group members that have gone silent and own no pending entries (old pods)."""
        for consumer in await self.r.xinfo_consumers(config.ORDERS_STREAM, config.PAYMENT_GROUP):
            if consumer["name"] != config.INSTANCE_ID and consumer["pending"] == 0 and consumer["idle"] > 60_000:
                await self.r.xgroup_delconsumer(config.ORDERS_STREAM, config.PAYMENT_GROUP, consumer["name"])
                log.info("removed dead consumer %s from the group", consumer["name"])

    async def run(self) -> None:
        try:
            await self.r.xgroup_create(config.ORDERS_STREAM, config.PAYMENT_GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        background = [
            asyncio.create_task(bus.heartbeat_loop(self.r, "payment")),
            asyncio.create_task(self.reclaim_loop()),
        ]
        log.info("worker %s joined group %s", config.INSTANCE_ID, config.PAYMENT_GROUP)
        try:
            while True:
                response = await self.r.xreadgroup(
                    config.PAYMENT_GROUP, config.INSTANCE_ID, {config.ORDERS_STREAM: ">"},
                    count=config.WORKER_CONCURRENCY, block=2000,
                )
                for _stream, messages in response or []:
                    for msg_id, fields in messages:
                        await self.dispatch(msg_id, fields)
        finally:
            for t in background:
                t.cancel()


async def main() -> None:
    r = bus.redis_client()
    await bus.wait_for_redis(r)
    pool = await db.connect()
    await PaymentWorker(r, pool).run()


if __name__ == "__main__":
    asyncio.run(main())

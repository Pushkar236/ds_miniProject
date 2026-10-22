"""Seat inventory held in Redis and changed only through atomic Lua scripts.

Redis runs a Lua script start to finish without interleaving other commands, so the
check-then-decrement below cannot race, no matter how many booking replicas call it at once.
That is what prevents overselling without any distributed lock.
"""
import redis.asyncio as redis

from . import config

# KEYS[1] inventory counter, KEYS[2] per-user counter
# ARGV[1] qty, ARGV[2] per-user limit
# returns {code, remaining}: 1 ok, -1 sold out, -2 unknown event, -3 per-user limit hit
RESERVE_LUA = """
local stock = redis.call('GET', KEYS[1])
if not stock then return {-2, 0} end
stock = tonumber(stock)
local qty = tonumber(ARGV[1])
local held = tonumber(redis.call('GET', KEYS[2]) or '0')
if held + qty > tonumber(ARGV[2]) then return {-3, stock} end
if stock < qty then return {-1, stock} end
redis.call('DECRBY', KEYS[1], qty)
redis.call('INCRBY', KEYS[2], qty)
return {1, stock - qty}
"""

# KEYS[1] inventory counter, KEYS[2] per-user counter, KEYS[3] released marker for this order
# ARGV[1] qty
# The SET NX marker makes the release idempotent: a retried or duplicated release of the
# same order returns its seats exactly once (at-least-once delivery, exactly-once effect).
RELEASE_LUA = """
if not redis.call('SET', KEYS[3], '1', 'NX', 'EX', 86400) then return 0 end
redis.call('INCRBY', KEYS[1], ARGV[1])
local left = redis.call('DECRBY', KEYS[2], ARGV[1])
if left <= 0 then redis.call('DEL', KEYS[2]) end
return 1
"""

RESERVE_CODES = {1: "OK", -1: "SOLD_OUT", -2: "UNKNOWN_EVENT", -3: "USER_LIMIT"}


class Inventory:
    def __init__(self, r: redis.Redis):
        self._reserve = r.register_script(RESERVE_LUA)
        self._release = r.register_script(RELEASE_LUA)

    async def reserve(self, event_id: int, user_id: str, qty: int) -> tuple[str, int]:
        code, remaining = await self._reserve(
            keys=[config.inventory_key(event_id), config.user_key(event_id, user_id)],
            args=[qty, config.MAX_TICKETS_PER_USER],
        )
        return RESERVE_CODES[int(code)], int(remaining)

    async def release(self, order_id: str, event_id: int, user_id: str, qty: int) -> bool:
        done = await self._release(
            keys=[config.inventory_key(event_id), config.user_key(event_id, user_id), config.released_key(order_id)],
            args=[qty],
        )
        return bool(done)

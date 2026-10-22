"""Redis helpers: client, pub/sub notifications, heartbeats and per-instance service headers."""
import asyncio
import json
import logging
import time

import redis.asyncio as redis
from fastapi import FastAPI, Request

from . import config

log = logging.getLogger(__name__)


def redis_client() -> redis.Redis:
    return redis.from_url(config.REDIS_URL, decode_responses=True)


async def wait_for_redis(r: redis.Redis, retries: int = 60) -> None:
    for attempt in range(1, retries + 1):
        try:
            await r.ping()
            return
        except (OSError, redis.RedisError) as exc:
            log.warning("redis not ready (attempt %d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(1)
    raise RuntimeError("could not connect to Redis")


async def publish(r: redis.Redis, kind: str, **data) -> None:
    """Fire-and-forget notification for the dashboard (Redis pub/sub -> gateway -> WebSocket)."""
    message = {"type": kind, "ts": time.time(), "instance": config.INSTANCE_ID, **data}
    try:
        await r.publish(config.EVENTS_CHANNEL, json.dumps(message))
    except redis.RedisError:
        log.exception("publish failed")


async def heartbeat_loop(r: redis.Redis, role: str, interval: float = 3.0) -> None:
    """Membership: each replica keeps a short-lived key alive. If a pod dies, its key expires."""
    key = f"instance:{role}:{config.INSTANCE_ID}"
    while True:
        try:
            await r.set(key, int(time.time()), ex=config.HEARTBEAT_TTL)
        except redis.RedisError:
            log.exception("heartbeat failed")
        await asyncio.sleep(interval)


def add_instance_header(app: FastAPI) -> None:
    """Tag every response with the replica that produced it, so load balancing is visible."""

    @app.middleware("http")
    async def _tag(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Instance"] = config.INSTANCE_ID
        return response

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "instance": config.INSTANCE_ID}

"""API gateway: single entry point for the browser.

Run: uvicorn app.gateway.main:app --port 8000

* Routes REST calls to the catalog and booking services (service discovery by DNS name).
* Opens a fresh connection per upstream call (force_close, no DNS cache), so each request is
  load-balanced across all replicas behind the service name (Docker DNS round-robin /
  Kubernetes Service) and newly scaled-up replicas receive traffic immediately. The trade-off
  is a TCP handshake per request -- cheap inside a cluster network.
* Bridges Redis pub/sub to browsers over WebSocket for the live activity feed.
* Reports cluster membership (heartbeats), per-replica counters and queue backlog.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.common import bus, config

logging.basicConfig(level=logging.INFO, format="%(asctime)s gateway %(levelname)s %(message)s")
log = logging.getLogger("gateway")

STATIC = Path(__file__).parent / "static"
state: dict = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    state["redis"] = bus.redis_client()
    await bus.wait_for_redis(state["redis"])
    state["http"] = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=1000, force_close=True, use_dns_cache=False),
        timeout=aiohttp.ClientTimeout(total=10),
    )
    yield
    await state["http"].close()
    await state["redis"].aclose()


app = FastAPI(title="TicketRush Gateway", lifespan=lifespan)
bus.add_instance_header(app)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


async def forward(method: str, base: str, path: str, request: Request | None = None) -> JSONResponse:
    body = await request.json() if request is not None else None
    try:
        async with state["http"].request(method, f"{base}{path}", json=body) as resp:
            payload = await resp.json(content_type=None)
            status, handled_by = resp.status, resp.headers.get("X-Instance", "?")
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        raise HTTPException(503, f"upstream {base} unavailable: {type(exc).__name__}")
    return JSONResponse(
        payload,
        status_code=status,
        headers={"X-Handled-By": handled_by, "X-Gateway": config.INSTANCE_ID},
    )


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/events")
async def list_events():
    return await forward("GET", config.CATALOG_URL, "/events")


@app.post("/api/events")
async def create_event(request: Request):
    return await forward("POST", config.CATALOG_URL, "/events", request)


@app.get("/api/events/{event_id}")
async def get_event(event_id: int):
    return await forward("GET", config.CATALOG_URL, f"/events/{event_id}")


@app.get("/api/events/{event_id}/audit")
async def audit_event(event_id: int):
    return await forward("GET", config.CATALOG_URL, f"/events/{event_id}/audit")


@app.post("/api/events/{event_id}/reset")
async def reset_event(event_id: int):
    return await forward("POST", config.CATALOG_URL, f"/events/{event_id}/reset")


@app.post("/api/bookings")
async def create_booking(request: Request):
    return await forward("POST", config.BOOKING_URL, "/bookings", request)


@app.get("/api/bookings/{order_id}")
async def get_booking(order_id: str):
    return await forward("GET", config.BOOKING_URL, f"/bookings/{order_id}")


@app.get("/api/cluster")
async def cluster():
    r: redis.Redis = state["redis"]
    alive: dict[str, list[str]] = {}
    async for key in r.scan_iter("instance:*", count=500):
        _, role, instance = key.split(":", 2)
        alive.setdefault(role, []).append(instance)

    booked, rejected, paid = await asyncio.gather(
        r.hgetall("stats:booking"), r.hgetall("stats:rejected"), r.hgetall("stats:payment")
    )

    def replicas(role: str, counters: dict, extra: dict | None = None) -> list[dict]:
        names = set(alive.get(role, [])) | set(counters)
        rows = [
            {"instance": n, "alive": n in alive.get(role, []), "handled": int(counters.get(n, 0)),
             **({"rejected": int(extra.get(n, 0))} if extra is not None else {})}
            for n in names
        ]
        return sorted(rows, key=lambda x: (not x["alive"], x["instance"]))

    queue = {"length": 0, "pending": 0, "lag": 0, "consumers": 0}
    try:
        queue["length"] = await r.xlen(config.ORDERS_STREAM)
        for group in await r.xinfo_groups(config.ORDERS_STREAM):
            if group["name"] == config.PAYMENT_GROUP:
                queue.update(pending=group["pending"], lag=group.get("lag") or 0, consumers=group["consumers"])
    except redis.ResponseError:
        pass  # stream not created yet

    return {
        "gateway": config.INSTANCE_ID,
        "booking": replicas("booking", booked, rejected),
        "payment": replicas("payment", paid),
        "queue": queue,
    }


@app.post("/api/cluster/reset-stats")
async def reset_stats():
    await state["redis"].delete("stats:booking", "stats:rejected", "stats:payment")
    return {"ok": True}


@app.websocket("/ws")
async def live_feed(ws: WebSocket):
    await ws.accept()
    pubsub = state["redis"].pubsub()
    await pubsub.subscribe(config.EVENTS_CHANNEL)

    async def pump():
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                await ws.send_text(msg["data"])

    async def watch_disconnect():
        while True:
            await ws.receive_text()

    tasks = [asyncio.create_task(pump()), asyncio.create_task(watch_disconnect())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except WebSocketDisconnect:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await pubsub.unsubscribe(config.EVENTS_CHANNEL)
        await pubsub.aclose()

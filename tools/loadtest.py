"""Flash-sale load test + correctness check for TicketRush.

    pip install aiohttp
    python tools/loadtest.py --url http://localhost:8080 --event 3 --requests 3000 --concurrency 200

1. Resets the event (unless --no-reset).
2. Fires N booking requests from many simulated users at the same time.
3. Prints throughput, latency percentiles, outcome counts and which gateway / booking replica
   served each request.
4. Waits for the payment queue to drain, then runs the consistency audit:
   no overselling, and Redis inventory + seats held in PostgreSQL == capacity.

Exit code 0 only if the audit passes, so it doubles as an end-to-end test.
"""
import argparse
import asyncio
import statistics
import sys
import time
from collections import Counter

import aiohttp


async def rush(session: aiohttp.ClientSession, url: str, event_id: int, total: int, concurrency: int, qty: int):
    statuses, replicas, gateways, latencies = Counter(), Counter(), Counter(), []
    next_index = 0

    async def lane():
        nonlocal next_index
        while next_index < total:
            i = next_index
            next_index += 1
            started = time.perf_counter()
            try:
                body = {"event_id": event_id, "user_id": f"load-{i}", "qty": qty}
                async with session.post(f"{url}/api/bookings", json=body) as resp:
                    await resp.read()
                    statuses[resp.status] += 1
                    replicas[resp.headers.get("X-Handled-By", "?")] += 1
                    gateways[resp.headers.get("X-Gateway", "?")] += 1
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                statuses[type(exc).__name__] += 1
            latencies.append((time.perf_counter() - started) * 1000)

    started = time.perf_counter()
    await asyncio.gather(*(lane() for _ in range(concurrency)))
    return statuses, replicas, gateways, latencies, time.perf_counter() - started


async def get(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url) as resp:
        return await resp.json()


async def wait_for_drain(session: aiohttp.ClientSession, url: str, event_id: int, timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        event = await get(session, f"{url}/api/events/{event_id}")
        queue = (await get(session, f"{url}/api/cluster"))["queue"]
        print(f"  pending orders={event['pending']:>5}  stream lag={queue['lag']:>5}  in flight={queue['pending']:>4}", end="\r")
        if event["pending"] == 0 and queue["lag"] == 0 and queue["pending"] == 0:
            print()
            return
        await asyncio.sleep(1)
    print("\n  timed out waiting for the queue to drain")


def histogram(title: str, counts: Counter, total: int) -> None:
    print(title)
    for name, n in sorted(counts.items()):
        print(f"  {name:<32} {n:>6}  {'#' * round(40 * n / total)}")


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default="http://localhost:8080")
    p.add_argument("--event", type=int, default=3, help="event id (3 = the 100-seat demo event)")
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--qty", type=int, default=1)
    p.add_argument("--no-reset", action="store_true")
    p.add_argument("--drain-timeout", type=float, default=180)
    args = p.parse_args()
    url = args.url.rstrip("/")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:
        if not args.no_reset:
            await session.post(f"{url}/api/events/{args.event}/reset")
            await session.post(f"{url}/api/cluster/reset-stats")
        event = await get(session, f"{url}/api/events/{args.event}")
        print(f"Event #{args.event} '{event['name']}': capacity {event['capacity']}, remaining {event['remaining']}")
        print(f"Firing {args.requests} requests x {args.qty} seat(s) with concurrency {args.concurrency}...\n")

        statuses, replicas, gateways, lat, elapsed = await rush(
            session, url, args.event, args.requests, args.concurrency, args.qty
        )
        lat.sort()
        print(f"Throughput : {args.requests / elapsed:,.0f} req/s ({elapsed:.2f}s)")
        print(f"Latency ms : p50={statistics.median(lat):.0f}  p95={lat[int(len(lat) * .95) - 1]:.0f}  max={lat[-1]:.0f}")
        print("Outcomes   : " + ", ".join(f"{k}={v}" for k, v in sorted(statuses.items(), key=str)))
        histogram("Served by gateway replica:", gateways, args.requests)
        histogram("Served by booking replica:", replicas, args.requests)

        print("\nWaiting for payment workers to drain the queue...")
        await wait_for_drain(session, url, args.event, args.drain_timeout)

        cluster = await get(session, f"{url}/api/cluster")
        print("Processed by payment worker:")
        for w in cluster["payment"]:
            print(f"  {w['instance']:<32} {w['handled']:>6}{'' if w['alive'] else '  (dead)'}")

        audit = await get(session, f"{url}/api/events/{args.event}/audit")
        print("\nAudit:", audit)
        # 202s can exceed capacity legitimately, because seats freed by failed payments are
        # sold again to later requests. The invariant is on what is held, not on 202s.
        ok = audit["consistent"] and not audit["oversold"]
        print(f"\n{'PASS' if ok else 'FAIL'}: {audit['confirmed']} confirmed of capacity {audit['capacity']}, "
              f"{audit['released']} seats released after failed/expired payments and resold, "
              f"oversold={audit['oversold']}")
        return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

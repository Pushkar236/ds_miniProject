# TicketRush: Project Report

Distributed Systems Mini Project: *design, develop and deploy a small distributed application with
containerisation/orchestration that demonstrates scalability and inter-service communication.*

---

## 1. Problem statement

Flash sales (concert tickets, IPL tickets, limited product drops) create a short, extreme spike of
traffic aimed at a **small, shared, contended resource**: the remaining seats. The system must:

1. **Never oversell.** Exactly `capacity` tickets may be sold, even when thousands of requests arrive
   at once and are handled by different servers.
2. **Scale horizontally.** Absorb the spike by adding replicas, not by buying a bigger machine.
3. **Stay responsive.** Do not make the buyer wait for a slow payment provider.
4. **Tolerate failures.** A crashed process must not lose orders, double-charge, or leak seats.
5. **Release unpaid seats.** Abandoned reservations must return to the pool automatically.

A single server with one database transaction per booking solves (1) but fails (2)–(4). Naively adding
replicas breaks (1): two replicas read "1 seat left" at the same time and both sell it. That tension
between scalability and consistency is the core of the project.

## 2. Objectives

| # | Objective | Achieved by |
|---|---|---|
| O1 | Correctness under concurrency | Atomic Redis Lua reservation; audit invariant |
| O2 | Horizontal scalability | Stateless services, per-request load balancing, HPA |
| O3 | Synchronous + asynchronous inter-service communication | REST between services; Redis Streams work queue; pub/sub → WebSocket |
| O4 | Fault tolerance | Ack-after-processing, XAUTOCLAIM, idempotent state transitions, reaper |
| O5 | Containerised, orchestrated deployment | Docker image, Compose, Kubernetes manifests (tested on a 3-node kind cluster) |
| O6 | Observability for the demo | Live dashboard: replicas, heartbeats, queue depth, per-instance counters, event feed |

## 3. Design

### 3.1 Services

See the architecture diagram in the [README](../README.md#1-architecture). There are four application
services built from **one Docker image** (the container command selects the service), plus Redis and
PostgreSQL.

### 3.2 Why Redis for inventory and PostgreSQL for orders?

* **Inventory** is a single hot counter per event that every booking request must touch. Redis runs
  each Lua script **atomically** (single-threaded command execution), so *check stock, check per-user
  limit, decrement* is one indivisible step. No distributed lock or `SELECT … FOR UPDATE` row lock that
  would serialise all replicas on one database row.
* **Orders** need durability and querying. PostgreSQL is the **system of record**. If Redis loses its
  data, the catalog service **rebuilds** each counter as `capacity − Σqty(PENDING, CONFIRMED)` using
  `SET NX`, so a live counter is never overwritten (reconciliation).

### 3.3 The consistency argument (why we cannot oversell)

Invariant, for every event: `redis_remaining + held_in_db = capacity`, where `held = PENDING + CONFIRMED`.

* Seats only leave the counter inside the atomic reserve script, which refuses if `stock < qty`, so
  the counter never goes below 0. Hence `held ≤ capacity`.
* Seats only come back through the release script, which is guarded by a per-order `SET NX` marker,
  so they come back **at most once** per order, even if the release is retried, duplicated by
  redelivery, or raced by the reaper and a worker.
* Every state change on an order is `UPDATE … WHERE status = 'PENDING'`, so exactly one actor (one
  worker, or one reaper replica) wins each order's transition out of PENDING.

The `/audit` endpoint checks the invariant, and `tools/loadtest.py` fails if it does not hold.

### 3.4 Asynchronous payment with a consumer group

Booking replies **202 Accepted** as soon as the seat is held and the order is queued. Payment (slow,
unreliable) happens in `payment-worker` replicas reading the `orders` Redis Stream through the consumer
group `payments`:

* Redis delivers each entry to **one** consumer, so adding workers divides the backlog (competing consumers).
* An entry is **acknowledged only after** the database update. If a worker dies, its entries stay in the
  group's *Pending Entries List*. Any live worker takes them over with `XAUTOCLAIM` after 30s idle.
* Delivery is therefore **at-least-once**. Idempotent processing (status guard, release marker, and a
  "still PENDING?" check *before* charging) turns that into an **exactly-once effect**.
* Workers prune consumers that have been silent for over 60s and own nothing, so the group membership
  reflects reality after pods are replaced.

### 3.5 Reservation holds: leases without a leader

An unpaid order holds seats for `HOLD_SECONDS` (120s). Every booking replica runs a reaper loop:

```sql
UPDATE orders SET status = 'EXPIRED' …
WHERE status = 'PENDING' AND created_at < now() - interval
RETURNING id, event_id, user_id, qty
```

PostgreSQL row locking guarantees that each expired row is returned to exactly one replica, so the
reapers coordinate **without leader election**, and the system keeps working if any of them die.

### 3.6 Load balancing and discovery

* Services find each other by **DNS name** (`booking`, `redis`, `postgres`) from environment variables.
  The same image runs in Compose and Kubernetes; only addresses change.
* The gateway opens **a new upstream connection per request** (`force_close`, no DNS cache). With
  keep-alive, a client stays pinned to whichever replica it first connected to, and newly scaled pods
  would get no traffic. A per-request TCP handshake is cheap inside a cluster network.
* In Compose, **nginx** balances across gateway replicas and re-resolves the service name every 5s.
  In Kubernetes, the **Service** (kube-proxy) does that job.

### 3.7 Membership and observability

Each replica refreshes `instance:<role>:<id>` with a 10s TTL every 3s. The gateway lists these keys to
show which replicas are alive. A crashed pod disappears within 10s (a simple heartbeat failure
detector). Every state change is published on `ticketrush:events` and streamed to dashboards.

### 3.8 Kubernetes specifics

| Resource | Purpose |
|---|---|
| `Namespace`, `ConfigMap`, `Secret` | Isolation and externalised configuration |
| `StatefulSet` + PVC for PostgreSQL | Stable identity and persistent storage |
| `Deployment` × 5 | Stateless services, rolling updates |
| `Service` (ClusterIP) | Discovery + L4 load balancing; `NodePort 30080` exposes the gateway |
| `HorizontalPodAutoscaler` × 2 | booking 2–10 pods @ 60% CPU, workers 1–8 @ 70% CPU |
| `startupProbe` / `readinessProbe` / `livenessProbe` | Safe start-up ordering, traffic only to ready pods, restart only stuck pods |
| Resource requests/limits | Required for CPU-percentage autoscaling; protects neighbours |
| Optional KEDA `ScaledObject` | Scale workers on **stream lag** (queue backlog) instead of CPU |

## 4. Failure analysis

| Failure | What happens | Outcome |
|---|---|---|
| Two replicas book the last seat simultaneously | Lua script serialises them in Redis | One gets 202, the other 409 |
| PostgreSQL insert fails after seats reserved | Booking releases seats (compensation), returns 503 | No leak |
| Booking crashes after INSERT, before XADD | Order stays PENDING; reaper expires it after the hold | Seats return after 120s |
| Payment worker killed mid-payment | Entry unacked → reclaimed by another worker via XAUTOCLAIM | Processed once; **tested** |
| Same entry delivered twice | `WHERE status='PENDING'` makes the 2nd update a no-op | No double confirm |
| Release attempted twice | `SET NX` marker → second is a no-op | No double restock |
| Hold expires while payment is in progress | Reaper wins the race; worker's update affects 0 rows and it backs off | Consistent (a real system would refund) |
| Redis restarts and loses data | Catalog rebuilds counters from PostgreSQL (`SET NX`) | Inventory restored |
| A booking/gateway replica dies | Service/LB stops routing to it; heartbeat expires | Others continue |
| Whole booking service down | Gateway fails fast with 503 | Recovers when back |
| Overloaded pods answer probes slowly | Tolerant probe timeouts keep them ready (see §5.5) | HPA keeps scaling |

**Known single points of failure (accepted for a mini project):** one Redis and one PostgreSQL instance.
Production would use Redis Sentinel/Cluster and PostgreSQL replication (e.g. Patroni or a managed DB).

## 5. Experiments and results

All numbers are from real runs on one laptop (14 logical cores, Windows 11, Docker Desktop).

### 5.1 Correctness under a flash sale (Docker Compose)

2,000 requests, concurrency 150, 100-seat event, 3 booking replicas, 2 workers.

| Metric | Result |
|---|---|
| 202 Accepted / 409 Sold out | 109 / 1,891 |
| Confirmed tickets | **100 / 100** |
| Seats released after failed payments and resold | 9 |
| Oversold | **No**; audit consistent |
| Requests per booking replica | 657 / 665 / 678 (even split) |

Repeated on the 500-seat and 300-seat events and on Kubernetes: always PASS.

### 5.2 Horizontal scaling (Docker Compose, measured inside the Docker network)

6,000 bookings on a 200,000-seat event (every request takes the full write path), concurrency 200.

| gateway × booking replicas | Throughput | p50 latency | p95 latency |
|---|---|---|---|
| 1 × 1 | 294 req/s | 741 ms | 1001 ms |
| 2 × 2 | 370 req/s | 503 ms | 872 ms |
| 4 × 4 | **470 req/s** | **394 ms** | 747 ms |

Throughput grows and latency falls as replicas are added. Scaling is **sub-linear** because every
replica, PostgreSQL, Redis, nginx, the workers *and* the load generator share the same laptop CPU.
During the 4×4 run total usage was about 10 cores. On real multiple nodes the stateless tiers would keep
scaling until the shared stores (PostgreSQL commits, then Redis) become the limit.

### 5.3 Finding the bottleneck: the single entry point

With 1 gateway and 3 booking replicas, `docker stats` showed the **gateway at ~110% CPU while each
booking replica sat at ~33%**. Booking alone handled about 2,700 req/s when hit directly, but only about
600 req/s through one gateway. Lesson: **every tier on the request path must scale**, including the
gateway. That is why Compose runs nginx in front of several gateway replicas, and Kubernetes runs
2+ gateway pods behind a Service.

A second finding: the first version used the `httpx` client in both the gateway and the load tester.
Under high concurrency its connection pool became the bottleneck (the host-side load test managed only
57 req/s with p95 ≈ 7s while all services sat idle). Switching to `aiohttp` raised it to 299 req/s
with p95 ≈ 760ms. **Measure before blaming the server.**

### 5.4 Fault tolerance: killing a worker mid-sale

During a 1,500-request rush, with 63 orders in flight, `docker kill ticketrush-payment-worker-1` (SIGKILL).
Result: the surviving worker **reclaimed 36 unacknowledged orders** with XAUTOCLAIM. Final audit:
282 confirmed + 18 remaining = 300 capacity, **consistent, not oversold**, no stuck PENDING orders.

### 5.5 Kubernetes autoscaling (3-node kind cluster)

`kubectl get hpa` sampled every 10s during a 40,000-request sustained load:

| t | booking CPU / target | booking pods | worker CPU / target | worker pods |
|---|---|---|---|---|
| 10s | 2% / 60% | 3 | 1% / 70% | 2 |
| 30s | 26% / 60% | 3 | 64% / 70% | **8** |
| 40s | 181% / 60% | 3 | 451% / 70% | 8 |
| 60s | 183% / 60% | **7** | 459% / 70% | 8 |
| 70s | 162% / 60% | **10** | 436% / 70% | 8 (max) |
| 90s | 71% / 60% | 10 (max) | 442% / 70% | 8 (max) |

Both autoscalers reacted within about 30s and spread new pods across both worker nodes. Throughput did not
rise further because every pod still shared one laptop's CPU. **Autoscaling adds pods, not hardware.**
On a real multi-node cluster (or with the Cluster Autoscaler adding nodes) the new pods would add capacity.

**Bug found by this experiment:** in the first run the booking HPA showed `<unknown>` and never scaled.
Under load, `/healthz` answers took over 1s (the default probe timeout), so pods flapped to *unready*, and
the HPA ignores unready pods' metrics, so it stopped scaling exactly when it was needed. The fix was
a `readinessProbe` timeout of 3s and a much more lenient `livenessProbe` (5s × 6 failures), so a busy
pod is never restarted. We also added a `startupProbe` after seeing pods restarted by liveness checks
while they were still waiting for Redis to come up.

### 5.6 Correctness fix found during testing

The first worker version charged first and checked the order status afterwards. A buyer whose hold had
already expired (seats already resold) could therefore be *charged*, with the update then ignored.
The worker now checks that the order is still PENDING **before** charging, and keeps the guarded
update for the remaining race with the reaper.

## 6. Limitations and future work

* Redis and PostgreSQL are single instances. Next steps: Redis Sentinel, PostgreSQL streaming replication.
* A successful charge that loses the race with the reaper should trigger a **refund** (another saga step).
* Add idempotency keys on `POST /bookings` so a client retry after a timeout cannot create two orders.
* Use a **waiting room / token bucket** at the gateway to shed load fairly during extreme spikes.
* Distributed tracing (OpenTelemetry) across gateway → booking → worker.
* Shard inventory counters for a single huge event (e.g. 10 counters × 1/10 capacity) to spread Redis load.

## 7. Viva preparation: likely questions

1. **Why not lock the row in PostgreSQL instead of using Redis?** It works, but every booking for an
   event would queue on one row lock, so the DB becomes the serialisation point. A Redis Lua script is
   in-memory, microseconds long, and keeps PostgreSQL out of the contended path.
2. **Is Redis Lua really atomic?** Yes. Redis executes a script as a single command, and no other command
   runs in between. (In Redis Cluster all keys must hash to one slot; we would use `{event_id}` hash tags.)
3. **What if Redis and PostgreSQL disagree?** PostgreSQL is the source of truth. The audit detects drift,
   and the catalog rebuilds counters from it.
4. **Why 202 instead of 200?** The order is accepted but not yet complete. Payment finishes asynchronously
   and the client polls `GET /bookings/{id}`.
5. **At-least-once vs exactly-once?** Streams + ack-after-processing give at-least-once. Idempotent
   handlers give an exactly-once *effect*. True exactly-once delivery is not achievable in general.
6. **How do new replicas get traffic?** Kubernetes adds the pod to the Service endpoints once it is ready.
   The gateway uses a new connection per request, so it reaches the new pod immediately.
7. **How does the HPA decide?** `desired = ceil(current × currentCPU / targetCPU)`, using CPU relative to
   the pod's *request*. That's why requests are mandatory, and why unready pods break it (§5.5).
8. **How is this different from your FA project?** FA: fixed nodes, gRPC/P2P, per-node SQLite, failover.
   Here: elastic stateless replicas, a shared contended resource, asynchronous work queues, and
   orchestration with autoscaling.
9. **CAP theorem?** For inventory we choose consistency: if Redis is unreachable, booking fails (503)
   rather than risk overselling.
10. **What is the bottleneck now?** On one laptop, CPU. On a real cluster, PostgreSQL commit rate. See
    the future-work items on sharding and batching.

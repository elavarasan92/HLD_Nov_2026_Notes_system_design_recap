# Uber (Ride-Hailing) — System Design (Interview Guide)

> Design a ride-hailing platform where a rider enters pickup + destination, sees a **fare estimate**, requests a ride,
> and is **matched with a nearby available driver** who accepts and navigates to pickup — with **millions of drivers
> streaming GPS pings**, matching in **seconds**, and **never double-booking a driver**.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 3–4) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates (→ location-update write rate) |
| 3 | 2 min | Core entities |
| 4 | 5 min | API design (fare, ride request, location update, accept) |
| 5 | 10 min | High-level design — fare estimate, request, match, accept |
| 6 | 15–20 min | Deep dives: location ingest + proximity search, update frequency, driver locking, no dropped requests, driver timeouts |
| 7 | 5 min | Geo-sharding, trade-offs, wrap-up |

**Key line to say early:** *"The hard parts are **(1) ingesting ~2M driver location writes/sec and answering 'who is
near me?' fast** — a geospatial-index problem, not a relational one — and **(2) consistency in matching**: one driver
gets one ride request at a time, and every request gets processed even under surge or crashes."*

---

## 1. Requirements

### Functional (top 4)
1. Riders enter a start location and destination and get a **fare estimate** (price + ETA).
2. Riders **request a ride** based on that estimate.
3. On request, riders are **matched with a nearby, available driver**.
4. Drivers can **accept / decline** a request and navigate to pickup / drop-off.

### Below the line (mention, don't design)
- Ratings (rider ↔ driver), scheduled rides, ride categories (X, XL, Comfort), pooling.
- Payments (delegate to Stripe/Adyen + webhooks), receipts, tipping.
- Live trip tracking for the rider (falls out of the location pipeline + WebSocket), surge pricing internals.

### Non-Functional
| Property | Target |
|----------|--------|
| Matching latency | Match (or fail) in **< 1 min**; proximity query in **tens of ms** |
| Matching consistency | **Strong** — a driver never holds two ride requests / rides at once; a ride is offered to one driver at a time |
| Throughput | ~**2M location updates/s** globally; surges of **~100k requests from one area** (concert, stadium) |
| Availability | High for location ingest and fare estimate (AP); matching/ride state favors consistency (CP) |
| Durability | Ride requests are never silently dropped (crash of a matcher must not lose a ride) |

### Out of scope
GDPR/privacy internals, fraud, CI/CD, detailed monitoring (mention briefly).

---

## 2. Capacity Estimates

- **Riders:** 100M DAU, ~10M rides/day ≈ **~120 rides/s avg**, **~1–2k/s peak**.
- **Drivers:** ~10M drivers (a few M online at peak), location ping every **~5 s** →
  10M / 5 ≈ **2M writes/s** — this is the number that drives the design.
- **Location payload:** `driverId + lat + lng + ts (+ heading, speed)` ≈ **~100 B** → ~200 MB/s ingress.
- **Hot state:** only the **latest** location per driver matters for matching → 10M × ~100 B ≈ **~1 GB** → fits in
  memory (Redis cluster).
- **Ride/fare storage:** 10M rides/day × ~1 KB ≈ **~10 GB/day**, ~3.6 TB/yr → ordinary sharded DB.
- **API:** ~100M users × ~10 actions ≈ 1B req/day ≈ **~12k req/s** avg (excluding location pings).

> Rides and fares are a **small, transactional** workload; driver locations are a **huge, ephemeral** one. Store them
> differently.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **Rider** | id, name, contact, payment methods |
| **Driver** | id, name, vehicle (make/model/plate), status (`OFFLINE`, `AVAILABLE`, `IN_RIDE`), rating |
| **Fare** | id, rider_id, pickup, destination, price, eta, expires_at |
| **Ride** | id, rider_id, driver_id, fare_id, status, pickup, destination, route, final_price, timestamps |
| **Location** | driver_id, lat, lng, ts (latest only for matching; history optional for trip replay/analytics) |

---

## 4. API Design

```http
POST  /v1/fare                         # → Fare (price, eta)
POST  /v1/rides                        # body: { fareId } → Ride (status=REQUESTED)
POST  /v1/drivers/location             # body: { lat, lng }  (driver client, every few seconds)
PATCH /v1/rides/{rideId}               # driver: { action: "accept" | "decline" }  → Ride (pickup coords)
PATCH /v1/rides/{rideId}/status        # driver: PICKED_UP / COMPLETED
GET   /v1/rides/{rideId}               # rider/driver: current ride state
```

**Fare request**
```json
{ "pickup": { "lat": 37.7749, "lng": -122.4194 }, "destination": { "lat": 37.8044, "lng": -122.2712 } }
```
**Response:** `201 { "fareId": "f_123", "price": 2450, "currency": "USD", "etaSeconds": 420, "expiresAt": "..." }`

**Security notes (say this):**
- `riderId` / `driverId` come from the **JWT/session**, never the body.
- Timestamps are **server-generated**.
- The ride request carries only `fareId` — the **price is read from the DB**, never trusted from the client.
- Fares expire (e.g. 2–5 min) so stale quotes can't be redeemed after surge changes.

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **PostgreSQL** (or DynamoDB) | Riders, drivers, fares, rides | Transactional, modest volume, conditional updates for state transitions |
| **Redis (GEO)** | Latest driver location + geospatial index | 2M writes/s in memory, `GEOSEARCH` radius queries in ms, TTL drops stale drivers |
| **Redis** | Driver locks (`SET NX PX`) | Atomic, TTL-based reservation of a driver for a pending offer |
| **Kafka** (or SQS) | Ride-request queue, location stream for analytics | Durable, partitioned, replayable |
| **Durable workflow engine** (Temporal/Cadence) — optional | Matching orchestration with timeouts | Human-in-the-loop retries survive crashes |

```sql
CREATE TABLE fares (
    id           UUID PRIMARY KEY,
    rider_id     UUID NOT NULL,
    pickup       GEOGRAPHY(POINT) NOT NULL,
    destination  GEOGRAPHY(POINT) NOT NULL,
    price_cents  INT NOT NULL,
    eta_seconds  INT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE rides (
    id           UUID PRIMARY KEY,
    rider_id     UUID NOT NULL,
    driver_id    UUID,
    fare_id      UUID NOT NULL REFERENCES fares(id),
    status       VARCHAR(16) NOT NULL,   -- REQUESTED, MATCHING, ACCEPTED, PICKED_UP, COMPLETED, CANCELLED, FAILED
    version      INT NOT NULL DEFAULT 0, -- optimistic locking
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    accepted_at  TIMESTAMPTZ,
    picked_up_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
-- At most one active ride per driver — final guard against double assignment
CREATE UNIQUE INDEX uq_active_ride_per_driver ON rides (driver_id)
    WHERE status IN ('ACCEPTED', 'PICKED_UP');
```

---

## 6. High-Level Architecture

```
 ┌─────────────┐                 ┌──────────────┐        ┌──────────────────┐     ┌──────────────────┐
 │ Rider Client│── fare/ride ───▶│ API Gateway  │───────▶│ Ride Service     │────▶│ 3rd-party Maps   │
 └─────────────┘                 │ auth, rate   │        │ fare, ride state │     │ (route, ETA)     │
        ▲                        │ limit        │        └───┬─────────┬────┘     └──────────────────┘
        │ push (ride status)     └──────┬───────┘            │         │ enqueue ride request
        │                               │                    ▼         ▼
 ┌──────┴──────┐                        │             ┌───────────┐  ┌─────────────────┐
 │ Notification│◀───────────────┐       │             │ Postgres  │  │ Kafka / queue   │ (partitioned by geo)
 │ Service     │                │       │             │ fares,    │  └────────┬────────┘
 │ APNs / FCM  │                │       │             │ rides     │           ▼
 └──────┬──────┘                │       │             └───────────┘  ┌─────────────────────┐   ┌─────────────┐
        │ offer                 │       │                    ▲       │ Ride Matching       │──▶│ Redis lock  │
        ▼                       └───────┼────────────────────┼───────│ Service / workflow  │   │ driver:{id} │
 ┌─────────────┐                        │                    │       └────────┬────────────┘   └─────────────┘
 │Driver Client│── location (every 5s) ─┤                    │                │ GEOSEARCH nearby
 └─────────────┘── accept/decline ──────┘                    │                ▼
                                        │            ┌───────────────┐  ┌──────────────────┐
                                        └───────────▶│ Location Svc  │─▶│ Redis GEO        │
                                                     └───────────────┘  │ (sharded by city)│
                                                                        └──────────────────┘
```

### 6.1 Fare estimate
1. Rider `POST /fare` → Gateway (auth, rate limit) → **Ride Service**.
2. Ride Service calls the **Maps API** for distance/duration, applies the pricing model (base + per-km + per-min ×
   surge multiplier for the pickup cell).
3. Persists a `Fare` (with expiry) and returns it.

### 6.2 Request a ride
1. Rider `POST /rides { fareId }` → Ride Service validates the fare (owner, not expired).
2. Inserts `Ride(status=REQUESTED)` and **enqueues** a match request (durable) — returns immediately; the rider app
   waits for a push / WebSocket update.

### 6.3 Match a driver
1. Drivers continuously `POST /drivers/location` → **Location Service** → `GEOADD drivers:{city} lng lat driverId`
   (+ `last_seen` with TTL).
2. **Ride Matching Service** consumes the request, runs `GEOSEARCH` around the pickup (expanding radius), filters to
   `AVAILABLE` drivers, ranks (ETA, rating, acceptance rate, heading).
3. For the top candidate: **acquire the driver lock** (7.3), send an **offer** via Notification Service; wait ~10 s.

### 6.4 Accept / decline
1. Driver `PATCH /rides/{id} { accept }` → Ride Service does a **conditional update**
   (`status = MATCHING AND offered_driver = me`) → `ACCEPTED`, sets `driver_id`, marks driver `IN_RIDE`.
2. Returns pickup coordinates; driver navigates with on-device GPS. Rider is notified.
3. Decline / timeout → release lock, offer to the **next** candidate.

---

## 7. Deep Dives

### 7.1 Location ingest & proximity search (core problem)
Two problems: **2M writes/s** and **"drivers within R km"** queries — B-tree indexes on `lat`/`lng` are bad at 2-D
range queries, and a full scan is a non-starter.

| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** Postgres table `(driver_id, lat, lng)` + range query | `WHERE lat BETWEEN … AND lng BETWEEN …` | Write load melts the DB; 2-D range scan on B-trees is slow |
| **Good:** **PostGIS / quadtree index** with batched writes | Buffer pings, bulk-upsert every few seconds; GiST/quadtree index | Accurate queries, but quadtrees rebalance badly under constant movement and the write load is still heavy |
| **Great:** **Redis GEO (geohash in a sorted set)** | `GEOADD` per ping; `GEOSEARCH FROMLONLAT … BYRADIUS 3 km ASC COUNT 20` | In-memory, ~100k+ ops/s per shard, O(log N) updates; latest-only data so loss is acceptable (drivers re-ping in 5 s) |
| **Alt:** **H3 / S2 hexagonal cells** (what Uber uses) | `cell = h3(lat,lng,res)`; `SADD cell:{id} driverId`; query the k-ring of neighboring cells | Uniform neighbor distance, great for surge-by-cell too; slightly more app logic |

**Geohash vs quadtree in one breath:** geohash = fixed grid encoded into a sortable string (prefix ≈ proximity, cheap
updates, good for high churn); quadtree = adaptive tree that splits dense areas (great for static, uneven data like
businesses, costly to keep balanced with moving points). Moving drivers → **geohash/H3**.

**Stale drivers:** store `last_seen` with TTL (or a separate sorted set by timestamp) and periodically `ZREM` drivers
not seen for > 30 s, so offline phones aren't matched.

**Durability:** Redis is the *index*, not the source of truth for rides — on failover, the index rebuilds within one
ping interval. Use replicas + AOF if desired.

### 7.2 Reducing location-update load
- **Adaptive ping interval on the client:** fast when moving / near a pickup / in a dense area; slow (30–60 s) when
  stationary or `IN_RIDE` on a highway; stop when `OFFLINE`.
- **Distance threshold:** only send if moved > N meters or heading changed.
- **Batching/compression** on the client; one persistent connection (WebSocket/gRPC stream) instead of an HTTPS
  request per ping.
- Location Service is **stateless**; publish pings to **Kafka** for analytics/trip replay, update Redis on the hot path.

> Don't neglect the client — smart clients can cut server load several-fold without hurting match quality.

### 7.3 Never send a driver two requests at once (consistency)
Requirement: one ride is offered to **one** driver at a time, and a driver has **at most one** outstanding offer.
(Same shape as Ticketmaster seat reservation.)

| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** application-level check in one matcher instance | In-memory set of "offered" drivers | Breaks with >1 matcher instance; lost on crash |
| **Good:** status column + DB transaction | `UPDATE drivers SET status='OFFERED' WHERE id=? AND status='AVAILABLE'` | Correct, but needs a **cron to roll back** expired offers — offers stuck if the job lags |
| **Great:** **distributed lock with TTL** | `SET driver_lock:{id} {rideId} NX PX 10000` | Atomic, auto-expires after the 10 s acceptance window — no cleanup job; only the lock holder can be accepted |

- On **accept**: verify lock value = `rideId`, then the DB conditional update + partial unique index
  (`uq_active_ride_per_driver`) are the **final guard**.
- Lock holder is checked again at accept time — if the lock expired and was taken by another ride, the accept fails.
- Matching also filters out drivers whose lock key exists, so they aren't even considered.

### 7.4 No dropped requests under peak load / crashes
- Ride Service **enqueues** match requests into **Kafka/SQS** instead of calling the matcher synchronously.
- Partition the queue **by geography** (city / H3 region) → matchers scale per region; ordering per region.
- Matcher **commits the offset only after** the ride reaches a terminal matching state (or hands it to a durable
  workflow) → a crashed instance's work is redelivered.
- Idempotency: ride state transitions are conditional (`WHERE status = 'MATCHING'`), so a redelivered message can't
  double-assign.
- Backpressure: if supply is short, surface a longer ETA / surge price rather than letting the queue grow unbounded;
  fail the ride with a clear status after the 1-min SLA.

### 7.5 Driver doesn't respond (timeouts)
The matching loop is a **multi-step, human-in-the-loop process**: offer → wait 10 s → next driver → … → fail after N
tries / 1 min.

| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** `sleep(10s)` in a request thread | Blocks threads, lost on crash | No |
| **Good:** delayed message | Publish "check ride X" with 10 s delay (SQS delay queue / Redis ZSET by due time); if not accepted, offer next | Works, but the state machine is spread across handlers |
| **Great:** **durable workflow (Temporal / Cadence)** | Workflow per ride: `for driver in candidates: offer; await signal(accept) with timeout 10s` | Timers and state survive crashes; retries/visibility built in (Uber built Cadence for exactly this) |

### 7.6 Scaling & latency
- **Geo-sharding:** shard Redis GEO, matchers, and queues **by region/city** — rides and drivers are local, so
  queries rarely cross shards. Handle borders by querying neighboring shards (k-ring) when the pickup is near an edge.
- **Hot regions** (stadium after a concert): split a city into finer cells; scale matchers for that partition.
- **Read replicas** for ride lookups; ride DB sharded by `ride_id` (or region).
- **Multi-region deployment** with users routed to their home region; failover per region.
- Push channel: **WebSocket** gateway for driver/rider real-time updates (offers, driver location to rider), APNs/FCM
  as fallback when the app is backgrounded.

### 7.7 Surge pricing (if asked)
- Stream processor (Flink/Kafka Streams) aggregates **demand (ride requests) vs supply (available drivers)** per H3
  cell per minute → multiplier stored in Redis.
- Fare estimate reads the multiplier for the pickup cell; the **fare object locks in** the price until it expires.

---

## 8. Scaling Storage

| Component | Strategy |
|-----------|----------|
| Driver locations | Redis GEO / H3 sets sharded by city/region; in-memory; TTL for staleness; history to Kafka → S3/OLAP |
| Driver locks | Redis cluster, keys co-located by region; TTL = offer window |
| Rides & fares | Postgres sharded by region or `ride_id`; read replicas; archive completed rides to cold storage |
| Match queue | Kafka partitioned by region; consumer group per region |
| Location / Matching services | Stateless, autoscale on pings/s and queue lag |
| Real-time connections | WebSocket gateway fleet with sticky routing (connection registry in Redis) |

---

## 9. Observability
- **Metrics:** time-to-match p50/p99, match success rate, offer acceptance/timeout rate, location ingest rate & lag,
  `GEOSEARCH` latency, queue lag per region, stale-driver count.
- **Alerts:** match failure rate spike in a region, queue lag > SLA, Redis shard latency, ping volume drop (client or
  gateway issue).
- **Tracing:** request → enqueue → match → offer → accept, correlated by `rideId`.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Redis GEO / H3 in memory | PostGIS / quadtree on disk | 2M writes/s; latest-only, loss-tolerant data |
| Geohash/H3 cells | Quadtree | Cheap updates for constantly moving points |
| Distributed lock with TTL | DB status + cleanup cron | Auto-expiry matches the 10 s offer window; no stuck drivers |
| Queue between Ride & Matching | Synchronous call | Absorbs surges; crash-safe redelivery |
| Durable workflow for offers | Delayed messages / threads | Timeouts + retries survive crashes |
| Adaptive client pings | Fixed 5 s pings | Big cut in write load with no accuracy loss where it matters |
| Geo-sharding | Hash sharding by driver id | Proximity queries stay on one shard |
| Server-side fare lookup by `fareId` | Client sends price | Prevents price tampering |

---

## 11. Common Follow-up Questions

1. **Redis GEO node dies?** Promote a replica; worst case the index for that region is rebuilt within one ping interval
   (drivers keep pinging). Rides/locks of record are in the DB.
2. **Two riders want the same driver at the same instant?** Only one `SET NX` succeeds; the other matcher moves to its
   next candidate.
3. **Driver's lock expires just as they tap accept?** Accept checks lock ownership + conditional DB update; if lost,
   respond "request no longer available".
4. **Pickup near a shard/city border?** Query the neighboring cells/shards (k-ring) and merge results.
5. **How to show the driver's live location to the rider?** Location Service publishes the assigned driver's pings to
   the rider's WebSocket channel (pub/sub keyed by `rideId`).
6. **Why not broadcast the request to all nearby drivers?** Simpler, but multiple accepts race and drivers get spammed;
   sequential offers with locks give consistency and fairness (broadcast to a small batch is a valid latency trade-off
   if the first-accept-wins is enforced atomically).
7. **ETA accuracy?** Precompute road-network routing (contraction hierarchies) or use a Maps provider; correct with
   live traffic and historical speeds per segment.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clear API + entities, working fare → request → match → accept flow, recognizes need for a spatial index, at least a DB-status-based lock for drivers |
| **Senior (E5)** | Details ≥ 2 of: Redis GEO/geohash vs quadtree, TTL distributed lock, queue for requests; discusses sharding/replication and trade-offs |
| **Staff+ (E6)** | Drives 3+ deep dives proactively: adaptive client pings, durable workflow timeouts, geo-sharding & hot cells, surge, failure modes — brings real-world insight |

---

## 13. One-Minute Summary
> A **Ride Service** computes fare estimates via a Maps API, persists an expiring **Fare**, and on request creates a
> **Ride** and **enqueues** it (Kafka, partitioned by region). Drivers stream locations (adaptive interval) to a
> stateless **Location Service** that writes the latest position into **Redis GEO / H3 cells**, sharded by city. A
> **Ride Matching Service** (ideally a **durable workflow**) runs a radius search, ranks candidates, and offers the ride to
> one driver at a time, reserving them with a **Redis `SET NX` lock whose TTL equals the 10 s offer window**; timeouts
> or declines move to the next driver. Accept is a **conditional DB update** guarded by a unique "one active ride per
> driver" index. Notifications go over WebSocket/APNs/FCM.

---

### References
- Hello Interview, *Uber — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/uber
- Karan Pratap Singh, *System Design: Uber* — https://medium.com/@karan99/system-design-uber-33593137a4fe

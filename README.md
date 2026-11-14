# HLD_Nov_2026_Notes_system_design_recap

System design (HLD) interview notes. Each folder has a full guide; this page is the **index + 1-minute recap**.

## Index

| #  | Topic                                   | Open                                                                 | Core problem (one line)                          |
|----|-----------------------------------------|----------------------------------------------------------------------|--------------------------------------------------|
| 1  | API Gateway                             | [ApiGateway.md](ApiGateway.md)                       | Single front door: auth, rate limit, route       |
| 2  | Chat System (WhatsApp)                  | [WhatsappChat.md](WhatsappChat.md)                       | Real-time delivery + offline inbox, no loss      |
| 3  | Distributed Cache (Redis)               | [DistributedCache.md](DistributedCache.md)           | Consistent hashing, TTL + LRU, hot keys          |
| 4  | Distributed Task Scheduler              | [DistributedTaskScheduler.md](DistributedTaskScheduler.md) | Run once/cron jobs, no SPOF, no duplicates |
| 5  | File Storage & Sync (Dropbox)           | [FileStorage.md](FileStorage.md)                     | Chunked resumable upload + sync                  |
| 6  | Logger System                           | [Logger.md](Logger.md)                   | 10 s dedup + ingest + search at scale            |
| 7  | Movie Ticket Booking (BookMyShow)       | [MovieTicketBooking.md](MovieTicketBooking.md)     | Never double-book a seat                         |
| 8  | Multi-Tenant SaaS                       | [MultiTenantSaas.md](MultiTenantSaas.md)           | Tenant isolation, cheap onboarding               |
| 9  | News Feed                               | [NewsFeed.md](NewsFeed.md)                           | Fan-out on write vs read, celebrities            |
| 10 | Notification System                     | [NotificationService](NotificationService.md)       | Multi-channel, no loss/dup, prefs, rate limit    |
| 11 | Order Management System                 | [OrderManagementSystem.md](OrderManagementSystem.md) | Saga + outbox, no double charge/oversell     |
| 12 | Rate Limiter                            | [RateLimiter](RateLimiter.md)                     | Token bucket in Redis + Lua, return 429          |
| 13 | Uber (Ride-Hailing)                     | [Uber.md](Uber.md)                                     | Geo proximity search + never double-book driver  |
| 14 | URL Shortener (Bitly)                   | [URLShortener.md](URLShortener.md)                   | Unique short codes, ms redirects                 |
| 15 | YouTube                                 | [Youtube.md](Youtube.md)                               | Resumable upload, transcoding DAG, ABR + CDN     |

---

## Interview Flow (same for every question, 45–60 min)

1. **Requirements** (5 min) — top 3 functional, non-functional (scale, latency, consistency vs availability), "below the line".
2. **Capacity estimates** (2 min) — only the numbers that change the design (QPS, storage, read:write ratio).
3. **Core entities → API → data model** (10 min).
4. **High-level design** (10–15 min) — make each functional requirement work end to end, simple first.
5. **Deep dives** (15–20 min) — attack the non-functional requirements; name trade-offs.
6. **Wrap up** — bottlenecks, monitoring, failure modes.

---

## Quick Recap (for revision)

### 1. API Gateway
- Front desk analogy: client talks to gateway only; gateway does **TLS, authN, rate limit, validation, routing, response shaping**.
- Stateless → scale horizontally behind an L4/L7 LB; deploy multi-region; it must not be a SPOF.
- Retries only for **idempotent** requests; supports WebSocket/streaming passthrough.
- Don't put business logic in it. Examples: Kong, AWS API Gateway, Envoy, Spring Cloud Gateway.

### 2. Chat System (WhatsApp)
- **WebSocket** per device; chat server registry (Redis) says which server holds a user's socket; **Pub/Sub** between servers.
- **Persist first, then deliver**; per-device **inbox** + client **ack** → offline delivery and no loss.
- Messages in Cassandra/DynamoDB keyed by `chatId`, sorted by time-based ID (Snowflake) for ordering.
- Media via **pre-signed URL** to blob store; heartbeats to detect dead sockets; presence = last-seen in cache.

### 3. Distributed Cache (Redis / Memcached)
- Single node: **HashMap + doubly linked list** → O(1) get/set + LRU; TTL by lazy check + background sweep.
- Distribute with **consistent hashing** (virtual nodes) → minimal key movement on add/remove.
- **Replication** (primary + replicas, async) for HA; failover via sentinel/gossip.
- Hot keys → replicate key / local L1 cache / key suffixing; stampede → **single-flight lock / jittered TTL / early refresh**.
- Patterns: cache-aside (default), write-through, write-back.

### 4. Distributed Task Scheduler
- Jobs in **PostgreSQL** (`next_run_at` indexed); history in Cassandra.
- Scheduler polls due jobs → pushes to queue; **leader election** (ZooKeeper/etcd lease) or partition ownership avoids double scheduling.
- `SELECT … FOR UPDATE SKIP LOCKED` / conditional update for claiming; workers use **visibility timeout + heartbeats**.
- At-least-once + **idempotency key** = effectively once. Priority queues, jitter to avoid thundering herd.

### 5. File Storage & Sync (Dropbox / Drive)
- Split files into **chunks (~4–5 MB)**, fingerprint each (SHA-256) → **resumable, parallel, dedup'd** uploads.
- Client uploads directly to S3 via **pre-signed URLs**; metadata (file → chunk list, versions) in a DB.
- Download via **CDN**; sync = client polls/long-poll/WebSocket for change log since cursor, fetches only changed chunks.
- Conflicts → keep both / version history; share via ACL table.

### 6. Logger System
- Coding core (LC 359): `HashMap<message, nextAllowedTs>` → print if `ts >= next`; clean up old entries (queue / time buckets).
- Pipeline: **app lib (async, non-blocking, drop on overflow) → agent → Kafka → processors (dedup, parse) → Elasticsearch/Loki + S3**.
- Distributed dedup: partition Kafka by **hash(message)** so one processor owns the 10 s window (or Redis `SET NX EX 10`).
- Never slow the app (bounded buffers, backpressure); hot/warm/cold retention tiers; search by app, level, time, text.

### 7. Movie Ticket Booking (BookMyShow)
- Booking needs **ACID** → PostgreSQL. Core: **no double booking**.
- Seat **hold with TTL** (Redis `SET NX EX` or DB row `status=HELD, expires_at`) during checkout, then confirm on payment.
- Optimistic locking / conditional update `WHERE status='AVAILABLE'`; **idempotency key** for payments.
- Hot shows → **virtual waiting queue**; search via Elasticsearch; read-heavy pages cached / CDN.

### 8. Multi-Tenant SaaS
- Tenancy models: **shared tables + `tenant_id`** (cheap) → **schema per tenant** → **DB per tenant** (isolation, cost ↑). Usually hybrid (big tenants get own DB).
- Tenant context from JWT/subdomain → thread-local → Hibernate multi-tenancy or **PostgreSQL Row-Level Security**.
- Users in many orgs → org-scoped tokens + RBAC per org.
- Deep dives: seconds-level onboarding, migrations across thousands of schemas, **noisy neighbour** (per-tenant rate limits/quotas).

### 9. News Feed
- **Fan-out on write** (push post IDs to followers' precomputed feeds in Redis) → fast reads.
- **Celebrities** → fan-out on read, merge at read time (**hybrid**).
- Feed stores only post IDs → **hydrate** from post cache; cursor-based pagination.
- Hot posts → replicated cache; async fan-out via Kafka workers.

### 10. Notification System
- Internal services → API → **Kafka per channel/priority** → channel workers → APNs/FCM, SMS (Twilio), email (SES).
- Check **user preferences, quiet hours, rate limits**, render **templates** before sending.
- Persist before ack; retries with backoff + **DLQ**; **idempotency key** for dedup.
- Scheduled sends via scheduler; bulk campaigns → batch fan-out in workers; separate queues so OTPs aren't stuck behind marketing.

### 11. Order Management System
- State machine `PLACED → PAID → SHIPPED → DELIVERED` (+ CANCELLED).
- Cross-service txn → **Saga** (orchestrated) with **compensations** (release stock, refund).
- **Transactional outbox** → reliable events; **idempotency keys** → no double orders/charges.
- Inventory: reserve with conditional decrement; flash sales → Redis counters + queue; reads via **CQRS** view.

### 12. Rate Limiter
- Algorithms: fixed window, sliding log, sliding window counter, **token bucket (recommended)**, leaky bucket.
- Lives in the **API gateway**; keyed by user / IP / API key; state in **central Redis** updated atomically via **Lua script**.
- Return **429** + `Retry-After` / `X-RateLimit-*` headers.
- Scale: shard Redis by key; on Redis failure choose **fail-open** (availability) vs fail-closed; dynamic rules from config store.

### 13. Uber
- Drivers send GPS every ~5 s → **geo index** in Redis (GEO / geohash) or quadtree; not the main DB.
- Match: find nearby drivers → send request to one at a time with timeout.
- **Distributed lock with TTL** on driver (Redis) → never two requests at once.
- Ride requests via queue so none are dropped; fare estimate service; surge pricing per geohash cell.

### 14. URL Shortener
- Short code: **base62 of a global counter** (Redis INCR / ticket ranges per server) or hash + collision check.
- Redirect: **302** (keeps analytics) vs 301 (browser caches); cache hot codes in Redis + CDN.
- Read ≫ write → cache-heavy; key-value store scales easily.
- Custom aliases (unique check), expiry (TTL), analytics async via Kafka, abuse checks.

### 15. YouTube
- Upload: **multipart resumable** to S3 via pre-signed URLs; metadata in DB.
- Processing: **transcoding DAG** (split → transcode to multiple resolutions/codecs → package HLS/DASH segments + manifest).
- Watch: **adaptive bitrate streaming** from **CDN**; client picks quality per segment.
- Cost: popular videos on CDN, long tail in cheaper storage; DRM / signed URLs for protection.

---

## Common Building Blocks (reuse everywhere)

| Need                                 | Use                                                    |
|--------------------------------------|--------------------------------------------------------|
| Big files / media                    | Blob store (S3) + **pre-signed URLs** + **CDN**        |
| Low-latency reads                    | Redis cache (cache-aside), TTL, consistent hashing     |
| Async / decouple / buffer spikes     | Kafka / SQS, consumer groups, **DLQ**                  |
| No duplicates                        | **Idempotency key**, conditional writes, `SET NX`      |
| No double booking                    | Hold with TTL + optimistic lock / DB constraint        |
| Reliable events after DB write       | **Transactional outbox** / CDC                         |
| Cross-service transaction            | **Saga** with compensations                            |
| Real-time to clients                 | WebSocket / SSE / long polling                         |
| Full-text / geo search               | Elasticsearch, Redis GEO / quadtree / geohash          |
| Unique IDs                           | Snowflake / base62 counter ranges                      |
| Single active coordinator            | Leader election (ZooKeeper / etcd lease)               |
| Protect services                     | Rate limiting, circuit breaker, backpressure           |

# Movie Ticket Booking (BookMyShow / Fandango / Ticketmaster) — System Design (Interview Guide)

> Design a platform where users browse movies, search shows by city/theater/time, pick seats on an interactive seat
> map, and pay — **without ever double-booking a seat**, even when millions of fans hit a blockbuster release at once.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 3) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates |
| 3 | 2 min | Core entities |
| 4 | 5 min | API design (start simple, evolve later) |
| 5 | 10–15 min | High-level design — one functional requirement at a time |
| 6 | 15 min | Deep dives: seat reservation, double booking, hot shows, search, caching |
| 7 | 5 min | Scaling, trade-offs, wrap-up |

**Key line to say early:** *"This system is **read-heavy and availability-first for browsing/search**, but
**consistency-first for booking** — a seat must never be sold twice."*

---

## 1. Requirements

### Functional (top 3)
1. Users can **view** a movie/show (details, theater, showtimes, **seat map with availability**).
2. Users can **search** movies/shows by keyword, city, theater, date, language, genre.
3. Users can **book** one or more seats for a show (reserve → pay → confirm).

### Below the line (mention, don't design)
- View booking history / e-tickets, cancellations & refunds.
- Admin/theater partners add movies, screens, shows.
- Dynamic pricing, coupons, food & beverage add-ons, notifications.

### Non-Functional
| Property | Target |
|----------|--------|
| Consistency | **Strong for booking** — no double booking |
| Availability | **High for view/search** (AP is fine; slightly stale availability is OK) |
| Scale | Blockbuster release: ~10M users hitting one movie's shows |
| Search latency | < 500 ms p99 |
| Read/write ratio | ~100 : 1 (read-heavy) |
| Reservation hold | Seats held ~10 min during checkout, auto-released on abandonment |

### Out of scope
GDPR / PII compliance, payment security internals (delegate to PCI-compliant gateway), CI/CD, backups.

---

## 2. Capacity Estimates

- 50M DAU, ~20 page views each → **1B reads/day ≈ 12k RPS avg**, **100k+ RPS peak** on release days.
- Bookings: ~5M/day ≈ **60 TPS avg**, spikes to **several thousand TPS** when a blockbuster opens.
- Inventory: 10k theaters × 5 screens × 5 shows/day × 200 seats ≈ **50M show-seats/day** → ~100 B each ≈ **5 GB/day**
  (archive past shows).
- Seat map payload ≈ 20–50 KB → cache aggressively (CDN / Redis).

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **Movie** | id, title, description, genre, language, duration, cast, rating |
| **Theater** | id, name, city, address, geo |
| **Screen** | id, theater_id, name, **seat layout** (rows, seat numbers, type, coordinates) |
| **Show** | id, movie_id, screen_id, start_time, end_time, base price |
| **ShowSeat (Ticket)** | id, show_id, seat_id, price, **status** (AVAILABLE / RESERVED / BOOKED), booking_id, version |
| **Booking** | id, user_id, show_id, seat ids, total, **status** (IN_PROGRESS / CONFIRMED / EXPIRED / CANCELLED), payment ref |
| **User** | id, name, email, phone |

> One `ShowSeat` row is generated per seat when a show is created (from the screen's seat layout). The client renders
> the seat map from `Screen.layout` + `ShowSeat.status`.
>
> **Booking** is separate from ShowSeat so multiple seats share one order, total and payment status.

---

## 4. API Design

```http
GET  /v1/movies/{movieId}                                   # movie details
GET  /v1/shows/{showId}                                     # show details + seat map + availability
GET  /v1/search?q=&city=&date=&language=&genre=&page=       # search movies/shows
POST /v1/bookings                                           # reserve seats  -> bookingId (hold 10 min)
POST /v1/bookings/{bookingId}/confirm                       # pay with gateway token
GET  /v1/bookings/{bookingId}                               # status
POST /v1/payments/webhook                                   # payment gateway callback (Stripe/Razorpay)
```

**Reserve request**
```json
{ "showId": "s-123", "seatIds": ["A7", "A8"] }
```
Header: `Idempotency-Key: <uuid>`.

**Reserve response:** `201 { "bookingId": "b-789", "expiresAt": "2026-11-14T15:30:00Z", "total": 640 }`

> Start with a single `POST /bookings` (seats + payment), then **evolve** into reserve + confirm once you introduce the
> seat-hold deep dive. Saying this out loud shows you're iterating deliberately.

---

## 5. Data Model

### PostgreSQL (source of truth — needs ACID for booking)
```sql
CREATE TABLE shows (
    id          UUID PRIMARY KEY,
    movie_id    UUID NOT NULL,
    screen_id   UUID NOT NULL,
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ NOT NULL
);

CREATE TABLE show_seats (
    id          UUID PRIMARY KEY,
    show_id     UUID NOT NULL REFERENCES shows(id),
    seat_id     TEXT NOT NULL,                       -- e.g. "A7"
    price       NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'AVAILABLE',   -- AVAILABLE / RESERVED / BOOKED
    booking_id  UUID,
    reserved_until TIMESTAMPTZ,                      -- only for DB-hold variant
    version     BIGINT NOT NULL DEFAULT 0,           -- optimistic locking
    UNIQUE (show_id, seat_id)
);
CREATE INDEX idx_show_seats_show ON show_seats (show_id);

CREATE TABLE bookings (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL,
    show_id     UUID NOT NULL,
    total       NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL,                       -- IN_PROGRESS / CONFIRMED / EXPIRED / CANCELLED
    payment_ref TEXT UNIQUE,                         -- idempotent webhook handling
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **PostgreSQL** | Movies, theaters, shows, show_seats, bookings | ACID transactions, row locks / OCC to prevent double booking |
| **Redis** | Seat holds (TTL locks), hot seat-map & show cache, waiting-room queue | Sub-ms ops, `SET NX PX`, TTL auto-expiry |
| **Elasticsearch** | Movie/show search (full-text, filters, geo, fuzzy) | `LIKE '%x%'` forces full table scans |
| **CDN** | Posters, trailers, static & cacheable JSON | Offload massive read traffic |
| **Kafka / CDC (Debezium)** | Sync Postgres → Elasticsearch, booking events → notifications | Decoupled, replayable |

> **Shared DB across Event/Booking services is OK here** — data is tightly coupled and booking needs a transaction
> across seats + booking. Don't parrot "database per service" blindly; justify the trade-off.

---

## 6. High-Level Architecture

```
                 ┌──────────┐
   Client ─────▶ │   CDN    │  posters, static, cached show JSON
     │           └──────────┘
     ▼
┌──────────────┐      ┌──────────────────┐
│ Load Balancer│────▶ │   API Gateway    │  auth, rate-limit, routing
└──────────────┘      └───┬────┬────┬────┘
                          │    │    │
           ┌──────────────┘    │    └─────────────────┐
           ▼                   ▼                      ▼
   ┌───────────────┐   ┌───────────────┐      ┌────────────────┐     ┌───────────────┐
   │ Movie/Show    │   │ Search        │      │ Booking        │───▶ │ Payment GW    │
   │ Service       │   │ Service       │      │ Service        │◀─── │ (Stripe etc.) │
   └──────┬────────┘   └──────┬────────┘      └───┬───────┬────┘     └───────────────┘
          │                   │                   │       │ webhook
          ▼                   ▼                   ▼       ▼
   ┌─────────────┐    ┌───────────────┐   ┌────────────┐ ┌──────────────┐
   │ Redis cache │    │ Elasticsearch │   │ Redis seat │ │ PostgreSQL   │
   └──────┬──────┘    └───────▲───────┘   │ locks (TTL)│ │ (source of   │
          │                   │ CDC       └────────────┘ │  truth)      │
          └──────────────────▶│◀──────────────────────────┤              │
                              └───────── Kafka ◀──────────┘──────────────┘

   For hot releases:  Client ─▶ Virtual Waiting Room (Redis queue + WebSocket/SSE) ─▶ Booking
```

### 6.1 View a show
1. Client `GET /shows/{id}` → Gateway → **Show Service**.
2. Static parts (movie, theater, layout) from **Redis/CDN cache**; seat **availability** from Postgres (or a short-TTL
   cache) + Redis holds.
3. Return show details + seat map.

### 6.2 Search
1. Client `GET /search?...` → Gateway → **Search Service**.
2. Naive: SQL `WHERE title LIKE '%x%'` → full scan, too slow.
3. Better: **Elasticsearch** inverted index, synced from Postgres via **CDC**; supports fuzzy match, filters, geo
   ("theaters near me"), and ranking.

### 6.3 Book seats (final flow)
1. User selects seats → `POST /bookings {showId, seatIds}`.
2. Booking Service acquires a **Redis lock per seat**: `SET lock:{showId}:{seatId} {bookingId} NX PX 600000`
   (all-or-nothing via Lua for multi-seat).
3. Insert `bookings` row with `IN_PROGRESS`; return `bookingId` + `expiresAt`; client shows a **10-min countdown**.
4. User pays; client tokenizes card with the gateway SDK (**we never touch raw card data — PCI**).
5. Gateway calls our **webhook** → in a single DB transaction:
   ```sql
   UPDATE show_seats SET status='BOOKED', booking_id=$1
    WHERE show_id=$2 AND seat_id = ANY($3) AND status='AVAILABLE';   -- must update N rows
   UPDATE bookings SET status='CONFIRMED', payment_ref=$4 WHERE id=$1 AND status='IN_PROGRESS';
   ```
   If fewer than N rows updated → rollback + **refund**.
6. Release Redis locks. If the user abandons, the lock **TTL expires** and seats become available automatically.

---

## 7. Deep Dives

### 7.1 Preventing double booking (core problem)
| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** check then insert | `SELECT status` then `UPDATE` in separate steps | Race condition → double booking |
| **OK:** pessimistic lock | `SELECT ... FOR UPDATE` on seat rows inside a txn | Correct, but locks held during user payment = disaster |
| **Good:** OCC / conditional update | `UPDATE ... SET status='BOOKED', version=version+1 WHERE id=? AND status='AVAILABLE' AND version=?` | Correct, no long locks; loser gets a clean failure |
| **Good:** status + expiry + cron | `status='RESERVED', reserved_until=now()+10m`; cron/sweeper releases expired holds | Works, but sweeper lag leaves seats stuck; extra DB load |
| **Great:** Redis TTL lock + DB final check | Hold in Redis with TTL (auto-release); confirm with a conditional DB update | Fast holds, auto-expiry, DB remains final guard |

**Always keep the DB as the final guard** (`UNIQUE` + conditional update). If Redis fails or a lock expires mid-payment,
the DB still prevents a second sale; worst case → refund one user.

### 7.2 Seat reservation (hold) during checkout
- Without holds, users fill in payment and *then* learn the seat is gone → terrible UX.
- **Redis**: `SET lock:{show}:{seat} {bookingId} NX PX 600000`.
  - Multi-seat: Lua script — acquire all or none.
  - Release only if value == bookingId (avoid deleting someone else's lock).
- **Edge: payment succeeds after lock expired** → conditional DB update fails if someone else booked → auto-refund. Or
  extend the hold when the user enters the payment step.
- Seat map shows a seat as unavailable if `BOOKED` in DB **or** locked in Redis.

### 7.3 Idempotency
| Layer | Technique |
|-------|-----------|
| Reserve API | `Idempotency-Key` → return same `bookingId` on retry |
| Payment | Gateway `PaymentIntent` keyed by `bookingId` |
| Webhook | Gateways retry → use `bookingId` / `payment_ref` (UNIQUE) as idempotency key; only transition `IN_PROGRESS → CONFIRMED` |
| Notifications | Dedup on `bookingId` |

### 7.4 Scaling reads (view API under 10M users)
- **Cache** movie, theater, screen layout, show metadata in Redis (long TTL, invalidate on change via CDC).
- **CDN** for posters/trailers and cacheable show JSON.
- Seat availability changes fast → short TTL (1–5 s) or read-through + hold overlay.
- **Horizontal scaling** of stateless services behind the LB; Postgres **read replicas** for non-booking reads.

### 7.5 Hot shows: millions booking at once
Problem: seat map goes stale instantly; users click seats that are already gone, retry storms hammer the DB.

| Option | Notes |
|--------|-------|
| **Good:** real-time seat updates via **SSE / WebSocket** | Push seat status changes; but with 10M users the map is all red in seconds — doesn't fix the core UX |
| **Great:** **Virtual waiting room** | Admit users in batches (FIFO / randomized queue in Redis sorted set); push "your turn" via WebSocket/SSE; only admitted users get a booking token |

- Waiting room protects the Booking Service + DB and gives a fair, predictable UX.
- Admission rate tuned to available seats & system throughput.
- Combine with **per-user rate limits** and bot protection (CAPTCHA, device fingerprint).

### 7.6 Low-latency search
| Option | Notes |
|--------|-------|
| **Bad:** SQL `LIKE '%x%'` | Full table scan |
| **Good:** Postgres full-text (`tsvector` + GIN) | OK at moderate scale, single DB load |
| **Great:** **Elasticsearch** | Inverted index, fuzzy, filters, geo, relevance; sync via CDC/Kafka |

### 7.7 Caching repeated searches
- Cache popular query results in **Redis** (key = normalized query + filters, short TTL).
- Elasticsearch node query/request cache for repeated filters.
- **CDN edge caching** for identical anonymous search URLs (e.g., "movies in Mumbai today").

### 7.8 Show creation & inventory
- When a show is created, bulk-insert one `show_seats` row per seat from the screen layout.
- Theater partners integrated via API / sync (some theaters run their own POS) → seat inventory must reconcile;
  treat the external system as authority and lock via its API if required.

---

## 8. Scaling Storage

| Component | Strategy |
|-----------|----------|
| PostgreSQL | **Shard by `show_id`** (or city) — all seats of one show live together so a booking stays a single-shard txn; partition `show_seats` by show date; archive past shows |
| Redis | Cluster; lock keys hash-tagged by show (`lock:{showId}:seat`) so multi-seat Lua scripts hit one slot |
| Elasticsearch | Index per region/city or by time; replicas for read throughput |
| Services | Stateless, autoscale on RPS; separate scaling for read vs booking paths |

---

## 9. Observability
- **Metrics:** booking success/failure rate, lock-contention rate, hold-expiry rate, payment-success-but-seat-lost
  (refunds), search p99, cache hit ratio, waiting-room queue length & admission rate.
- **Alerts:** refund spike, DB lock waits, Redis latency, webhook failures.
- **Tracing:** propagate `bookingId` across Gateway → Booking → Payment → Webhook.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| CP for booking, AP for browse | One model for all | Double booking is unacceptable; slightly stale browse data is fine |
| Redis TTL hold + DB final check | DB `reserved_until` + sweeper | Instant auto-expiry, less DB load; DB still guarantees correctness |
| OCC conditional update | `SELECT FOR UPDATE` | No long-held locks; contention per seat is short |
| Shared Postgres for events + bookings | DB per service | Tightly coupled data + need for ACID transaction |
| Elasticsearch for search | SQL `LIKE` / Postgres FTS | Latency, fuzzy, geo, ranking at scale |
| Virtual waiting room | Real-time seat push only | Protects backend and gives fair UX under extreme demand |

---

## 11. Common Follow-up Questions

1. **Redis goes down mid-checkout?** DB conditional update still prevents double booking; holds are lost (UX hit),
   users may race at confirm time — loser is refunded.
2. **Payment succeeded but hold expired and seat was sold?** Conditional update fails → automatic refund + apology/
   alternative seats.
3. **User selects 4 seats, 1 is taken?** Lua all-or-nothing lock → reject and refresh map; optionally suggest
   adjacent seats.
4. **How to prevent bots/scalpers?** Waiting room, CAPTCHA, per-user/per-card limits, device fingerprinting.
5. **Cancellations/refunds?** Booking `CANCELLED` → seats back to `AVAILABLE` in the same txn → refund async via
   payment gateway; publish event to invalidate caches.
6. **Why not hold seats in DB with `FOR UPDATE` during payment?** Locks would be held for minutes → connection
   exhaustion and blocked readers.
7. **Multiple theaters in different time zones?** Store UTC; render in theater's local time zone.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clear APIs & data model, working view + book flow, solves double booking with status + timeout + sweeper |
| **Senior (E5)** | Moves fast through HLD; Elasticsearch for search, Redis distributed lock for holds, caching, sharding/replication, discusses hot shows |
| **Staff+ (E6)** | Drives 2–3 deep dives proactively (waiting room, consistency under failure, refunds, partner inventory sync) with real-world trade-offs |

---

## 13. One-Minute Summary
> Clients go through a CDN, load balancer and API Gateway to **Show**, **Search** and **Booking** services. Show
> metadata is cached in **Redis/CDN**; search runs on **Elasticsearch** synced from **PostgreSQL** via CDC. Booking
> is split into **reserve** and **confirm**: seats are held with a **Redis TTL lock** (auto-released after ~10 min),
> payment is handled by a PCI-compliant gateway, and an **idempotent webhook** finalizes the booking with a
> **conditional update in a Postgres transaction** — the DB is the final guard against double booking. For blockbuster
> releases a **virtual waiting room** admits users in batches to protect the system and keep things fair.

---

### References
- Evan King, *Ticketmaster — System Design Problem Breakdown*, Hello Interview —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/ticketmaster

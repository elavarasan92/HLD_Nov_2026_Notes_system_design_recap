# URL Shortener (Bitly / TinyURL) — System Design (Interview Guide)

> Design a service that turns a long URL into a short one (`https://sho.rt/aZ3k9Qx`) and **redirects** anyone who opens
> the short link to the original URL — with **unique** short codes, **millisecond** redirects, and a read load that
> dwarfs writes by orders of magnitude.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 2–3) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates (→ short-code length) |
| 3 | 2 min | Core entities |
| 4 | 5 min | API design (incl. 301 vs 302) |
| 5 | 10 min | High-level design — shorten flow, redirect flow |
| 6 | 15–20 min | Deep dives: unique short codes, fast redirects (caching), scaling, analytics, custom aliases/expiry |
| 7 | 5 min | Scaling, trade-offs, wrap-up |

**Key line to say early:** *"This is **extremely read-heavy** (~100–1000 reads per write). The hard parts are
**generating short, unique codes without collisions or coordination bottlenecks**, and **serving redirects in a few
ms** — which is mostly a caching problem."*

---

## 1. Requirements

### Functional (top 3)
1. Users can **create a short URL** from a long URL, optionally with a **custom alias** and **expiration time**.
2. Anyone opening a short URL is **redirected** to the original long URL.
3. Short codes are **unique** — one short code never maps to two different long URLs.

### Below the line (mention, don't design)
- Click analytics (counts, geo, referrer, device), dashboards.
- User accounts, link management (edit/delete), QR codes, link-in-bio pages.
- Malicious/phishing URL detection.

### Non-Functional
| Property | Target |
|----------|--------|
| Uniqueness | **Strong** — no two long URLs share a code |
| Redirect latency | **< 100 ms p99** end to end, ideally a few ms server-side |
| Availability | **99.99%** for redirects (AP for reads; brief staleness after create is OK) |
| Scale | ~1B stored URLs, **100M DAU**, read:write ≈ **100:1 – 1000:1** |
| Short code | As short as possible (≈ 6–8 chars), **not easily guessable** (nice-to-have) |
| Durability | Mappings must never be lost (a broken link is permanent damage) |

### Out of scope
Spam/abuse classification internals, billing, GDPR, CI/CD.

---

## 2. Capacity Estimates

- **Writes:** 100M new URLs/day ≈ **~1.2k/s avg**, a few k/s peak.
- **Reads:** 100:1 → 10B redirects/day ≈ **~115k/s avg**, **~500k+/s peak** (viral links).
- **Storage:** 100M/day × 365 × 10 yrs ≈ **365B records** worst case; at ~500 B each ≈ **~180 TB** over 10 years.
  (With 1B active URLs: ~500 GB — fits comfortably in a few sharded DB nodes.)
- **Short-code length** (Base62 `[0-9a-zA-Z]`):

  | Length | Combinations |
  |--------|-------------|
  | 6 | 62⁶ ≈ **56.8B** |
  | 7 | 62⁷ ≈ **3.5T** |
  | 8 | 62⁸ ≈ **218T** |

  → **7 chars** covers 365B URLs with lots of headroom.
- **Cache:** 80/20 rule — caching ~20% of daily hot URLs ≈ 20M × 500 B ≈ **10 GB** → fits in a Redis cluster easily.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **URL mapping** | short_code (PK), long_url, created_at, expires_at, user_id, is_custom |
| **User** | id, email, api_key, plan / quota |
| **Click (analytics)** | short_code, ts, ip/geo, user_agent, referrer |

> The mapping is a simple **key → value** lookup; that drives most storage and caching choices.

---

## 4. API Design

```http
POST   /v1/urls                 # create short URL
GET    /{shortCode}             # redirect (302 / 301)
GET    /v1/urls/{shortCode}     # metadata (owner only)
DELETE /v1/urls/{shortCode}     # delete / disable (owner only)
```

**Create request**
```json
{ "longUrl": "https://example.com/some/very/long/path?x=1", "customAlias": "summer-sale", "expiresAt": "2027-01-01T00:00:00Z" }
```
**Response:** `201 { "shortUrl": "https://sho.rt/summer-sale", "shortCode": "summer-sale" }`

**Redirect response**
```http
HTTP/1.1 302 Found
Location: https://example.com/some/very/long/path?x=1
```

### 301 vs 302
| Code | Behavior | Use when |
|------|----------|----------|
| **301 Moved Permanently** | Browser/CDN **caches** the redirect → later clicks never reach us | Minimizing server load matters most |
| **302 Found** (or 307) | Every click hits our service | We want **analytics**, ability to **edit/expire/disable** links |

> Default to **302** — analytics and control over expiry/abuse takedowns usually matter more. Mention the trade-off.

- Validate `longUrl` (scheme http/https, length limit, not pointing to our own domain → loops), check against a
  malicious-URL blocklist.
- Rate-limit creation per user/IP/API key.

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **PostgreSQL** (or DynamoDB / Cassandra) | `urls` mapping, users | Simple KV access; Postgres is fine up to billions of rows with sharding; NoSQL scales writes more easily |
| **Redis** | Hot `short_code → long_url` cache, global counter (`INCR`) | Sub-ms reads, atomic counters |
| **CDN / edge** | Optionally cache redirects at the edge | Viral links never hit origin |
| **Kafka → ClickHouse / BigQuery** | Click events for analytics | Async, append-only, OLAP queries |

```sql
CREATE TABLE urls (
    short_code  VARCHAR(16) PRIMARY KEY,
    long_url    TEXT NOT NULL,
    user_id     UUID,
    is_custom   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ
);
CREATE INDEX idx_urls_user ON urls (user_id);
-- Optional: dedup same long URL per user
-- CREATE UNIQUE INDEX idx_urls_user_long ON urls (user_id, md5(long_url));
```

> The `PRIMARY KEY` on `short_code` is the **final guard against collisions**, whatever generation strategy is used.

---

## 6. High-Level Architecture

```
                         ┌──────────────┐
  Browser ── GET /aZ3k ─▶│ CDN / Edge   │── miss ──┐
                         └──────────────┘          ▼
  Client ── POST /urls ──────────────────▶ ┌──────────────┐     ┌────────────────────┐
                                           │ Load Balancer│────▶│ API Gateway        │ auth, rate limit
                                           └──────────────┘     └───┬────────────┬───┘
                                                                    │            │
                                                         write path │            │ read path
                                                                    ▼            ▼
                                                   ┌──────────────────┐   ┌──────────────────┐
                                                   │ Write (Shorten)  │   │ Read (Redirect)  │
                                                   │ Service          │   │ Service          │
                                                   └───┬─────────┬────┘   └───┬─────────┬────┘
                                                       │         │            │ 1. cache │ 3. click event
                                       counter range   ▼         │            ▼          ▼
                                                ┌───────────┐    │     ┌───────────┐  ┌─────────┐
                                                │ Redis     │    │     │ Redis     │  │ Kafka   │──▶ Analytics
                                                │ INCR      │    │     │ cache     │  └─────────┘    (ClickHouse)
                                                └───────────┘    ▼     └─────┬─────┘
                                                         ┌──────────────────┐│ 2. miss
                                                         │ DB (sharded by   │◀┘
                                                         │ short_code)      │
                                                         │ + read replicas  │
                                                         └──────────────────┘
```

> Split **read** and **write** services — they have wildly different load profiles and scale independently.

### 6.1 Create a short URL
1. Client `POST /urls` → Gateway (auth, rate limit) → **Write Service**.
2. Validate the long URL. If `customAlias` given → try insert with that code (conflict → `409`).
3. Otherwise **generate a unique code** (see 7.1), insert `(short_code, long_url, ...)`.
4. Optionally warm the cache; return `201` with the short URL.

### 6.2 Redirect
1. Browser `GET /{code}` → CDN (if cached) → LB → **Read Service**.
2. Look up `code` in **Redis**; on miss read from DB (replica), populate cache (cache-aside).
3. Not found / expired → `404` (or `410 Gone`).
4. Return `302 Location: longUrl`; **asynchronously** emit a click event to Kafka (never block the redirect).

---

## 7. Deep Dives

### 7.1 Generating unique short codes (core problem)
| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** random prefix of the long URL / random string, no check | `random(7)` | Collisions grow (birthday paradox) → silently overwrites links |
| **OK:** **hash + collision resolution** | `base62(md5(longUrl))[0:7]`; on PK conflict, append salt/counter and retry | Same URL → same code (natural dedup), but extra DB round trips on collision; truncated hash collides more as table grows |
| **Good:** random 7-char Base62 + `INSERT ... ON CONFLICT` retry | Generate, insert, retry on conflict | Simple and unguessable; collision rate rises as keyspace fills (at 365B / 3.5T ≈ 10% → noticeable retries) |
| **Great:** **global counter + Base62 encoding** | `id = INCR url_counter` (Redis) → `base62(id)` | Guaranteed unique, no collision checks, short codes; counter is a single point → mitigate with **counter batching** |
| **Great (alt):** **Key Generation Service (KGS)** | Pre-generate random unique keys offline into a `keys` table; app servers fetch batches and mark them used | No runtime collisions, unguessable; extra service + key storage, must avoid handing the same key twice |

**Counter batching:** each Write Service instance grabs a range (`INCRBY url_counter 1000` → owns ids N-999..N) and
hands them out locally. Redis is hit once per 1000 URLs; an instance crash just "wastes" a few ids (fine).

**Predictability:** sequential ids make codes enumerable (`aZ3k9Qx`, `aZ3k9Qy`, ...). If that matters:
- Shuffle ids with a **bijective scramble** (e.g. multiply by a large prime mod 62⁷, or a Feistel cipher/Hashids)
  before Base62 → still unique, no longer sequential.
- Or use **Snowflake-style ids** (timestamp + machine id + sequence) — no central counter, but codes are longer
  (64-bit → ~11 Base62 chars).

```text
base62(n):
  alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  while n > 0: out = alphabet[n % 62] + out; n //= 62
  e.g. 11157 → "2TX"
```

### 7.2 Fast redirects
- **Redis cache-aside** for `short_code → long_url`; LRU eviction; TTL ≤ `expires_at`.
- Hit rate is very high (Zipf distribution — few links get most clicks).
- **CDN / edge** caching of the 302 for viral links (short `Cache-Control` max-age keeps analytics/expiry roughly
  accurate) — or run the redirect at the edge (edge KV store).
- DB **read replicas** for cache misses; reads tolerate replication lag (just-created links can be served from the
  primary or warmed into cache on create).
- Cache **negative results** briefly (`code → NOT_FOUND`) to blunt scans of random codes.

### 7.3 Scaling writes & storage
- 1.2k writes/s is modest — a single Postgres primary handles it; sharding is about **storage growth** and
  availability.
- **Shard by `short_code`** (hash) → redirect lookups hit exactly one shard.
- Counter is independent of the shard (Redis with AOF/replication; or per-shard counters with shard id in the code).
- If Redis counter state is lost → restart from `max(id)` in the DB (+ safety gap) to avoid reissuing ids.
- NoSQL (DynamoDB/Cassandra) is a natural fit: pure KV, horizontal scaling built in.

### 7.4 Custom aliases & expiration
- Custom alias: validate charset/length, reserved words (`api`, `admin`), insert with PK constraint → `409` on clash.
  Keep custom aliases from colliding with generated ones (e.g. different length, or a separate namespace).
- Expiration: check `expires_at` on read (return `410`); a background job **deletes/archives** expired rows; cache TTL
  never outlives `expires_at`.
- Should expired codes be **reused**? Usually **no** — old links in the wild would point to new content.

### 7.5 Same long URL shortened twice?
| Option | Notes |
|--------|-------|
| Always new code | Simplest; allows per-user/per-campaign analytics |
| Return existing code | Index on `hash(long_url)` (per user); saves space, but shared analytics |

> Pick per product requirement; Bitly-style products usually create a new code per user/campaign.

### 7.6 Analytics without slowing redirects
- Read Service publishes `{code, ts, ip, ua, referrer}` to **Kafka** fire-and-forget (or buffered in-memory batch).
- Stream processor aggregates (per code/minute/country) into **ClickHouse / Druid**; raw events to S3.
- Real-time counters, if needed: Redis `INCR clicks:{code}` (approximate is fine).
- Note: with **301**, most clicks never reach us → analytics impossible; another reason for 302.

### 7.7 Abuse & security
- Rate limit creation per user/IP; CAPTCHA for anonymous users.
- Check long URLs against Safe Browsing / internal blocklists on create **and** periodically; disable links centrally
  (302 makes takedown effective immediately).
- Prevent redirect loops (long URL on our own domain).
- Non-sequential codes to prevent enumeration of private links.

---

## 8. Scaling Storage

| Component | Strategy |
|-----------|----------|
| DB | Shard by hash(`short_code`); read replicas; archive expired/cold links |
| Redis cache | Cluster, LRU, ~10s of GB; replicas for availability |
| Redis counter | Replicated + AOF; range batching keeps load tiny; recover from DB max id |
| Read Service | Stateless, autoscale on RPS; multi-region with geo-DNS, edge redirects |
| Analytics | Kafka partitions by `short_code`; OLAP store scaled independently |

---

## 9. Observability
- **Metrics:** redirect p50/p99, cache hit ratio, 404/410 rate, create rate, collision/retry rate, counter range
  fetches, Kafka lag for click events.
- **Alerts:** cache hit ratio drop, DB latency, spikes in 404s (enumeration/scanning), abuse reports.
- **Tracing:** sample redirects end to end (edge → service → cache/DB).

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| 302 redirect | 301 | Analytics, expiry, takedown control; cost = more traffic (mitigated by cache/CDN) |
| Counter + Base62 (batched) | Hash + collision retry | Guaranteed uniqueness, no retries, shortest codes |
| Scrambled ids / KGS | Plain sequential ids | Codes not guessable/enumerable |
| Redis cache-aside + CDN | DB only | Read-heavy, Zipf-distributed traffic |
| Separate read/write services | One service | Independent scaling for 100:1+ load ratio |
| Shard by `short_code` | Shard by user | Redirect (the hot path) is always a single-shard lookup |
| Async click analytics | Synchronous write per click | Keeps redirect latency in ms |

---

## 11. Common Follow-up Questions

1. **Redis counter dies?** Use replicated Redis with persistence; on failover, bump counter beyond DB `max(id)`. Ranges
   already handed out keep working. Alternatively, Snowflake ids need no central counter.
2. **How short can codes be?** 7 Base62 chars ≈ 3.5T codes — enough for decades at 100M/day.
3. **Why not just MD5 the URL?** 128 bits is too long; truncating to 7 chars causes collisions that need checks and
   retries.
4. **How to handle a viral link with millions of clicks/sec?** CDN/edge caching of the redirect + in-process LRU on Read
   Service instances; the DB never sees it.
5. **Multi-region?** Geo-DNS to the nearest region; reads served from regional cache/replicas; writes to a primary
   region (or per-region counter ranges / region prefix in ids so writes are local).
6. **User deletes a link?** Delete row + invalidate cache (+ CDN purge); 302 means it takes effect quickly.
7. **Can short codes be case-insensitive?** Then Base36 → need ~8 chars for the same keyspace; trade-off for
   readability (e.g. printed links).

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clean API, working create + redirect, hash or counter-based codes with collision handling, basic caching |
| **Senior (E5)** | Justifies 301 vs 302, counter + Base62 with batching (or KGS), Redis cache + replicas, sharding by code, async analytics |
| **Staff+ (E6)** | Drives deep dives: predictability vs uniqueness, counter failure recovery, edge redirects & multi-region, abuse handling, cost/latency trade-offs |

---

## 13. One-Minute Summary
> A **Write Service** validates long URLs and creates short codes by taking ids from a **Redis counter in batches** and
> **Base62-encoding** them (optionally scrambled to avoid enumeration); the DB `PRIMARY KEY` on `short_code` is the final
> uniqueness guard, and custom aliases just insert-or-409. A separate **Read Service** handles redirects with **Redis
> cache-aside**, DB **read replicas** and **CDN/edge caching** for viral links, returning **302** so we keep analytics
> and can expire or take down links. Clicks are streamed asynchronously via **Kafka** to an OLAP store. The DB is
> **sharded by short_code**, so every redirect is a single-key, single-shard lookup.

---

### References
- ByteByteGo, *Design a URL Shortener* —
  https://bytebytego.com/courses/system-design-interview/design-a-url-shortener
- Hello Interview, *Bitly — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/bitly

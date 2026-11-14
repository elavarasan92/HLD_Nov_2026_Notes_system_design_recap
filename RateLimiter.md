# Distributed Rate Limiter — System Design (Interview Guide)

> Design a server-side rate limiter that caps how many requests a client (user / IP / API key) can make in a time
> window — e.g., *100 requests/min per user* — and rejects the excess with **HTTP 429**. It must work across many
> gateway instances, add almost no latency, and stay up under attack.

**Why rate limit?** Prevent DoS / abuse and resource starvation, control cost (especially paid third-party APIs),
protect backends from overload, and enforce fair usage / pricing tiers.

Real-world examples: Twitter ~300 tweets / 3 h, Google Docs API 300 reads / user / 60 s, Stripe & AWS API Gateway use
token bucket, Shopify uses leaky bucket.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Clarify: client vs server side, identify by what, scale, distributed?, inform users? |
| 2 | 2 min | Back-of-the-envelope estimates |
| 3 | 3 min | Core entities + interface |
| 4 | 5 min | Placement (in-service / middleware / API gateway) + client identification |
| 5 | 10 min | Algorithms — compare 5, pick one (Token Bucket) |
| 6 | 5 min | Central state in Redis + atomicity (Lua) + 429 response |
| 7 | 15 min | Deep dives: scale (sharding), HA (fail open/closed), latency, hot keys, dynamic rules |
| 8 | 5 min | Monitoring, trade-offs, wrap-up |

**Key line to say early:** *"Rate limiting must be enforced **server-side** — clients can't be trusted. I'll put it
in the **API gateway**, keep counters in a **shared Redis cluster**, and make each check a **single atomic Lua
script**. Slight inconsistency across nodes is acceptable; added latency is not."*

> Interviewers ask this question differently — some want low-level design/code of the algorithm, others want the
> distributed architecture. **Ask** which one they care about.

---

## 1. Requirements

### Clarifying questions
| Question | Typical answer |
|----------|----------------|
| Client-side or server-side? | Server-side |
| Throttle by IP, user ID, API key? | Flexible, configurable rules (combinations) |
| Scale? | Large — ~1M req/s, ~100M DAU |
| Distributed environment? | Yes, many gateway instances |
| Separate service or in app code? | Your call |
| Inform throttled users? | Yes (429 + headers) |

### Functional
1. **Identify clients** by user ID, IP address, or API key (and combinations).
2. **Limit requests** using **configurable rules** (per user, per IP, per endpoint, global).
3. **Reject** excess with **HTTP 429** + helpful headers (limit, remaining, reset / retry-after).

### Non-Functional
| Property | Target |
|----------|--------|
| Latency overhead | < 10 ms per check (ideally ~1 ms) |
| Throughput | 1M requests/sec, 100M DAU |
| Availability | High; limiter failure must not take down the API |
| Consistency | **Eventual** is OK — slight over/under-limiting across nodes acceptable |
| Memory | As small as possible |
| Accuracy | Limit excessive requests reliably |

### Below the line
- Analytics / complex queries on rate-limit data, long-term persistence.
- Strong consistency across all nodes / regions.

---

## 2. Capacity Estimates

- 1M req/s; each check = 1 Redis round trip (Lua script) → **~1M Redis ops/s**.
- One Redis node ≈ 100–200k simple ops/s → realistically ~50–100k rate-limit checks/s →
  **~10–20 shards** (+ replicas).
- State per client per rule (token bucket): `tokens + last_refill` ≈ ~50–100 B with key overhead.
  100M users × 2 rules × 100 B ≈ **~20 GB** (only active keys live thanks to TTL) — fits a Redis cluster.
- Sliding window *log* would need one entry per request → 1000 req/min × millions of users → **far too much memory**.

---

## 3. Core Entities & Interface

| Entity | Description |
|--------|-------------|
| **Rule** | Policy: scope (user / IP / API key / endpoint / global), limit, window or bucket size + refill rate, tier |
| **Client** | The thing being limited: `user:alice`, `ip:1.2.3.4`, `key:abc123` |
| **Request** | Carries client identity, endpoint, timestamp → determines which rules apply |

**Interface** (internal, called by gateway/middleware):
```text
isRequestAllowed(clientId, ruleId) -> { allowed: bool, limit, remaining, resetAt }
```

**Rule config** (Lyft-ratelimit style YAML):
```yaml
domain: api
descriptors:
  - key: user_tier
    value: free
    rate_limit: { unit: minute, requests_per_unit: 100 }
  - key: endpoint
    value: /v1/search
    rate_limit: { unit: minute, requests_per_unit: 10 }
  - key: auth_type
    value: login
    rate_limit: { unit: minute, requests_per_unit: 5 }
```

**429 response**
```http
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1763113800
Retry-After: 30
Content-Type: application/json

{ "error": "rate_limit_exceeded", "message": "Try again in 30 seconds" }
```
Headers are also sent on **successful** responses so well-behaved clients can self-throttle.

---

## 4. Where to Place the Rate Limiter

| Option | Pros | Cons |
|--------|------|------|
| **Client-side** | Reduces load from good clients | Can be forged/bypassed — **never** the security layer |
| **In-process (each service)** | No extra hop; full context (user plan, business data) | Each instance sees only its slice of traffic → needs shared state anyway; duplicated in every service/language |
| **Dedicated rate-limit service** | Centralized logic, any service can call it | Extra network hop per request; becomes critical dependency |
| **API Gateway / middleware** ✅ | Central, protects all backends, already does auth/TLS/routing; managed options (AWS API GW, Kong, Envoy) | Only sees request-level info (headers, path, IP); limited to gateway's algorithms if third-party |

**Choose: API Gateway** (with shared Redis). Guidelines: fits existing microservice + gateway stack; build in-house only
if you need custom algorithms and have engineering capacity; otherwise use a commercial gateway.

### Identifying clients (from the request only — avoid extra lookups)
| Key | Source | Watch out |
|-----|--------|-----------|
| **User ID** | JWT in `Authorization` header | Only for authenticated traffic |
| **IP** | `X-Forwarded-For` / socket | NAT / corporate proxies share IPs → set higher IP limits |
| **API key** | `X-API-Key` | Developer APIs, per-plan quotas |

**Layer multiple rules** and enforce the **most restrictive**:
- Per user: 1000 req/h · Per IP: 100 req/min · Per endpoint: search 10/min · Global: 50k req/s.
- Alice under her user quota but her IP is over → **blocked**.

---

## 5. Rate Limiting Algorithms

| Algorithm | How it works | Pros | Cons | Used by |
|-----------|--------------|------|------|---------|
| **Token bucket** ✅ | Bucket of capacity `B`; refilled at `r` tokens/s; each request takes 1 token; empty → reject | Simple, O(1) memory (2 values), **allows bursts** up to `B` while enforcing average rate | Tuning `B` and `r`; idle clients start with full bucket | Amazon, Stripe |
| **Leaky bucket** | FIFO queue of size `B`; processed at fixed outflow rate; full → drop | Smooth, constant output rate; bounded memory | Bursts fill queue with old requests → new ones dropped; adds latency; 2 params to tune | Shopify |
| **Fixed window counter** | Counter per fixed window (e.g., per minute), reset at boundary | Simplest, tiny memory, quota resets at human-friendly times | **Boundary burst**: 100 at 12:00:59 + 100 at 12:01:00 = 2× limit in 2 s | — |
| **Sliding window log** | Store timestamp of every request (Redis ZSET); drop old ones; count | **Exact** for any rolling window | **Memory heavy** (1 entry per request, even rejected ones); O(log n) per op | — |
| **Sliding window counter** | `current + previous × overlap%` (e.g., 30% into minute → `cur + prev×0.7`) | Good accuracy, 2 counters of memory, smooths spikes | Approximation (assumes even distribution) — Cloudflare measured only ~0.003% wrong decisions | Cloudflare |

### Recommendation: **Token Bucket**
Best balance of simplicity, memory, and real-world bursty API traffic. Mention sliding window counter as the
alternative when you need stricter rolling-window accuracy.

**How many buckets?** One per (client × rule): e.g., 1 post/s + 150 friend-adds/day + 5 likes/s = 3 buckets per user;
one per IP for IP rules; one global bucket for a system-wide cap.

### Sliding window counter worked example
Limit 7/min; previous minute had 5, current has 3; now 30% into current minute →
`3 + 5 × 0.7 = 6.5` → round down to 6 → **allowed** (next one would hit the limit).

---

## 6. High-Level Architecture

```
                         ┌─────────────────────────┐
  Clients ──▶ DNS/CDN ─▶ │  Load Balancer (L4/L7)  │   (DDoS protection: Cloudflare / AWS Shield upstream)
                         └────────────┬────────────┘
                                      ▼
                ┌─────────────────────────────────────────┐
                │   API Gateway fleet (stateless)         │
                │   ┌──────────────────────────────────┐  │
                │   │ Rate-limit middleware             │  │
                │   │  1. extract clientId (JWT/IP/key) │  │
                │   │  2. match rules (local cache)     │  │
                │   │  3. EVALSHA token_bucket.lua      │──┼──────▶ ┌──────────────────────────┐
                │   │  4. allow → forward / deny → 429  │  │        │ Redis Cluster            │
                │   └──────────────────────────────────┘  │ ◀───── │ (sharded by client key,  │
                └───────────────┬─────────────┬───────────┘        │  master + replicas,      │
                                │ allowed     │ denied             │  16,384 hash slots)      │
                                ▼             ▼                    └──────────────────────────┘
                       ┌────────────────┐  429 + headers
                       │  API Servers   │  (optionally enqueue to MQ for later processing)
                       └────────────────┘

   ┌────────────────────────┐   push/poll   ┌──────────────────────────────┐
   │ Rules store            │ ────────────▶ │ Gateway in-memory rule cache │
   │ (etcd/ZooKeeper/DB/S3) │               └──────────────────────────────┘
   └────────────────────────┘
   Metrics/logs ─▶ Prometheus / Grafana / alerts
```

### Request flow
1. Request hits a gateway instance (LB spreads traffic — any instance may see any client).
2. Middleware extracts `clientId` and loads applicable **rules from local cache**.
3. Runs **one atomic Lua script** on the Redis shard that owns the key.
4. Allowed → forward to API servers (with rate-limit headers). Denied → **429** + headers (drop, or enqueue if the use
   case tolerates delayed processing, e.g., order intake).

### Why centralized Redis (not local memory)?
If Alice sends 50 requests to Gateway A and 50 to Gateway B, local counters each see 50 → both allow → she gets 2× her
limit. **Sticky sessions** fix it but are neither scalable nor flexible. **Shared Redis** = single source of truth.

Why Redis over a DB: in-memory (sub-ms), built-in `INCR`, `EXPIRE` (auto-cleanup), Lua atomicity, replication.

---

## 7. Making It Correct: Race Conditions & Atomicity

**The race:** read counter (3) → check → write (4). Two concurrent requests both read 3 and both write 4 — two requests
consumed, counter advanced by one, limit exceeded.

- `MULTI/EXEC` alone **doesn't fix it** if the read (`HMGET`) happens outside the transaction.
- Locks fix it but are **slow**.
- ✅ **Lua script** — Redis executes it atomically (single-threaded): read → refill → decide → write in one step.
- Alternative for sliding-log: **ZSET** ops (`ZREMRANGEBYSCORE` + `ZADD` + `ZCARD`) inside `MULTI` / Lua.

### Token bucket Lua script
```lua
-- KEYS[1] = bucket key, e.g. "rl:{user:alice}:api"
-- ARGV[1] = capacity, ARGV[2] = refill_rate (tokens/sec), ARGV[3] = now (ms), ARGV[4] = cost
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts     = tonumber(state[2]) or now

tokens = math.min(capacity, tokens + (math.max(0, now - ts) / 1000) * rate)

local allowed = 0
if tokens >= cost then
  tokens  = tokens - cost
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) * 2)   -- idle buckets disappear
return { allowed, math.floor(tokens) }
```
Load once with `SCRIPT LOAD`, call with `EVALSHA` (sends a hash, not the script, per request).

> Use **Redis server time** (`redis.call('TIME')`) or NTP-synced gateways — clock skew between gateways otherwise
> skews refills.

### Fixed window (simplest, if asked)
```text
INCR  rl:{user:alice}:202611141519
EXPIRE rl:{user:alice}:202611141519 60   (only when INCR returned 1)
allow if value <= limit
```

---

## 8. Deep Dives

### 8.1 Scaling to 1M req/s — shard Redis
- Single Redis ≈ 50–100k checks/s → need ~10+ shards.
- **Shard by client key** so all of a client's state lives on **one** shard (user ID / IP / API key hashed).
- Use **consistent hashing** — or simply **Redis Cluster** (16,384 hash slots, automatic routing & resharding).
- Use **hash tags** (`rl:{user:alice}:...`) so multiple rules for one client land on the same slot if a single script
  must touch them together.
- Gateways are stateless → scale horizontally behind LB.

### 8.2 High availability & fault tolerance
**When a Redis shard is down, choose a failure mode:**

| Mode | Behavior | When |
|------|----------|------|
| **Fail-open** | Allow all requests | Limiter is mainly for fairness; backend can absorb load; availability > protection |
| **Fail-closed** | Reject (429/503) | Limiter protects fragile backends; failures often coincide with traffic spikes (viral event) → avoid cascading collapse |
| **Degrade to local** | Fall back to in-memory per-gateway limiter (limit ÷ N gateways) | Best of both — approximate protection without a global dependency |

Hello Interview picks **fail-closed** for a social platform; ByteByteGo stresses the limiter must not take down the
whole system. **Say it depends — and mention the local-fallback hybrid.**

**Prevent failures:** master-replica per shard with automatic failover (Redis Cluster / Sentinel); **short timeouts**
(e.g., 5–10 ms) + **circuit breaker** around Redis calls; multi-AZ deployment. Accept that async replication may lose a
few increments on failover — fine for rate limiting.

### 8.3 Minimizing latency
- **Connection pooling** — persistent connections, no TCP/TLS handshake per check (biggest easy win).
- **Single round trip** — one `EVALSHA` per check (Lua), pipelining when checking multiple rules.
- **Geo-distribution / edge** — gateways + Redis per region (Cloudflare runs limiters at 190+ edge PoPs). Cross-region
  sync is **eventually consistent**; options: per-region quotas (global limit ÷ regions) or async counter aggregation.
- **Local pre-check / batching** (advanced): gateway keeps a small local token allowance and syncs with Redis
  periodically — fewer round trips, slight inaccuracy. Mention only if asked.

### 8.4 Hot keys (one client flooding a shard)
| Scenario | Mitigation |
|----------|------------|
| **Legitimate** high-volume (pipelines, aggressive mobile refresh) | Client-side rate limiting in SDKs honoring headers; batch endpoints; premium tiers / dedicated capacity |
| **Abusive** (bots, DDoS) | **Auto-blocklist** after repeated violations (e.g., 10× in 1 min → block for 15 min), checked in local gateway cache; **edge DDoS protection** (Cloudflare, AWS Shield, WAF) before traffic reaches the limiter; L3/L4 limits (iptables) |
| Shared IPs (NAT, campus Wi-Fi) | Higher IP limits; prefer authenticated user limits |

### 8.5 Dynamic rule configuration
| Approach | How | Trade-off |
|----------|-----|-----------|
| **Poll** | Workers / gateways periodically pull rules from DB/S3/disk into local cache (ByteByteGo's design) | Simple; changes take up to one poll interval |
| **Push** | Rules in **etcd / ZooKeeper / Consul**; gateways watch and update instantly | Near real-time; extra infra |

Rules are versioned, validated, and rolled out gradually (canary) — a bad rule can block all traffic.

### 8.6 Handling rejected requests
- **Fail fast (default)**: return 429 immediately — predictable latency, no resource buildup.
- **Queue for later**: only for async/batch work (e.g., orders during overload) via a message queue. For interactive
  APIs, queuing causes unpredictable latency and retry storms.
- **Hard vs soft limit**: hard = never exceed; soft = allow short bursts over the limit (token bucket capacity is a
  form of soft limiting).

---

## 9. Observability
- **Metrics:** allowed vs throttled per rule/endpoint/tier, Redis latency p99, Redis errors & timeouts, fail-open
  events, hot keys, blocklist size.
- **Tune rules from data:** too many valid requests dropped → relax rule; limiter ineffective during flash sales →
  switch to burst-friendly algorithm (token bucket).
- **Alerts:** spike in 429s, Redis shard down, fail-open mode entered, latency SLO breach.
- **Shadow mode:** new rules run in "log only" before enforcing.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Server-side at API gateway | Client-side / in-service | Can't trust clients; centralized, protects all backends |
| Token bucket | Sliding window log / counter | O(1) memory, burst-friendly, industry standard |
| Central Redis | Local in-memory / sticky sessions | Consistent view across gateways; stateless gateways |
| Lua script | Locks / MULTI-EXEC with outside read | Atomic read-modify-write, single round trip, no locks |
| Redis Cluster sharding by client | Single Redis | 1M req/s exceeds one node |
| Eventual consistency (multi-region) | Global strong consistency | Latency matters more than exact counts |
| Fail-closed (or local fallback) | Fail-open | Protect backends when it matters most — but it depends |

---

## 11. Common Follow-up Questions

1. **Why not store counters in a database?** Disk latency; rate-limit state is ephemeral and needs sub-ms ops + TTL.
2. **Clock skew between gateways?** Use Redis server time inside the Lua script, or NTP.
3. **Limit at other layers?** Yes — L3/L4 via iptables / cloud firewall / DDoS services; L7 at gateway; per-service
   concurrency limits (bulkheads) deeper.
4. **Different limits per pricing tier?** Tier comes from JWT claim / API key metadata → selects rule.
5. **Cost-based limiting?** Token bucket `cost` param: an expensive endpoint consumes 10 tokens, cheap one 1.
6. **How should clients avoid being throttled?** Cache responses, respect `Retry-After`, exponential backoff with
   jitter, batch requests, handle 429 gracefully.
7. **Multi-region global limit?** Split quota per region, or async-aggregate counts and accept small overshoot.
8. **Concurrent request limiting (not rate)?** Stripe-style "concurrent requests limiter": counter of in-flight
   requests, increment on start, decrement on finish (with TTL safety).

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Explains one algorithm well (token bucket), places limiter at gateway, Redis as shared state, recognizes need to shard when asked |
| **Senior (E5)** | Compares algorithms with trade-offs, atomicity (Lua / MULTI-EXEC), Redis Cluster / consistent hashing, connection pooling, fail-open vs fail-closed, hot keys, dynamic config |
| **Staff+ (E6)** | Drives production concerns unprompted: multi-region consistency, local fallback, shadow-mode/canary rule rollouts, observability, layered defense (edge + gateway + service) |

---

## 13. One-Minute Summary
> A server-side rate limiter runs as middleware in a stateless **API gateway** fleet. Each request is keyed by user ID,
> IP, or API key and checked against **configurable rules** cached locally (pushed from etcd or polled from a store),
> enforcing the most restrictive match. State lives in a **sharded, replicated Redis Cluster**, and each check is a
> single atomic **token-bucket Lua script** (O(1) memory, burst-friendly, no race conditions, one round trip). Excess
> requests get **429** with `X-RateLimit-*` / `Retry-After` headers. We scale with sharding by client key, keep
> latency low with connection pooling and regional deployments (eventual consistency across regions), handle hot keys
> with blocklists and edge DDoS protection, and choose **fail-closed or local fallback** when Redis is unavailable.

---

### References
- Alex Xu, *System Design Interview — Design a Rate Limiter*, ByteByteGo —
  https://bytebytego.com/courses/system-design-interview/design-a-rate-limiter
- Evan King, *Rate Limiter — System Design Problem Breakdown*, Hello Interview —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/distributed-rate-limiter
- Stripe — *Scaling your API with rate limiters*: https://stripe.com/blog/rate-limiters
- Cloudflare — *Counting things, a lot of different things*:
  https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
- Lyft ratelimit: https://github.com/lyft/ratelimit

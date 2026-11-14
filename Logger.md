# Logger System (Distributed Logging + Dedup + Search) — System Design (Interview Guide)

> Design a logger system that **receives log messages with timestamps from many applications**, **prints / stores each
> message only if the same message has not been printed in the last 10 seconds**, and lets engineers **search** logs by
> application, level, time range and free text — at **millions of log lines per second**, without the logging pipeline
> ever slowing down or taking down the applications that produce the logs.

Asked at Apple, Google, Microsoft, Amazon. It is really **two questions glued together**:

1. **LLD / coding core** — LeetCode 359 *Logger Rate Limiter*: `shouldPrintMessage(timestamp, message)`.
2. **HLD** — a centralized log platform (ELK / Loki / Splunk / CloudWatch Logs style) with ingestion, dedup, indexing,
   retention and search.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Clarify: coding vs architecture? dedup key? exact vs approximate 10 s? how many apps, volume, search latency, retention |
| 2 | 5 min | Solve the single-node `shouldPrintMessage` (HashMap) + memory cleanup + thread safety |
| 3 | 3 min | Back-of-the-envelope estimates |
| 4 | 2 min | Core entities |
| 5 | 5 min | API — batch ingest, search, live tail |
| 6 | 10–15 min | High-level design: client library → agent → gateway → Kafka → processor (dedup) → index + object store → query |
| 7 | 15 min | Deep dives: distributed dedup, durability/backpressure, index choice, multi-tenancy, search at scale, cost/retention |
| 8 | 5 min | Trade-offs, wrap-up |

**Key line to say early:** *"Logging must never hurt the app: the client logs **asynchronously** to a local buffer and
an agent ships batches. Dedup is a **keyed 10-second window**, so I'll **partition the stream by the dedup key** — then
every key lives on exactly one processor and dedup is a local hash-map lookup, no distributed lock. Storage is split:
a **time-partitioned search index** for recent hot data and **cheap object storage** for everything else."*

> Ask which half they care about. Some interviewers only want the class (step 2) done well with follow-ups
> (memory, concurrency, distributed); others want the platform.

---

## 1. Requirements

### Clarifying questions
| Question | Typical answer |
|----------|----------------|
| What is "the same message"? | Same **app + level + normalized message** (template), not raw string with IDs/timestamps in it |
| Window semantics? | Print if not printed in the **last 10 s** (measured from the last *printed* occurrence) |
| Event time or arrival time? | Event (client) timestamp, tolerate small skew / out-of-order |
| Are suppressed duplicates thrown away? | Not printed, but **counted** ("repeated 42 times") — never silently lose ERRORs |
| How many apps / volume? | Thousands of services, ~1M lines/s avg, ~3M/s peak |
| Search latency? | Recent data (last 24 h): seconds. Older: minutes is OK |
| Retention? | 7–30 days searchable, 1 year+ archived (audit: up to 7 years) |

### Functional
1. **Ingest** log messages `(timestamp, app, level, message, fields…)` from many applications / hosts.
2. **Deduplicate**: emit (print / index) a message only if the same key wasn't emitted in the last **10 s**; count the
   suppressed ones.
3. **Multi-application / multi-tenant**: per-app isolation, quotas, retention and access control.
4. **Search** by app, level, time range, structured fields (`trace_id`, `user_id`) and full text; sort by time; page.
5. **Live tail** (stream new logs for an app/filter).

### Below the line (mention, don't design)
- Metrics & tracing pipelines (logs ≠ metrics — count with metrics, not by counting log lines).
- Alerting rules on log patterns, dashboards, ML anomaly detection.
- Log-based billing.

### Non-Functional
| Property | Target |
|----------|--------|
| Producer overhead | **Non-blocking**, ~µs per call on the hot path; app never fails because logging is down |
| Ingest throughput | ~1M lines/s avg, **~3M/s peak**, bursty (incidents spike volume 10×) |
| Durability | At-least-once from agent onward; no loss of WARN/ERROR once accepted |
| Freshness | Searchable within **< 10–30 s** of emission |
| Search latency | p95 < 2–3 s over last 24 h; long-range queries async |
| Availability | Ingest 99.9%+ (buffer when downstream is down); search can degrade |
| Isolation | One noisy app can't starve others |
| Security | PII / secrets redacted, per-tenant access control, encryption at rest |

---

## 2. Part 1 — The Coding Core (`shouldPrintMessage`)

```text
shouldPrintMessage(1,  "foo") -> true   // next allowed for "foo" = 11
shouldPrintMessage(2,  "bar") -> true   // next allowed for "bar" = 12
shouldPrintMessage(3,  "foo") -> false  // 3 < 11
shouldPrintMessage(11, "foo") -> true   // 11 >= 11, next allowed = 21
```

### 2.1 Basic — HashMap `message → nextAllowedTimestamp` (O(1) time)
```java
class Logger {
    private static final int WINDOW = 10;
    private final Map<String, Integer> nextAllowed = new HashMap<>();

    boolean shouldPrintMessage(int timestamp, String message) {
        Integer next = nextAllowed.get(message);
        if (next != null && timestamp < next) {
            return false;
        }
        nextAllowed.put(message, timestamp + WINDOW);
        return true;
    }
}
```
Store **next allowed time**, not last time — one comparison, and the window lives in one place.

### 2.2 Follow-ups the interviewer will push on

| Follow-up | Answer |
|-----------|--------|
| **Memory grows forever** (every unique message stays) | Evict expired keys: FIFO queue of `(ts, msg)`, pop entries older than `now − 10` on each call (timestamps non-decreasing); **or** a periodic sweep `map.values().removeIf(n -> n <= now)`; **or** two rotating buckets (current / previous 10 s). Memory = O(unique msgs in 10 s) |
| **Thread safety** | `ConcurrentHashMap.compute(...)` makes check-and-set atomic per key (below). A global `synchronized` works but serializes all callers |
| **Out-of-order timestamps** | Never move the window backwards: `next = max(existing, ts + 10)`; tolerate small skew, route far-past events separately |
| **Count the suppressed ones** | Keep `suppressedCount` per key; when the key is printed again (or evicted) emit `"<msg> (repeated N times)"` — like syslog's *"last message repeated N times"* |
| **Different window per level/app** | Key → policy lookup (ERROR: 1 s or no dedup, DEBUG: 60 s) |
| **Multiple servers** | Needs shared or partitioned state → §8.1 |

```java
class ConcurrentLogger {
    private static final long WINDOW_MS = 10_000;
    private final ConcurrentHashMap<String, Long> nextAllowed = new ConcurrentHashMap<>();

    boolean shouldPrint(long timestampMs, String key) {
        boolean[] allowed = {false};
        nextAllowed.compute(key, (k, next) -> {
            if (next == null || timestampMs >= next) {
                allowed[0] = true;
                return timestampMs + WINDOW_MS;
            }
            return next;
        });
        return allowed[0];
    }

    // Called from a scheduled task every few seconds to bound memory.
    void evictExpired(long nowMs) {
        nextAllowed.values().removeIf(next -> next <= nowMs);
    }
}
```

> **What is the key?** Hashing the raw string makes `"user 17 failed"` and `"user 18 failed"` different keys, so nothing
> is deduped. Use a **fingerprint** = `hash(app, level, message_template)` where the template is the constant message
> (structured logging, §5.2) or numbers/UUIDs are masked (`"user <*> failed"`).

---

## 3. Capacity Estimates

- **Sources:** ~5,000 services, ~100k hosts/containers.
- **Volume:** ~1M lines/s avg (≈ 86B lines/day), **~3M/s peak**; incidents spike 10× for minutes.
- **Size:** structured JSON ≈ **500 B/line** → 1M × 500 B = **500 MB/s ≈ 43 TB/day raw**.
- **After dedup + sampling:** repetitive noise (health checks, retry loops, the same stack trace) is often 30–70% of
  volume → assume ~50% kept → **~20 TB/day** to store.
- **Compression:** ~5–10× (columnar / zstd) → **~2–4 TB/day on disk**; × replication 2–3 → ~10 TB/day.
  - Hot 7 days in the search index ≈ **70 TB** (+ index overhead); 1 year in S3 (compressed) ≈ **~1 PB** → object
    storage, not SSD.
- **Dedup state:** unique keys per 10 s window, say ~1M × ~64 B ≈ **64 MB** — fits in memory across a few processors.
- **Search:** ~1k engineers, ~10–50 QPS, spiky during incidents. **Massively write-heavy.**

> The write volume dominates: it drives Kafka, async ingestion, and "index less, store more in S3".

---

## 4. Core Entities

| Entity | Key fields |
|--------|-----------|
| **LogEvent** | event_id, timestamp (event time), ingest_ts, app_id, env, host, level, message, fingerprint, trace_id, fields{} |
| **Application (tenant)** | app_id, team, quota (lines/s, bytes/day), retention policy, dedup policy, ACL |
| **DedupState** | fingerprint → next_allowed_ts, suppressed_count (ephemeral) |
| **RetentionPolicy** | app_id / level → hot_days, cold_days |
| **Query** | query_id, filters, time_range, status, result_location (async queries) |

---

## 5. Producer Side — How Apps Should Log

### 5.1 Client logging library (shared, owned by the platform team)
- **Structured JSON** with a **shared schema** (`timestamp`, `level`, `service`, `env`, `version`, `host`, `message`,
  `trace_id`, `user_id`, `error_type`, `duration_ms`) — consistent field names make fields indexable across services.
- **Async**: append to an in-memory ring buffer; a background thread writes to **stdout**. Buffer full → **drop
  DEBUG/INFO first, never block** the request thread. Trade-off: entries still buffered are lost if the process crashes.
- **Lazy formatting**: `log.debug("x={}", x)` / suppliers — no string building when the level is off
  (at 10k req/s, 0.1 ms of logging per request ≈ 1 CPU-second every second).
- **Context injection** (MDC): service/version/env once at startup, `trace_id` / `request_id` per request.
- **Redaction in the library**: mask `password`, `token`, card numbers — protection doesn't depend on every developer.
- **Optional client-side dedup** (the Part 1 class) for chatty loops — cheapest place to drop noise, but only local.

### 5.2 Logging hygiene (what makes dedup & search work)
- **Constant message + variable fields**: `message="Payment charge failed"`, `order_id=842` → stable fingerprint,
  countable events.
- Correct **levels**: someone must act → ERROR; handled-but-unusual → WARN; normal → INFO; detail → DEBUG (off in prod,
  switched on at runtime per service). A wrong password is not an ERROR.
- **Log an error once, where it's handled** (not at every layer); **summaries instead of per-item lines in loops**.
- **Never** log secrets or full card numbers; be careful with PII (GDPR / PCI DSS apply to logs too).
- Containers are ephemeral → write to **stdout**, let the agent ship; never rely on files inside the container.

---

## 6. API Design

### Ingest (agent → gateway)
```http
POST /v1/logs:batch
Authorization: Bearer <app-token>
Content-Encoding: zstd
{ "app_id": "payments", "events": [ { "event_id": "uuid", "timestamp": "...", "level": "ERROR",
                                       "message": "Payment charge failed", "fields": {...} }, ... ] }
-> 202 Accepted            # durably in Kafka (acks=all); not yet searchable
-> 429 Too Many Requests   # tenant quota exceeded, Retry-After
```
Batches of ~1–5 MB or 1 s, compressed. In practice also OTLP / Fluent forward protocol.

### Search
```http
GET  /v1/logs/search?app=payments&level=ERROR&q="charge failed"&from=...&to=...&limit=100&cursor=...
     -> { events: [...], nextCursor }                      # sync: recent / small ranges
POST /v1/queries   { filters, from, to }   -> { queryId }    # async: long ranges over the archive
GET  /v1/queries/{queryId}                  -> { status, resultsUrl }
GET  /v1/logs/tail?app=payments&minLevel=WARN   (WebSocket / SSE)   # live tail
```

### Admin
```http
PUT /v1/apps/{appId}/policies   { quota, retention, dedupWindowSec, sampling }
```

---

## 7. High-Level Architecture

```
  ┌────────────── Hosts / Pods (thousands of apps) ──────────────┐
  │ App ─▶ logging lib (async, JSON, redact) ─▶ stdout            │
  │                     ▼                                         │
  │        Agent (Fluent Bit / Vector / OTel collector)           │
  │        tail, parse, batch, compress, local disk buffer        │
  └─────────────────────┬─────────────────────────────────────────┘
                        │ HTTPS batches
                        ▼
              ┌─────────────────────┐
              │ Ingestion gateway   │  auth (app token), schema check,
              │ (stateless, L7 LB)  │  per-tenant quota / rate limit
              └─────────┬───────────┘
                        │ produce, key = fingerprint (app+level+template)
                        ▼
              ┌─────────────────────┐
              │ Kafka  logs.raw     │  durable buffer, replay, absorbs spikes
              └─────────┬───────────┘
                        ▼
              ┌──────────────────────────────┐
              │ Stream processors (Flink /   │  parse, enrich, redact,
              │ Kafka Streams)               │  10 s DEDUP (keyed state), sample
              └───┬──────────────┬───────────┘
                  │              │
       logs.clean │              │ all kept events
                  ▼              ▼
      ┌────────────────────┐  ┌──────────────────────────┐
      │ Indexers (hot)     │  │ Archiver → S3 (Parquet,  │
      │ OpenSearch / Loki /│  │ partitioned app/date/hr) │
      │ ClickHouse         │  └────────────┬─────────────┘
      └─────────┬──────────┘               │ Athena / Trino (async)
                ▼                          ▼
              ┌───────────────────────────────────┐
              │ Query service (auth, tenancy,     │◀── UI / CLI / API
              │ fan-out, merge, cursor, tail)     │
              └───────────────────────────────────┘
```

### 7.1 Ingest flow
1. App logs → lib → stdout. Agent tails, batches, compresses; on failure **buffers to local disk** and retries with
   exponential backoff + jitter.
2. Gateway authenticates the app token, validates schema/size, enforces the tenant quota, and **produces to Kafka with
   `acks=all`** → only then `202`. Gateway is stateless and horizontally scaled.
3. Kafka is the shock absorber: indexers can be slow or down for hours without losing data (retention 1–3 days).

### 7.2 Process & dedup flow
1. Gateway computes `fingerprint = hash(app, level, template)` and uses it as the **Kafka key** → all copies of a
   message land in the **same partition → same processor instance**.
2. Keyed state `fingerprint → (next_allowed_ts, suppressed_count)` with a **TTL timer** (Part 1 logic):
   - `ts >= next_allowed` → emit event (with `repeat_count` = suppressed since last emit), set `next = ts + 10 s`.
   - else → `suppressed_count++`, drop.
3. Output to `logs.clean` → indexers; archiver writes kept events to S3.

### 7.3 Search flow
1. Query service authorizes (user ↔ app ACL), picks backends by time range: last N days → index; older → S3 via
   Trino/Athena (async).
2. Fans out only to the **time-based indices** in range (pruning), merges by timestamp, returns a **cursor**
   (`search_after`), not offset paging.

---

## 8. Deep Dives

### 8.1 Distributed dedup — where does the 10 s state live?

| Option | Verdict |
|--------|---------|
| **Bad:** each app server / gateway dedups locally | Same message from 50 pods is printed 50× per window |
| **Good:** central **Redis** `SET dedup:{fp} 1 NX PX 10000` → `OK` = print | Atomic, simple, correct across servers. But a network hop per line at 3M/s, Redis becomes a hot critical dependency, one crash-looping error hammers one shard |
| **Great:** **partition the stream by fingerprint** → keyed local state in the processor (Flink keyed state / RocksDB with TTL) | No coordination, no remote calls, scales with partitions, state checkpointed → survives restarts. **Chosen** |
| **Plus:** client-side dedup in the logging lib | Cuts volume at the source; server-side handles cross-instance duplicates |

Details to mention:
- **Hot key** (one error at 500k/s, all in one partition): client-side pre-dedup + O(1) drop in the processor; worst
  case salt the key and do a two-stage aggregate.
- **Event time vs processing time**: event timestamp with small allowed lateness (watermark); clamp absurd client clocks
  to ingest time.
- **Exactly 10 s isn't sacred**: after failover, state restored from the last checkpoint may let one duplicate through —
  acceptable, dedup is a noise filter, not a correctness guarantee. Say this explicitly.
- **Never dedup away information**: always carry `repeat_count`; per-level policies (no dedup for AUDIT, shorter window
  for ERROR).

### 8.2 Durability & backpressure (don't lose logs, don't hurt apps)
- Chain of buffers: **lib ring buffer → agent disk buffer → Kafka (RF=3, acks=all) → indexer bulk queues**.
- **At-least-once** from the agent onward; retry duplicates removed by `event_id` (idempotent indexing: doc
  `_id = event_id`).
- Overload: gateway returns **429**, agents back off and buffer to disk; disk buffer full → **drop lowest levels first**
  (DEBUG → INFO), never ERROR/AUDIT.
- The app **never blocks** — losing some DEBUG lines beats an outage caused by logging.

### 8.3 Storage / index choice

| Option | How it indexes | Good at | Cost |
|--------|----------------|---------|------|
| **Elasticsearch / OpenSearch** | Full inverted index on fields + text | Arbitrary full-text search, aggregations | Expensive: index ≈ data size, heavy ingest CPU |
| **Grafana Loki** | Indexes only **labels** (app, env, level); content in compressed chunks on S3, grep at query time | Very cheap; great when you always filter by app + time | Full-text over long ranges is slow (brute force) |
| **ClickHouse** (columnar) | Sorted by `(app, timestamp)`, skip indexes / token bloom filters | Fast filters + aggregations, 10×+ compression | Less flexible free-text relevance |
| **S3 + Parquet + Athena/Trino** | Partition pruning (app/date/hour) | Cheapest long-term, compliance | Minutes-latency queries |

Reasonable answer: **hot = OpenSearch or ClickHouse (7–30 days), cold = S3 Parquet (1+ year)**; mention Loki-style
"index labels, not content" as the cost-optimized alternative.

**Index layout (OpenSearch):**
- **Time-based indices** (daily or rollover at ~50 GB/shard): `logs-payments-2026.11.14`. Deleting old data = drop an
  index (no expensive deletes); queries touch only days in range.
- **ILM**: hot (SSD, replicas) → warm (HDD, force-merged, read-only) → snapshot to S3 / delete.
- Map high-cardinality IDs (`trace_id`, `user_id`) as `keyword`; **don't** index every arbitrary field — mapping
  explosion. Unknown fields go into a flattened / unindexed blob.

### 8.4 Multi-tenancy (multiple applications)
- **Ingest isolation**: per-app quotas at the gateway (token bucket — see *rate-limiter* design); dedicated Kafka topics
  for the very largest tenants.
- **Index strategy**:
  - *Index per tenant per day* — clean isolation/retention, but thousands of small apps → **shard explosion**.
  - Hybrid: *shared index with `app_id` + routing* for small apps, *dedicated indices* for big ones.
- **Per-app retention & dedup policies** (policy service, cached in gateway/processors).
- **Access control**: the query service injects `app_id IN (allowed apps)` into every query — never trust the client
  filter.

### 8.5 Search at scale
- **Prune first**: time range → index/partition set; `app_id` → routing/shards. Most queries are *"app X, last 15 min,
  level ≥ WARN"* — optimize for that.
- **Scatter-gather + merge** by timestamp; top-K, more on demand.
- **Cursor pagination** (`search_after` on `(timestamp, event_id)`), no deep `from/size`.
- **Limits / timeouts** per query; long ranges become **async jobs** over S3 with results written to S3.
- **Cache** identical recent queries (dashboards refreshing every 10 s).
- **Correlation**: search by `trace_id` returns one request's story across all services (ID generated at the API
  gateway, propagated via headers).

### 8.6 Live tail
- Query service consumes `logs.clean` (or a per-tenant topic), applies the filter in memory, pushes over WebSocket/SSE.
  Rate-limit/sample — a human can't read 10k lines/s.

### 8.7 Cost control
- **Dedup** (this design) + **sampling** (keep all WARN/ERROR, e.g. 10% of successful-request INFO).
- **Dynamic log levels** — flip one service to DEBUG for 15 min without redeploying.
- **Retention tiers** by log type:

| Log type | Hot (searchable) | Cold (S3) |
|----------|------------------|-----------|
| Debug | 1–3 days | none |
| Application | 7–30 days | ≤ 90 days |
| Access | 30 days | ≤ 1 year |
| Audit / security | 30–90 days | 1–7 years (compliance, immutable / WORM) |

- **Use metrics for counting** (RPS, error rate), not log lines.

### 8.8 Security & compliance
- TLS everywhere, per-app ingest tokens, encryption at rest (KMS).
- Second-line **redaction in the processor** (regex for card numbers, JWTs, emails) in case a library missed it.
- GDPR erasure in immutable logs is hard → keep PII out or pseudonymize `user_id`; time-bounded retention.
- Audit logs: append-only, tamper-evident (hash chain / S3 Object Lock).

---

## 9. Observability of the Logging Platform Itself
- **Metrics:** lines/s & bytes/s per tenant, 429 rate, agent buffer fill / drops by level, Kafka consumer lag, dedup
  ratio (suppressed / total), end-to-end freshness (emit → searchable), indexing rejections, query p95.
- **Alerts:** growing consumer lag (freshness SLO), any WARN/ERROR drops, tenant at quota, disk watermarks.
- The platform's own logs go to a **separate minimal pipeline** — don't feed the logger into itself.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Async logging, drop low levels when full | Synchronous writes | App latency/availability > completeness of DEBUG logs |
| stdout + node agent | App ships directly to backend | App stays simple; agent buffers, retries, batches |
| Kafka between ingest and index | Direct to index | Absorbs spikes, replay, decouples slow indexers |
| Partition by fingerprint + local keyed state | Central Redis `SET NX PX` | No per-line network hop, no hot central dependency |
| Dedup with `repeat_count` | Silently drop duplicates | Less noise without losing "this happened 10k times" |
| Approximate 10 s across failover | Exactly-once dedup | Dedup is a noise filter; exactness is expensive |
| Hot index + S3 archive | Everything in the index | ~10× cheaper; old logs rarely queried |
| Time-based indices + ILM | One big index | Cheap deletion, time pruning, tiering |
| Shared index for small tenants | Index per tenant | Avoids shard explosion |
| Cursor paging | Offset paging | Deep pages are expensive in distributed search |

---

## 11. Common Follow-up Questions

1. **Same message from 100 pods within 10 s — printed how many times?** Once: the fingerprint routes all copies to one
   processor. With only client-side dedup it would be 100.
2. **Messages differ only by an ID — deduped?** Only if the fingerprint uses the **template** (structured message or
   masked variables).
3. **Processor crashes?** Consumer group rebalances, keyed state restored from the last checkpoint, replay from committed
   offsets → maybe one duplicate print, no loss (idempotent `event_id`).
4. **Out-of-order / skewed timestamps?** Event time with allowed lateness; far-off clocks clamped to ingest time;
   `next = max(next, ts + 10)` never moves backwards.
5. **Logging backend down for 2 h?** Agents buffer to disk, Kafka holds 1–3 days; indexers catch up (scale up
   temporarily). Apps unaffected.
6. **One app floods 10× its quota during an incident?** Gateway 429s that tenant only; agent buffers, then drops
   DEBUG/INFO; other tenants unaffected. Dedup collapses the repeated stack trace anyway.
7. **All logs of one failed request across 20 services?** `trace_id` propagated in headers, indexed as `keyword`.
8. **Why not keep 1 year in Elasticsearch?** Index overhead and SSD cost at PB scale; S3 Parquet + Athena instead.
9. **"Last 6 months" trend of an error?** Async query over partitioned Parquet, or pre-aggregated counts per
   fingerprint per hour.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Correct `shouldPrintMessage` with a HashMap, discusses memory cleanup; basic pipeline: agents → queue → index → search UI |
| **Senior (E5)** | Thread-safe, bounded logger; distributed dedup via partition-by-key vs Redis `SET NX PX`; Kafka buffer, at-least-once + idempotent indexing; time-based indices, retention tiers, multi-tenant quotas |
| **Staff+ (E6)** | Drives cost (dedup, sampling, label-only indexing, S3 tiering), hot keys, event-time semantics, failover dedup guarantees, shard explosion vs tenant isolation, PII/compliance, platform self-observability |

---

## 13. One-Minute Summary
> Apps log **structured JSON asynchronously** through a shared library (context, redaction, never blocks) to stdout; a
> **node agent** batches, compresses and disk-buffers to a stateless **ingestion gateway** that authenticates the app,
> enforces **per-tenant quotas**, and writes to **Kafka** keyed by a **fingerprint (app + level + message template)**.
> Because every copy of a message lands on the same **stream processor**, the **10-second dedup** is the classic
> `fingerprint → nextAllowedTs` map held as checkpointed keyed state with TTL — print if `ts ≥ next`, else count it and
> emit `repeat_count` later. Kept events go to a **time-partitioned search index** (OpenSearch/ClickHouse, hot 7–30 days,
> ILM) and to **S3 Parquet** for cheap archive queried asynchronously. A **query service** enforces tenant ACLs, prunes
> by time/app, scatter-gathers with cursor paging, and serves live tail. At-least-once delivery + `event_id`
> idempotency, sampling, and retention tiers keep it reliable and affordable.

---

### References
- Hello Interview, *Design a Logger System* —
  https://www.hellointerview.com/community/questions/logger-system/cm6jx0wxh016bui4b2oqtwudo
- AlgoMaster, *Logging Best Practices* — https://algomaster.io/learn/system-design/logging
- LeetCode 359, *Logger Rate Limiter* — https://leetcode.com/problems/logger-rate-limiter/

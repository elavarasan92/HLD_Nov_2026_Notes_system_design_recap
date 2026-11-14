# Distributed Cache (Redis / Memcached) — System Design (Interview Guide)

> Design an **in-memory key-value cache** spread across many machines that supports **get / set / delete** with
> **TTL** and **LRU eviction**, holds **~1 TB** of data, serves **~100k req/s** at peak with **< 10 ms** latency, and
> stays **highly available** when nodes fail — eventual consistency is acceptable.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Clarify: API surface, eviction policy, TTL, data size, QPS, durability?, consistency?, read/write mix |
| 2 | 3 min | Back-of-the-envelope estimates → number of nodes |
| 3 | 2 min | Core entities |
| 4 | 3 min | API |
| 5 | 10–15 min | High-level design — **start with a single node** (hash map + doubly linked list + TTL), then add nodes |
| 6 | 15 min | Deep dives: partitioning (consistent hashing), replication & failover, hot keys / hot writes, latency, cache patterns, stampedes |
| 7 | 5 min | Trade-offs, wrap-up |

**Key line to say early:** *"This is two problems. First, a **single-node** data structure that gives O(1) get/set,
TTL and LRU. Second, **distribution**: partition keys across nodes with consistent hashing, replicate for
availability, and handle hot keys. The cache is a **fast, partial, disposable copy** — the database stays the source
of truth."*

> Clarify which flavour they want: designing **the cache system itself** (build Redis/Memcached) vs **using a cache in
> front of a DB** (cache-aside, invalidation). Most interviewers want the former, but expect questions on the latter.

---

## 1. Requirements

### Functional
1. `set(key, value)`, `get(key)`, `delete(key)` on key-value pairs.
2. Optional **TTL** per key — expired keys are never returned.
3. **LRU eviction** when memory is full (confirm: LRU vs LFU vs FIFO vs custom).

### Below the line (mention, don't design)
- Configurable cache size per tenant, rich data structures (lists, sorted sets), pub/sub, Lua scripting.
- Durability across restarts (RDB snapshots / AOF), transactions, complex queries.
- Strong consistency.

### Non-Functional
| Property | Target |
|----------|--------|
| Latency | **< 10 ms** for get/set (realistically sub-ms in-DC) |
| Scale | **1 TB** data, **100k req/s** peak, grows horizontally |
| Availability | Highly available; survive node / AZ failure — **AP over CP** |
| Consistency | **Eventual** — a read may briefly see stale data after failover |
| Durability | Not required — a lost entry is a cache miss, not data loss |

---

## 2. Capacity Estimates

- **Memory:** 1 TB data + ~20–30% overhead (pointers, LRU list, hash table, fragmentation) ≈ **1.3 TB**.
  With 32–64 GB usable RAM per node → **~20–40 primary shards**. Add 1–2 replicas each → **40–120 nodes**.
- **Throughput:** a single Redis/Memcached node does ~100k+ simple ops/s, so 100k req/s fits on a few nodes — **memory,
  not QPS, sizes the cluster.** But hot keys can still overload one node (deep dive).
- **Network:** 100k req/s × ~1 KB avg value ≈ **100 MB/s** cluster-wide — fine. Large values (≥ 100 KB) change this
  quickly; cap value size.
- **Keys:** 1 TB / ~1 KB ≈ **~1B keys**.

> Say out loud: "Memory drives node count; QPS and hot keys drive replication and client design."

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **Entry** | key, value (bytes), expires_at, prev / next (LRU list pointers) |
| **Shard / Node** | node_id, host:port, role (PRIMARY / REPLICA), hash-slot range / ring tokens |
| **Cluster config** | slot → node map (or ring), epoch / version |

---

## 4. API Design

Keep it tiny; usually a binary/text TCP protocol (RESP, Memcached protocol) over persistent connections, not HTTP —
HTTP headers and handshakes add avoidable latency.

```text
GET    key                       -> value | NIL
SET    key value [TTL seconds]   -> OK
DEL    key                       -> 1 | 0
```

Nice extras if asked: `MGET` (batch), `SET NX` (only-if-absent — used for locks / stampede protection),
`INCR` (atomic counters), `EXPIRE key ttl`.

---

## 5. High-Level Design

### 5.1 Single node — get / set / delete in O(1)
- **Hash map** `key → node` for O(1) lookup.
- Single-threaded event loop (Redis style) avoids locks; or a sharded map with striped locks (Memcached style).

### 5.2 TTL / expiration
Two mechanisms, used together:
- **Lazy (passive):** on `get`, if `now > expires_at` → delete and return NIL. Cheap, but never-read keys leak memory.
- **Active (background):** periodically **sample** a few keys with TTLs (e.g. 20), delete the expired ones; if > 25%
  were expired, repeat. Bounded CPU, eventually cleans up. (Redis does exactly this.)
- Alternative: a min-heap / timing wheel ordered by `expires_at` — precise, but O(log n) maintenance per write.

### 5.3 LRU eviction
- **Hash map + doubly linked list.** The list is ordered by recency: head = most recent, tail = least recent.
  - `get` hit → move node to head — O(1).
  - `set` → insert at head; if memory > limit → **evict from tail** — O(1).
  - `delete` → unlink — O(1).
- **Approximate LRU** (Redis): store a last-access timestamp per key, on eviction sample ~5 keys and drop the oldest.
  Saves 2 pointers per key and avoids list contention; accuracy is close to true LRU.
- Other policies: **LFU** (frequency counters with decay; better for skewed, stable popularity), **FIFO**, **random**,
  **volatile-*** (evict only keys with TTL).

```
 HashMap                 Doubly linked list (recency)
 ┌────────┐       HEAD ⇄ [k7] ⇄ [k2] ⇄ [k9] ⇄ ... ⇄ [k4] ⇄ TAIL
 │ k2 ────┼──────────────▲                              ▲
 │ k4 ────┼──────────────────────────────────────────────┘  ← evict here
 └────────┘
```

### 5.4 Going distributed
One node can't hold 1.3 TB and is a single point of failure → **partition (shard) keys across N nodes**, each node
running the structure above.

```
        ┌──────────── Application servers ─────────────┐
        │  cache client library (knows the slot map,   │
        │  connection pool per node, timeouts)          │
        └───────┬──────────────┬──────────────┬─────────┘
                │ hash(key)    │              │
                ▼              ▼              ▼
          ┌──────────┐   ┌──────────┐   ┌──────────┐
          │ Shard A  │   │ Shard B  │   │ Shard C  │   ... N shards
          │ primary  │   │ primary  │   │ primary  │
          └────┬─────┘   └────┬─────┘   └────┬─────┘
          async│repl          │              │
          ┌────▼─────┐   ┌────▼─────┐   ┌────▼─────┐
          │ replica  │   │ replica  │   │ replica  │   (other AZ)
          └──────────┘   └──────────┘   └──────────┘
                ▲
                │ membership, slot map, failover
        ┌───────┴──────────────────────────────────┐
        │ Config / coordination (gossip, or         │
        │ ZooKeeper / etcd / Sentinel)              │
        └───────────────────────────────────────────┘
                              │ miss
                              ▼
                     ┌──────────────────┐
                     │ Database (truth) │
                     └──────────────────┘
```

Request path: client computes `hash(key)` → looks up owning shard in its cached slot map → sends over a pooled TCP
connection → hit returns the value; miss → app reads DB and (cache-aside) writes back to the cache.

---

## 6. Deep Dives

### 6.1 Partitioning — which node owns a key?
| Option | Verdict |
|--------|---------|
| **Bad:** `hash(key) % N` | Simple, but changing N remaps ~all keys → mass misses → DB stampede |
| **Good:** **consistent hashing** ring with **virtual nodes** (100–200 vnodes per node) | Adding/removing a node moves only ~1/N of keys; vnodes smooth load and let bigger machines take more tokens |
| **Great (Redis Cluster):** **fixed hash slots** — `CRC16(key) % 16384` → slot → node | Rebalancing = moving whole slots between nodes; slot map is small and easy to gossip; supports **hash tags** `{user:42}:cart` to co-locate related keys for multi-key ops |

**Who does the routing?**
- **Client-side (smart client):** client holds the slot map, talks directly to the owner — fewest hops, lowest
  latency. On a stale map the node replies `MOVED` / `ASK` and the client refreshes.
- **Proxy (Twemproxy, Envoy, mcrouter):** thin clients, centralised routing, connection multiplexing — one extra hop.
- **Any-node forwarding:** any node forwards to the owner — simplest client, extra hop, extra load.

### 6.2 Replication & availability
- Each shard = **1 primary + 1–2 replicas** in different AZs.
- **Async replication** (primary acks the client, then streams to replicas): lowest latency; a failover can lose the
  last few writes — acceptable for a cache (it's a miss, not data loss).
- **Failure detection:** heartbeats/gossip; when a majority of primaries (or Sentinels / a coordinator) agree the
  primary is down → **promote a replica**, bump the config epoch, clients refresh the slot map.
- **Split brain:** a partitioned old primary must stop accepting writes — e.g. stop if it can't reach a majority /
  any replica for `node_timeout` (Redis `min-replicas-to-write`).
- **Read from replicas** to scale reads — accept slightly stale values.
- Peer-to-peer / leaderless (Dynamo-style quorum `R + W > N`) is possible but overkill for a cache; mention as the
  stronger-consistency alternative.

### 6.3 Hot keys (read skew)
One celebrity profile / flash-sale product can saturate a single shard no matter how many shards exist.
- **Detect:** per-key counters / sampling on nodes (Redis `--hotkeys`, LFU counters), client-side metrics.
- **Read replicas** for that shard and spread reads across them.
- **Key replication / salting:** store copies as `key#1 … key#k` on different shards; reads pick a random suffix,
  writes update all k.
- **L1 local in-process cache** (few seconds TTL) in the app for the hottest keys — removes the network hop entirely.

### 6.4 Hot writes
- Counters/rate-limit keys hammered by writes → **split the key** (`counter:{shard}`) and sum on read, or **batch /
  coalesce** writes in the client and flush periodically.
- Writes can't go to replicas, so salting + aggregation is the main tool.

### 6.5 Latency (< 10 ms, typically < 1 ms)
- **Persistent connection pools** per node — no TCP/TLS handshake per request.
- **Pipelining / batching** (`MGET`, pipelined commands) to amortise RTTs.
- **Tight client timeouts** (few ms) + fallback to DB — a slow cache must never be slower than no cache.
- Keep values small (compress large ones, cap size), avoid O(n) commands (`KEYS *`) on the hot path.
- Co-locate cache in the same AZ/region as the app; avoid cross-region hops.

### 6.6 Cache stampede / thundering herd
A popular key expires (or a node restarts cold) → thousands of misses hit the DB at once.
- **Request coalescing / single-flight:** only one request rebuilds; others wait or get the stale value. Implement with
  `SET lock:key NX EX 5` or Memcached **leases**.
- **TTL jitter:** `ttl = base ± random(10%)` so keys don't expire together.
- **Early / probabilistic refresh** (refresh-ahead): refresh before expiry with a probability that rises near TTL.
- **Serve stale while revalidating** when availability > freshness.
- **Warm-up** new/replaced nodes before taking full traffic.

### 6.7 Cache read/write patterns (how apps use it)
| Pattern | How | Trade-off |
|---------|-----|-----------|
| **Cache-aside (lazy loading)** | App reads cache → on miss reads DB, writes cache; on write updates DB then **deletes** cache key | Most common; only caches what's read; stale window possible; first read is slow |
| **Read-through** | Cache itself loads from DB on miss | Simpler app code; cache coupled to DB |
| **Write-through** | Write to cache and DB synchronously | Cache always fresh; higher write latency; caches unread data |
| **Write-behind (write-back)** | Write to cache, flush to DB async | Fastest writes; risk of data loss on cache failure |
| **Write-around** | Write only to DB; cache filled on read | Avoids polluting cache with write-once data |

> Prefer **delete on write** over **update on write**: two concurrent writers updating the cache can leave it with the
> older value; delete + lazy reload avoids that race (remaining race is narrow; add short TTL as a backstop).

### 6.8 Invalidation & consistency
- **TTL** as the universal backstop for staleness.
- **Explicit delete** after DB commit; or **CDC** (Debezium on the DB binlog → Kafka → invalidation consumer) so every
  writer path invalidates reliably.
- **Versioned keys** (`user:v3:42`) when the value schema changes — no mass delete needed.
- Multi-level (L1 local + L2 shared): broadcast invalidations via pub/sub, or keep L1 TTLs very short.

### 6.9 Scaling out / rebalancing
- Add a node → assign it slots/vnodes → **migrate keys** in the background (slot-by-slot, `MIGRATE` / `ASK`
  redirects during the move) → flip ownership.
- Watch hit rate and DB QPS during rebalancing; throttle migration.
- Or simply accept misses on moved keys (Memcached + consistent hashing does this) — cheaper, DB takes the hit.

### 6.10 Memory management
- **Slab allocator** (Memcached): fixed-size chunk classes avoid fragmentation; per-slab LRU.
- **jemalloc + active defrag** (Redis).
- Set `maxmemory` below physical RAM (leave room for replication buffers / fork), pick an eviction policy, alert on
  evictions and fragmentation ratio.

### 6.11 Durability (if asked)
- **RDB snapshots** (fork + copy-on-write) for fast restarts / warm cache; **AOF** (append-only log, fsync every second)
  for less loss. Both cost CPU/IO and memory headroom — usually off or snapshot-only for a pure cache.

---

## 7. Observability
- **Metrics:** hit ratio (per key prefix / route, not just global), p50/p99 latency, ops/s per node, evictions/s,
  expired keys/s, memory used & fragmentation, replication lag, connected clients, `MOVED` redirect rate.
- **Alerts:** hit-ratio drop, eviction spikes, a node far above peers in QPS (hot key), replication lag, failovers.
- **Tracing:** tag spans with cache hit/miss and node so DB fallbacks are visible.

---

## 8. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Hash map + doubly linked list | Sorted structure / heap | O(1) get, set, evict |
| Approximate (sampled) LRU | Exact LRU | Less memory per key, no list contention, near-identical hit rate |
| Lazy + sampled active expiry | Timer per key / heap | Bounded CPU, no per-key timers |
| Hash slots / consistent hashing | Modulo hashing | Only ~1/N keys move on resize |
| Smart client routing | Proxy | One fewer hop; proxy is simpler for many languages |
| Async replication | Sync / quorum | Lower latency; losing a few writes = misses, acceptable |
| AP (eventual consistency) | CP | Cache must stay up; DB is the truth |
| Delete-on-write (cache-aside) | Update-on-write | Avoids concurrent-writer races |
| No persistence | RDB / AOF | Faster, simpler; cache can be rebuilt from DB |
| L1 local + L2 shared | L2 only | Kills hot-key load; harder invalidation |

---

## 9. Common Follow-up Questions

1. **A shard's primary dies?** Gossip/Sentinel detects it, promotes a replica, bumps the epoch; clients get `MOVED` or
   refresh the map. Unreplicated writes are lost → misses → DB reload.
2. **Whole cache cluster is down?** App falls back to DB with **circuit breaker + rate limiting** so the DB isn't
   crushed; serve degraded responses for non-critical features.
3. **One key gets 1M req/s?** Local L1 cache + key salting across shards + replica reads.
4. **Values of 10 MB?** Don't — cap value size; store in a blob store and cache a pointer, or chunk and compress.
5. **Why not just a bigger box?** RAM and NIC ceilings, single point of failure, long cold-start after restart.
6. **LRU vs LFU?** LRU is simple and adapts quickly; LFU resists one-off scans polluting the cache (scan resistance) —
   W-TinyLFU (Caffeine) combines both.
7. **Multi-key operations across shards?** Not atomic; use hash tags to co-locate keys in one slot, or do it in the app.
8. **Multi-region?** Independent cache per region (each fronting a local DB replica), invalidate cross-region via the
   replicated change stream; don't do synchronous cross-region cache calls.

---

## 10. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clear requirements, O(1) LRU with hash map + linked list, TTL handling, shards keys across nodes with hashing; knows modulo hashing breaks on resize |
| **Senior (E5)** | Consistent hashing / hash slots with vnodes, primary-replica async replication + failover, hot keys, connection pooling, stampede protection, cache-aside invalidation trade-offs |
| **Staff+ (E6)** | Drives deep dives: approximate LRU & memory overhead, split brain, rebalancing with live migration, multi-level caching & invalidation via CDC, hot writes, multi-region, operating metrics |

---

## 11. One-Minute Summary
> Each node is an in-memory **hash map + doubly linked list** giving O(1) get/set/delete and **LRU eviction**; TTLs
> expire **lazily on read** plus via a **sampled background sweep**. Keys are partitioned across ~30 shards (sized by
> memory: 1.3 TB / ~48 GB) using **consistent hashing / 16384 hash slots** so resizing moves only ~1/N of keys. A
> **smart client** keeps the slot map and pooled TCP connections and talks straight to the owning shard. Each shard has
> **async replicas in other AZs**; gossip/Sentinel detects failure and promotes a replica — AP, eventually consistent,
> lost writes are just misses. **Hot keys** get local L1 caches, salting and replica reads; **stampedes** are stopped
> with single-flight locks, TTL jitter and stale-while-revalidate. The database remains the source of truth.

---

### References
- Hello Interview, *Distributed Cache — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/distributed-cache
- AlgoMaster, *Distributed Caching* — https://algomaster.io/learn/system-design/distributed-caching

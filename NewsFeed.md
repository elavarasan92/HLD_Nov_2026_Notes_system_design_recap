# News Feed (Facebook / Instagram / Twitter Timeline) — System Design (Interview Guide)

> Design a service where users **create posts**, **follow** other users, and see a **feed** of posts from the people
> they follow (reverse chronological, infinite scroll) — for **billions of users**, with feeds loading in
> **< 500 ms**, even when someone follows thousands of accounts or is followed by millions.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 3–4) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates (→ fan-out cost, feed cache size) |
| 3 | 2 min | Core entities |
| 4 | 5 min | API design (cursor pagination) |
| 5 | 10 min | High-level design — post, follow, naive feed (fan-out on read), pagination |
| 6 | 15–20 min | Deep dives: fan-out on write vs read, **celebrity problem (hybrid)**, hot posts, caching layers |
| 7 | 5 min | Scaling, trade-offs, wrap-up |

**Key line to say early:** *"This is a **fan-out** problem. Reading is far more frequent than posting, so we want to
**precompute feeds on write** — but that breaks for **celebrities** with millions of followers, so the answer is a
**hybrid push/pull** model. I'll build a naive version first, then fix the scaling problems."*

---

## 1. Requirements

### Functional (top 4)
1. Users can **create posts** (text, plus media references).
2. Users can **follow** other users (uni-directional; "friend" = two follows).
3. Users can **view a feed** of posts from people they follow, **newest first**.
4. Users can **page** through their feed (infinite scroll).

### Below the line (mention, don't design)
- Likes, comments, shares; notifications.
- Ranked / ML-personalized feed (EdgeRank-style scoring), ads injection.
- Privacy / restricted visibility, mute/block (mention as a fan-out filter).
- Media upload/transcoding (assume pre-signed upload to blob store + CDN).

### Non-Functional
| Property | Target |
|----------|--------|
| Availability | **High** — prefer **availability over consistency** (AP) |
| Freshness | Eventual consistency; a new post may take **up to ~1 min** to appear in followers' feeds |
| Latency | Post creation and feed load **< 500 ms p99** (feed from cache: tens of ms) |
| Scale | **2B users**, ~1B DAU; read:write ≈ **100:1** |
| Graph | Unlimited following / followers (celebrities with **100M+** followers) |

### Out of scope
Auth internals (assume JWT with userId), spam/abuse ML, billing, CI/CD.

---

## 2. Capacity Estimates

- **Feed reads:** 1B DAU × ~10 feed loads/day ≈ 10B/day ≈ **~115k/s avg**, **~300k+/s peak**.
- **Posts:** 1B DAU × ~0.5 posts/day ≈ 500M/day ≈ **~6k/s avg**.
- **Fan-out writes (push):** avg ~200 followers → 500M × 200 ≈ **100B feed inserts/day ≈ ~1.2M/s** → must be async,
  batched, and must **skip celebrities**.
- **Precomputed feed storage:** keep last **~200–500 post IDs** per user; 200 × 8–10 B ≈ **2 KB/user** →
  2B users ≈ **4 TB** (only active users in Redis → ~1–2 TB across a cluster).
- **Posts:** 500M/day × ~1 KB (metadata + text, media in blob store) ≈ **0.5 TB/day** → ~180 TB/yr.

> Takeaway: reads are huge and latency-sensitive → **precompute**; fan-out writes are huge too → **async workers**,
> and celebrities **can't** be pushed.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **User** | id, name, avatar_url, follower_count, is_celebrity (derived) |
| **Follow** | follower_id, followee_id, created_at |
| **Post** | id (time-sortable, e.g. Snowflake), author_id, content, media_urls[], created_at |
| **Feed (precomputed)** | user_id → ordered list of post_ids (newest first, capped) |

---

## 4. API Design

```http
POST   /v1/posts                          # create post           → 201 { postId }
PUT    /v1/users/{id}/followers           # follow (idempotent)   → 204
DELETE /v1/users/{id}/followers           # unfollow              → 204
GET    /v1/feed?cursor={cursor}&limit=20  # read feed             → { items: Post[], nextCursor }
```

**Create post request**
```json
{ "content": "Hello world", "mediaIds": ["m_123"] }
```

**Feed response**
```json
{
  "items": [ { "postId": "1834...", "author": { "id": "u42", "name": "Ann", "avatarUrl": "..." },
               "content": "Hello world", "mediaUrls": ["https://cdn..."], "createdAt": "2026-11-14T10:00:00Z" } ],
  "nextCursor": "1833..."
}
```

- **userId comes from the JWT**, never from the body/URL (`/me` semantics).
- **PUT** for follow → idempotent (double-click safe).
- **Cursor pagination**, not offset: cursor = **oldest post id / timestamp seen**; next page = posts older than it.
  Stable under new inserts at the head (offset pagination would shift and duplicate items).
- Rate-limit posting per user (spam).

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **DynamoDB / Cassandra** | `posts`, `follows` | Massive write throughput, simple key/range access, horizontal scaling |
| **Redis cluster** | Precomputed **feed** lists, **post cache**, **user cache**, counters | Sub-ms reads, sorted sets / lists |
| **Kafka** | Post-created events → fan-out workers | Durable, partitioned, replayable async fan-out |
| **Blob store + CDN** | Images / videos | Media never flows through app servers |
| *(Optional)* Graph DB | Friend-of-friend recommendations | Not needed for 1-hop follow lookups — KV + index is enough |

### Tables
```text
posts        PK: post_id                       → author_id, content, media, created_at
             GSI: (author_id, created_at DESC) → "latest posts by author" (pull path & pagination fallback)

follows      PK: follower_id, SK: followee_id  → "whom do I follow?"  (range query) / "do I follow X?" (point)
             GSI: followee_id, follower_id     → "who follows X?"     (fan-out target list)

feed (Redis) key: feed:{user_id}  ZSET  member=post_id  score=created_at   (trimmed to last N ≈ 500)
```

> Post IDs are **time-sortable (Snowflake)** so `post_id` order == time order — handy as a pagination cursor and for
> merging lists.

---

## 6. High-Level Architecture

```
                         ┌──────────────┐
  Client ─────────────▶  │ API Gateway  │  auth (JWT), rate limit, routing
  (web / mobile)         │ + LB         │
                         └──┬────┬────┬─┘
                            │    │    │
              POST /posts   │    │    │  GET /feed
           ┌────────────────┘    │    └──────────────────────┐
           ▼                     ▼ PUT follow                ▼
   ┌───────────────┐     ┌───────────────┐          ┌──────────────────┐
   │ Post Service  │     │ Follow Service│          │ Feed Service     │
   └──┬─────────┬──┘     └──────┬────────┘          └──┬──────┬─────┬──┘
      │ 1.write │ 2.event       │                      │1.ids │2.hydrate  3.merge celeb posts
      ▼         ▼               ▼                      ▼      ▼     │
 ┌─────────┐ ┌───────┐   ┌─────────────┐      ┌────────────┐ ┌──────────────┐
 │ Posts DB│ │ Kafka │   │ Follows DB  │      │ Feed Cache │ │ Post / User  │
 │ (+GSI   │ └───┬───┘   │ (+reverse   │      │ (Redis     │ │ Cache (Redis)│
 │ author) │     │       │  GSI)       │      │  ZSETs)    │ └──────────────┘
 └─────────┘     ▼       └─────▲───────┘      └─────▲──────┘
           ┌──────────────┐    │ followers of X     │ ZADD + trim
           │ Fan-out      │────┘ (paged)            │
           │ Workers      │─────────────────────────┘
           └──────────────┘   skip if author is celebrity
```

> Separate **Post / Follow / Feed** services: very different load profiles (feed reads ≫ posts), scale independently.

### 6.1 Create a post
1. `POST /posts` → Gateway → **Post Service** → write to `posts` (and post cache).
2. Publish `PostCreated{postId, authorId, createdAt}` to **Kafka** (outbox / CDC so DB write and event can't diverge).
3. Return `201` immediately — fan-out is **async**.

### 6.2 Follow
1. `PUT /users/{id}/followers` → **Follow Service** → upsert `(follower, followee)`; increment follower count.
2. Optionally backfill the followee's recent posts into the follower's feed (async).

### 6.3 Naive feed (start here, then fix) — *fan-out on read*
1. Get everyone I follow (`follows` by `follower_id`).
2. For each, fetch recent posts via `posts` GSI `(author_id, created_at)` **older than cursor**.
3. Merge-sort by time, return top N.

> Say it out loud: *"This works for small graphs but a user following 5k accounts → 5k queries per feed load. Next I'll
> fix it by precomputing feeds."*

### 6.4 Pagination
Cursor = oldest `post_id` (time-sortable) the client has seen → `ZREVRANGEBYSCORE feed:{uid} (cursor -inf LIMIT 0 20`.
When the client scrolls past the cached window (~500 items), fall back to the naive pull path (rare).

---

## 7. Deep Dives

### 7.1 Fan-out on write vs fan-out on read (core problem)
| Model | How | Pros | Cons |
|-------|-----|------|------|
| **Fan-out on read (pull)** | Build feed at request time from followees' posts | No wasted work for inactive users; no write amplification; new follows instantly reflected | Slow reads (N queries for N followees); high read latency at scale |
| **Fan-out on write (push)** | On post, append post_id to every follower's precomputed feed | **Fast reads** (single cache lookup); real-time | Write amplification; **celebrity post → millions of writes**; wasted work for inactive users |
| **Hybrid (chosen)** | **Push** for normal users; **pull** for celebrities, merged at read time | Fast reads + bounded fan-out | More complex read path (merge) |

### 7.2 Users following many accounts (fan-out on read is too slow)
- Precompute a **feed per user** (Redis ZSET of post_ids, capped at ~200–500) → feed read = **1 cache read + hydrate**.
- Product lever: **cap follows** (Facebook caps friends at 5,000) or accept slightly delayed feeds for extreme followers.

### 7.3 Users with many followers — the **celebrity / hot-key** problem
Pushing one post from an account with 100M followers = 100M writes → fan-out queue backs up, everyone's feed lags.

| Option | Verdict |
|--------|---------|
| **Bad:** synchronous fan-out in Post Service | Post request takes minutes / times out |
| **Good:** async fan-out via **Kafka + worker fleet**, follower list paged in chunks (e.g. 1k followers per message) | Scales horizontally, retries, respects the ~1 min freshness SLA for normal users; still huge for celebrities |
| **Great:** **hybrid** — mark authors with > *K* followers (e.g. 100k) as **celebrities; don't fan them out**. At read time, Feed Service pulls recent posts of the (few) celebrities the user follows from the **post cache** (`posts:author:{id}` list) and **merges** with the precomputed feed | Bounded write amplification; celebrity posts are cached once and read by many |

Extra refinements:
- **Only fan out to active users** (logged in within last N days); inactive users' feeds are built on demand on next login.
- **Priority queues**: fan-out to recently active followers first.
- Celebrity status flips with hysteresis (avoid flapping around the threshold).

### 7.4 Hot posts — uneven reads
A celebrity's post is hydrated by millions of feed reads within minutes → hot partition in Posts DB / one Redis shard.
- **Post cache** in front of DB (posts are immutable-ish → cache with long TTL, invalidate on edit/delete).
- **Replicate hot keys** across *k* cache nodes (`post:{id}#r{0..k-1}`, client picks random replica) or a small
  **in-process LRU** on Feed Service instances for the hottest posts.
- Writes are rare (edits) → replication/invalidations are cheap.

### 7.5 Feed retrieval & hydration
1. `ZREVRANGE feed:{uid}` → post_ids (normal followees).
2. Fetch celebrity followees' recent post_ids from `posts:author:{celebId}` lists; **k-way merge** by post_id/time.
3. **Batch-hydrate** (`MGET`) posts and authors from Post/User caches (DB on miss); filter deleted/blocked/muted.
4. Media served via **CDN URLs**; return JSON + `nextCursor`.

### 7.6 Cache architecture (ByteByteGo's layers)
| Layer | Contents |
|-------|----------|
| **News feed** | `feed:{uid}` → post_ids |
| **Content** | Post objects; **hot cache** for viral posts |
| **Social graph** | Followers / following lists |
| **Action** | Has user liked / replied to a post |
| **Counters** | Like, reply, follower, following counts |

> Store **IDs only** in feed lists — hydrate from content caches. Keeps feed cache small and edits/deletes consistent.

### 7.7 Consistency & edge cases
- **Deleted post:** remove from Post cache/DB; feeds keep the stale id → **filtered at hydration** (lazy cleanup).
- **Unfollow:** filter at read time by the followee set, optionally async-remove entries from the feed.
- **New follow:** backfill last few posts of the followee into the follower's feed asynchronously.
- **Fan-out worker idempotency:** `ZADD` is idempotent by member → Kafka redelivery is safe.
- **Cache loss (Redis node down):** rebuild a user's feed on demand via the pull path, then repopulate.

---

## 8. Scaling Storage

| Component | Strategy |
|-----------|----------|
| Posts DB | Partition by `post_id`; GSI by `author_id`; archive old posts to cheaper storage |
| Follows DB | Partition by `follower_id`; reverse GSI by `followee_id`; page large follower lists |
| Feed cache | Redis cluster sharded by `user_id`; cap ~500 ids; only for active users; rebuild on miss |
| Post / user cache | Redis cluster; hot-key replication; long TTLs |
| Fan-out | Kafka partitioned by `author_id`; stateless worker fleet autoscaled on consumer lag |
| Services | Stateless, autoscale; multi-region with regional caches, async cross-region replication |

---

## 9. Observability
- **Metrics:** feed p50/p99, feed-cache hit ratio, hydration fan-out size, **fan-out lag** (post → visible in feed),
  Kafka consumer lag, celebrity-merge latency, hot-key QPS.
- **Alerts:** fan-out lag > freshness SLA (1 min), cache hit drop, hot shard CPU.
- **Tracing:** sample feed requests end to end (gateway → feed service → caches → DB).

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| **Hybrid push/pull** | Pure push or pure pull | Fast reads for everyone, bounded write amplification for celebrities |
| Async fan-out via Kafka | Synchronous fan-out | Post latency stays low; retries; horizontal scaling |
| Feed stores **post IDs only** | Full post objects | Small cache, edits/deletes consistent, one source of truth |
| Cursor (post_id) pagination | Offset | Stable under new inserts; O(log n) seek |
| KV store + reverse index for follows | Graph DB | Only 1-hop queries needed; KV scales simply |
| Eventual consistency (≤ 1 min) | Strong consistency | Availability + throughput; users don't notice small delays |
| Fan-out only to active users | Fan-out to all | Avoids wasted work on inactive users |

---

## 11. Common Follow-up Questions

1. **Celebrity threshold?** Follower count > K (e.g. 100k), tuned by fan-out cost; hysteresis to avoid flapping.
2. **User follows many celebrities?** Merge cost grows with #celebrities followed; cap merged sources, cache each
   celebrity's recent-post list, or periodically materialize "celebrity digest" into the user's feed.
3. **Ranked feed instead of chronological?** Candidate generation (same push/pull) → ranking service (ML scores
   features like affinity, recency, engagement) → re-rank top few hundred candidates per request.
4. **User scrolls past 500 items?** Fall back to pull path from Posts DB (rare, slower is acceptable).
5. **Post edited/deleted?** Update/invalidate post cache; feeds hold IDs, so hydration reflects it automatically.
6. **Redis loses a feed?** Rebuild on demand via fan-out-on-read for that user, then repopulate.
7. **Real-time "new posts" banner?** Long-poll / SSE / WebSocket notifying "N new posts" — client re-fetches the head.
8. **Multi-region?** Users pinned to a home region; posts replicated async; fan-out runs in each region for its users.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clean API + entities, working naive design, recognizes fan-out-on-read is slow, proposes precomputed feeds |
| **Senior (E5)** | Push vs pull trade-offs, async fan-out with queue + workers, **hybrid for celebrities**, cursor pagination, caching layers |
| **Staff+ (E6)** | Drives deep dives: hot-key replication, active-user-only fan-out, consistency edge cases (delete/unfollow/backfill), cost per user, ranked-feed extension, multi-region |

---

## 13. One-Minute Summary
> **Post Service** writes posts to a KV store (DynamoDB/Cassandra, with an `author_id + created_at` index) and emits a
> **Kafka** event. **Fan-out workers** page through the author's followers (reverse index on the follows table) and
> push the post ID into each **active** follower's **Redis feed** (capped ZSET of IDs) — except for **celebrities**,
> whose posts are **pulled at read time** and merged (**hybrid push/pull**). The **Feed Service** reads the precomputed
> IDs, merges celebrity posts, **batch-hydrates** from post/user caches (hot posts replicated), and returns a page with
> a **post-id cursor**. Everything is eventually consistent within ~1 min, stateless, and horizontally scalable.

---

### References
- ByteByteGo, *Design a News Feed System* —
  https://bytebytego.com/courses/system-design-interview/design-a-news-feed-system
- Hello Interview, *FB News Feed — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/fb-news-feed

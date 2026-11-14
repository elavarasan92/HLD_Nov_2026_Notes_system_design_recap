# Notification System (Push / SMS / Email / In-App) — System Design (Interview Guide)

> Design a platform service that lets internal services send notifications to users over **push (APNs/FCM), SMS,
> email and in-app** channels — honoring user preferences, quiet hours and rate limits, scheduling future sends, and
> fanning out **millions of messages in minutes** without losing or duplicating any.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 3–4) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates |
| 3 | 2 min | Core entities |
| 4 | 5 min | API design |
| 5 | 10–15 min | High-level design — one functional requirement at a time |
| 6 | 15 min | Deep dives: reliability, dedup/idempotency, fan-out, priority, rate limiting, scheduling, providers |
| 7 | 5 min | Scaling, trade-offs, wrap-up |

**Key line to say early:** *"This is a **write-heavy, asynchronous** pipeline. The API only **accepts** a request
durably and returns fast; delivery happens via **queues + channel workers**. We target **at-least-once delivery with
idempotent dedup**, so effectively-once to the user — exactly-once across third-party providers is impossible."*

---

## 1. Requirements

### Functional
1. Internal services can **send a notification** to a user (or list of users) on one or more channels:
   **push, SMS, email, in-app**.
2. **Templates & personalization** — `templateId` + variables, localized by user locale.
3. **User preferences** — opt-in/opt-out per channel and category (marketing vs transactional), **quiet hours**,
   unsubscribe.
4. **Scheduled & bulk sends** — "send at 9 AM user-local time", campaigns to a segment of millions.

### Below the line (mention, don't design)
- Delivery/open/click analytics dashboards, A/B testing of templates.
- Rich in-app inbox UI, read/unread sync across devices.
- Campaign builder UI / segmentation engine (assume it hands us a list or segment id).

### Non-Functional
| Property | Target |
|----------|--------|
| Reliability | **No lost notifications** once accepted (durable queue before ack) |
| Delivery semantics | At-least-once + **dedup** → user sees it once |
| Latency | Transactional (OTP, security alert) **< few seconds** end to end; marketing can lag minutes |
| Scale | ~1B notifications/day, bursts of **10M+ in minutes** for campaigns |
| Availability | High; one failing channel/provider must not block others |
| Ordering | Not globally required; best-effort per user where it matters |
| Respect user | Preferences, quiet hours, per-user frequency caps are **never** violated |

### Out of scope
Building our own SMTP/SMS gateway or push infrastructure (we use SES/SendGrid, Twilio, APNs/FCM), content moderation,
GDPR internals (mention data retention).

---

## 2. Capacity Estimates

- **1B notifications/day** ≈ **~12k/s avg**; campaign peaks **~100k–200k/s**.
- Channel mix (example): push 60%, in-app 20%, email 15%, SMS 5% → SMS ≈ 50M/day (**cost driver**, ~$0.01 each).
- Record per notification ≈ 1 KB (metadata + rendered payload ref) → **~1 TB/day**; keep hot 30 days, archive the rest.
- Device tokens: 500M users × ~2 devices ≈ **1B tokens** × ~200 B ≈ **200 GB**.
- Provider limits are the real ceiling: e.g. email provider N k/s per account, SMS per-number/per-country throughput,
  APNs/FCM connection & per-device limits → **throttling is a first-class concern**.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **Notification (request)** | id, idempotency_key, source_service, user_ids / segment_id, template_id, variables, channels, priority, category, scheduled_at |
| **Message (per user × channel)** | id, notification_id, user_id, channel, rendered content ref, **status** (PENDING / SENT / DELIVERED / FAILED / SUPPRESSED), attempts, provider_msg_id |
| **Template** | id, version, channel, locale, subject/body with placeholders |
| **UserPreference** | user_id, channel, category, enabled, quiet_hours, timezone, frequency cap |
| **Device / Contact** | user_id, push token + platform, email, phone, verified, last_seen |
| **DeliveryEvent** | message_id, event (SENT / DELIVERED / BOUNCED / OPENED / CLICKED), ts |

> **Notification ≠ Message.** One request fans out into many per-user, per-channel messages — each tracked, retried
> and deduplicated independently.

---

## 4. API Design

```http
POST   /v1/notifications                   # send now or scheduled (single user, list, or segment)
GET    /v1/notifications/{id}              # aggregate status
GET    /v1/notifications/{id}/messages     # per-user/channel status (paginated)
DELETE /v1/notifications/{id}              # cancel a scheduled/in-flight campaign

GET    /v1/users/{userId}/preferences      # read preferences
PUT    /v1/users/{userId}/preferences      # update opt-in/out, quiet hours
POST   /v1/users/{userId}/devices          # register/refresh push token
GET    /v1/users/{userId}/inbox?cursor=    # in-app notifications

POST   /v1/webhooks/{provider}             # delivery receipts / bounces from SES, Twilio, ...
```

**Send request**
```json
{
  "userIds": ["u-1", "u-2"],
  "templateId": "order_shipped",
  "variables": { "orderId": "o-42", "eta": "Tue" },
  "channels": ["push", "email"],
  "category": "transactional",
  "priority": "high",
  "scheduledAt": null
}
```
Header: `Idempotency-Key: <uuid>` → retries return the same `notificationId`.

**Response:** `202 Accepted { "notificationId": "n-789", "status": "QUEUED" }` — **never block on delivery.**

> Callers authenticate service-to-service (mTLS / OAuth client credentials) and get a **per-service quota** so a buggy
> producer can't flood users.

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **Kafka** | Ingestion + per-channel/per-priority topics, retry topics, DLQ | Durable, replayable, partitioned for throughput |
| **Cassandra / DynamoDB** | `messages`, `delivery_events`, in-app inbox | Huge write volume, partition by `user_id`, TTL for retention |
| **PostgreSQL** | Templates, preferences, notification requests, campaigns | Relational, low volume, strong consistency |
| **Redis** | Dedup keys, rate-limit/frequency counters, preference & template cache, device-token cache | Sub-ms ops, `SET NX EX`, atomic counters |
| **Object store (S3)** | Large rendered emails, attachments, archives | Cheap, keeps the messages table small |
| **Scheduler store** | Delayed jobs (time-bucketed table or Redis ZSET) | Efficient "due now" scans |

```sql
-- PostgreSQL
CREATE TABLE user_preferences (
    user_id     UUID NOT NULL,
    channel     TEXT NOT NULL,             -- push / sms / email / in_app
    category    TEXT NOT NULL,             -- transactional / marketing / social ...
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    quiet_start TIME,
    quiet_end   TIME,
    timezone    TEXT NOT NULL DEFAULT 'UTC',
    PRIMARY KEY (user_id, channel, category)
);

CREATE TABLE notifications (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    source_service  TEXT NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL,         -- QUEUED / SCHEDULED / FANNING_OUT / DONE / CANCELLED
    scheduled_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_service, idempotency_key)
);
```

```text
-- Cassandra
messages_by_user  (user_id, created_at DESC, message_id) -> channel, status, notification_id, content_ref   TTL 30d
messages_by_id    (message_id) -> user_id, channel, status, attempts, provider_msg_id
```

---

## 6. High-Level Architecture

```
 Internal services (Orders, Auth, Marketing ...)
          │  POST /notifications (Idempotency-Key)
          ▼
 ┌──────────────┐    ┌────────────────────┐   persist request    ┌──────────────┐
 │ API Gateway  │──▶ │ Notification API   │ ───────────────────▶ │ PostgreSQL   │
 │ authN, quota │    │ validate, dedup,   │                      └──────────────┘
 └──────────────┘    │ 202 Accepted       │── scheduled? ──▶ ┌──────────────┐
                     └─────────┬──────────┘                  │ Scheduler    │
                               │ publish (outbox)            │ (time buckets│
                               ▼                             │  / ZSET)     │
                     ┌────────────────────┐ ◀── due jobs ─── └──────────────┘
                     │ Kafka: ingest      │
                     └─────────┬──────────┘
                               ▼
                     ┌────────────────────┐   prefs, templates, devices
                     │ Fan-out / Router   │ ◀──────── Redis cache ◀── Postgres / Cassandra
                     │ - expand segment   │
                     │ - check prefs      │
                     │ - quiet hours/caps │
                     │ - render template  │
                     └─────────┬──────────┘
          ┌─────────────┬──────┴───────┬──────────────┐   per channel × priority topics
          ▼             ▼              ▼              ▼
   ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
   │ Push worker│ │ SMS worker │ │Email worker│ │In-app wrkr │   rate limit, retry, dedup
   └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
         ▼              ▼              ▼              ▼
     APNs / FCM    Twilio / SNS   SES / SendGrid   Inbox store + WebSocket/SSE gateway
         │              │              │
         └──────── delivery receipts / webhooks ────▶ Status Service ─▶ Cassandra (messages, events)
                                                                     ─▶ Kafka (analytics)
  Failures ─▶ retry topics (exponential backoff) ─▶ DLQ after N attempts
```

### 6.1 Send a single transactional notification
1. Service calls `POST /notifications` with `Idempotency-Key`.
2. **Notification API** validates, checks `(source_service, idempotency_key)` → if seen, returns the existing id.
3. Persists the request and **publishes to Kafka** (use the **transactional outbox** so DB row + event can't diverge),
   returns `202`.
4. **Router** consumes: loads preferences/contacts from cache, drops suppressed channels (opt-out, quiet hours for
   non-critical), renders the template per locale, emits one **Message** per user × channel to the channel topic.
5. **Channel worker** checks the dedup key, applies rate limits, calls the provider, records `SENT` + `provider_msg_id`.
6. Provider webhook → **Status Service** updates `DELIVERED` / `BOUNCED`; bounces & invalid tokens feed back into
   contact/device cleanup.

### 6.2 Scheduled notification
- `scheduledAt` set → store in the **Scheduler** instead of Kafka. A poller claims due buckets (e.g. per-minute buckets
  `SELECT ... WHERE bucket = :now_minute FOR UPDATE SKIP LOCKED`, or Redis `ZRANGEBYSCORE` + atomic claim) and
  publishes to the ingest topic.
- "9 AM **user-local** time" → bucket per timezone; the campaign is split into ~24+ timezone cohorts.

### 6.3 Bulk / campaign send
- Request carries a `segmentId`. Don't expand millions of users in one go: a **Fan-out service** pages through the
  segment (cursor), emitting **batches of ~1k users** as sub-jobs to Kafka → horizontally parallel and resumable
  (checkpoint the cursor).
- Campaign status = aggregate counters (Redis / periodic rollup), not a scan of millions of rows.

### 6.4 In-app notifications
- Write to the **inbox store** (Cassandra, partition `user_id`, sorted by time).
- If the user is online, push through a **WebSocket/SSE gateway** (connection registry in Redis: `user → gateway node`);
  otherwise the client pulls `GET /inbox` on next open. Unread badge = Redis counter.

---

## 7. Deep Dives

### 7.1 Reliability — never lose an accepted notification
| Option | Verdict |
|--------|---------|
| **Bad:** API calls providers synchronously | Provider slowness = API outage; lost on crash |
| **OK:** write DB, cron polls for pending rows | Durable, but DB becomes a hot queue; poll latency |
| **Great:** **outbox + Kafka**, consumers commit offsets **after** processing | Durable, replayable, horizontally scalable |

- Kafka `acks=all`, replication factor 3, `min.insync.replicas=2`.
- Workers are idempotent, so a crash after send but before offset commit → re-send is caught by **dedup**.
- **Retries** with exponential backoff + jitter via **tiered retry topics** (`email.retry.1m`, `.10m`, `.1h`) so retries
  don't block the main partition; after N attempts → **DLQ** + alert + replay tooling.
- Classify errors: **retryable** (5xx, timeout, 429) vs **permanent** (invalid token/number, hard bounce → mark
  contact invalid, don't retry).

### 7.2 Deduplication / idempotency
| Layer | Technique |
|-------|-----------|
| API | `UNIQUE(source_service, idempotency_key)` → same `notificationId` on retry |
| Fan-out | Deterministic `messageId = hash(notificationId, userId, channel)` |
| Worker | `SET dedup:{messageId} 1 NX EX 86400` in Redis before sending; skip if it exists |
| Provider | Pass `messageId` as provider idempotency/reference key where supported |
| Webhooks | Status transitions are monotonic (`SENT → DELIVERED`, never back); dedup on `provider_msg_id + event` |

> Exactly-once across an external provider is impossible (crash between "provider accepted" and "we recorded it").
> At-least-once + dedup shrinks duplicates to that tiny window.

### 7.3 Priority & isolation
- Separate topics/consumer groups per **priority** (`critical` OTP/security, `high` transactional, `low` marketing)
  and per **channel** → a 50M-user marketing blast never delays an OTP.
- Reserved worker capacity and provider quota for critical traffic; marketing workers can be paused/throttled.
- **Bulkheads per provider**: circuit breaker opens when Twilio degrades; the SMS backlog builds in Kafka without
  affecting email/push.

### 7.4 Rate limiting & user experience
- **Provider limits**: token bucket per provider account/region (Redis, or worker-local with central allocation) so we
  stay under SES/Twilio/APNs quotas and avoid getting throttled or blocked.
- **Per-user frequency caps**: e.g. ≤ 3 marketing pushes/day → Redis `INCR cap:{user}:{day}` with TTL; exceeded →
  `SUPPRESSED`.
- **Quiet hours**: non-critical messages in the user's quiet window are **deferred** to the scheduler, not dropped.
- **Per-source quotas** at the gateway protect users from a misbehaving internal service.
- **Collapse/aggregate**: "Alice and 12 others liked your post" — buffer social notifications for a short window and
  merge them (collapse key on push).

### 7.5 Fan-out at scale (10M+ users in minutes)
- Segment expansion in **paged batches**, checkpointed → restartable, parallel across partitions.
- Partition channel topics by `user_id` → per-user ordering and even spread.
- Workers autoscale on **consumer lag**; throughput is ultimately bounded by provider quotas → smooth the send over the
  allowed window instead of bursting.
- Pre-render shared template parts once; only substitute per-user variables.

### 7.6 Scheduling
| Option | Notes |
|--------|-------|
| **Bad:** `sleep` / in-memory timers in workers | Lost on restart, doesn't scale |
| **Good:** DB table indexed by `scheduled_at`, poll with `SKIP LOCKED` | Simple, durable; poll load grows with volume |
| **Great:** **time-bucketed** store (per-minute partitions in Cassandra/DynamoDB) or sharded **Redis ZSET**, claimed atomically | Cheap "what's due now" lookups, horizontally scalable |

Cancellation = mark the request `CANCELLED`; workers check status before sending (cheap cached lookup).

### 7.7 Third-party providers
- **Abstraction layer** per channel with multiple providers (e.g. SES + SendGrid) → **failover** on errors/latency,
  cost/region-based routing.
- Push tokens rot: remove on `Unregistered` / `410` from APNs/FCM; refresh on app open.
- Email deliverability: SPF/DKIM/DMARC, warmed IPs, separate IP pools for transactional vs marketing, honor
  **List-Unsubscribe**, auto-suppress hard bounces and spam complaints.
- SMS: per-country sender IDs/short codes, regulations (opt-in, STOP keyword), cost controls.

### 7.8 Preferences & templates
- Read-heavy → cache in Redis (invalidate on update via event); the preference check happens in the Router **before**
  rendering to avoid wasted work.
- Transactional/security messages may **bypass** marketing opt-outs but never channel-level legal opt-outs (e.g. SMS
  STOP).
- Templates are **versioned**; a message stores the version used, for auditability. Fall back to a default locale if a
  translation is missing.

---

## 8. Scaling Storage

| Component | Strategy |
|-----------|----------|
| Kafka | Topics per channel × priority, partitioned by `user_id`; scale partitions with consumer count |
| Cassandra / DynamoDB | Partition by `user_id` (inbox, history), TTL 30–90 days, archive to S3 for analytics |
| PostgreSQL | Small (templates, prefs, requests); read replicas + Redis cache; shard prefs by `user_id` if needed |
| Redis | Cluster; dedup keys & counters with TTL to bound memory |
| Workers | Stateless, autoscale on consumer lag; per-provider concurrency caps |

---

## 9. Observability
- **Metrics:** accepted vs sent vs delivered per channel/provider, end-to-end latency by priority, consumer lag,
  retry & DLQ rates, suppression reasons, bounce/complaint rates, provider error & throttle rates, cost per channel.
- **Alerts:** OTP latency SLO breach, DLQ growth, provider circuit open, bounce-rate spike (deliverability risk).
- **Tracing:** propagate `notificationId` / `messageId` from API → Router → Worker → provider → webhook.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Async accept (`202`) + queue | Synchronous send | Isolation from slow providers, absorbs bursts |
| At-least-once + dedup | Exactly-once | Exactly-once across external providers is not achievable |
| Kafka | RabbitMQ / SQS | Throughput, replay, partitioned ordering; SQS is fine if managed & simpler is preferred |
| Separate topics per channel & priority | One shared queue | Marketing blasts can't delay OTPs; independent scaling |
| Cassandra for messages/inbox | Postgres | Massive write volume, time-series per user, TTL |
| Defer in quiet hours | Drop | Better UX; message is still useful later |
| Multiple providers per channel | Single provider | Availability, cost and regional routing |

---

## 11. Common Follow-up Questions

1. **Provider is down?** Circuit breaker → fail over to the secondary provider; if none, messages wait in Kafka/retry
   topics; critical OTPs can fall back to another channel (SMS → voice/email).
2. **Worker crashes after the provider accepted but before recording?** Re-processed after restart; dedup key /
   provider idempotency key prevents most duplicates; rare duplicates are the accepted cost of at-least-once.
3. **How do you guarantee an OTP arrives in seconds during a 50M marketing blast?** Separate critical topic +
   dedicated workers + reserved provider quota.
4. **User changes preferences after a campaign was queued?** Re-check preferences at send time in the worker (cached),
   not only at fan-out.
5. **Ordering?** Partitioning by `user_id` gives per-user order within a channel; cross-channel ordering isn't
   guaranteed (and rarely needed).
6. **Send at 9 AM local time for every user?** Bucket by timezone; the scheduler releases each cohort when its local
   9 AM arrives.
7. **Cancel a campaign mid-flight?** Mark it cancelled; fan-out stops paging; workers drop messages for cancelled
   notifications.
8. **Avoid spamming users?** Frequency caps, aggregation/collapse, category-level opt-outs, quiet hours.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clean API + entities, async queue between API and channel workers, per-channel workers calling providers, basic retries |
| **Senior (E5)** | Durability (outbox, Kafka acks), end-to-end idempotency & dedup, priority isolation, preferences/quiet hours, scheduling, rate limiting, batched fan-out |
| **Staff+ (E6)** | Drives deep dives proactively: provider failover & deliverability, why exactly-once is impossible and how to mitigate, campaign-scale fan-out, cost control, multi-region, operational tooling (DLQ replay, observability) |

---

## 13. One-Minute Summary
> Internal services call a **Notification API** with an **idempotency key**; the request is persisted, published to
> **Kafka** via an outbox and acknowledged with **202**. A **Router/Fan-out** service expands users (batched for
> segments), applies **preferences, quiet hours and frequency caps**, renders **versioned templates**, and emits
> per-user, per-channel messages onto **channel × priority topics**. Stateless **channel workers** dedup in Redis,
> respect **provider rate limits**, call **APNs/FCM, Twilio and SES** (with failover and circuit breakers), and retry
> via **backoff topics** into a **DLQ**. Delivery receipts update status in **Cassandra**; in-app messages land in an
> inbox and are pushed over **WebSocket/SSE**. A **scheduler** handles delayed and timezone-aware sends. Result:
> **at-least-once with dedup**, OTPs isolated from marketing blasts, and horizontal scaling on consumer lag.

---

### References
- Hello Interview, *Notification System — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/notification-system
- AlgoMaster, *Design a Notification Service* —
  https://algomaster.io/learn/system-design-interviews/design-notification-service

# Distributed Task Scheduler — System Design (Interview Guide)

> Design a system (think AWS Step Functions / EventBridge Scheduler / Quartz cluster / Temporal-lite) that runs arbitrary
> tasks at a future time, **once** or **on a recurring (cron) schedule**, with no single point of failure and minimal
> duplicate execution.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Clarify functional + non-functional requirements |
| 2 | 3 min | Back-of-the-envelope estimates |
| 3 | 5 min | API design |
| 4 | 5 min | Data model + storage choices |
| 5 | 10 min | High-level architecture (submission path + execution path) |
| 6 | 15 min | Deep dives: leader election, dedup/idempotency, worker failure, priority, precision |
| 7 | 5 min | Scaling, sharding, monitoring, trade-offs |

**Key line to say early:** *"Exactly-once execution is impossible in a distributed system. I'll design for
**at-least-once delivery + idempotent tasks**, which gives effectively-once outcomes."*

---

## 1. Requirements

### Functional
- **Submit task** — one-off (run at time `T` / after delay) or recurring (cron expression).
- **Task state** — `PENDING → SCHEDULED → RUNNING → COMPLETED | FAILED | CANCELLED | DEAD`.
- **Cancel** an upcoming task.
- **Query** task status and **execution history** (audit log).
- **Retries** with backoff and a max-attempt limit; dead-letter after exhaustion.
- *(Optional)* Priorities, multi-tenancy, callbacks/webhooks on completion.

### Non-Functional
| Property | Target |
|----------|--------|
| Submission throughput | ≥ 10,000 tasks/sec |
| Execution burst | ≥ 50,000 tasks/sec (e.g., top-of-the-hour cron spike) |
| Availability | 99.99% |
| Durability | Zero task loss once acknowledged |
| Precision | Execute within ~200 ms of scheduled time |
| Guarantee | At-least-once execution, minimal duplicates |

### Out of scope (call it out)
- Long-running multi-step workflows with checkpoints → **Temporal / Step Functions**.
- Massive fan-out/fan-in (10k sub-tasks then aggregate) → workflow engine / Celery chord.
- Cross-service sagas with compensation → saga orchestrator, not a scheduler.

---

## 2. Capacity Estimates

- 10k submissions/s × 86,400 s ≈ **~864M tasks/day** at sustained peak (average will be lower).
- Task row ≈ 1 KB (metadata + payload) → **~0.9 TB/day** at peak → need **sharding + partitioning + archival**.
- Execution log row ≈ 200 B, several per task (start, heartbeat, end) → write-heavy → **wide-column store**.
- Redis timer window: only tasks due in the next few minutes are staged → e.g., 5 min × 50k/s = 15M entries × ~100 B
  ≈ **1.5 GB** — fits a small Redis cluster.

---

## 3. API Design

```http
POST   /v1/tasks                                            # create task
GET    /v1/tasks/{task_id}                                  # status
DELETE /v1/tasks/{task_id}                                  # cancel (only if not yet running)
GET    /v1/tasks/{task_id}/executions?cursor=...&limit=50   # paginated history
```

**Create request**
```json
{
  "name": "send-invoice-emails",
  "type": "ONE_OFF | RECURRING",
  "run_at": "2026-11-15T03:00:00Z",
  "cron": "0 3 * * *",
  "priority": "CRITICAL | HIGH | NORMAL | LOW",
  "payload": { "tenantId": "t-42", "batch": 7 },
  "target": { "kind": "HTTP | QUEUE | LAMBDA", "uri": "https://..." },
  "retry_policy": { "max_attempts": 3, "backoff": "EXPONENTIAL", "base_ms": 1000 },
  "timeout_sec": 300
}
```
Header: `Idempotency-Key: <uuid>` — a client retry of `POST` must not create two tasks.

**Response:** `201 Created { "task_id": "...", "status": "PENDING", "next_run_at": "..." }`

---

## 4. Data Model & Polyglot Persistence

| Store | Role | Why |
|-------|------|-----|
| **PostgreSQL** (sharded) | Source of truth: task definitions, schedules, state | ACID, row locks, `FOR UPDATE SKIP LOCKED`, partial indexes |
| **Redis** (cluster, Sorted Sets) | Near-term timer buffer, leader lock | ZSET score = epoch ms → `ZRANGEBYSCORE` is O(log N); atomic Lua |
| **Kafka / RabbitMQ / SQS** | Durable execution queue | Decouples scheduling from execution, absorbs bursts, visibility timeouts |
| **Cassandra** | Execution history / audit logs | Very high write throughput, time-series partitioning |

### PostgreSQL schema
```sql
CREATE TABLE tasks (
    id             UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id      TEXT NOT NULL,
    name           TEXT NOT NULL,
    payload        JSONB NOT NULL DEFAULT '{}',
    priority       SMALLINT NOT NULL DEFAULT 2,       -- 0=critical .. 3=low
    status         TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING/SCHEDULED/RUNNING/COMPLETED/FAILED/CANCELLED/DEAD
    run_at         TIMESTAMPTZ NOT NULL,              -- next run time
    cron_expr      TEXT,                              -- NULL for one-off
    attempt_count  INT NOT NULL DEFAULT 0,
    max_attempts   INT NOT NULL DEFAULT 3,
    worker_id      TEXT,
    started_at     TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    version        BIGINT NOT NULL DEFAULT 0,         -- optimistic locking
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    error          TEXT,
    PRIMARY KEY (id, run_at)                          -- partition key must be part of PK
) PARTITION BY RANGE (run_at);                         -- monthly partitions

-- Relay/worker poll: only pending rows
CREATE INDEX idx_tasks_runnable ON tasks (run_at, priority) WHERE status = 'PENDING';
-- Watchdog: only running rows
CREATE INDEX idx_tasks_stuck    ON tasks (last_heartbeat)   WHERE status = 'RUNNING';

-- One row per scheduled occurrence => dedup
CREATE TABLE task_executions (
    execution_id  TEXT PRIMARY KEY,   -- hash(task_id + scheduled window)
    task_id       UUID NOT NULL,
    scheduled_at  TIMESTAMPTZ NOT NULL,
    status        TEXT NOT NULL
);
```

### Cassandra (history)
```sql
CREATE TABLE execution_log (
    task_id      uuid,
    bucket       text,          -- e.g. yyyy-MM to bound partition size
    executed_at  timeuuid,
    attempt      int,
    worker_id    text,
    status       text,
    duration_ms  int,
    error        text,
    PRIMARY KEY ((task_id, bucket), executed_at)
) WITH CLUSTERING ORDER BY (executed_at DESC);
```

---

## 5. High-Level Architecture

```
                         ┌──────────────┐
  Client ──HTTPS──────▶  │ API Gateway  │  (auth, rate-limit per tenant)
                         └──────┬───────┘
                                ▼
                     ┌─────────────────────┐        ┌──────────────┐
                     │ Task Mgmt Service   │──────▶ │ PostgreSQL   │  source of truth
                     │ (validate, persist) │        │ (sharded)    │
                     └─────────────────────┘        └──────┬───────┘
                                                           │ poll tasks due in next N min
                                                           ▼
                                                  ┌─────────────────┐
                                                  │ Relay / Loader  │  (leader per shard)
                                                  └────────┬────────┘
                                                           │ ZADD score=run_at
                                                           ▼
                                                  ┌─────────────────┐
                                                  │ Redis ZSETs     │  time-bucketed timer
                                                  └────────┬────────┘
                                                           │ every ~100ms, Lua: pop score<=now
                                                           ▼
                                                  ┌─────────────────┐
                                                  │ Scheduler nodes │
                                                  └────────┬────────┘
                                                           │ publish (per tenant/priority topic)
                                                           ▼
                                                  ┌─────────────────┐
                                                  │ Kafka / SQS     │  durable exec queue
                                                  └────────┬────────┘
                                                           ▼
                                                  ┌─────────────────┐      ┌────────────┐
                                                  │ Worker pool     │────▶ │ Cassandra  │ exec logs
                                                  │ (heartbeats)    │      └────────────┘
                                                  └────────┬────────┘
                                                           │ status update (OCC version)
                                                           ▼
                                                       PostgreSQL
                     ┌──────────────┐
                     │ Watchdog     │  re-queues tasks with stale heartbeats
                     └──────────────┘
```

### Submission path
1. Client → API Gateway → **Task Management Service**.
2. Validate payload / cron, check `Idempotency-Key`.
3. Insert into PostgreSQL with `status = PENDING`, compute `run_at` (next cron fire for recurring).
4. Return `201`. *(If `run_at` falls inside the Redis window, push to Redis directly for low latency.)*

### Execution path
1. **Relay** periodically (e.g., every 30 s) queries Postgres for tasks with `run_at <= now + 5 min`, marks them
   `SCHEDULED`, and `ZADD`s them into Redis with `score = run_at epoch ms`.
2. **Scheduler nodes** poll Redis every ~100 ms with an **atomic Lua script** (`ZRANGEBYSCORE ... LIMIT` + `ZREM`)
   so two schedulers never pop the same task.
3. Popped tasks are published to **Kafka/SQS** (topic per tenant and/or priority).
4. **Workers** consume, execute, heartbeat, then mark `COMPLETED` / `FAILED` in Postgres and append to Cassandra.
5. For **recurring** tasks, compute the next `run_at` from the cron expression and write the next occurrence.

```lua
-- Atomic pop of due tasks: KEYS[1]=zset, ARGV[1]=now_ms, ARGV[2]=batch size
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due > 0 then redis.call('ZREM', KEYS[1], unpack(due)) end
return due
```

> **Why two tiers (Postgres → Redis)?** Polling a huge Postgres table every 100 ms doesn't scale. Postgres holds
> billions of future tasks durably; Redis holds only the *next few minutes*, giving sub-second precision cheaply.
> If Redis is lost, the relay reloads from Postgres — Postgres is always the source of truth.

---

## 6. Deep Dives

### 6.1 Single-node failure modes (why we distribute)
| Failure | Symptom | Fix |
|---------|---------|-----|
| **Dead scheduler** | Machine dies, cron silently stops | Multiple nodes + **leader election** |
| **Slow job** | Run takes longer than interval; next run overlaps | **Per-job lock** / skip-if-running |
| **Lost job** | Worker crashes mid-run, marked done on ACK | **Heartbeats**; complete only on explicit completion signal |

### 6.2 Leader election (who is allowed to schedule)
Needed for singleton components (relay per shard, cron expander). Use a **lease lock with TTL** in Redis / etcd /
ZooKeeper.

```
SET scheduler:leader <node_id> NX PX 30000     # acquire
renew every TTL/3 — only if value == node_id (check-and-extend in Lua)
```
- `NX` → atomic "only one winner".
- Renew at **1/3 TTL** → tolerates two missed renewals.
- Failover gap ≈ TTL (keep it 10–30 s).
- **On failed renewal, stop scheduling immediately** — before any further decision.
- **Fencing tokens:** a paused (GC / network) ex-leader may still act. Attach a monotonically increasing token
  (etcd revision / Redis `INCR`) to every write; storage rejects stale tokens.
- Prefer **etcd / ZooKeeper** (consensus-based) when correctness matters; a single-instance Redis lock is acceptable
  only because downstream work is idempotent.

> Alternative to one global leader: **partition ownership** — split the timer keyspace into N buckets, each with its
> own lease, so many schedulers work in parallel, each owning a subset.

### 6.3 Deduplication & idempotency
Duplicates come from: client retries, scheduler retries, clock skew, broker redelivery, worker crash after the side
effect but before ACK.

| Layer | Technique |
|-------|-----------|
| API | `Idempotency-Key` header → store key → return existing task |
| Scheduling | `execution_id = hash(task_id + scheduled_window)`; `INSERT ... ON CONFLICT DO NOTHING` |
| State transitions | **Optimistic concurrency**: `UPDATE ... SET status='RUNNING', version=version+1 WHERE id=? AND version=?` |
| Execution target | Pass `execution_id` as idempotency header to webhook / downstream service |
| Task code | Must be idempotent (check "already sent / already processed?") |

```sql
INSERT INTO task_executions (execution_id, task_id, scheduled_at, status)
VALUES ($1, $2, $3, 'PENDING')
ON CONFLICT (execution_id) DO NOTHING;   -- rowcount 0 => duplicate, skip enqueue
```
The key includes the scheduled window, so the same task can run again in the *next* window — dedup is per period,
not forever.

### 6.4 Worker fault tolerance (visibility timeout + heartbeats)
- Worker pulls a message → broker **hides it** for a *visibility timeout* and returns a **receipt handle**.
- Worker **heartbeats** (extends visibility / updates `last_heartbeat`) every ~10 s.
- On success → `ACK` / delete the message and mark `COMPLETED`.
- On crash → timeout expires → message becomes visible → another worker picks it up (**at-least-once**).
- **Watchdog** (DB-backed variant) runs every minute:

```sql
-- retry
UPDATE tasks SET status='PENDING', run_at=NOW(), worker_id=NULL, attempt_count=attempt_count+1
WHERE status='RUNNING' AND last_heartbeat < NOW() - INTERVAL '60 seconds' AND attempt_count < max_attempts;

-- dead letter
UPDATE tasks SET status='DEAD'
WHERE status='RUNNING' AND last_heartbeat < NOW() - INTERVAL '60 seconds' AND attempt_count >= max_attempts;
```
Rule of thumb: **timeout ≈ 5–6 × heartbeat interval** — too tight and you reap healthy-but-slow workers.

Retries use **exponential backoff + jitter**; exhausted tasks go to a **DLQ** with alerting.

### 6.5 Priority & starvation
- Separate queues/topics per priority: `CRITICAL > HIGH > NORMAL > LOW`; workers drain top-down
  (or weighted fair consumption, e.g., 8:4:2:1).
- **Priority aging:** tasks waiting beyond a threshold are bumped one level → prevents starvation.
- DB-backed worker poll without contention:

```sql
UPDATE tasks SET status='RUNNING', worker_id=$1, started_at=NOW()
WHERE id = (
  SELECT id FROM tasks
  WHERE status='PENDING' AND run_at <= NOW()
  ORDER BY priority, run_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

### 6.6 Precision & thundering herd
- Redis poll every **100 ms** + prefetch window → ~200 ms precision.
- Midnight / top-of-hour cron spikes → optional **jitter** (e.g., 0–30 s) for tasks that tolerate it.
- Kafka absorbs bursts; autoscale workers on **consumer lag**.
- **NTP-synced clocks**; never trust client time — compute `run_at` server-side in UTC.

### 6.7 Multi-tenancy
- Per-tenant rate limits at the gateway and per-tenant queues/topics → noisy-neighbour isolation.
- Shard key = `tenant_id`.

---

## 7. Scaling Storage

| Component | Strategy |
|-----------|----------|
| PostgreSQL | **Shard by `tenant_id`** across clusters; **monthly range partitions** on `run_at` keep B-tree indexes small; drop/archive old partitions |
| Redis | **Time-bucketed keys** (e.g., `timer:{shard}:{minute}`) spread across hash slots → no single hot ZSET blocking the single-threaded event loop |
| Kafka | Partition by `tenant_id` / `task_id`; partition count = max consumer parallelism |
| Cassandra | Partition by `(task_id, month)` to bound partition size; TTL for retention |
| Workers | Stateless; autoscale horizontally on queue lag |

---

## 8. Observability
- **Metrics:** schedule lag (`actual_start - run_at`) p50/p99, queue depth/lag, success/failure rate, retries, DLQ
  size, leader changes, stuck-task count.
- **Alerts:** lag > SLA, DLQ growth, no leader, spike in reaped tasks.
- **Tracing:** propagate `task_id` / `execution_id` across scheduler → broker → worker → downstream.
- **Duplicate detection:** count executions per `execution_id` > 1.

---

## 9. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| At-least-once + idempotency | Exactly-once | Exactly-once is impossible across crashes/partitions |
| Postgres + Redis two-tier | Postgres only (`SKIP LOCKED`) | Postgres-only is simpler and fine up to millions/day; Redis tier needed for 50k/s + 200 ms precision |
| Kafka between scheduler and workers | Direct dispatch | Buffering, replay, backpressure, decoupling |
| Lease-based leader | Consensus (Raft via etcd) | Lease is simpler; use etcd + fencing when duplicates are costly |
| Cassandra for logs | Postgres | Log write volume would bloat the OLTP database |
| Jitter | Exact time | Smooths spikes when precision isn't required |

**Simpler answer at smaller scale:** one Postgres table + Redis leader lock + `FOR UPDATE SKIP LOCKED` workers +
heartbeat watchdog handles millions of jobs/day with minimal infrastructure. Start there; add the Redis timer tier and
Kafka when throughput or precision demands it.

---

## 10. Common Follow-up Questions

1. **Redis crashes?** Rebuild from Postgres (`status IN ('PENDING','SCHEDULED') AND run_at <= now + window`).
2. **Scheduler pops from Redis but crashes before publishing to Kafka?** Task is still `SCHEDULED` in Postgres; a
   reconciliation sweep re-loads overdue `SCHEDULED` tasks that never reached `RUNNING`/`COMPLETED`.
3. **Cancel a task already in Redis/Kafka?** Mark `CANCELLED` in Postgres + `ZREM`; the worker checks status (with
   version) before executing and skips cancelled tasks.
4. **Avoid overlapping runs of a recurring job?** Per-task lock (`SET lock:task:{id} NX PX ...`) or a
   "skip if previous execution still RUNNING" policy.
5. **Clock skew?** NTP, server-side UTC, idempotency window keys, fencing tokens.
6. **Next cron run?** Parse cron in UTC (or the task's time zone, handling DST explicitly) and insert the next
   occurrence when the current one is scheduled.
7. **When to use Temporal instead?** Long-running checkpointed workflows, fan-out/fan-in, sagas.

---

## 11. One-Minute Summary
> Clients submit tasks via REST; definitions persist in **sharded PostgreSQL**. A leader-elected **relay** moves
> near-due tasks into **Redis sorted sets**; **schedulers** atomically pop due tasks every 100 ms via Lua and publish to
> **Kafka**. Stateless **workers** consume with visibility timeouts and heartbeats, update state with **optimistic
> locking**, and log history to **Cassandra**. A **watchdog** retries stuck tasks with backoff and dead-letters
> exhausted ones. We guarantee **at-least-once** execution and get effectively-once outcomes through **idempotency
> keys** at every layer.

---

### References
- Akhil Sharma, *Building a Distributed Job Scheduler* — Algoroq Blog (Apr 2026)
- *Distributed Task Scheduler: System Design Interview* — https://www.youtube.com/watch?v=zl8BPH74GY4

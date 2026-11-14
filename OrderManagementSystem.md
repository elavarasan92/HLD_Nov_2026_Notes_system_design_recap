# Order Management System (OMS) — System Design (Interview Guide)

> Design the order backbone of an e-commerce platform: users **place**, **view** and **cancel** orders; the system
> **reserves inventory**, **takes payment**, and drives each order through its lifecycle
> (`PLACED → PAID → SHIPPED → DELIVERED`) — **never double-charging, never overselling**, and surviving flash-sale spikes.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 3) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates |
| 3 | 2 min | Core entities + order state machine |
| 4 | 5 min | API design (incl. idempotency keys) |
| 5 | 10 min | High-level design — place order, cancel, status updates |
| 6 | 15–20 min | Deep dives: distributed transaction (Saga), outbox, idempotency, inventory/overselling, flash sales, read path |
| 7 | 5 min | Scaling, trade-offs, wrap-up |

**Key line to say early:** *"Placing an order spans **three services that each own their data** — Order, Inventory,
Payment — so there is no single ACID transaction. The hard parts are **keeping them consistent with a Saga**,
**never charging twice or overselling** under retries and spikes, and doing it with **reliable event publishing**."*

---

## 1. Requirements

### Functional (top 3)
1. Users can **place an order** (cart → order): stock is reserved and payment is captured.
2. Users can **view an order** and its **status** (`PLACED, PAID, SHIPPED, DELIVERED, CANCELLED, FAILED`).
3. Users can **cancel an order** before it ships (release stock, refund payment).

### Below the line (mention, don't design)
- Cart, catalog, search, pricing/promotions engine, tax calculation.
- Returns/RMA, partial shipments, split orders across sellers/warehouses.
- Fraud scoring internals, invoicing, loyalty points.

### Non-Functional
| Property | Target |
|----------|--------|
| Consistency | **Strong** for an order's state, stock counts and payments — **no oversell, no double charge** |
| Availability | **99.99%** for order placement and order view |
| Latency | Place order **< 500 ms p99** to "accepted"; order view **< 100 ms** |
| Scale | **10M orders/day**, **10–50× spikes** during sales events |
| Durability | An accepted order is **never lost** |
| Security | Auth on every call, PCI-DSS scope minimized (tokenized cards) |

### Out of scope
Warehouse management internals, carrier integrations, CI/CD, billing of sellers.

---

## 2. Capacity Estimates

- **Orders:** 10M/day ≈ **~115/s avg**; flash sale peak **~5–10k/s** (and far more *attempts* than successes).
- **Order reads** (status checks, order history): ~10:1 → **~1.2k/s avg**, tens of k/s at peak.
- **Storage:** order + ~3 items + events ≈ **~5 KB** → 10M × 5 KB ≈ **50 GB/day** ≈ **~18 TB/year** → shard + archive.
- **Events:** each order emits ~5–10 lifecycle events → **~1k events/s avg**, 100k/s peak → Kafka handles easily.
- **Hot items:** during a sale a few SKUs get most traffic → **inventory row contention**, not raw volume, is the bottleneck.

> Writes are modest on average; the design is driven by **correctness under retries/failures** and **spikes on hot SKUs**.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **Order** | order_id, user_id, status, total_amount, currency, idempotency_key, version, created_at |
| **OrderItem** | order_id, sku, quantity, unit_price |
| **Inventory** | sku, available, reserved |
| **Reservation** | reservation_id, order_id, sku, qty, status, expires_at |
| **Payment** | payment_id, order_id, amount, status, provider_ref, idempotency_key |
| **OrderEvent** (outbox) | event_id, aggregate_id, type, payload, created_at |

### Order state machine
```
            reserve OK         pay OK            ship             deliver
 PENDING ───────────▶ PLACED ─────────▶ PAID ─────────▶ SHIPPED ─────────▶ DELIVERED
    │                   │   │             │
    │ reserve fail      │   │ user cancel │ user cancel (before ship)
    ▼                   │   ▼             ▼
  FAILED ◀──────────────┘  CANCELLING ──────────▶ CANCELLED
          pay fail /             (refund + release stock)
          timeout
```

> Transitions are **validated in the domain** (`order.markPaid()` rejects if not `PLACED`) — that alone makes
> duplicate/out-of-order events harmless.

---

## 4. API Design

```http
POST   /v1/orders                      # place order        (Idempotency-Key header required)
GET    /v1/orders/{orderId}            # order details + status
GET    /v1/users/me/orders?cursor=...  # order history (paginated)
POST   /v1/orders/{orderId}/cancel     # cancel             (Idempotency-Key header)
```

**Place order request**
```http
POST /v1/orders
Authorization: Bearer <jwt>
Idempotency-Key: 7f6c1e2a-...
```
```json
{ "items": [{ "sku": "SKU-123", "quantity": 2 }], "shippingAddressId": "addr_9", "paymentMethodToken": "pm_tok_abc" }
```
**Response:** `202 Accepted { "orderId": "ord_01H...", "status": "PENDING" }` — client polls `GET` or gets a push/webhook.

- **`202` + async** rather than a synchronous `201 PAID`: payment providers can take seconds; we don't hold
  connections or threads across three services.
- Prices are **recomputed server-side** — never trust client totals.
- `user_id` comes from the JWT, never from the body; `GET` checks ownership (or admin role).
- Cancel returns `409` if the order is already `SHIPPED`.

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **PostgreSQL** (Order DB) | orders, order_items, outbox, saga state | ACID within the service, constraints, optimistic locking |
| **PostgreSQL** (Inventory DB) | stock per SKU, reservations | Atomic conditional updates on stock |
| **PostgreSQL** (Payment DB) | payments, ledger | Auditability, strong consistency |
| **Kafka** | Domain events between services | Durable, ordered per key, replayable |
| **Redis** | Order-view cache, idempotency responses, hot-SKU stock counters for sales | Sub-ms reads, atomic ops |
| **Elasticsearch / read DB** | Order history & search (CQRS read model) | Rich queries without loading the write DB |

> **Each service owns its database** — no cross-service joins or shared tables; integration only via APIs and events.

```sql
-- Order Service
CREATE TABLE orders (
    order_id         UUID PRIMARY KEY,
    user_id          UUID          NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    total_amount     NUMERIC(12,2) NOT NULL,
    currency         CHAR(3)       NOT NULL,
    idempotency_key  VARCHAR(64)   NOT NULL,
    version          BIGINT        NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, idempotency_key)               -- dedups client retries
);
CREATE INDEX idx_orders_user_created ON orders (user_id, created_at DESC);

CREATE TABLE order_items (
    order_id    UUID REFERENCES orders(order_id),
    sku         VARCHAR(32)   NOT NULL,
    quantity    INT           NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE outbox (
    event_id     UUID PRIMARY KEY,
    aggregate_id UUID        NOT NULL,
    type         VARCHAR(64) NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Inventory Service
CREATE TABLE inventory (
    sku        VARCHAR(32) PRIMARY KEY,
    available  INT NOT NULL CHECK (available >= 0),   -- final guard against oversell
    reserved   INT NOT NULL DEFAULT 0
);
CREATE TABLE reservations (
    reservation_id UUID PRIMARY KEY,
    order_id       UUID        NOT NULL,
    sku            VARCHAR(32) NOT NULL,
    quantity       INT         NOT NULL,
    status         VARCHAR(16) NOT NULL,              -- HELD, CONFIRMED, RELEASED
    expires_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (order_id, sku)                            -- idempotent reserve
);
```

---

## 6. High-Level Architecture

```
  Web / Mobile (JWT)
        │
        ▼
 ┌──────────────┐   ┌──────────────────────────┐
 │ Load Balancer│──▶│ API Gateway              │ authN (JWT), rate limit, routing
 └──────────────┘   └───┬──────────────────┬───┘
                        │ commands         │ queries
                        ▼                  ▼
              ┌──────────────────┐   ┌──────────────────┐     ┌───────────────┐
              │ Order Service    │   │ Order Query Svc  │◀────│ Redis / ES    │ read model
              │ (saga orchestr.) │   └──────────────────┘     └───────▲───────┘
              └──┬───────────┬───┘                                    │ projections
                 │ tx        │ outbox relay (CDC)                     │
                 ▼           ▼                                        │
          ┌──────────┐  ┌─────────────────────────── Kafka ───────────┴──────────────┐
          │ Order DB │  │ order-events · inventory-events · payment-events           │
          └──────────┘  └──┬──────────────────┬──────────────────┬──────────────────┘
                           ▼                  ▼                  ▼
                 ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
                 │ Inventory Svc    │ │ Payment Svc      │ │ Notification Svc │ email / SMS / push
                 │ + Inventory DB   │ │ + Payment DB     │ └──────────────────┘
                 └──────────────────┘ └────────┬─────────┘
                                               ▼
                                      Payment provider (Stripe/Adyen)
                                      ── webhook ──▶ Payment Svc
```

| Component | Responsibility |
|-----------|----------------|
| **API Gateway** | Single entry point: JWT validation, rate limiting, routing |
| **Order Service** | Creates orders, owns the state machine, **orchestrates the saga** |
| **Inventory Service** | Reserves / confirms / releases stock |
| **Payment Service** | Authorizes / captures / refunds via provider (tokenized cards only) |
| **Notification Service** | Consumes events, sends email/SMS/push — never blocks the order flow |
| **Order Query Service** | Serves order view & history from a read model (CQRS) |

### 6.1 Place an order (happy path)
1. Client `POST /orders` with `Idempotency-Key` → Gateway (auth, rate limit) → **Order Service**.
2. Order Service recomputes prices, inserts order `PENDING` **+ outbox event `OrderCreated`** in **one local
   transaction**; returns `202`.
3. Saga step 1 — **Inventory** reserves stock (`HELD`, TTL ~15 min) → emits `StockReserved`. Order → `PLACED`.
4. Saga step 2 — **Payment** authorizes/captures with the provider (idempotency key = order_id) → `PaymentSucceeded`.
5. Order → `PAID`; Inventory confirms reservation (`HELD → CONFIRMED`); fulfillment is triggered.
6. **Notification** consumes `OrderPaid` and emails the user — asynchronously.

### 6.2 Cancel an order
1. `POST /orders/{id}/cancel` → Order Service checks state (`PLACED`/`PAID` only; `SHIPPED` → `409`).
2. Order → `CANCELLING`; saga runs **compensations**: `RefundPayment` (if paid), `ReleaseStock`.
3. When both acknowledge → `CANCELLED`, notification sent.

### 6.3 Status updates (shipping)
- Fulfillment/carrier integrations publish `Shipped` / `Delivered` → Order Service applies the transition
  (version-checked) → projection updates the read model → user notified.

---

## 7. Deep Dives

### 7.1 Distributed transaction: Saga (core problem)
| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** synchronous REST chain, no compensation | Order → Inventory → Payment over HTTP | Partial failures leave reserved stock / charged cards with no order; cascading latency/failures |
| **Bad:** **2PC / XA** across services | Coordinator locks all DBs | Blocking, coordinator SPOF, unsupported by payment providers and Kafka; kills availability |
| **Good:** **Choreography saga** | Each service reacts to the previous event | Loose coupling, no central brain; flow is implicit and hard to follow/debug past 3–4 steps |
| **Great:** **Orchestration saga** | Order Service (or Temporal/Camunda) sends commands, tracks saga state, triggers compensations | Explicit flow, one place for timeouts/retries; orchestrator must be durable (state in Order DB) |

**Compensations** (semantic undo, in reverse order):

| Step | Action | Compensation |
|------|--------|--------------|
| 1 | Create order `PENDING` | Mark `FAILED` / `CANCELLED` |
| 2 | Reserve stock | Release reservation |
| 3 | Capture payment | Refund |

- **Payment fails** → release stock, order `FAILED`, notify user (retry with another card = new attempt).
- **Inventory fails** (out of stock) → order `FAILED`; nothing to refund because payment hasn't happened yet.
- Order steps so the **hardest-to-undo** step (payment capture) runs **last**; consider **authorize first, capture
  on ship** so cancellation becomes a cheap void instead of a refund.
- **Timeouts:** saga state has a deadline; if no reply (e.g. lost provider webhook) → query provider status, then
  continue or compensate. Reservations auto-expire as a backstop.

### 7.2 Reliable events: Transactional Outbox
Problem: "save order, then publish to Kafka" — a crash between the two loses the event (or publishes for a rolled-back order).
- Write the event to an **`outbox` table in the same DB transaction** as the state change.
- A **relay** publishes outbox rows to Kafka — **CDC (Debezium)** tailing the WAL (preferred), or a poller.
- Delivery is **at-least-once** → every consumer must be **idempotent** (7.3).
- Kafka key = `order_id` → all events for an order are **ordered** within a partition.

### 7.3 Idempotency (no double orders, no double charges)
- **Client → Order Service:** `Idempotency-Key` header; `UNIQUE (user_id, idempotency_key)` — a retry returns the
  existing order instead of creating a new one. (Optionally cache the response in Redis for 24 h.)
- **Consumers:** store processed `event_id`s (`processed_events` table, same tx as the side effect) and skip
  duplicates; or rely on natural keys (`reservations UNIQUE (order_id, sku)`).
- **State machine guards:**
  `UPDATE orders SET status='PAID', version=version+1 WHERE order_id=? AND status='PLACED' AND version=?`
  → 0 rows = duplicate/out-of-order event, ignore.
- **Payment provider:** pass `order_id` as the provider's idempotency key — retries never create a second charge.

### 7.4 Inventory & overselling
| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** read stock, check in app, then write | `SELECT` → `if (avail >= q)` → `UPDATE` | Race condition → oversell |
| **Good:** pessimistic lock | `SELECT ... FOR UPDATE` | Correct, but serializes all buyers of a hot SKU |
| **Great:** **atomic conditional update** | `UPDATE inventory SET available = available - :q, reserved = reserved + :q WHERE sku = :sku AND available >= :q` | Single statement, no app-level race; 0 rows → out of stock; `CHECK (available >= 0)` as final guard |

- **Reserve, don't decrement:** stock moves `available → reserved` with a **TTL** (`expires_at`); a sweeper releases
  expired `HELD` reservations (abandoned/failed payments) so stock isn't stuck.
- Multi-item orders: reserve all lines in one local transaction (all-or-nothing within Inventory DB) — sort SKUs to
  avoid deadlocks.

### 7.5 Flash sales (hot SKUs, 10–50× spikes)
- **Admission control first:** rate limit at the gateway, a **virtual waiting room / queue** for sale pages.
- **Redis stock counter** for the hot SKU: `DECRBY` via Lua (`if stock >= q then decr`) — rejects the 99% of losers
  in sub-ms without touching the DB; winners proceed to the real reservation (DB remains source of truth; reconcile).
- **Shard the counter** (split 1,000 units into 10 buckets of 100) to spread a single hot key.
- **Buffer writes:** accepted orders go to Kafka; Order/Inventory consumers drain at a steady rate (backpressure)
  instead of the DB absorbing the spike.
- Pre-scale pods (Kubernetes HPA + scheduled scale-up before known sales); pre-warm caches.

### 7.6 Read path: order view & history (CQRS)
- `GET /orders/{id}` → **Redis cache-aside** (short TTL, invalidated/updated on each state-change event).
- Order history and admin search served from a **read model** (Elasticsearch or a denormalized Postgres replica)
  built by consuming order events — eventually consistent by ~seconds.
- **Read-your-writes:** right after placing, read from the write DB (or return the order in the `202` body) so the
  user never sees "order not found".
- DB **read replicas** for anything else read-heavy.

### 7.7 Payment integration
- Never store raw card data — client tokenizes with the provider (**PCI scope minimized**).
- Provider calls are slow/flaky: timeouts + **retries with backoff** + **circuit breaker**; outcome confirmed by
  **webhook** (verify signature) and periodic reconciliation against the provider's report.
- Payment Service keeps an append-only **ledger** of auth/capture/refund for audit.

### 7.8 Failure handling summary
| Failure | Handling |
|---------|----------|
| Order Service crashes mid-saga | Saga state persisted in Order DB; on restart, resume from last step |
| Kafka unavailable | Outbox keeps events; relay publishes when Kafka recovers |
| Duplicate / out-of-order event | Idempotent consumers + version-checked state transitions |
| Payment webhook lost | Saga timeout → poll provider → continue or compensate |
| Consumer keeps failing on a message | Bounded retries → **DLQ** + alert; no blocking of the partition |
| Stock reserved but order abandoned | Reservation TTL + sweeper releases it |

---

## 8. Scaling

| Component | Strategy |
|-----------|----------|
| Order DB | Shard by **`user_id`** (order history = single-shard query); `order_id` embeds shard id; archive old orders to cold storage |
| Inventory DB | Shard by **`sku`**; hot SKUs handled via Redis counters + bucketed stock |
| Payment DB | Shard by `order_id`; modest volume, prioritize consistency |
| Services | Stateless, autoscaled independently on Kubernetes (Order Service scales separately from Payment) |
| Kafka | Partition by `order_id`; consumers scale up to partition count |
| Read side | Redis cluster + Elasticsearch scaled independently of write DBs |
| Multi-region | Active-active per user's home region; inventory per warehouse region |

---

## 9. Observability & Security
- **Metrics:** order placement p99, saga success/failure/compensation rate, time in each state, oversell attempts
  (0-row updates), payment provider latency/error rate, outbox lag, Kafka consumer lag, DLQ depth.
- **Alerts:** orders stuck in `PENDING`/`PLACED` beyond SLA, stock/payment reconciliation mismatches, DLQ growth.
- **Tracing:** correlation id = `order_id` propagated through HTTP headers and Kafka message headers.
- **Security:** JWT auth at the gateway, role-based authorization (customer vs support vs admin), ownership checks on
  every order read, HTTPS/mTLS between services, tokenized payments, audit log of state changes.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Orchestration saga | 2PC / choreography | Available, explicit flow and compensations; cost = orchestrator complexity |
| Transactional outbox + CDC | Dual write (DB then Kafka) | No lost or phantom events |
| `202` + async status | Synchronous `201 PAID` | Payment latency doesn't hold threads/connections; absorbs spikes |
| Atomic conditional stock update | `SELECT FOR UPDATE` / app check | No oversell without serializing all buyers |
| Reserve with TTL | Decrement on order | Abandoned/failed orders free stock automatically |
| Authorize → capture later | Capture immediately | Cancellation is a cheap void, not a refund |
| CQRS read model | Query write DB | Order history at scale without loading OLTP DB; cost = seconds of staleness |
| Shard orders by `user_id` | By `order_id` | History is single-shard; lookup by id uses id-embedded shard |
| Database per service | Shared DB | Independent scaling and deployments; cost = eventual consistency |

---

## 11. Common Follow-up Questions

1. **User double-clicks "Place order"?** Same `Idempotency-Key` → unique constraint returns the existing order.
2. **Payment succeeds but Order Service crashes before marking `PAID`?** Event is in Kafka (at-least-once); on restart
   the consumer re-processes it; state guard makes it idempotent.
3. **Why not 2PC?** Blocks on coordinator failure, holds locks across network calls, and external providers/Kafka
   don't participate in XA.
4. **Customer cancels while payment is in flight?** Order goes `CANCELLING`; when `PaymentSucceeded` arrives the
   orchestrator immediately issues a refund/void and releases stock.
5. **How do you prevent oversell in a flash sale?** Redis pre-check to shed load + atomic conditional `UPDATE` with
   `CHECK (available >= 0)` as the source of truth.
6. **How does the user learn the order status?** Poll `GET /orders/{id}` or receive push/WebSocket/email driven by
   Notification Service consuming order events.
7. **Partial stock (2 of 3 items)?** Product decision: fail the whole order (simpler) or split/back-order — mention,
   don't design.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clean APIs, service split (Order/Inventory/Payment/Notification), DB per service, status lifecycle, basic async via Kafka |
| **Senior (E5)** | Saga with compensations, transactional outbox, idempotency keys + idempotent consumers, atomic stock reservation with TTL, caching, sharding by user |
| **Staff+ (E6)** | Drives deep dives: orchestration vs choreography trade-offs, flash-sale admission control & hot keys, auth/capture strategy, reconciliation, multi-region, failure-mode analysis |

---

## 13. One-Minute Summary
> An **Order Service** accepts orders with an **idempotency key**, writes the order `PENDING` plus an **outbox event**
> in one local transaction and returns `202`. It **orchestrates a Saga**: **Inventory** atomically reserves stock with a
> TTL (conditional `UPDATE ... WHERE available >= q`, so no oversell), then **Payment** authorizes/captures through a
> tokenized provider using the order id as its idempotency key. Failures trigger **compensations** (release stock,
> refund) and the order ends `FAILED`/`CANCELLED`. Events flow through **Kafka** (keyed by `order_id`, at-least-once,
> idempotent consumers), feeding **Notification** and a **CQRS read model** (Redis/Elasticsearch) for fast order views.
> Each service **owns its database**; orders shard by `user_id`, inventory by `sku`, and flash sales are absorbed by
> gateway rate limits, waiting rooms and Redis stock counters in front of the DB.

---

### References
- Harjeet Kaur, *Design an Order Management System (OMS)* —
  https://medium.com/@kaurharjeet122/design-an-order-management-system-oms-f6244aa8023d
- Chris Richardson, *Saga pattern* — https://microservices.io/patterns/data/saga.html
- Chris Richardson, *Transactional outbox* — https://microservices.io/patterns/data/transactional-outbox.html

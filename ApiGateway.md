# API Gateway — System Design (Interview Guide)

> An **API gateway** is the single, client-facing front door to a set of backend services. Clients call the gateway;
> the gateway **validates** the request, applies shared **middleware** (auth, rate limiting, TLS, logging), **routes**
> it to the right service, shapes the response, and returns it — so the public API stays stable while the backend
> changes behind it.

**Hotel analogy:** guests talk to the front desk, not to housekeeping, maintenance, and the kitchen separately. The
front desk knows who to call. The gateway is that front desk.

Two ways this shows up in interviews:
1. **As a component** in a bigger design (Uber, Ticketmaster, News Feed…) — draw it, say one sentence, move on.
2. **As the question itself** ("Design an API gateway") — then you need the internals below (sections 3–8).

---

## 0. How to Use It in an Interview

**Key line to say (component usage):** *"I'll put an API gateway in front of the services to handle **routing** and
basic middleware — auth, rate limiting, TLS termination — and move on."*

- The #1 reason for a gateway is **request routing**. Candidates often list middleware and forget to say this.
- It's a thin, boring component. You are far more likely to **lose time** on it than to under-explain it.
- One box labelled **"API Gateway + LB"** is usually enough; don't split client-LB / gateway / service-LB unless asked.

| Use a gateway when… | Skip it when… |
|---------------------|---------------|
| Microservices architecture (many services behind one API) | Single monolith behind a load balancer |
| Multiple client types (web, mobile, partners) | One trusted client |
| Shared front-door rules repeated across services | Gateway would just forward everything unchanged |
| Public API must stay stable while backend splits/renames | Team can't operate another critical component well |
| Public protocol ≠ internal protocol (HTTPS ↔ gRPC) | |

---

## 1. Why a Gateway? (The Problem Without One)

Example: a product page needs catalog, pricing, inventory, recommendations, and cart. Without a gateway:

| Problem | What goes wrong |
|---------|-----------------|
| **Clients know too much** | Client must know `checkout-service` was split into `order`/`payment`/`fulfillment`. Every backend refactor can force a mobile release — and old app versions live for months. |
| **Duplicated front-door logic** | Every public service re-implements auth, rate limits, CORS, validation, request IDs → they drift (one accepts expired tokens, another logs PII). |
| **Chatty clients** | 5–7 calls per screen on a mobile network = more TLS handshakes, timeouts, retries, half-loaded screens. |
| **Protocol mismatch** | Clients want HTTPS/JSON; services use gRPC, queues, streams. |

---

## 2. Core Responsibilities

| Responsibility | What it means | Interview relevance |
|----------------|---------------|---------------------|
| **Routing** | Map `/api/orders` → order-service | ⭐ Core reason it exists |
| **Authentication** | Verify JWT (issuer, audience, expiry), API keys, mTLS, OAuth2/OIDC | ⭐ High |
| **Rate limiting / quotas** | Per user / tenant / API key / IP / route; return `429` | ⭐ High |
| **IP allow/deny lists** | Block abusive sources early | ⭐ High |
| **TLS termination** | HTTPS ends at the gateway; plain/mTLS inward | Medium |
| **Request validation** | Headers, content-type, body size/shape, API version | Medium |
| **Protocol translation** | JSON/HTTP ↔ gRPC | Low (uncommon in practice) |
| **Response shaping** | Strip internal fields, standard error format, rename fields across versions | Medium |
| **Caching** | Safe, repeatable, non-user-specific reads | Medium |
| **Observability** | Logs, metrics, traces, request IDs | Medium |
| **Traffic control** | Canary (5% to v2), blue/green, regional routing, timeouts | Medium |
| Others | Compression, CORS, response timeouts, service discovery | Mention only if relevant |

> **Gateway authorizes coarsely, services authorize finely.** The gateway knows the user is signed in and may call
> the orders API; the **order service** must still decide whether that user can read order `12345`.

---

## 3. Request Lifecycle

```
Client ──HTTPS──▶ [LB] ──▶ API Gateway ─────────────────────────────────────────▶ Backend service
                           1. Validate  (URL, headers, body size/shape, version)
                           2. Middleware (TLS, authN, rate limit, IP filter, logging)
                           3. Route     (path + method + headers + query → service)
                           4. Call backend (optionally translate HTTP → gRPC)
                           5. Transform response (format, strip fields, std errors)
                           6. Cache (optional) + emit metrics/traces
Client ◀───────────────────────────── response ◀────────────────────────────────
```

Order matters: **cheap rejections first** (malformed request, bad token, over-limit) so bad traffic never consumes
backend capacity.

**Worked example — food delivery `POST /orders`:**
1. Gateway checks body size, content type, required headers, API version.
2. Validates the user's JWT.
3. Applies the order-creation rate limit (write paths get tighter limits).
4. Routes to order-service.
5. **Order service owns the business workflow** — pricing, inventory reservation, payment, driver assignment.
6. Gateway returns the response in the public format; records metrics and trace.

---

## 4. Routing

The gateway keeps a **routing table** matched on path, method, host, headers, and query params.

| Public request | Backend |
|----------------|---------|
| `GET /api/products/{id}` | catalog-service |
| `POST /api/orders` | order-service |
| `GET /api/users/me` | profile-service |
| `POST /api/chat/completions` | ai-orchestration-service |

```yaml
routes:
  - match: { path: /api/users/*, methods: [GET, PUT] }
    upstream: user-service
  - match: { path: /api/orders/*, header: { X-Client-Version: "12" } }
    upstream: order-service-v2
    weight: 5            # canary: 5% of matching traffic
  - match: { host: partner.example.com }
    upstream: partner-api
```

Advanced routing: by host (`api.` vs `partner.`), by header/version, tenant → home region, weighted canaries,
streaming requests only to streaming-capable backends.

**Implementation note (if designing one):** compile routes into a **prefix trie** (or radix tree) per host for
O(path length) lookup; resolve upstream instances via **service discovery** (Consul / k8s / Eureka) cached locally.

> Routing rules are **production code**: review, test, validate, stage rollouts, keep rollback fast. A one-line
> route change can take down the whole API.

---

## 5. Caching at the Gateway

| Good candidates | Bad candidates |
|-----------------|----------------|
| Public product metadata, anonymous catalog pages | User-specific data, secrets |
| Feature-flag bootstrap, static config | Checkout / payment state |
| Expensive read-only partner endpoints | Personalized (incl. AI) responses |

Strategies: **full-response** caching, **partial** caching, invalidation via **TTL** or **events**. Store in-memory
(per instance) or in a shared cache (Redis).

> ⚠️ Cache key must include everything that changes the response — **tenant ID, auth scope, locale, version**.
> A missing tenant ID in the key leaks data across customers.

---

## 6. Scaling & High Availability

### Horizontal scaling
Gateways are **stateless** → add instances behind a load balancer. Shared state (rate-limit counters, cache) lives in
Redis, not in the gateway.

| Layer | Who load-balances |
|-------|-------------------|
| Client → Gateway | Dedicated LB in front (AWS ELB/ALB, NGINX, HAProxy) |
| Gateway → Service | The gateway itself, across healthy service instances |

### Global distribution
1. **Regional deployments** — gateways in multiple regions, close to users (CDN-like).
2. **GeoDNS / anycast** — route users to the nearest gateway.
3. **Config sync** — routes and policies consistent across regions (versioned config, pushed from a control plane).

### Not a single point of failure
- Multiple instances across **multiple AZs**, health checks, autoscaling.
- **Control plane / data plane split**: config store + admin API (control) pushes config to stateless proxies (data).
  If the control plane dies, proxies keep serving with last-known-good config.
- **Fail closed for security** (can't verify token → reject). **Fail predictably for traffic** (clear 503/504, not
  random behavior). Rate limiter dependency down → usually fail open with local fallback limits.
- Per-upstream **timeouts, circuit breakers, bulkheads** so one slow service can't exhaust gateway connections.

### Retries need idempotency
Retrying `GET /products` is safe. Retrying `POST /orders` can create duplicates unless the request carries an
**idempotency key** or the service dedupes. Limit retries (count, budget), and only retry safe methods/status codes.

### Long-lived & streaming traffic
WebSockets, SSE, gRPC streams, uploads, token-streaming AI responses need tuned idle timeouts, **no response
buffering**, connection draining on deploy, and no blind retries.

---

## 7. Gateway vs. Neighbors

| Component | Primary role |
|-----------|--------------|
| **Load balancer** | Spread traffic across healthy instances (L4/L7) |
| **Reverse proxy** | Forward requests, terminate TLS, proxy-level rules |
| **Kubernetes Ingress / Gateway API** | Expose HTTP(S) services into a cluster (Gateway API = newer, richer model) |
| **API gateway** | Client-facing API boundary: routing + shared edge policy |
| **Service mesh** (Istio, Linkerd) | **East-west** service-to-service traffic: mTLS identity, retries, metrics |
| **BFF** (Backend for Frontend) | Backend built for **one** client experience (mobile, web) — its own API shape, aggregation, release pace |

Typical production path: `Client → CDN/WAF → LB → API Gateway → (BFF) → Service mesh → Services`.

**Gateway vs BFF:** use the gateway for shared concerns (auth, routing, limits, TLS, logging). When web and mobile
need different shapes/aggregation, add a **BFF** per client — don't keep adding `if mobile…` branches to the shared
gateway until it becomes an unowned application.

**One gateway or many?** Start with one. Split by audience/risk as you grow — public, partner, internal-admin,
regional — to shrink blast radius and clarify ownership, at the cost of more systems to run.

---

## 8. Anti-Patterns

| Anti-pattern | Why it's bad |
|--------------|--------------|
| **Business logic in the gateway** (discounts, fraud, fulfillment rules) | Batch jobs/internal tools bypass the gateway and miss the rules; wrong owner. Aggregation for a client-facing API is OK; business decisions are not. |
| **Gateway as SPOF** | All traffic flows through it — deploy like critical infra (multi-AZ, autoscale, fast rollback). |
| **Retries without idempotency** | Duplicate orders / payments. |
| **Logging sensitive data** | Central logs + tokens/API keys/passwords/card data/full prompts = breach. Mask, hash, allow-list fields. |
| **"It's just config"** | Route/auth/limit config is production code — review, test, staged rollout. |
| **Gateway as the only authZ layer** | Services must still enforce resource-level permissions. |
| **Adding one "because the diagram looks empty"** | Extra hop, extra latency, extra thing to operate. |

---

## 9. Observability

Record per route / method / status / tenant / client type:
- Request counts, **p50/p95/p99 latency** (gateway overhead vs upstream time separately)
- Upstream error rates, timeouts, circuit-breaker opens
- Rate-limit decisions, authentication failures
- Request & trace IDs (generate if missing, propagate via `traceparent` / `X-Request-ID`)
- Request size / token usage where relevant

Never log auth headers, API keys, passwords, raw payment data, or full AI prompts by default.

---

## 10. Popular API Gateways

| Type | Options | Notes |
|------|---------|-------|
| **Managed** | AWS API Gateway | REST + WebSocket, throttling, API keys/usage plans, Lambda, CloudWatch |
| | Azure API Management | Strong OAuth/OIDC, policy-based config, developer portal |
| | Google Cloud API Gateway / Apigee | GCP integration, gRPC, OpenAPI; Apigee = full API management |
| **Open source / self-hosted** | Kong | Built on NGINX, large plugin ecosystem |
| | Tyk | GraphQL support, analytics, multi-DC |
| | Envoy / Envoy Gateway, NGINX, HAProxy, Traefik | Proxies commonly used as gateways |
| | Spring Cloud Gateway, Express Gateway | Framework-based (Java / Node.js) |

**Managed vs self-hosted:** managed = least ops, most expensive, provider's model. Self-hosted = control over latency,
plugins, streaming, networking — but you own upgrades, capacity, patches, and incidents. *The wrong choice is the one
nobody operates well.*

---

## 11. Common Follow-up Questions

| Question | Short answer |
|----------|--------------|
| Isn't the gateway a bottleneck / SPOF? | Stateless, horizontally scaled, multi-AZ behind an LB; data plane keeps last-known-good config. |
| Where do rate-limit counters live? | Shared Redis (atomic Lua) — see [rate-limiter](../rate-limiter/README.md). Local fallback if Redis is down. |
| How does the backend know who the user is? | Gateway validates the JWT and forwards verified claims (e.g. `X-User-Id`) over a trusted internal network / mTLS. |
| Gateway vs load balancer? | LB spreads load; gateway understands the API (routes, auth, limits, shaping). Often one box in interviews. |
| Gateway vs service mesh? | Gateway = north-south (client → system). Mesh = east-west (service ↔ service). |
| How to roll out a new service version? | Weighted routing / canary at the gateway, watch error/latency, then shift traffic. |
| How to handle WebSockets? | Gateway upgrades the connection and pins it to a backend; tune idle timeouts, drain on deploy. |
| How much latency does it add? | Typically low single-digit ms; keep middleware cheap (local JWT verify via cached JWKS, no per-request DB calls). |

---

## 12. One-Minute Summary

An API gateway is the **single entry point** in a microservices system. Its core job is **routing**; around that it
runs cheap, shared **middleware** — validation, authentication, rate limiting, IP filtering, TLS termination,
caching, and observability — rejecting bad traffic before it reaches services. It is **stateless** and scales
horizontally behind a load balancer, optionally deployed per region with GeoDNS. Keep it **thin**: no business logic,
coarse authZ only, retries only with idempotency, config treated as code. Use it for microservices with multiple
clients; skip it for a simple monolith. In most interviews: *draw it, say "routing + middleware", move on.*

### References
- Hello Interview — [API Gateway deep dive](https://www.hellointerview.com/learn/system-design/deep-dives/api-gateway)
- AlgoMaster — [API Gateways](https://algomaster.io/learn/system-design/api-gateways)

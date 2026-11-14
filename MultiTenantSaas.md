# Multi-Tenant SaaS Backend (Slack / Notion / Figma-style) — System Design (Interview Guide)

> Design a backend that serves **many customers (tenants)** — c1, c2, … c10,000 — from **one** application, where
> **no tenant can ever see another's data**, onboarding a tenant takes seconds, cost doesn't grow N× with tenants, and
> users can belong to **multiple organizations** with different roles in each.

Typical prompt: *"Design a multi-tenant backend for distributing codes to clients c1, c2, … Expose
`POST /issuecode` and `GET /issuecode/detail?id=`. Make it scalable and highly available."*

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Requirements: tenant count & size distribution, isolation/compliance needs, B2B vs B2C |
| 2 | 2 min | Back-of-the-envelope estimates |
| 3 | 3 min | Core entities (Tenant/Organization, User, Membership, Role, Permission) |
| 4 | 5 min | API design (tenant-scoped) |
| 5 | 10 min | **Tenancy model**: DB-per-tenant vs schema-per-tenant vs shared tables (+ hybrid) |
| 6 | 10 min | Tenant resolution, request context, data routing, AuthN/AuthZ (org tokens, RBAC) |
| 7 | 10 min | Deep dives: noisy neighbour, onboarding/migrations, scaling & sharding, security |
| 8 | 5 min | Observability, billing, trade-offs, wrap-up |

**Key line to say early:** *"Tenant isolation is a **security boundary**, not a filter. Tenant identity comes from a
**verified token**, is propagated as **request context**, and is enforced at **every layer** — app, cache, queue and
database (row-level security or schema) — so one missing `WHERE` clause can't leak data."*

---

## 1. Requirements

### Functional
1. **Tenant lifecycle** — create (self-serve signup), configure, suspend, offboard/delete (with data export).
2. **Users & organizations** — one global identity; a user can **belong to many orgs**; invite members; enterprise
   **auto-join by email domain**.
3. **Tenant-scoped business APIs** — e.g., `POST /issuecode`, `GET /issuecode/{id}`, documents, etc.
4. **Org-level RBAC** — roles (Admin, Member/Collaborator) with permissions (`read:documents`, `create:documents`).
5. **Authentication** — email/password, passwordless, social (Google/GitHub), **MFA**, enterprise **SSO (SAML/OIDC)**.

### Non-Functional
| Property | Target |
|----------|--------|
| Isolation | **Zero cross-tenant data leakage** (hard requirement) |
| Scalability | 10k+ tenants; mix of tiny and huge tenants (power-law) |
| Availability | 99.9–99.99%; one tenant's issue must not take down others |
| Fairness | Noisy-neighbour protection (per-tenant quotas) |
| Cost efficiency | Shared infra for small tenants; no N× infra bill |
| Operability | Onboarding in seconds; migrations across all tenants automated |
| Compliance | Data residency (EU/US), per-tenant encryption keys, audit logs (enterprise tier) |

### Below the line
Billing engine internals, analytics warehouse, white-labeling/custom domains, marketplace/plugins.

---

## 2. Capacity Estimates

- 10k tenants; 95% small (< 50 users), 4% mid, **1% enterprise** (10k+ users) — **power-law**.
- 2M total users, 200k concurrent → ~20k RPS avg, 100k RPS peak.
- Data: small tenant ~1 GB, enterprise ~1 TB → total ~15–20 TB → **shared clusters + dedicated for whales**.
- Postgres schemas: thousands per DB is OK; **tens of thousands** strains catalog, migrations and connection pools →
  schema-per-tenant needs **multiple DB clusters** at this scale.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **Tenant / Organization** | id, name, slug, plan/tier, region, **isolation_mode** (shared/schema/dedicated), shard/DB locator, status |
| **User** | id, email, auth identities (password, Google, SSO), MFA settings — **global**, not per tenant |
| **Membership** | user_id, tenant_id, status (invited/active), joined_at |
| **Role** | id, name (Admin, Member, Collaborator) — defined by an **org template**, same for all tenants |
| **Permission** | `read:documents`, `create:documents`, `issue:code` … — defined system-wide |
| **MembershipRole** | membership_id, role_id |
| **Invitation** | tenant_id, email, role, token, expires_at |
| **Domain / SSO connector** | tenant_id, verified email domain, IdP config (enterprise auto-join / JIT provisioning) |
| **Business data** (e.g., Code, Document) | id, **tenant_id**, … |

> **Org template:** permissions and roles are defined once and applied to every organization — *"what works for one
> client works for all clients"*. Tenants assign roles; they don't invent permission models (enterprise custom roles
> can be a later extension).

---

## 4. API Design

```http
# Control plane
POST   /v1/tenants                               # self-serve create (creator becomes Admin)
POST   /v1/tenants/{tenantId}/invitations        # invite member with role
POST   /v1/invitations/{token}/accept
GET    /v1/me/tenants                            # orgs the user belongs to (org switcher)
DELETE /v1/tenants/{tenantId}                    # offboard (soft delete -> export -> purge)

# Data plane (tenant-scoped)
POST   /v1/issuecode                             # issue/distribute a code
GET    /v1/issuecode/{id}
GET    /v1/documents
POST   /v1/documents
```

**How is the tenant identified?** (pick one, explain the trust model)
| Method | Example | Notes |
|--------|---------|-------|
| **Org-scoped access token** ✅ | JWT `aud: urn:org:{tenantId}`, `scope: read:documents` | Issued only if user is a member — **tamper-proof**, carries permissions |
| Subdomain | `acme.app.com` | Great UX; must still be validated against the token |
| Path | `/v1/tenants/{tenantId}/documents` | Explicit; validate membership |
| Header | `X-Tenant-ID: c1` | Simple, fine for **trusted service-to-service** calls; **never trust it alone from end users** |

**Request** `POST /v1/issuecode` (`Authorization: Bearer <org token>`, `Idempotency-Key`)
```json
{ "campaignId": "summer-26", "recipient": "user-42" }
```
**Response** `201 { "id": "code_9f2", "code": "SUMR-8K2Q-7XPL", "expiresAt": "..." }`

> ⚠️ Don't default to a shared/`public` schema when the tenant is missing — **reject with 400/401**. A silent default
> is how data ends up in the wrong place.

---

## 5. Tenancy Models (the core decision)

| Model | How | Isolation | Cost | Ops | Best for |
|-------|-----|-----------|------|-----|----------|
| **DB-per-tenant (silo)** | Each tenant gets its own database/cluster | ★★★ strongest; easy per-tenant backup/restore, residency, encryption keys | $$$ grows linearly | Hard: N migrations, N pools, N monitors | Few large / regulated enterprise tenants |
| **Schema-per-tenant (bridge)** | One DB, a schema per tenant (`c1.codes`, `c2.codes`); `SET search_path` per request | ★★ good; no shared tables | $$ | Medium: migrate every schema; catalog bloat at 10k+ | Tens–low thousands of tenants, B2B |
| **Shared tables (pool)** | All tenants in the same tables with a `tenant_id` column | ★ logical only — one missing filter leaks data (mitigate with **RLS**) | $ cheapest | Easy: one schema, one migration | Many small tenants, B2C/SMB, 10k–1M tenants |
| **Hybrid / tiered** ✅ | Pool for free/SMB, schema or dedicated DB for enterprise; tenant catalog says where each lives | Per tier | Optimized | Needs routing layer | Real SaaS at scale |

### Recommendation
- **Interview answer:** start with **schema-per-tenant** (clean isolation, shared infra, trivial onboarding) **or
  shared tables + Postgres RLS** if tenant count is very high — and evolve into a **hybrid**: move "whale" tenants to
  dedicated databases via a **tenant catalog**.
- Say explicitly: *"The app code stays the same across models because tenant routing is hidden behind a
  connection/context layer."*

---

## 6. High-Level Architecture

```
                 ┌──────────────────────────┐
  Browser/App ─▶ │ CDN / WAF                │   per-tenant custom domains, DDoS protection
                 └────────────┬─────────────┘
                              ▼
                 ┌──────────────────────────┐        ┌──────────────────────────────┐
                 │ API Gateway              │◀──────▶│ Identity Provider (Logto,    │
                 │ - verify JWT (JWKS)      │  OIDC  │ Auth0, Cognito, Keycloak)    │
                 │ - resolve tenantId       │        │ users, orgs, memberships,    │
                 │ - per-tenant rate limit  │        │ roles, MFA, SSO              │
                 └────────────┬─────────────┘        └──────────────────────────────┘
                              ▼
          ┌─────────────────────────────────────────────┐
          │ Stateless App Services (horizontally scaled)│
          │  TenantFilter → TenantContext (ThreadLocal) │
          │  AuthZ: required scopes per endpoint        │
          │  Tenant-aware DataSource / connection router│
          └───────┬───────────────┬─────────────┬───────┘
                  │               │             │
                  ▼               ▼             ▼
       ┌─────────────────┐ ┌────────────┐ ┌──────────────────────────────┐
       │ Tenant Catalog  │ │ Redis      │ │ Data tier (by tier)          │
       │ tenantId →      │ │ keys       │ │  Pool DB: shared tables + RLS│
       │ tier, region,   │ │ prefixed   │ │  Bridge DB: schema per tenant│
       │ DB/shard/schema │ │ t:{id}:... │ │  Silo DB: dedicated cluster  │
       └─────────────────┘ └────────────┘ └──────────────────────────────┘
                  ▲
       ┌──────────┴───────────┐     ┌──────────────────────────────┐
       │ Control Plane        │────▶│ Provisioning workers          │
       │ onboarding, plans,   │     │ CREATE SCHEMA / DB, migrate,  │
       │ suspend, offboard    │     │ seed, register in catalog     │
       └──────────────────────┘     └──────────────────────────────┘
   Kafka events carry tenant_id │ Logs/metrics/traces tagged with tenant_id │ Billing metering per tenant
```

### Request flow (`GET /v1/documents`)
1. Frontend gets an **org token** for the selected org from the IdP (only issued if the user is a member).
2. Gateway verifies signature/issuer/audience via **JWKS**, extracts `tenantId` (from `aud`) and `scope`.
3. App **TenantFilter** stores `tenantId` in request context (ThreadLocal / async context).
4. AuthZ middleware checks required scope (`read:documents`) → 403 if missing.
5. **Tenant router** looks up the tenant catalog (cached) → picks DataSource + schema, or sets RLS variable.
6. Query runs; filter **clears** context in `finally`.

---

## 7. Implementation Patterns

### 7.1 Tenant context (Spring Boot)
```java
public final class TenantContext {
    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();
    public static void set(String tenantId) { CURRENT.set(tenantId); }
    public static String get() { return CURRENT.get(); }
    public static void clear() { CURRENT.remove(); }
}

@Component
public class TenantFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String tenantId = TenantClaims.fromVerifiedJwt(req);   // never from a raw client header
        if (tenantId == null || !TenantCatalog.isActive(tenantId)) {
            res.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        TenantContext.set(tenantId);
        try {
            chain.doFilter(req, res);
        } finally {
            TenantContext.clear();   // avoid leaking tenant into pooled threads
        }
    }
}
```
> Async work (`@Async`, executors, Kafka consumers, reactive) must **propagate** the tenant explicitly (task
> decorator / message header) — ThreadLocal doesn't cross threads.

### 7.2 Schema-per-tenant (Hibernate `SCHEMA` multi-tenancy)
```java
public class TenantSchemaResolver implements CurrentTenantIdentifierResolver<String> {
    public String resolveCurrentTenantIdentifier() { return TenantContext.get(); }
    public boolean validateExistingCurrentSessions() { return true; }
}

public class SchemaConnectionProvider implements MultiTenantConnectionProvider<String> {
    // getConnection(tenant): validate tenant against catalog (allow-list, never string-concat user input),
    // then: SET search_path TO "<tenant_schema>"; on release: RESET search_path
}
```

### 7.3 Shared tables + PostgreSQL Row-Level Security
```sql
ALTER TABLE codes ENABLE ROW LEVEL SECURITY;
ALTER TABLE codes FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON codes
  USING      (tenant_id = current_setting('app.tenant_id')::uuid)
  WITH CHECK (tenant_id = current_setting('app.tenant_id')::uuid);

-- per transaction, set by the app from TenantContext:
SET LOCAL app.tenant_id = 'c1-uuid';
```
- App DB role must **not** be table owner / `BYPASSRLS`.
- Composite indexes **lead with `tenant_id`**: `(tenant_id, created_at)`; unique keys include `tenant_id`.

### 7.4 Org-level RBAC with org tokens
- IdP issues an **organization token**: `aud = urn:org:{tenantId}`, `scope = "read:documents create:documents"`
  (scopes = intersection of requested scopes and the user's role permissions in **that** org).
- Backend middleware: verify JWT → extract tenant from `aud` → check `requiredScopes`.

```text
Admin        -> read:documents, create:documents
Collaborator -> read:documents
Creator of an org -> Admin (automatic); invited users -> Member/Collaborator by default
```

### 7.5 Build vs buy identity
Auth (passwords, social login, MFA, SSO/SAML, org membership, invitations, M2M tokens) is weeks–months of work and
security-critical → use an **IdP** (Logto, Auth0, Okta, Cognito, Keycloak). Backend talks to its **Management API**
with **machine-to-machine (client-credentials)** tokens to create orgs, add members and assign roles.

---

## 8. Deep Dives

### 8.1 Tenant onboarding (self-serve, seconds)
1. `POST /tenants` → control plane writes catalog row `status=PROVISIONING` (idempotent on request key).
2. Provisioning worker (async, retryable):
   - Pool: nothing to create — seed rows with `tenant_id`.
   - Bridge: `CREATE SCHEMA` (name from **validated allow-list**, e.g., `t_<uuid>`), run migrations (Flyway/Liquibase
     per schema), `GRANT` to app role.
   - Silo: provision DB via IaC (Terraform) from a template.
3. Create org in IdP, add creator as **Admin**, apply org template.
4. Mark `ACTIVE`; emit `TenantCreated` event.
- **Pre-provision a warm pool** of schemas/DBs to make silo/bridge onboarding instant.

### 8.2 Schema migrations across thousands of tenants
- Migrations must be **backward compatible** (expand → migrate → contract) because tenants upgrade at different times.
- Run per tenant in **waves/canaries**, track version per tenant in the catalog, parallel with concurrency limit.
- Failed tenant → retry/alert without blocking others. Schema-diff tooling catches drift.

### 8.3 Noisy neighbour & fairness
- **Per-tenant rate limits & quotas** at gateway (token bucket keyed by `tenantId`, limits by plan).
- **Per-tenant connection pool caps**, statement timeouts, query cost limits.
- Separate **queues/partitions per tier**; weighted fair scheduling for background jobs.
- **Bulkheads**: enterprise tenants on dedicated pools/clusters ("cell-based" architecture).
- Detect hot tenants from metrics → **migrate them** to their own shard/DB (online move: dual-write or logical
  replication, then flip the catalog pointer).

### 8.4 Scaling the data tier
| Model | Scaling approach |
|-------|------------------|
| Pool | Shard by `tenant_id` (Citus / app-level sharding via catalog); all of a tenant's data on one shard → single-shard transactions |
| Bridge | Many DB clusters, N schemas each; catalog maps tenant → cluster + schema |
| Silo | One cluster per tenant; automate everything |
- **Connection pooling is the hidden limit**: per-tenant DataSources × app instances explode connection counts →
  use **PgBouncer** (transaction mode; set `search_path`/RLS var per transaction) and route by cluster, not by tenant.
- **Cells**: group tenants into independent stacks (app + DB + cache) per region → blast-radius containment.

### 8.5 Isolation beyond the database
| Layer | Technique |
|-------|-----------|
| Cache | Keys prefixed `t:{tenantId}:...`; never cache cross-tenant aggregates |
| Messaging | `tenant_id` in every event header; consumers set TenantContext from it |
| Object storage | Prefix per tenant `s3://bucket/{tenantId}/...` + IAM conditions / per-tenant buckets for silo |
| Search | Index per tenant (small count) or mandatory `tenant_id` filter / filtered aliases |
| Logs & traces | Tag with `tenant_id`; access to logs scoped by support role |
| Encryption | Per-tenant data keys (KMS envelope encryption); enterprise **BYOK**; crypto-shredding on offboarding |

### 8.6 Security checklist
- Tenant ID **only from verified token** (or trusted internal mTLS caller), never from user-controlled input alone.
- Validate schema/DB names against the catalog — **no string concatenation** into SQL (`SET search_path` injection).
- Least-privilege DB roles; app role can't `BYPASSRLS` or touch other schemas.
- Non-guessable tenant IDs (UUIDs), no tenant enumeration via error messages.
- **Automated cross-tenant leak tests**: CI seeds tenants A and B and asserts A can never read/update B.
- Audit log per tenant (who did what), admin "impersonation" is explicit and logged.

### 8.7 Enterprise features
- **SSO (SAML/OIDC)** per tenant; **JIT provisioning**; **auto-join by verified email domain**.
- **Org-level MFA enforcement**, IP allow-lists, SCIM user provisioning.
- **Data residency**: tenant pinned to region in catalog; routing at gateway by tenant region.

### 8.8 Offboarding
Suspend (read-only) → export (data portability) → retention period → hard delete (`DROP SCHEMA` / drop DB / delete
rows by `tenant_id` in batches / crypto-shred keys) → purge caches, search indexes, backups per policy.

---

## 9. Observability & Billing
- **Per-tenant metrics:** RPS, latency p99, error rate, DB time, storage, queue lag — find noisy neighbours fast.
- **Tenant-aware tracing:** `tenant_id` + `user_id` on every span.
- **Usage metering** (API calls, codes issued, seats, storage) → events → billing (Stripe) and quota enforcement.
- **Alerts:** cross-tenant access attempts (403 spikes), migration failures per tenant, tenant SLO breaches.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Hybrid tiered tenancy | One model for all | Cheap for small tenants, strong isolation for enterprise |
| Schema-per-tenant (mid scale) | Shared tables | Stronger isolation, simple per-tenant restore; costs: migrations × N, catalog limits |
| Shared tables + RLS (huge scale) | Schema per tenant | Scales to 100k+ tenants cheaply; RLS turns isolation from "remember the filter" into DB-enforced |
| Tenant from org token | `X-Tenant-ID` header | Tamper-proof, membership-checked, carries permissions |
| Buy IdP (Logto/Auth0) | Build auth | Security-critical, weeks of work, SSO/MFA out of the box |
| Org template RBAC | Per-tenant custom permission models | Consistent, maintainable; custom roles later for enterprise |
| Tenant catalog routing | Hard-coded routing | Enables moving tenants between shards/tiers without code changes |
| Cell-based architecture | Single global stack | Limits blast radius; per-region residency |

---

## 11. Common Follow-up Questions

1. **A tenant grows 100×. What now?** Move it from pool/bridge to a dedicated DB: logical replication / dual-write,
   verify, flip catalog pointer, drain old data.
2. **How do you prevent a forgotten `WHERE tenant_id = ?`?** RLS (DB-enforced), Hibernate filters / repository base
   classes, and automated cross-tenant tests.
3. **User in multiple orgs — how does the UI switch?** `GET /me/tenants` → user picks org → frontend requests an org
   token for that org; each tab/request carries its own org token.
4. **Cross-tenant reporting for internal admins?** Separate analytics pipeline (CDC → warehouse) with strict access,
   never via the tenant data plane.
5. **Background jobs and Kafka consumers?** Every message carries `tenant_id`; consumer sets and clears TenantContext;
   per-tenant fairness in job queues.
6. **Postgres limit on schemas?** No hard limit, but catalog bloat, slow `pg_dump`, migrations and pool sizing hurt
   beyond a few thousand per DB → spread across clusters or switch to pool+RLS.
7. **Backup/restore a single tenant?** Easy for silo/bridge (dump the schema/DB); for pool, need logical export by
   `tenant_id` or PITR into a scratch DB and copy rows.
8. **Per-tenant customization?** Config/feature flags per tenant in the catalog; JSONB custom fields; avoid per-tenant
   code branches.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Explains the three tenancy models with pros/cons, picks one, tenant identification + `tenant_id` scoping, stateless horizontal scaling |
| **Senior (E5)** | Tenant context propagation, schema routing / RLS, org-token RBAC, onboarding + migrations across tenants, noisy neighbour (per-tenant limits, pools), security pitfalls (header trust, search_path injection) |
| **Staff+ (E6)** | Hybrid tiers + tenant catalog, moving tenants between shards online, cell-based architecture, residency & BYOK, metering/billing, operational tooling and leak testing as first-class concerns |

---

## 13. One-Minute Summary
> Users authenticate through an **IdP** that models **organizations, memberships and org-template RBAC**; for each
> request the frontend presents an **org-scoped token** that the gateway verifies, yielding a trusted `tenantId` and
> permission scopes. Stateless services put the tenant in **request context** and a **tenant router** — backed by a
> cached **tenant catalog** — sends queries to the right place: **shared tables with Postgres RLS** for small tenants,
> **schema-per-tenant** for mid-size, and **dedicated databases** for enterprise. Onboarding is an automated control-
> plane workflow (create schema/DB, migrate, create org, assign Admin). We protect fairness with **per-tenant rate
> limits, pool caps and cells**, isolate caches/queues/storage by tenant prefix, and keep everything observable and
> billable **per tenant** — so one tenant can never see, slow down, or break another.

---

### References
- *The Multi-Tenant System Design Question That Changed How I Build Backends Forever* — Stackademic (via Freedium
  mirror): https://blog.stackademic.com/the-multi-tenant-system-design-question-that-changed-how-i-build-backends-forever-8a3586db6391
- Logto, *Build a multi-tenant SaaS application: A complete guide from design to implementation* —
  https://logto.medium.com/build-a-multi-tenant-saas-application-a-complete-guide-from-design-to-implementation-d109d041f253
- Original Logto blog post: https://blog.logto.io/build-multi-tenant-saas-application
- Logto sample repo: https://github.com/logto-io/multi-tenant-saas-sample
- Logto, *Multi-tenancy implementation with PostgreSQL*: https://blog.logto.io/implement-multi-tenancy

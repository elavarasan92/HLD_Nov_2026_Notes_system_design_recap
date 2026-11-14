# File Storage & Sync (Dropbox / Google Drive) — System Design (Interview Guide)

> Design a cloud file storage service where users **upload** files (up to **50 GB**) from any device, **download**
> them anywhere, **share** them with other users, and have a desktop client that **automatically syncs** a local
> folder across all their devices.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (top 4) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates (→ why bytes must bypass app servers) |
| 3 | 2 min | Core entities (File bytes vs FileMetadata vs User) |
| 4 | 5 min | API design (will evolve to presigned URLs) |
| 5 | 10 min | High-level design — upload, download, share, sync |
| 6 | 15–20 min | Deep dives: large files (chunking, resumable multipart), fast upload/download/sync (CDC, delta, compression, CDN), security (signed URLs), conflicts/versioning |
| 7 | 5 min | Scaling, trade-offs, wrap-up |

**Key line to say early:** *"Split the **control plane** (metadata, permissions, presigned URLs) from the **data
plane** (bytes go **directly** between client and blob storage). The hard parts are **huge files** — chunked,
resumable, parallel uploads — and **efficient sync**, where only changed chunks travel."*

---

## 1. Requirements

### Functional (top 4)
1. Users can **upload** a file from any device.
2. Users can **download** a file from any device.
3. Users can **share** a file with other users and see files shared with them.
4. Files **automatically sync** across a user's devices (desktop sync client).

### Below the line (mention, don't design)
- In-browser editing / collaborative editing, file preview without download.
- Per-user storage quotas, file **versioning** history UI, virus/malware scanning.
- Full-text search over file contents.

### Non-Functional
| Property | Target |
|----------|--------|
| Availability | **High** — prefer **availability over consistency** (seconds of staleness across regions is fine) |
| File size | Up to **50 GB** per file |
| Durability | **Never lose a file** (11 nines via blob storage replication); recover from corruption |
| Latency | Upload / download / sync as **fast as the network allows** |
| Security | Only authorized users can access a file; encryption in transit + at rest |
| Scale | ~**500M** users, **100M DAU**, billions of files, exabyte-scale storage |

> **CAP note:** Choose CP only when every read *must* see the latest write (e.g. stock trading). A file uploaded in
> Germany being visible in the US a few seconds later is fine → **AP**.

### Out of scope
Building blob storage itself (assume S3 / GCS / Azure Blob), billing, CI/CD.

---

## 2. Capacity Estimates

- **Users:** 100M DAU; each uploads/edits ~2 files/day → **200M uploads/day ≈ ~2.3k/s avg**, ~10k/s peak.
- **Reads (downloads + sync pulls):** a few × writes → **~10k–20k/s**; metadata reads (list/changes) much higher.
- **Storage:** avg file ~1 MB (heavy tail to 50 GB); 200M × 1 MB ≈ **200 TB/day** raw → ~**70 PB/year** before dedup.
  → Dedup + compression matter at this scale.
- **Metadata:** ~1 KB per file × 100B files ≈ **100 TB** → sharded metadata DB.
- **Upload time for the max file:** 50 GB × 8 / 100 Mbps ≈ **4000 s ≈ 1.1 h** → a single HTTP request is not viable
  (timeouts, payload limits — e.g. API Gateway caps bodies at ~10 MB — and no resume on network drop).

> Conclusion: app servers must **never proxy bytes**; clients upload **chunks directly** to blob storage.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **File (bytes)** | Stored in blob storage (S3) — as one object or as content-addressed chunks |
| **FileMetadata** | file_id (UUID), owner_id, name, folder/path, size, mime_type, fingerprint (SHA-256), status (`uploading`/`uploaded`), s3_key, chunks[], version, created_at, updated_at |
| **Chunk** | file_id, index, fingerprint, size, status, etag |
| **Share (ACL)** | file_id, user_id, permission (`read`/`write`) |
| **User / Device** | user_id, email; device_id, last_sync_cursor |

> `file_id` identifies the **record**; `fingerprint` identifies the **content** (two users uploading the same bytes
> share a fingerprint, not a file_id).

---

## 4. API Design

```http
POST   /v1/files                         # initiate upload → file_id + presigned URL(s)
PATCH  /v1/files/{fileId}/chunks          # report chunk uploaded {index, etag}
POST   /v1/files/{fileId}/complete        # finalize multipart upload
GET    /v1/files/{fileId}                 # metadata + signed download URL
POST   /v1/files/{fileId}/shares          # { "emails": ["bob@x.com"], "permission": "read" }
DELETE /v1/files/{fileId}/shares/{userId}
GET    /v1/files?sharedWithMe=true        # list own / shared files
GET    /v1/changes?cursor={cursor}        # sync: change events since cursor → { events[], nextCursor }
WS     /v1/notifications                  # push "something changed" to devices
```

**Initiate upload**
```json
{ "name": "video.mp4", "size": 53687091200, "mimeType": "video/mp4",
  "fingerprint": "sha256:9f2c…", "chunkFingerprints": ["sha256:a1…", "sha256:b2…"] }
```
**Response:** `201 { "fileId": "f_123", "uploadId": "s3-upload-xyz", "parts": [{ "index": 1, "url": "https://s3…?X-Amz-Signature=…" }, …] }`

- Identify the user from the **validated session/JWT**, never from a `userId` in the body or headers.
- Use a **cursor** (monotonic change sequence) for `/changes` rather than a wall-clock `since` timestamp — avoids
  clock skew and missed events.
- *"I'll start with simple APIs and evolve them"* — upload/download change significantly once presigned URLs and
  chunking come in.

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **S3 / blob storage** | File bytes / chunks | Cheap, 11-nines durability, multipart upload, presigned URLs, lifecycle tiers |
| **PostgreSQL (sharded) or DynamoDB** | FileMetadata, chunks, shares, change log | Main query = "files for user / folder"; either works — don't over-index on the choice |
| **Redis** | Hot metadata, share-list cache, WebSocket presence (user → gateway node) | Fast permission checks and fan-out |
| **CDN (CloudFront)** | Edge cache for downloads via **CDN signed URLs** | Lower latency, offloads S3 |
| **Kafka / SQS** | Change events → notification service, async jobs (thumbnails, scanning) | Decouple sync fan-out |

```sql
CREATE TABLE files (
    file_id      UUID PRIMARY KEY,
    owner_id     UUID NOT NULL,
    parent_id    UUID,                       -- folder
    name         TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL,
    mime_type    TEXT,
    fingerprint  CHAR(64),                   -- SHA-256 of content
    status       TEXT NOT NULL,              -- uploading | uploaded | deleted
    s3_key       TEXT,
    version      BIGINT NOT NULL DEFAULT 1,  -- optimistic concurrency
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_files_owner_parent ON files (owner_id, parent_id);
CREATE INDEX idx_files_owner_fp     ON files (owner_id, fingerprint);   -- resume / dedup lookup

CREATE TABLE file_chunks (
    file_id     UUID REFERENCES files,
    idx         INT,
    fingerprint CHAR(64) NOT NULL,
    etag        TEXT,
    status      TEXT NOT NULL,               -- not-uploaded | uploaded
    PRIMARY KEY (file_id, idx)
);

CREATE TABLE shares (
    file_id    UUID REFERENCES files,
    user_id    UUID,
    permission TEXT NOT NULL,
    PRIMARY KEY (user_id, file_id)           -- "shared with me" is a single-partition query
);

CREATE TABLE changes (                        -- per-user change log for sync
    user_id  UUID,
    seq      BIGINT,                          -- monotonically increasing per user = cursor
    file_id  UUID,
    op       TEXT,                            -- created | updated | deleted | shared
    PRIMARY KEY (user_id, seq)
);
```

---

## 6. High-Level Architecture

```
  ┌──────────────────────────────┐
  │ Client (desktop / web / app) │
  │  Watcher · Chunker · Indexer │
  │  Local DB (chunk index)      │
  └──┬──────────────┬─────────┬──┘
     │ metadata API │ WS      │ bytes (presigned URLs) ───────────────────────────┐
     ▼              ▼         │                                                    │
  ┌──────────────────────┐    │ download via CDN signed URL                        │
  │ LB + API Gateway     │    ▼                                                    ▼
  │ auth, rate limit     │  ┌──────────┐   miss    ┌──────────────────────────────────┐
  └──┬──────────────┬────┘  │   CDN    │──────────▶│ S3 (blob storage)                │
     │              │       └──────────┘           │ multipart upload, SSE encryption │
     ▼              ▼                              └───────────────┬──────────────────┘
  ┌──────────────┐ ┌────────────────────┐                          │ upload-complete event
  │ File Service │ │ Notification Svc   │◀── change events ──┐     ▼
  │ (control     │ │ (WebSocket/SSE)    │                    │  ┌──────────────┐
  │  plane)      │ └────────────────────┘                    └──│ Kafka / SQS  │
  └──┬───────┬───┘                                              └──────▲───────┘
     │       │ sign URLs locally (no S3 call)                          │
     ▼       └─────────────────────────────────────────────────────────┘ emit change
  ┌──────────────────────────────┐   ┌─────────┐
  │ Metadata DB (sharded)        │◀─▶│ Redis   │ share cache, presence
  │ files · chunks · shares ·    │   └─────────┘
  │ changes                      │
  └──────────────────────────────┘
```

> The **File Service never touches file bytes** — it stores metadata, checks permissions, and **signs URLs**
> (a purely local crypto operation).

### 6.1 Upload — storage options
| Option | How | Verdict |
|--------|-----|---------|
| **Bad:** store file in the DB | BLOB column | DB bloat, terrible for 50 GB files |
| **OK:** upload through app server → S3 | Client POSTs bytes to File Service, which streams to S3 | Bytes cross the network twice; app servers become a bandwidth bottleneck; request-size limits |
| **Great:** **presigned URL, direct to S3** | File Service writes metadata (`uploading`), returns presigned PUT URL(s); client uploads directly; S3 event / complete call flips status to `uploaded` | App servers out of the data path; scales with S3 |

### 6.2 Download — options
| Option | Verdict |
|--------|---------|
| Proxy through File Service | Same bandwidth bottleneck as upload |
| Presigned S3 GET URL | Good — direct from S3 |
| **CDN signed URL** (CloudFront) | **Great** — served from nearest edge; CDN pulls from S3 on miss |

Large downloads use HTTP **Range requests** → parallel byte ranges and resume after interruption; the client doesn't
need to know the original chunk boundaries.

### 6.3 Share
| Option | Verdict |
|--------|---------|
| `shareList` array on FileMetadata | Easy "who can see this file", but "files shared with **me**" needs a full scan |
| + **Cache** `user → [fileIds]` | Fast, but must keep cache and DB in sync |
| **Great:** separate `shares` table keyed by `(user_id, file_id)` | "Shared with me" = one partition read; permission check = PK lookup; can still cache in Redis |

### 6.4 Sync
**Local → Remote** (client agent):
1. **Watcher** listens for OS file events (`FSEvents` on macOS, `ReadDirectoryChangesW`/`FileSystemWatcher` on Windows,
   `inotify` on Linux).
2. **Chunker** splits the changed file and fingerprints each chunk; **Indexer** diffs against the **local DB** to find
   which chunks changed.
3. Upload only the new chunks (presigned URLs), then commit the new chunk list/metadata to the File Service.

**Remote → Local:**
| Option | Verdict |
|--------|---------|
| Polling `GET /changes` | Simple; slow to notice changes, wasteful when nothing changed |
| WebSocket / SSE push | Near-real-time; connections drop, messages can be missed |
| **Hybrid (Great)** | **One WebSocket per device** pushes "changes available"; client calls `GET /changes?cursor=` to pull; **periodic polling** (every few minutes) as a safety net so nothing is ever lost |

> Alternative (classic write-ups): a **request queue** shared by all clients → Synchronization Service, and a
> **per-client response queue** so offline devices pick up missed updates when they reconnect. A durable per-user
> **change log + cursor** achieves the same with less infrastructure.

---

## 7. Deep Dives

### 7.1 Large files (50 GB) — chunked, resumable uploads (core problem)
Problems with a single POST: timeouts, payload limits, restart-from-zero on network drop, no progress bar.

**Chunk on the client** (5–10 MB pieces; adapt to network). Chunking on the server defeats the purpose — the whole
file would still have to arrive in one request.

**Flow (S3 Multipart Upload under the hood):**
1. Client computes a **file fingerprint** (SHA-256 of content) and a fingerprint per chunk.
2. Client asks "do I already have a file with this fingerprint?" → if `status=uploading`, **resume**: fetch chunk
   statuses and upload only missing ones. If `uploaded`, it's a duplicate → done (dedup).
3. Otherwise `POST /files` → File Service calls `CreateMultipartUpload`, stores metadata (`uploading`) + chunk rows,
   returns `uploadId` and a **presigned URL per part** (`uploadId` + `partNumber`).
4. Client uploads parts **in parallel** directly to S3; after each part it `PATCH`es `{index, etag}`. Backend
   verifies with S3 `ListParts` before marking the chunk `uploaded` (don't trust the client blindly).
5. When all chunks are `uploaded` → backend calls `CompleteMultipartUpload` with part numbers + ETags; only after S3
   confirms does it set file `status=uploaded` and emit a change event.

**Keeping chunk status honest:**
| Option | Verdict |
|--------|---------|
| Client-reported PATCH only | Client may lie or crash between upload and PATCH → inconsistent |
| **Client PATCH + server verification (ListParts / ETag)** | Trust but verify — recommended |
| S3 event notifications | Only fire on **complete** multipart, not per part → good for the final status flip |

**Cleanup:** an S3 lifecycle rule aborts incomplete multipart uploads after N days; a job removes stale `uploading`
rows.

> Say "I'd use S3 Multipart Upload" **and** be able to explain how it works — that's the bar.

### 7.2 Fast uploads, downloads & sync
- **Parallel chunk uploads** with adaptive chunk size → saturate available bandwidth.
- **Delta sync:** only changed chunks are uploaded/downloaded.
- **Content-Defined Chunking (CDC):** with fixed-size chunks, inserting 1 byte at the start shifts every boundary →
  every chunk "changes". CDC picks boundaries with a **rolling hash (Rabin fingerprint)** so an edit only affects
  nearby chunks — this is what makes delta sync work in practice.
- **Compression on the client** (Zstandard — fast, good ratio; Gzip/Brotli alternatives) — only when worth it:
  great for text, pointless for already-compressed media (jpg/png/mp4). **Compress before encrypting** (encrypted
  data looks random and won't compress).
- **CDN** for downloads; **Range requests** for parallel/resumable downloads.
- **Dedup:** content-addressed chunks (`s3_key = sha256`) → identical chunks stored once across files/users
  (watch out for cross-user dedup as a side channel — scope per user or tenant if that matters).

### 7.3 Security
1. **In transit:** HTTPS/TLS everywhere.
2. **At rest:** S3 server-side encryption (SSE-KMS); optionally client-side encryption for zero-knowledge tiers.
3. **Access control:** `shares` table is the ACL; checked on every metadata/URL request.
4. **Signed URLs with short TTL** (e.g. 5 min): a leaked link expires quickly. Signed URLs are **bearer tokens** —
   for higher security add IP binding or require auth cookies. CloudFront validates the signature with the
   registered public key, checks expiry/restrictions, then serves or denies.

### 7.4 Conflicts & versioning
- Default: **last-write-wins** using server-assigned order (not client clocks).
- Better: **optimistic concurrency** — client sends the `version` it edited from; on mismatch the server keeps both
  and creates a **"conflicted copy"** (what Dropbox actually does) instead of silently losing an edit.
- Versioning: never overwrite — a new version is a **new chunk list** pointing to (mostly shared) chunks; old
  versions retained per policy, unreferenced chunks garbage-collected.

### 7.5 Metadata scaling
- Shard metadata by **`owner_id`** (or namespace/workspace id) → listing a folder or a user's change log hits one
  shard.
- Shared files: `shares` partitioned by `user_id`; cache hot share lists in Redis.
- Per-user **change log** (`user_id, seq`) gives cheap, ordered `/changes?cursor=` reads; for shared files, fan out a
  change row to each member (or to a shared-folder namespace log that members also read).
- RDBMS gives strong consistency within a shard; NoSQL (DynamoDB/Cassandra) gives easy horizontal scaling with
  tunable consistency — either is defensible.

### 7.6 Notification fan-out
- WebSocket gateways are stateful: store `user_id → gateway node` in Redis; Notification Service routes events.
- Notifications are **hints** ("pull changes"), not payloads → a lost message is harmless thanks to cursor-based
  pulls + periodic polling.

---

## 8. Scaling Storage

| Component | Strategy |
|-----------|----------|
| Blob storage | S3 scales horizontally; lifecycle to Infrequent Access / Glacier for cold files; cross-region replication for DR |
| Metadata DB | Shard by owner/namespace; read replicas; archive deleted rows |
| File Service | Stateless, autoscale; only small JSON requests |
| Notification Service | Horizontal WebSocket gateways; Redis presence map; Kafka partitioned by user_id |
| CDN | Edge caching for popular/shared files |
| Dedup | Content-addressed chunks + reference counts; async GC |

---

## 9. Observability
- **Metrics:** upload/download throughput, multipart success/abort rate, resume rate, chunk dedup ratio, sync lag
  (change → device applied), WebSocket connection count, CDN hit ratio, signed-URL 403s.
- **Alerts:** spike in failed `CompleteMultipartUpload`, stale `uploading` files, change-log lag, S3 5xx.
- **Tracing:** initiate → part uploads → complete → notification → peer device pull.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Presigned URLs, direct to S3 | Proxy bytes through app servers | App servers out of the data path; scale & cost |
| Client-side chunking (5–10 MB) | Single upload request | Resumable, parallel, progress bar, delta sync |
| Content-defined chunking | Fixed-size chunks | Small edits don't invalidate every chunk |
| CDN signed URLs | Direct S3 GET | Edge latency; short-lived access control |
| WebSocket hint + cursor pull + polling | Pure polling / pure push | Real-time and lossless |
| AP, eventual consistency | Strong global consistency | Seconds of staleness acceptable; higher availability |
| Conflicted copy on version mismatch | Silent last-write-wins | No lost edits |
| Shard metadata by owner | Shard by file_id | Folder listing & change log stay single-shard |

---

## 11. Common Follow-up Questions

1. **Upload dies at 49 GB?** Resume: lookup by fingerprint → fetch chunk statuses → upload only missing parts.
2. **Same file uploaded twice?** File fingerprint matches an `uploaded` record → create metadata pointing to existing
   content (dedup); no bytes re-sent.
3. **How do other devices learn about a change?** Change log row + WebSocket hint → device pulls `/changes?cursor=`;
   polling catches anything missed while offline.
4. **Two devices edit offline, then reconnect?** Version check fails for the second → server keeps both as
   "conflicted copy"; user resolves.
5. **Shared link leaked publicly?** Signed URLs expire in minutes; revoke share → no new URLs issued; optionally IP
   binding.
6. **Why not chunk on the server?** The whole file would still need to reach the server in one go — losing resume,
   parallelism and request-size safety.
7. **Huge folder with millions of files?** Paginate listings; per-namespace change logs; avoid fan-out per file by
   sharing at folder/namespace level.
8. **Need chunked downloads too?** Not required — after `CompleteMultipartUpload` it's one object; use Range requests
   for parallel/resumable download.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clear API + data model; working upload, download, share; separates bytes (blob) from metadata; reasons toward "don't upload twice" and "show progress / resume" when probed |
| **Senior (E5)** | Moves quickly to presigned URLs + CDN; drives the large-file deep dive: client chunking, fingerprints, resumable multipart upload, chunk-status verification; hybrid sync |
| **Staff+ (E6)** | Content-defined chunking & delta sync, dedup trade-offs, conflict handling/versioning, change-log/cursor design, metadata sharding, signed-URL threat model, cost (storage tiers, egress) |

---

## 13. One-Minute Summary
> A stateless **File Service** (control plane) stores **metadata, chunk state and shares** in a sharded DB and hands
> out **presigned URLs**; clients **chunk files locally** (content-defined, 5–10 MB), fingerprint them, and upload
> chunks **in parallel directly to S3** via **multipart upload** — resumable after any interruption and deduped by
> fingerprint. Downloads go through a **CDN with short-lived signed URLs** and HTTP Range requests. A desktop agent
> (**watcher → chunker → indexer → local DB**) uploads only changed chunks; other devices get a **WebSocket hint**,
> pull `/changes?cursor=`, and **poll periodically** as a safety net. We favor **availability**, encrypt in transit and
> at rest, and resolve concurrent edits with **version checks + conflicted copies**.

---

### References
- Hello Interview, *Dropbox — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/dropbox
- Anu Upadhyay, *Design Dropbox — A System Design Interview Question* (Medium) —
  https://medium.com/@anuupadhyay1994/design-dropbox-a-system-design-interview-question-6b58b528214

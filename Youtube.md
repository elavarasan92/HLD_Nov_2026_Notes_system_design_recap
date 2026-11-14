# YouTube (Video Upload & Streaming) — System Design (Interview Guide)

> Design a platform where creators **upload** large videos (up to 10s of GB) and viewers **stream** them instantly,
> on any device and any network — with **resumable** uploads, **adaptive bitrate** playback, and a read load that
> dwarfs writes by orders of magnitude.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Functional (upload + watch) + non-functional requirements, "below the line" items |
| 2 | 3 min | Back-of-the-envelope estimates (storage, egress bandwidth, CDN cost) |
| 3 | 2 min | Core entities |
| 4 | 5 min | API design (presigned URL instead of uploading through our servers) |
| 5 | 10 min | High-level design — upload flow, processing pipeline, streaming flow |
| 6 | 15–20 min | Deep dives: transcoding DAG + adaptive bitrate, resumable/multipart upload, scaling reads (CDN, cache), cost |
| 7 | 5 min | Error handling, trade-offs, wrap-up |

**Key line to say early:** *"Only two features, but the non-functionals make it hard: **huge files** (never proxy bytes
through app servers — upload straight to blob storage), **CPU-heavy transcoding** (a parallel DAG of per-segment work),
and **massive, latency-sensitive egress** (segment-based adaptive streaming served from a CDN). Bandwidth and CDN cost
dominate everything."*

---

## 1. Requirements

### Functional (top 2)
1. Users can **upload videos** (with title, description, etc.).
2. Users can **watch (stream) videos** on web, mobile and smart TV, and switch quality (auto or manual).

### Below the line (mention, don't design)
- View counts, likes, comments, search, recommendations, home feed.
- Channels, subscriptions, notifications.
- Live streaming (mention differences at wrap-up).
- Content moderation / copyright detection internals, ads.

### Non-Functional
| Property | Target |
|----------|--------|
| Availability | **High** — favor availability over consistency (a new video appearing a few seconds late is fine) |
| Large files | Uploads of **10s of GB**; **resumable** after network failure |
| Playback latency | Start in **< ~1–2 s**, minimal rebuffering, works on **low bandwidth** (adaptive bitrate) |
| Scale | **~1M uploads/day**, **~100M views/day** (read:write ≈ 100:1, extremely skewed / long-tail) |
| Durability | Original + transcoded videos must never be lost |
| Global | Large share of international users → serve from the edge |
| Cost | CDN egress is the biggest bill — design to reduce it |

### Out of scope
Bot/fake-account protection, monitoring internals, billing, CI/CD.

---

## 2. Capacity Estimates

- **Uploads:** 1M/day ≈ **~12/s avg**, a few dozen/s peak. Avg raw size ~300 MB →
  **~300 TB/day** of originals. Transcoded renditions (several resolutions × codecs) add **~1–2×** more →
  **~0.5–1 PB/day** total, **~200–350 PB/year**.
- **Metadata:** 1M/day × 365 ≈ **~365M video rows/year** at ~1–2 KB each → ~0.5 TB/year — small, but read-hot.
- **Views:** 100M/day ≈ **~1.2k playback starts/s avg**, ~5k/s peak.
- **Egress:** avg watch ~5 min at ~3 Mbps ≈ ~110 MB/view → 100M × 110 MB ≈ **~11 PB/day ≈ ~1 Tbps average**.
  → Impossible from a single origin; **CDN is mandatory**.
- **CDN cost** (≈ $0.02/GB at volume): 11 PB/day ≈ 11M GB × $0.02 ≈ **~$220k/day**.
  → Cost optimization is a legitimate deep dive.
- **Transcoding compute:** 1M videos/day × multiple renditions; encoding is CPU-bound → thousands of worker cores,
  autoscaled on queue depth.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **User** | id, name, channel info |
| **Video** (blob) | original file in blob storage + transcoded **segments** per rendition |
| **VideoMetadata** | video_id (PK), uploader_id, title, description, status (`UPLOADING → PROCESSING → READY / FAILED`), duration, primary manifest URL, thumbnail URL, chunks (for resumable upload), created_at |
| **Manifest** | primary manifest (lists renditions) → media manifests (list segment URLs per rendition) |

> Bytes live in **blob storage**; the DB only stores **metadata and URLs**. That split drives the whole design.

---

## 4. API Design

```http
POST  /v1/videos                       # create metadata, start upload → returns videoId + presigned (multipart) URLs
PATCH /v1/videos/{videoId}/chunks      # report uploaded part (partNumber, ETag) → resumable progress
POST  /v1/videos/{videoId}/complete    # finish multipart upload → triggers processing
GET   /v1/videos/{videoId}             # metadata incl. manifest URL + status
```

**Create request**
```json
{ "title": "My trip", "description": "...", "fileSize": 4294967296, "contentType": "video/mp4",
  "chunks": [ { "partNumber": 1, "fingerprint": "sha256:..." }, ... ] }
```
**Response:** `201 { "videoId": "v_9f3k", "uploadId": "...", "parts": [ { "partNumber": 1, "url": "https://s3/...signed" } ] }`

**Watch response** — the client gets metadata, then streams **directly from the CDN**:
```json
{ "videoId": "v_9f3k", "title": "My trip", "status": "READY",
  "manifestUrl": "https://cdn.example.com/v_9f3k/master.m3u8", "thumbnailUrl": "https://cdn.example.com/v_9f3k/thumb.jpg" }
```

> Mention that the APIs **evolve**: the naive `POST /upload` with the file in the body becomes "get presigned URLs";
> the naive `GET /video` returning bytes becomes "return metadata + manifest URL".

- Authenticate every metadata call; presigned URLs are **short-lived** and scoped to one object/part.
- Validate size/content type up front; rate-limit uploads per user.

---

## 5. Background: Video Streaming Basics (say it in 1 minute)

| Term | Meaning |
|------|---------|
| **Codec** | Compression algorithm (H.264, H.265/HEVC, VP9, AV1). Trade-offs: encode time, device support, compression ratio, quality |
| **Container** | File format holding video/audio/metadata (`.mp4`, `.mov`, `.ts`, `.webm`) |
| **Bitrate** | Bits per second; higher resolution/FPS → higher bitrate |
| **Rendition / format** | One codec + container + resolution/bitrate combo (e.g. 1080p H.264) |
| **Segment** | A few seconds of one rendition, independently playable (cut on **GOP** boundaries) |
| **Manifest** | Primary manifest → list of renditions; media manifest → list of segment URLs |
| **Streaming protocol** | **HLS** (Apple) or **MPEG-DASH** — both are plain HTTP, so they cache perfectly in CDNs |

**Adaptive bitrate (ABR):** the player downloads the primary manifest, picks a rendition based on measured bandwidth
and buffer, and can **switch rendition at every segment boundary** — this is what makes playback smooth on bad networks.

---

## 6. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **Cassandra / DynamoDB** (or sharded Postgres) | `video_metadata` | Point lookups by `video_id`, horizontally partitioned, highly available |
| **Redis** | Metadata cache (hot videos) | Viral videos → read hot-spots on single partitions |
| **S3 / blob store (originals bucket)** | Original uploads | Multipart upload, durable, cheap; can tier to cold storage |
| **S3 / blob store (transcoded bucket)** | Segments + manifests + thumbnails | Origin for the CDN |
| **CDN** | Edge cache of segments + manifests | ~1 Tbps of egress near users |
| **Queue (Kafka / SQS)** | Upload-complete events, processing task queues, completion events | Decouple and absorb bursts |
| **Workflow engine (Temporal / Step Functions)** | Orchestrate the processing DAG | Retries, state, fan-out/fan-in |

```sql
-- Logical schema (Cassandra: partition key = video_id)
CREATE TABLE video_metadata (
    video_id        TEXT PRIMARY KEY,
    uploader_id     TEXT,
    title           TEXT,
    description     TEXT,
    status          TEXT,          -- UPLOADING | PROCESSING | READY | FAILED
    original_url    TEXT,          -- s3://originals/...
    manifest_url    TEXT,          -- https://cdn/.../master.m3u8
    thumbnail_url   TEXT,
    duration_sec    INT,
    chunks          LIST<FROZEN<chunk>>, -- {part_number, fingerprint, etag, status}
    created_at      TIMESTAMP
);
-- Videos by uploader (channel page) → separate table partitioned by uploader_id
```

> **Partition by `video_id`** — the hot path is always a single-video point lookup; no cross-video transactions needed.

---

## 7. High-Level Architecture

```
                  ┌──────────────────────────── 3. upload parts directly (presigned) ───────────────────┐
                  │                                                                                     ▼
 ┌────────┐ 1. POST /videos    ┌──────────────┐   ┌───────────────┐    ┌──────────────┐        ┌──────────────────┐
 │ Client │───────────────────▶│ LB / API GW  │──▶│ Video Service │───▶│ Metadata DB  │        │ S3 originals     │
 │ web /  │◀── 2. presigned ───│ auth, rate   │   │ (stateless)   │◀──▶│ (Cassandra)  │        │ (multipart)      │
 │ mobile │       URLs         └──────────────┘   └──────┬────────┘    └──────▲───────┘        └────────┬─────────┘
 │ / TV   │                                              │ cache-aside        │ 7. status=READY,         │ 4. ObjectCreated
 └───┬────┘                                       ┌──────▼──────┐             │    manifest URL          ▼
     │                                            │ Redis cache │      ┌──────┴──────────────────────────────────┐
     │ 8. GET /videos/{id} → manifest URL         └─────────────┘      │ Video Processing Service (DAG)          │
     │                                                                 │ split → transcode segments (parallel)   │
     │ 9. GET master.m3u8 / segments                                   │ → audio, thumbnails, captions → manifests│
     ▼                                                                 └──────┬──────────────────────────────────┘
 ┌──────────┐   miss    ┌──────────────────────┐   5. write segments + manifests │
 │   CDN    │──────────▶│ S3 transcoded bucket │◀────────────────────────────────┘
 │  (edge)  │           └──────────────────────┘
 └──────────┘
```

> Two very different planes: a small **control plane** (API + metadata) and a huge **data plane** (blob storage + CDN).
> Video bytes **never** pass through our API servers.

### 7.1 Upload flow
1. Client `POST /videos` with metadata (+ chunk fingerprints) → Video Service writes row with `status=UPLOADING`,
   starts an **S3 multipart upload**, returns **presigned URLs** per part.
2. Client uploads parts **in parallel directly to S3**, reporting each part's ETag (`PATCH .../chunks`).
3. Client calls `complete` → S3 assembles the object and emits `ObjectCreated` → event on a queue.
4. **Processing Service** picks it up, sets `status=PROCESSING`, runs the DAG (8.1).
5. Outputs (segments, manifests, thumbnails) are written to the **transcoded bucket**; a completion event updates
   metadata to `READY` with the manifest URL and invalidates/warms the cache.
6. Client is notified (poll / push) that the video is ready.

### 7.2 Watch flow
1. Client `GET /videos/{id}` → Video Service → Redis → DB → returns metadata + `manifestUrl`.
2. Player fetches the **primary manifest** from the CDN, picks a rendition, fetches its **media manifest**.
3. Player downloads **segments** sequentially from the nearest CDN edge, switching rendition per segment (ABR).
4. CDN miss → origin pull from the transcoded bucket. After step 1, the backend is **not involved** in playback.

### 7.3 Naive options to dismiss (show trade-off thinking)
| Option | Verdict |
|--------|---------|
| Upload file through API servers | Ties up app servers for minutes, huge memory/bandwidth, no resumability → **bad** |
| Store/serve the **original** file and download whole video | Long start time, huge on mobile, device may not support the codec → **bad** |
| Serve one transcoded MP4 per resolution, client picks | Works, but can't adapt mid-playback, still downloads too much → **OK** |
| **Segmented renditions + manifests (HLS/DASH) via CDN** | Fast start, adaptive, cache-friendly → **great** |

---

## 8. Deep Dives

### 8.1 Video processing pipeline (transcoding as a DAG)
Output of the pipeline: **segments for every rendition** + **manifests** in the transcoded bucket.

```
                        ┌─▶ transcode seg 1 → {240p..4K} × {H.264, VP9/AV1} ──┐
 original ─▶ inspect ─▶ split by GOP ─▶ ... (segments × renditions in parallel) ├─▶ generate manifests ─▶ mark READY
   (S3)     (validate)  ├─▶ transcode seg N → ...                             ─┤
                        ├─▶ extract + encode audio tracks ─────────────────────┤
                        ├─▶ thumbnails / watermark ────────────────────────────┤
                        └─▶ captions / transcript (speech-to-text) ────────────┘
```

- **Why a DAG:** steps have one-way dependencies with big fan-out/fan-in; segments are independent → **embarrassingly
  parallel**. Per-creator variations (watermark, custom thumbnail) are just different DAG configs.
- **Orchestration:** use a workflow engine (**Temporal**, Step Functions), or a custom
  **preprocessor → DAG scheduler → resource manager → task workers** (ByteByteGo's model, inspired by Facebook SVE).
- **Resource manager:** task queue (priority), worker queue (utilization), running queue; scheduler matches the
  highest-priority task to the best worker. Different worker pools: encoder (CPU/GPU), thumbnail, watermark, merger.
- **Intermediate data in S3**, pass **URLs** between tasks — workers stay stateless; retries reuse persisted
  segments.
- **Autoscale workers on queue depth**; queues absorb upload bursts.
- **Priority:** popular creators / short videos first; expensive codecs (AV1) can be produced **later**, only for videos
  that become popular.

### 8.2 Resumable uploads (multipart)
1. Client splits the file into **~5–100 MB chunks**, each with a **fingerprint** (hash).
2. Metadata stores `chunks: [{partNumber, fingerprint, status: NOT_UPLOADED}]`.
3. Each part is PUT to its presigned URL; S3 returns an **ETag** → client reports it → server verifies (via S3
   `ListParts`) and marks `UPLOADED`.
4. On disconnect, client re-fetches metadata (or `ListParts`) and **uploads only missing parts**.
5. `CompleteMultipartUpload` → exactly one object-created event → processing starts.
6. Lifecycle rule **aborts stale multipart uploads** (e.g. after 7 days) to avoid paying for orphaned parts.

> In practice this is S3 multipart upload / GCS resumable upload — but walking through it shows depth.

**Faster uploads:**
- **Parallel part uploads** (bandwidth-bound, not latency-bound).
- **Upload to the nearest region / edge** (S3 Transfer Acceleration, regional upload buckets) and replicate later.
- **Pipeline upload + processing:** client splits by GOP and the backend transcodes segments **while the rest is still
  uploading** → video ready much sooner (cost: garbage segments from abandoned uploads).

### 8.3 Scaling reads & low-latency streaming
| Component | How it scales |
|-----------|--------------|
| **Video Service** | Stateless, horizontally scaled behind LB |
| **Metadata DB** | Cassandra: consistent hashing on `video_id`, leaderless replication; read from any replica for hot rows |
| **Metadata cache** | Redis, LRU, partitioned by `video_id` — absorbs viral-video hot keys (+ small in-process cache) |
| **Segments/manifests** | **CDN** caches both → playback never touches the backend; nearest edge = low latency |
| **S3 origin** | Elastic, but single-region → **cross-region replication** for popular content; **origin shield** to collapse CDN misses |

- **Low bandwidth:** ABR ladder down to 144p/240p, short segments (2–6 s) for fast start and quick switches.
- **Fast start:** low starting rendition, small first segment, prefetch the first segments of likely-next videos.

### 8.4 Cost optimization (the long tail)
View distribution is **long-tail** — a few videos get most views, most get almost none.
- **CDN only for popular content**; long-tail served from origin / high-capacity video servers.
- **Fewer renditions for unpopular videos**; encode extra renditions/codecs (AV1, 4K) **on demand** when popularity grows.
  Better codecs (AV1/VP9) cut egress ~30–50% for hot videos — worth the compute.
- **Regional distribution:** push content only to regions where it's popular.
- **Storage tiering:** move originals and cold renditions to infrequent-access / archive storage.
- At very large scale: **own CDN + ISP partnerships** (Netflix Open Connect, YouTube Google Global Cache).

### 8.5 Security & content protection
- **Presigned URLs**: short TTL, scoped to a single object/part, size limits.
- **Signed CDN URLs / cookies** for private or paid content.
- **DRM** (Widevine, FairPlay, PlayReady) or **AES-128 HLS encryption** for premium content; **watermarking**.
- Moderation / copyright (Content ID) as a DAG step or async job → takedown = flip status + **CDN purge**.

### 8.6 Error handling
| Failure | Handling |
|---------|----------|
| Upload part fails | Retry the part; resume from last confirmed part |
| Client can't split video (old app) | Upload whole file; server-side splitting |
| Malformed / unsupported video | **Non-recoverable** → stop DAG, `status=FAILED`, tell the user |
| Segment transcode fails | **Recoverable** → retry with backoff on another worker; after N tries fail the video |
| Worker dies mid-task | Task lease/heartbeat expires → rescheduled (tasks must be **idempotent**, deterministic output keys) |
| Scheduler / queue node down | Replicated queues; workflow engine persists state and resumes |
| API server down | Stateless → LB routes elsewhere |
| Cache node down | Replicas; fall back to DB; replace node |
| DB node down | Replication (Cassandra RF=3, quorum); leader failover if using a primary/replica DB |

---

## 9. Scaling Storage

| Component | Strategy |
|-----------|----------|
| Metadata DB | Partition by `video_id`; RF=3; separate `videos_by_uploader` table for channel pages |
| Redis cache | Cluster, LRU, hot-key replicas |
| Originals | S3 multipart; lifecycle to cold/archive tier after processing |
| Transcoded | S3 + cross-region replication for hot content; per-popularity rendition set |
| CDN | Cache segments & manifests with long TTLs (immutable segment URLs); origin shield |
| Processing | Queue-depth-driven autoscaling of worker pools (spot instances are fine — tasks are retryable) |

---

## 10. Observability
- **Metrics:** upload success rate, time-to-ready (upload → `READY`), DAG task latency/failure per step, queue depth,
  playback start time, **rebuffering ratio**, bitrate distribution, CDN hit ratio, egress cost.
- **Alerts:** rising transcode failures, queue backlog, CDN hit ratio drop, playback error spikes per region/device.
- **Tracing:** follow a `videoId` through upload → each DAG task → completion.

---

## 11. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| Presigned direct-to-S3 multipart upload | Upload through API servers | No app-server bottleneck; resumable; parallel parts |
| Segmented HLS/DASH + ABR | Whole-file MP4 per resolution | Fast start, adapts to bandwidth, CDN-friendly |
| DAG + workflow engine | Single monolithic transcode job | Parallel per segment, per-step retries, configurable pipelines |
| Message queues between stages | Synchronous pipeline | Loose coupling, burst absorption, independent scaling |
| Cassandra partitioned by `video_id` | Relational DB | Pure point lookups, high availability, easy horizontal scale |
| CDN for popular, origin for long tail | CDN for everything | Large cost saving with little UX impact |
| Lazy encoding of extra renditions | Encode everything up front | Compute only spent on videos people watch |
| Pipelined upload + processing | Process after full upload | Faster time-to-ready; cost = garbage from abandoned uploads |

---

## 12. Common Follow-up Questions

1. **Why not upload via our servers?** Multi-GB bodies hold connections for minutes and burn bandwidth/memory; S3
   handles this better and gives resumability for free.
2. **How does the player adapt to bandwidth?** It measures throughput + buffer level and picks the rendition per
   segment from the manifest (HLS/DASH ABR).
3. **A video goes viral?** CDN serves segments; metadata hot key absorbed by Redis + in-process cache + extra
   replicas; S3 origin protected by origin shield.
4. **Resume watching where I left off?** Store `(user_id, video_id) → position` in a KV store, updated every few
   seconds (fire-and-forget, last-write-wins).
5. **View counts?** Don't write to the metadata row per view — stream view events to Kafka, aggregate (approximate is
   fine), periodically write totals; dedupe/anti-fraud offline.
6. **Live streaming differences?** Much stricter latency (LL-HLS / WebRTC), real-time transcoding of small chunks
   (less parallelism needed), no time for slow retries.
7. **Takedowns?** Mark video blocked in metadata, purge CDN, delete or lock blobs; detection at upload (DAG step) or
   via user reports.
8. **Same video uploaded twice?** Content hash / perceptual fingerprint → reuse existing renditions (dedupe storage).

---

## 13. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clear API + data model, working upload/watch design, blob storage via presigned URLs, converges on multipart upload and segment-based streaming, one deep dive |
| **Senior (E5)** | Moves quickly through HLD; details multipart/resumable upload, transcoding pipeline producing segments + manifests for ABR, CDN + metadata caching for hot videos |
| **Staff+ (E6)** | Drives the deep dives: DAG orchestration & failure semantics, pipelined upload/processing, cost optimization (long tail, lazy encoding, own CDN), multi-region; treats interviewer as a peer |

---

## 14. One-Minute Summary
> A stateless **Video Service** stores metadata in **Cassandra partitioned by video_id** (cached in **Redis**) and hands
> clients **presigned S3 multipart URLs**, so multi-GB videos upload **directly, in parallel and resumably** to blob
> storage. Upload completion triggers a **processing DAG** (orchestrated by a workflow engine with queue-driven
> autoscaled workers) that splits the video by GOP, **transcodes every segment into multiple renditions in parallel**,
> extracts audio/thumbnails/captions and writes **HLS/DASH segments + manifests** to a transcoded bucket, then marks the
> video `READY`. Viewers fetch metadata once, then stream segments from the **CDN** with **adaptive bitrate**, so the
> backend is out of the playback path. Cost is controlled by exploiting the **long tail**: CDN and extra renditions only
> for popular videos, cold storage for the rest.

---

### References
- ByteByteGo, *Design YouTube* — https://bytebytego.com/courses/system-design-interview/design-youtube
- Hello Interview, *YouTube — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/youtube

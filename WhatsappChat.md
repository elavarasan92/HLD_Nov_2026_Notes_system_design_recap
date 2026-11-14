# Chat System (WhatsApp / Messenger) — System Design (Interview Guide)

> Design a messaging service for **1:1 and small group chats (≤ 100 members)** that delivers messages in real time
> (**< 500 ms**) to online users, holds messages for offline users, works across **multiple devices per user**, shows
> **online / last-seen presence**, and supports media attachments — at **billions of users** scale, without losing
> messages.

---

## 0. How to Drive the Interview (45–60 min)

| Step | Time | What to cover |
|------|------|---------------|
| 1 | 5 min | Clarify: 1:1 vs group, group size, mobile/web, scale, media, history retention, E2E encryption |
| 2 | 3 min | Back-of-the-envelope estimates |
| 3 | 2 min | Core entities |
| 4 | 5 min | API — WebSocket commands + a few HTTP endpoints |
| 5 | 10–15 min | High-level design — one functional requirement at a time, **start on a single chat server** |
| 6 | 15 min | Deep dives: scale-out & routing, multi-device, reliability (acks, heartbeats, seq numbers), ordering, presence |
| 7 | 5 min | Trade-offs, wrap-up |

**Key line to say early:** *"1:1 is just a group chat with 2 participants, so I'll design for groups. The problem has
two halves: **durable delivery** (persist first, ack from the client, inbox for offline) and **real-time delivery**
(persistent WebSocket + routing to whichever server holds the recipient's connection)."*

---

## 1. Requirements

### Functional
1. Create **group chats** (2–100 participants; 1:1 is the special case) and add/remove members.
2. **Send / receive messages** in real time.
3. **Offline delivery** — users receive messages sent while they were offline (held up to 30 days, or forever if
   history is server-side — clarify!).
4. **Media** (images, video, files) in messages.
5. **Multi-device** — phone, tablet, laptop stay in sync.
6. **Presence** — online indicator / "last seen".

### Below the line (mention, don't design)
- Audio/video calls, stories/status, business accounts, message search.
- Registration, profile management, contacts sync.
- Spam/abuse detection, full E2E-encryption protocol (Signal) details.

### Non-Functional
| Property | Target |
|----------|--------|
| Latency | **< 500 ms** end to end for online recipients |
| Deliverability | **No lost messages** — at-least-once + client-side dedup |
| Scale | ~1B users, **~200M concurrent connections**, tens of billions of messages/day |
| Availability | Survive loss of any single chat server / node / AZ; reconnect transparently |
| Ordering | Consistent order per chat across all devices (not necessarily true send order) |
| Privacy | Store messages on the server **no longer than necessary** (delete after delivery or TTL) |

---

## 2. Capacity Estimates

- **Users:** 1B registered, 500M DAU, **~200M concurrently connected**.
- **Messages:** 500M DAU × ~40 msg/day ≈ **20B msgs/day ≈ 230k/s avg, ~1M/s peak**.
- **Writes:** each message → 1 `messages` row + 1 inbox row **per recipient device** → several × the message rate.
  Mostly 1:1, so ~2–3 writes/message → **~1M+ writes/s peak** → needs a horizontally partitioned KV / wide-column store.
- **Storage:** ~100 B text + metadata ≈ 200 B → 20B × 200 B ≈ **4 TB/day** (text only). Media is separate (blob store
  + CDN) and dwarfs this.
- **Connections:** at ~1–2M connections per tuned host (WhatsApp/Erlang figure) → **100–200 chat servers**;
  at a more conservative 100k–500k/host → **400–2,000**. Each idle WebSocket ≈ 10 KB RAM → 1M conns ≈ 10 GB.
- **Read:write ≈ 1:1** for 1:1 chats; recent messages are hot, old history rarely read (long tail).

> Do these just-in-time: the connection count is what forces scale-out, and the write rate drives the DB choice.

---

## 3. Core Entities

| Entity | Key fields |
|--------|-----------|
| **User** | user_id, name, last_seen |
| **Client (device)** | client_id, user_id, platform, push_token, last_active |
| **Chat** | chat_id, name, type (DIRECT / GROUP), created_by, created_at |
| **ChatParticipant** | chat_id, user_id, role, joined_at |
| **Message** | message_id, chat_id, sender_id, client_msg_id, content / attachment_ref, server_ts, seq |
| **Inbox (per client)** | client_id, message_id — "undelivered to this device" |
| **Attachment** | attachment_id, blob_url, mime, size (encryption key held by clients) |

---

## 4. API Design

**Why WebSocket?** Messages flow both ways at high frequency and the server must **push**. Compare:

| Technique | Verdict |
|-----------|---------|
| Short polling | Wasteful — most polls return "nothing new"; latency = poll interval |
| Long polling | Better, but a stateless LB may land the poll on the wrong server; hard to detect disconnects; reconnect churn |
| SSE | Server→client only; client still needs HTTP for sends |
| **WebSocket (over TLS)** | **Persistent, bidirectional, full duplex**, works through firewalls (443). Chosen. |

Everything that isn't real-time (login, profile, media upload, history paging) stays plain **HTTP** behind an L7 LB.
Chat servers sit behind an **L4 LB** — we need no L7 routing on a long-lived socket, and L4 is cheaper/faster.

### WebSocket commands (client → server)
```text
createChat             { participants[], name }                       -> { chatId }
sendMessage            { chatId, clientMsgId, text?, attachmentIds? } -> { messageId, serverTs, seq }   # SUCCESS/FAIL
modifyChatParticipants { chatId, userId, op: ADD|REMOVE }             -> SUCCESS/FAIL
ack                    { messageId }                                   # "this device has it, stop resending"
heartbeat              { }                                             # liveness, every ~5–30 s
sync                   { chatId → lastSeq }                            # catch up after reconnect
```

### WebSocket commands (server → client)
```text
newMessage  { chatId, messageId, senderId, text, attachments, serverTs, seq }  -> client replies ack
chatUpdate  { chatId, participants[] }                                          -> client replies ack
presence    { userId, status: ONLINE|OFFLINE, lastSeen }
```

### HTTP
```http
POST /v1/auth/login                         -> token + chat-server endpoint (from service discovery)
POST /v1/attachments                        -> { attachmentId, presignedUploadUrl }
GET  /v1/attachments/{id}                   -> presigned / CDN download URL
GET  /v1/chats/{chatId}/messages?before=seq -> history page (if history is server-side)
POST /v1/devices                            -> register client + push token
```

> `clientMsgId` (UUID generated on the device) makes `sendMessage` **idempotent**: a retried send after a dropped ack
> returns the same `messageId` instead of creating a duplicate.

---

## 5. Data Model

### Storage choices
| Store | Role | Why |
|-------|------|-----|
| **Cassandra / DynamoDB / HBase** | `messages`, `inbox`, `chat_participants` | ~1M writes/s, horizontal scale, partition-key access, TTL. Messenger used HBase, Discord Cassandra |
| **PostgreSQL / MySQL** (sharded) | Users, profiles, contacts, device registry | Relational, low volume, strong consistency |
| **Redis** | Pub/Sub routing, presence/last-seen, connection registry, rate limits | Sub-ms, ephemeral |
| **S3 / blob store + CDN** | Media attachments | Cheap, bandwidth offloaded from chat servers |
| **ZooKeeper / etcd / Consul** | Chat-server membership, consistent-hash ring | Service discovery |

Why not a relational DB for messages? Huge append-only volume, random access into a long tail of old data makes big
B-tree indexes expensive, and every query is keyed by chat or user anyway → **wide-column / KV store fits naturally**.

```text
-- Cassandra / DynamoDB
chats              PK chat_id                                  -> name, type, created_at
chat_participants  PK chat_id,   SK user_id                    -> role, joined_at
  GSI              PK user_id,   SK chat_id                    -> "all chats for a user"
messages           PK chat_id,   SK message_id (time-sortable) -> sender_id, content, attachments, server_ts   TTL
inbox              PK client_id, SK message_id                 -> chat_id                                       TTL 30d
clients            PK user_id,   SK client_id                  -> platform, push_token, last_active
```

- Partition `messages` by **`chat_id`** — every read ("load this chat") hits one partition. For very hot/huge chats
  add a time bucket: `PK (chat_id, bucket)` to cap partition size.
- `inbox` rows are deleted on `ack`; TTL cleans up abandoned devices.

### Message ID
Must be **unique** and **sortable by time** (`created_at` alone collides). Options:

| Option | Verdict |
|--------|---------|
| DB auto-increment | Not available / not scalable in NoSQL |
| **Snowflake-style 64-bit ID** (ts + machine + seq) | Globally unique, roughly time-ordered, no coordination. Good default |
| **Per-chat sequence number** | Strict, gap-free order within a chat; easy "what did I miss?" (`seq > lastSeq`). Needs an atomic counter per chat (Redis `INCR` / conditional write) |

Common answer: Snowflake `message_id` for identity + per-chat `seq` for ordering & sync.

---

## 6. High-Level Architecture

```
            ┌──────────── Clients (phone / tablet / web, many per user) ────────────┐
            │  HTTPS (login, media, history)          WSS (messages, acks, presence) │
            ▼                                          ▼
   ┌──────────────────┐                        ┌──────────────────┐
   │ L7 LB / API GW   │                        │ L4 LB            │
   └────────┬─────────┘                        └────────┬─────────┘
            ▼                                           ▼
   ┌──────────────────┐  which server?  ┌─────────────────────────────────────┐
   │ API servers      │ ──────────────▶ │ Service discovery (ZooKeeper/etcd)  │
   │ auth, profile,   │                 └─────────────────────────────────────┘
   │ groups, media URL│                         ┌───────────────────────────────────┐
   └───┬──────────┬───┘                         │ Chat servers (stateful, WebSocket) │
       │          │ presigned URL               │ local map: client_id → socket      │
       │          ▼                             └──┬──────────┬──────────┬───────────┘
       │   ┌─────────────┐    upload/download      │ write    │ pub/sub  │ offline?
       │   │ S3 + CDN    │ ◀──── directly from     ▼          ▼          ▼
       │   └─────────────┘       the client  ┌──────────┐ ┌────────┐ ┌──────────────┐
       ▼                                     │Cassandra │ │ Redis  │ │ Push service │
   ┌──────────────┐                          │messages, │ │pub/sub,│ │ (APNs / FCM) │
   │ Postgres     │                          │inbox,    │ │presence│ └──────────────┘
   │ users,devices│                          │chats     │ └────────┘
   └──────────────┘                          └──────────┘
```

- **Stateless:** API servers (login, profile, groups, media URLs) — scale freely behind an L7 LB.
- **Stateful:** chat servers (+ presence, often co-located) — each holds persistent sockets; a client sticks to one
  server while it's healthy.
- **Third party:** push notifications for offline devices (see the *notification-system* design).

Build it up one requirement at a time — **start with a single chat server**, then scale out in the deep dives.

### 6.1 Create a chat
1. Client sends `createChat` over the socket.
2. Server writes `chats` + one `chat_participants` row per member (single transaction for small chats; batch near the
   100 limit).
3. Returns `chatId`; members get `chatUpdate`.

### 6.2 Send / receive a message (single server, everyone online)
1. Sender → `sendMessage`.
2. Server looks up participants, then each recipient's socket in an **in-memory map `client_id → socket`**, and pushes
   `newMessage`.

Works only because everyone is on one host and online — fixed next.

### 6.3 Offline delivery — persist first, inbox per device, client ack
1. Sender → `sendMessage { chatId, clientMsgId, ... }`.
2. Server assigns `messageId` / `seq`, then **(a)** writes `messages` and **(b)** writes an `inbox` row for **every
   recipient device**.
3. Only after the write succeeds → reply `SUCCESS { messageId }` to the sender (✓ "sent").
4. Push `newMessage` to each connected device; device replies `ack` → server **deletes that inbox row** (✓✓
   "delivered").
5. Disconnected device: send a **push notification**. On reconnect the server reads that device's `inbox`, fetches the
   messages, pushes them, and deletes on ack.
6. TTL on `inbox` / `messages` removes undelivered or old data (privacy requirement).

> **Durability rule:** a message is "sent" only once it is in the DB. Pub/Sub and sockets are best-effort on top; the
> inbox is the source of truth for what a device still needs.

### 6.4 Media
- Never stream large blobs through chat servers. Client calls `POST /attachments` → gets a **presigned S3 URL** →
  uploads directly → sends `sendMessage { attachmentIds }`.
- Recipients download via CDN / presigned URL. With E2E encryption the client encrypts the blob and puts the key in
  the (encrypted) message, so the server only stores ciphertext.
- Lifecycle rule deletes blobs after all recipients downloaded or after N days.

---

## 7. Deep Dives

### 7.1 Scaling out — which server holds the recipient's socket?
One host can't hold 200M connections and is a single point of failure. With N chat servers, User A (server 1)
messaging User C (server 2) is a **routing problem**.

| Option | Verdict |
|--------|---------|
| **Bad:** broadcast every message to every chat server | O(servers) fan-out per message — doesn't scale |
| **Bad:** Kafka topic per user | Kafka isn't built for ~1B topics; per-topic overhead is huge |
| **Good:** connection registry (`user → server`) in Redis + direct server-to-server RPC | Works; registry must be kept fresh on every connect/disconnect, races on reconnect |
| **Good:** **consistent hashing** — `hash(userId)` picks the chat server; service discovery holds the ring and tells clients where to connect | Sender's server knows the target without a lookup. Rebalancing on scale-out forces reconnects; hot users skew load |
| **Great:** **Redis Pub/Sub, channel per user** — on connect the chat server `SUBSCRIBE user:{id}`; to send, `PUBLISH user:{id}` | Any server can deliver to any user, servers stay decoupled, LB can be plain least-connections. Pub/Sub is at-most-once, which is fine because the **inbox already made it durable** |

Redis Pub/Sub itself is sharded (Redis Cluster sharded pub/sub: hash channel → node). Subscriptions are cheap and
in-memory.

**Partition channels by user or by chat?** WhatsApp is dominated by 1:1 chats with a 100-member cap → **per-user
channels** (one subscription per connected user, a group send = ≤ 100 publishes). Per-chat channels would mean each
server subscribes to every chat of every connected user — mostly redundant for 1:1. For **large chats** (the
"celebrity" case) go **hybrid**: chats above a threshold (e.g. 25+) get a chat-level channel that servers subscribe to;
publish there instead of N per-user publishes (briefly publish to both when a chat crosses the threshold).

**Service discovery flow:** client logs in via HTTP → API server asks ZooKeeper/etcd for the best chat server (geo,
load) → returns its host → client opens WSS there. Servers register/deregister with heartbeats.

### 7.2 Multiple devices per user
- Add a **`clients`** table (`user_id → client_ids`), cap at e.g. **3–5 devices** to bound storage/fan-out.
- **Inbox is per client**, not per user — phone acked ≠ laptop has it.
- Send path: participants → their clients → inbox row per client → publish to `user:{id}` (every server holding any of
  that user's devices is subscribed and delivers to its local sockets). The sender's **other** devices get it too.
- Alternative sync model: each device keeps **`cur_max_message_id` / last `seq` per chat** and pulls `seq > cur_max`
  from the KV store on reconnect — no per-device inbox writes; works well when history is server-side.
- Deactivate stale clients (no activity for 30 days) to stop accumulating inbox rows.

### 7.3 Dead WebSocket connections
A socket can look open while the network is gone; TCP keepalive takes minutes to notice.
- **Application heartbeats** (ping every ~5–30 s; miss 2–3 → close and clean up subscriptions).
- Client **reconnects with exponential backoff + jitter** (avoid a thundering herd after a server dies), then
  **syncs**: drains its inbox / sends `lastSeq` per chat.
- On chat-server crash its clients reconnect elsewhere via the LB; nothing is lost because pending messages are in the
  inbox.

### 7.4 Messages dropped by Pub/Sub or sockets
Pub/Sub is at-most-once; a publish can race a reconnect. Since the inbox is durable everything is **eventually**
delivered — to make it **quick**:
- **Sequence numbers per chat:** client sees `seq` 41 then 43 → requests 42 immediately.
- **Server-side resend:** unacked `newMessage` is retried after a timeout.
- **Periodic sync/poll** (on app foreground, every few minutes) as a final backstop.
- Duplicates from retries are dropped on the client by `messageId` (and on the server by `clientMsgId`).

### 7.5 Ordering
Guaranteeing true send order across distributed senders needs buffering/watermarks — added latency users don't want.
- Chat servers sync clocks via **NTP**; stamp each message with **server receive time** (or a per-chat `seq`).
- Clients display by that server order → **every device shows the same order**, even if arrival order differs.
- Occasionally a message "pops in" above a later one — acceptable UX.
- A per-chat `seq` (atomic counter / conditional write) gives strict total order per chat when needed.

### 7.6 Presence — online indicator & last seen
- **Connect:** presence store `user → { status: ONLINE, last_active_at }` (Redis).
- **Logout:** set OFFLINE + `last_seen = now`.
- **Disconnects are flaky** (tunnel, elevator): don't flip status on every drop. Use **heartbeats** — client pings every
  ~5 s; none for ~30 s → offline. Implement as a Redis key with TTL refreshed by heartbeats
  (`SET presence:{u} <ts> EX 30`) → expiry = offline; `last_seen` = last heartbeat time.
- **Fan-out of status changes** is the expensive part: push changes only to contacts with an **open chat / small
  friend lists**; otherwise **fetch presence lazily** when a user opens the chat, plus periodic refresh.
- Respect privacy settings ("hide last seen") at read time.

### 7.7 Group chat fan-out
- **Small groups (≤ 100–500, WhatsApp/WeChat style):** fan-out on write — copy into each recipient's inbox. Simple:
  every client only reads its own inbox.
- **Large channels (Discord/Slack style, thousands+):** fan-out on read — store once per channel, clients pull by
  `seq`; push only lightweight "new activity" signals.

### 7.8 Push notifications for offline devices
- No live socket (no subscriber / no ack within a timeout) → enqueue to the **push service** → APNs/FCM. Collapse
  multiple messages per chat, respect mute settings.
- With E2E encryption push only a wake-up / encrypted payload; the app fetches and decrypts.

### 7.9 End-to-end encryption (if time allows)
- Signal protocol: per-device identity keys; the server stores only **public prekeys**; messages are encrypted per
  recipient **device** (another reason for per-device fan-out).
- Server sees metadata (who/when), not content → no server-side search; history lives on devices.

---

## 8. Scaling Storage
- **Messages:** partition by `chat_id` (+ time bucket for hot chats); replication factor 3; quorum writes.
- **Inbox:** partition by `client_id`; short-lived, delete-heavy rows → tune compaction (tombstones!).
- **Hot vs cold:** recent messages in the KV store / cache; archive old history to cheaper storage if kept forever.
- **Multi-region:** home a user to a region; cross-region messages go through replicated queues; clients connect to the
  nearest edge (geo-DNS); **cell-based** deployment limits blast radius.

---

## 9. Observability
- **Metrics:** connections per server, send→deliver latency (p50/p99), ack rate, inbox size/age, Pub/Sub publish
  failures, reconnect rate, push fallback rate.
- **Alerts:** rising inbox age (delivery stuck), reconnect storms, uneven connection distribution.
- **Tracing:** carry `messageId` / `clientMsgId` from sender → chat server → DB → Pub/Sub → recipient ack.

---

## 10. Trade-offs to Mention

| Decision | Alternative | Why chosen |
|----------|-------------|------------|
| WebSocket | Long polling / SSE | Bidirectional, low latency, server push |
| L4 LB for chat servers | L7 LB | No path/header routing needed on a long-lived socket; cheaper, faster |
| Persist + inbox **before** delivery | Fire-and-forget push | Durability; Pub/Sub can then be at-most-once |
| Client `ack` + resend | Trust the socket write | A socket write ≠ device received it |
| Redis Pub/Sub per user | Consistent hashing / registry | Decouples servers, simple LB; durability handled by inbox |
| Per-user channels (+ per-chat for big groups) | Per-chat only | 1:1-dominated traffic, bounded group size |
| Wide-column/KV for messages | Relational | Massive writes, partition-key access, TTL, long tail |
| Server timestamp / seq ordering | True send order | Lower latency; consistent across devices |
| Fan-out on write (small groups) | Fan-out on read | Simple client sync; bounded by group cap |
| Heartbeat presence with grace period | Status on every connect/disconnect | Avoids flapping on flaky networks |

---

## 11. Common Follow-up Questions

1. **Chat server crashes?** Clients detect via heartbeat, reconnect (backoff + jitter) elsewhere, re-subscribe and drain
   their inbox — nothing lost because the message was persisted before delivery.
2. **Sender gets no ack and retries?** `clientMsgId` dedups on the server; `messageId` dedups on the recipient.
3. **Sent / delivered / read ticks?** Sent = DB write ack; delivered = recipient device `ack`; read = client sends
   `read { chatId, upToSeq }`, relayed to the sender like a tiny message.
4. **One of 5 devices is off for a week?** Its inbox accumulates (bounded by TTL); on return it drains or does a full
   `seq` sync; devices inactive > 30 days are deregistered.
5. **Group of 100k members?** Switch to fan-out on read: store once, chat-level channel, clients pull by `seq`, lazy
   presence.
6. **Typing indicators?** Ephemeral events over Pub/Sub only — never persisted, fine to drop.
7. **Why not Kafka for delivery?** Great for durable pipelines (analytics, push fan-out), but per-user topics don't
   scale and its partition model isn't suited to routing to one of 200M sockets.
8. **Deploying chat servers without dropping everyone?** Drain gradually: stop accepting new connections and ask
   clients to reconnect over minutes, not all at once.

---

## 12. What's Expected at Each Level

| Level | Bar |
|-------|-----|
| **Mid (E4)** | Clear requirements, WebSocket-based API, working HLD: persist messages, inbox for offline users, media via blob store. Scaling has rough edges but knows the flaws |
| **Senior (E5)** | Speeds through HLD; scales chat servers with Pub/Sub or consistent hashing and explains trade-offs (per-user vs per-chat channels), acks + idempotency, heartbeats, multi-device inbox, ordering, presence |
| **Staff+ (E6)** | Drives deep dives proactively, 2–3 levels deep on failure modes: reconnect storms, Pub/Sub loss vs inbox durability, hot chats, tombstone-heavy inbox storage, multi-region/cell architecture, E2E-encryption implications |

---

## 13. One-Minute Summary
> Clients hold a **WebSocket** to a stateful **chat server** (L4 LB, chosen via service discovery); everything else is
> HTTP to stateless API servers. On `sendMessage` (idempotent via `clientMsgId`) the server assigns a time-sortable
> `messageId` / per-chat `seq`, **persists** to **Cassandra/DynamoDB**, writes an **inbox row per recipient device**,
> then acks the sender. It **publishes to per-user Redis Pub/Sub channels**; whichever servers hold those users'
> sockets push `newMessage`, and each device **acks** to delete its inbox row. Offline devices get a **push
> notification** and drain their inbox on reconnect. Media goes direct to **S3/CDN** via presigned URLs. **Heartbeats**
> detect dead sockets and drive presence/last-seen (Redis TTL); **sequence numbers** plus periodic sync close gaps from
> at-most-once Pub/Sub; display order uses server timestamps so all devices agree.

---

### References
- ByteByteGo, *System Design Interview — Design a Chat System* —
  https://bytebytego.com/courses/system-design-interview/design-a-chat-system
- Hello Interview, *WhatsApp — System Design Problem Breakdown* —
  https://www.hellointerview.com/learn/system-design/problem-breakdowns/whatsapp

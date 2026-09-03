# Stage 4 — Delivery guarantees

**This is built. Kept as the record of what was planned and why.**
The result, with real numbers, is in the README under
[Delivery guarantees](../README.md#delivery-guarantees).

Two things came out different from the plan below:

- **Step 5 (a `last_seq` map in the `connected` frame) was not built.** It
  saves the client one round trip and costs a query on every connect, and the
  client needs a gap read anyway. Not worth it yet.
- **`pageParams` needed a `gap` flag**, not just a number. `after_seq=0` is a
  real request — "I have nothing, start me at the beginning" — so `afterSeq > 0`
  could not be used to tell the two reads apart.

## Context

### Where we are now

`ROADMAP.md` tracks the project in stages. Stages 0–3 are done:

- **Stage 0** — graceful shutdown (`cmd/api/main.go`, `App.Shutdown`).
- **Stage 1** — WebSocket delivery on one node (`internal/ws`, `cmd/wsload`).
- **Stage 2** — logging, request id, Prometheus, rate limit, panic recovery.
- **Stage 3** — two nodes behind nginx, Redis Pub/Sub fan-out, presence in a
  Redis sorted set, `SetTrustedProxies`, `cmd/splitcheck`.

**Stage 4 is next, and it is not started.** Nothing in the code carries a
sequence number yet.

### The problem Stage 4 fixes

Live push is fire-and-forget. The code says so in two places:

- `internal/server/ws.go:136` — "a delivery that fails here is a missed live
  push … Stage 4 is where that gap gets closed properly."
- `internal/cluster/cluster.go:14` — "a node that is down when a message is
  published never learns about it."

So a client that drops its socket for 3 seconds loses every message sent in
those 3 seconds. It never finds out. Today the only repair is to re-read
history by hand, and the client cannot tell *how much* it missed, because
`id` is global — a jump from id 100 to id 140 may mean 0 missed messages or 40.

### The outcome we want

1. Every message has a **per-room sequence number** (`seq`): 1, 2, 3 … with no
   holes inside one room.
2. A client remembers its last `seq` per room. After reconnecting it asks the
   server for everything after that number and gets it.
3. Delivery is **at-least-once**: a message may arrive twice, never zero times.
   The client drops a `seq` it already has.

---

## Design decisions

### 1. Where `seq` comes from

Add `conversations.last_seq`. Allocate inside the **same transaction** as the
message insert:

```sql
UPDATE conversations SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq;
INSERT INTO messages (..., seq) VALUES (..., $seq);
```

The `UPDATE` takes a row lock, so two senders in one room are serialised and
cannot get the same number. Both statements commit together, so a crash between
them leaves no hole.

Rejected alternatives:

- **Use the global `id`.** Free, but has holes per room, so a client cannot
  prove it is up to date.
- **One Postgres sequence per room.** Needs `CREATE SEQUENCE` on every new room
  and leaks numbers on rollback.
- **Redis `INCR`.** Fast, but the counter and the row are two systems — the
  exact dual-write problem Stage 5 exists to teach. Keep it in one transaction.

Known cost, to measure in step 8: writes to **one** room now serialise on one
row lock. Different rooms are unaffected.

### 2. Two dedupe keys, two directions

Do not mix these up. Say it in the README.

| Key | Direction | Protects against |
| --- | --- | --- |
| `client_msg_id` | client → server | a retried `POST` writing the message twice |
| `seq` | server → client | a re-delivered push showing the line twice |

`client_msg_id` and its unique index already exist and do not change.

### 3. How the client asks for the gap

Extend the existing history endpoint with a forward cursor:

```
GET /conversations/:id/messages?after_seq=42&limit=100
```

- Returns messages **oldest first** (the opposite of the current `before_id`
  page, which is newest first). Order matters: the client applies them in order.
- Keeps `before_id` exactly as it is — that path is covered by tests and still
  serves "scroll up through history".
- `after_seq` and `before_id` together = `400`. They are two different reads.

The socket stays **delivery only**. It reads no client frames. That rule is in
`internal/server/ws.go:54` and this stage does not break it — a gap fetch is a
normal authenticated REST call that reuses `memberOnly`.

### 4. The reconnect order the client must follow

This is the part that is easy to get wrong, so it goes in the README and in the
test tool:

1. Open the socket **first** and buffer every frame that arrives.
2. Then call `?after_seq=<last_seq>`.
3. Apply the gap, then apply the buffer.
4. Drop any `seq` already seen.

Fetching before connecting leaves a new hole between the two calls.

---

## Steps

### Step 1 — Migration `00004_message_seq.sql`

New file in `internal/database/migrations/`. Follow the style of
`00002_rooms_and_members.sql`: real data movement, a working `Down`, comments
that say *why*.

Up:

```sql
ALTER TABLE conversations ADD COLUMN last_seq bigint NOT NULL DEFAULT 0;
ALTER TABLE messages      ADD COLUMN seq      bigint NOT NULL DEFAULT 0;

-- Backfill: existing rows get an order from their own id, per room.
UPDATE messages m SET seq = n.rn
FROM (SELECT id, row_number() OVER (PARTITION BY conversation_id ORDER BY id) AS rn
      FROM messages) n
WHERE m.id = n.id;

UPDATE conversations c SET last_seq = COALESCE(
  (SELECT max(seq) FROM messages WHERE conversation_id = c.id), 0);

CREATE UNIQUE INDEX idx_msg_seq ON messages (conversation_id, seq);
```

The unique index is the safety net: if the allocation is ever wrong, the insert
fails loudly instead of quietly overwriting a client's view.

Down: drop the index, then the two columns.

### Step 2 — Models

`internal/database/models.go`:

- `Conversation` gets `LastSeq uint`.
- `Message` gets `Seq uint`.

No GORM tags — the SQL file owns the schema, as the comment at the top of that
file says.

### Step 3 — Store

`internal/database/conversations.go`:

- **`CreateMessage`** — wrap in `s.db.Transaction`. Inside: `Raw("UPDATE
  conversations SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq")`,
  then `tx.Create(msg)` with that seq. On `gorm.ErrDuplicatedKey` return an
  error from the transaction so it rolls back — the seq must **not** be
  consumed by a retry — then read the existing row *outside* the transaction,
  exactly as the current code does at `conversations.go:161-174`.
- **`ListMessagesAfterSeq(ctx, conversationID, afterSeq uint, limit int)`** —
  new method. `WHERE conversation_id = ? AND seq > ?`, `ORDER BY seq ASC`,
  `Limit(limit)`, `Preload("Sender")`. It uses the new unique index.
- Add both to the `Service` interface in `internal/database/database.go` with
  the same comment style as its neighbours.

### Step 4 — HTTP layer

`internal/server/conversations.go`:

- `messageDTO` gets `Seq uint \`json:"seq"\``.
- `messagePageDTO` gets `NextAfterSeq *uint \`json:"next_after_seq"\`` — set
  when the page is full, so the client knows to ask again. A big gap after a
  long disconnect needs more than one page.
- `pageParams` reads `after_seq`, rejects it together with `before_id`, and
  reuses the same `maxPageSize` cap.
- `ListMessagesHandler` branches: `after_seq` present → `ListMessagesAfterSeq`
  (ascending); otherwise the current `before_id` path, unchanged.

### Step 5 — Push path

`internal/server/ws.go` needs **no** change to the cluster code. The Redis
envelope in `internal/cluster/cluster.go` carries the marshalled `wsEnvelope`
as raw JSON, so `seq` inside `messageDTO` travels between nodes for free.

Add one thing: on connect, the `"connected"` frame already sent at
`ws.go:83-89` should also carry the server's view, so the client knows where to
start. Keep it small — the user id it already has, plus nothing else if that
means an extra query. Decide when writing it: a per-room `last_seq` map costs
one query per connect, and it saves the client a round trip. Do it only if the
query is a single `SELECT conversation_id, last_seq` over the user's rooms.

### Step 6 — Metrics

`internal/metrics/metrics.go`, same namespace `chat`:

- `gap_messages_total` (counter) — messages served through `after_seq`.
- `gap_size` (histogram) — how many messages one recovery asked for. Buckets
  1, 5, 10, 50, 100, 500. This is the number that tells you whether the gap
  problem is real in production.

### Step 7 — `cmd/gapcheck`

A new tool, in the style of `cmd/splitcheck`. It proves the fix instead of
claiming it.

1. Log in two seeded users, create a shared room.
2. User B opens a socket on node `:8081` and records every `seq` it sees.
3. User A sends messages steadily over REST.
4. **Break it**: kill B's socket for a few seconds while A keeps sending. Also
   run a variant that does `docker compose kill redis` instead.
5. B reconnects using the four-step order from the Design section.
6. Report: sent, seen live, recovered by gap, duplicates dropped, **missing**.

The success line is `missing = 0` with `duplicates >= 0`. That sentence is the
Stage 4 result, and it goes in the README.

Add a `gapcheck` target to the `Makefile` next to `splitcheck`.

### Step 8 — Tests

Follow the existing split: real Postgres for the store, `fakeDB` for handlers.

`internal/database/conversations_test.go`:

- seq starts at 1 and has no holes;
- 20 concurrent senders in one room produce 1..20 with no repeat and no hole
  (this is the test that proves the row lock works);
- a repeated `client_msg_id` returns the first row **and does not burn a seq**;
- `ListMessagesAfterSeq` is ascending, respects the limit, and returns empty at
  the head;
- the migration round-trips (extend `TestMigrateDownAndUpMovesOwnerToMember`'s
  pattern in a new test for 00004, including the backfill).

`internal/server/conversations_test.go` — the `fakeDB` there mirrors the real
unique indexes, so give it a seq counter too. Then: `after_seq` returns the
gap ascending, `after_seq` + `before_id` is 400, the limit cap applies,
`next_after_seq` is set only on a full page, a non-member still gets 404.

`internal/server/ws_test.go` — the pushed frame carries a `seq`, and two sends
in a row carry `n` then `n+1`.

### Step 9 — Write the numbers down

The ROADMAP rule is Build → Break → Fix → **write the numbers in the README**.

- `README.md` Status section: replace "Still missing: a message can be lost
  while a client reconnects" with what now happens.
- New README section "Sequence numbers and gap recovery": the `last_seq`
  design, the two dedupe keys table, the four-step reconnect order, and real
  `cmd/gapcheck` output.
- Endpoint table: document `?after_seq=`.
- Measure and record: messages/second into one hot room before and after the
  row lock (use `cmd/wsload -room-size`), and the gapcheck result.
- `ROADMAP.md`: tick `- [x] Stage 4 — Delivery guarantees`.

### Step 10 — Small fix while we are here

`internal/server/auth.go:19` has the only TODO in the repo: `tokenTTL` is 120
minutes, but three places still say 15 minutes (`README.md:43`,
`internal/server/ws.go:119`, `internal/server/refresh.go:158`). Make code and
docs agree. Suggestion: keep 120 minutes for dev but read it from an env var
with a 15-minute default, so the docs become true.

---

## What Stage 4 does NOT do

Say this in the README so the next stage has a reason to exist:

- Redis Pub/Sub is still fire-and-forget. Stage 4 lets a client *notice and
  repair* a miss; it does not stop the miss.
- The handler still writes Postgres and then publishes. That dual write is
  Stage 5's outbox.
- The client must actually ask for the gap. A client that never reconnects
  never learns anything.

---

## Verification

Order matters — cheapest first.

1. `make test` and `make test-race` — unit tests, no Docker.
2. `make itest` — store tests against real Postgres (testcontainers).
3. `make migrate status` on a database with existing messages, then check the
   backfill:
   `SELECT conversation_id, count(*), min(seq), max(seq) FROM messages GROUP BY 1;`
   `max(seq)` must equal `count(*)` for every room.
4. `docker compose up` → `make seed` → `make splitcheck`. Stage 3 must still
   pass — the seq work must not break cross-node delivery.
5. `make gapcheck`. Expect `missing = 0`.
6. `make gapcheck` again with `docker compose kill redis` during the send
   window. Live push stops; the gap fetch must still recover everything.
7. `curl` the metrics: `curl -s localhost:8080/metrics | grep chat_gap`.
8. `make wsload` for the hot-room throughput number for the README.

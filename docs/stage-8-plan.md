# Stage 8 — Scale the data, then prove it

Decisions taken before the build:

- Load tool: **k6**, driving REST reads only. `cmd/wsload` keeps the sockets.
- Replica: **one streaming read replica** of Postgres, in `docker-compose.yml`.
- Cache: **in-process, per node**, invalidated by the NATS fan-out each node
  already receives.
- Partitioning: **hash by `conversation_id`**, not by time.
- Proof tool: **`cmd/lagcheck`**.

---

## Context

### Where we are

Stage 1 measured 5000 sockets and found the limit was not the fan-out. It was
the database. The pool sat at 25 of 25 and piled up **17.8 s of waiting in four
seconds of traffic**, and the p99 of 787 ms was a message waiting for a database
handle. The README says so and calls it "a Stage 8 problem, and now it is a
measured one".

Everything since then has made the system wider — two nodes, a broker, a second
service, a trace across all of it. Nothing has made the data layer bigger. There
is still one Postgres, one pool of 25, and one `messages` table that grows
forever.

### The problem Stage 8 fixes

Three separate problems that all land on the same box:

1. **Every read competes with every write** for the same 25 connections.
   History reads are the most common request in a chat app and the least
   urgent, and they are queued behind message inserts.
2. **The hottest rows are fetched again and again.** Ten people in one busy
   room open it and the same 50 messages are read 10 times.
3. **`messages` only grows.** Its indexes grow with it, and there is no plan
   for the day it has 200 million rows.

And under all three: nobody knows where the real limit is, because there is no
repeatable load test. Stage 1's numbers came from a socket tool, not a read
test.

### The outcome

A read path that does not touch the primary in the common case, a table that is
split under the hood, and — the part that matters most — a k6 script that prints
p50, p95 and p99 before and after every one of those changes, so each one has to
earn its place.

---

## Design decisions

### 1. k6 runs first, and it only drives REST reads

The baseline must exist before the first change, or no change can be defended.
So step one of the build is a k6 script and a recorded run against the code as
it is today.

k6 does **not** get the WebSocket path. [`cmd/wsload`](../cmd/wsload) already
opens 5000 sockets, honours `Retry-After`, and produces the Stage 1 numbers.
Rewriting that in JavaScript would give a second tool that measures the same
thing worse.

| k6 covers | `cmd/wsload` keeps |
| --- | --- |
| `POST /auth/login`, `GET /conversations`, history reads, gap reads, sends | sockets, fan-out latency, slow clients |

k6 runs from its own container in a new `load` compose profile, so nothing has
to be installed on the laptop.

### 2. The missing index is fixed before anything is added

`ListMessages` ([`internal/database/conversations.go:301`](../internal/database/conversations.go#L301))
is the main history read:

```sql
SELECT * FROM messages
WHERE conversation_id = $1 AND id < $2 AND deleted_at IS NULL
ORDER BY id DESC LIMIT 50
```

There is no index on `(conversation_id, id)`. Only `idx_messages_conversation_id
(conversation_id)` exists, so Postgres reads every row of the room and sorts
them. `ListMessagesAfterSeq` is fine — it rides `idx_msg_seq (conversation_id,
seq)` from migration `00004`.

Migration `00007` adds `idx_msg_history (conversation_id, id DESC)`. This is a
one-line change and it may move the p99 more than the replica does. If it does,
that is the most useful result in the stage, and it can only be seen because of
decision 1.

### 3. The replica is a real streaming replica

Not a second empty database. `psql_replica` runs `pg_basebackup` from
`psql_bp` on first start and then follows it. That means:

- the primary gets a `replicator` role and `wal_level=replica`,
- the replica is read-only — a write against it errors, which is the safety net
  for a routing mistake,
- the lag is real and measurable, so `cmd/lagcheck` has something to measure.

### 4. The second pool lives inside `service`, and only four queries use it

[`internal/database/database.go:197`](../internal/database/database.go#L197) is
`type service struct { db *gorm.DB }` — one field, one pool. It becomes two,
`db` and `read`, with a `readDB()` helper.

Routing is decided **per query, in the database package**, not per request in a
middleware. A handler must not have to know which database it is talking to.

`BLUEPRINT_DB_READ_HOST` unset means `read` is the same handle as `db`. That is
the local default, so `make run` and every test are unchanged — the same shape
as `OTEL_EXPORTER_OTLP_ENDPOINT` in Stage 7.

Two things follow from a second handle, and both must be decided, not
discovered: `Health()` ([`database.go:306`](../internal/database/database.go#L306))
and `Close()` ([`database.go:378`](../internal/database/database.go#L378)) both
assume one handle, and `PoolStats()` ([`database.go:365`](../internal/database/database.go#L365))
returns one `sql.DBStats` behind the `PoolStats` interface at
[`internal/metrics/metrics.go:420`](../internal/metrics/metrics.go#L420), which
`RegisterDBPool` publishes. The counters gain a `pool="primary"` /
`pool="read"` label.

### 5. Only pages that can no longer change go to the replica

This is the sharpest decision in the stage.

| Read | Goes to | Why |
| --- | --- | --- |
| History, `before_id > 0` (scrolling back) | **replica** | Old rows. A replica cannot be behind on a row written an hour ago. |
| History, first page (`before_id = 0`) | cache, then **primary** | The newest page is exactly the page a lagging replica gets wrong. |
| `?after_seq=` gap read | **primary** | See below. |
| `ListConversationsForUser`, `UnreadForUser` | **replica** | A badge that is 50 ms old is still a correct badge. |
| Everything else | **primary** | |

The gap read must stay on the primary, and the reason is a real bug, not
caution. `listGap` ([`internal/server/conversations.go:395`](../internal/server/conversations.go#L395))
sets `NextAfterSeq` only when the page comes back **full**. A short page means
"you are caught up". A lagging replica returns a short page for a different
reason — "I do not have those rows yet" — and the client cannot tell the two
apart. It would stop asking, and those messages would never be fetched. That
throws away Stage 4's guarantee to save one query.

The same logic keeps `EnsureMember` ([`conversations.go:105`](../internal/database/conversations.go#L105))
on the primary: a user added to a room one second ago must not get a 403.

The outbox relay is already safe by accident and must stay that way on purpose.
`TryOutboxLock` ([`internal/database/outbox.go:62`](../internal/database/outbox.go#L62))
takes a dedicated `*sql.Conn` out of the pool and holds it for the life of the
leadership, because the advisory lock is session-scoped. That session has to be
the primary's.

### 6. A replica that is down is not an outage

Every replica-routed query falls back to the primary on a connection error, logs
it once, and increments `db_read_fallback_total`. Stage 3 already learned this
the hard way: a shared dependency that can take the service down is a shared
dependency that will.

The replica is **not** in `/readyz`, for the same reason Redis is not.

### 7. The cache is in-process, and NATS is what clears it

Every node already receives every message. `SubscribeFanout`
([`internal/broker/consume.go:55`](../internal/broker/consume.go#L55)) creates a
per-node consumer on `chat.message.>`, because each node holds different
sockets. So the invalidation hook is one call inside `deliverMessage`
([`internal/server/server.go:281`](../internal/server/server.go#L281)), and it
needs no new infrastructure at all.

```
relay -> NATS -> api1   cache.Drop(roomID) ; hub.Broadcast()
              \
               -> api2  cache.Drop(roomID) ; hub.Broadcast()
```

| Rejected | Why |
| --- | --- |
| Shared Redis cache | Puts a Redis client back in the API nodes, which is exactly what Stage 6 removed, and adds a network hop to the path that is supposed to be fast. |
| TTL only, no invalidation | Makes every busy room stale for the length of the TTL. The message the user just sent is the one they will notice missing. |

Rules the cache must follow:

- **One shape only.** It holds the default first page — no `before_id`, no
  `after_seq`, `limit` 50. Any other request skips the cache. More shapes means
  a key space nobody can invalidate correctly.
- **Per room, never per user.** The value is a `[]messageDTO`. Nothing
  user-specific goes in. Unread counts are per user and stay a database read.
- **Bounded.** An LRU with a fixed entry count, e.g. 512 rooms. Unbounded is a
  memory leak with a nicer name.
- **A 30 s TTL on top of the invalidation.** The fan-out is `DeliverNew` with
  acks off, so a node that lost its NATS connection misses invalidations and
  never learns. The TTL is the backstop, and the size of that stale window is
  one of the numbers this stage should write down.
- Metrics: `cache_hits_total`, `cache_misses_total`, `cache_entries`.

### 8. Partitioning is by hash on `conversation_id`, not by time

A unique index on a partitioned table must contain the partition key. `messages`
has two unique indexes and both already start with `conversation_id`:

- `idx_msg_seq UNIQUE (conversation_id, seq)` — Stage 4's ordering
- `idx_msg_client UNIQUE (conversation_id, sender_id, client_msg_id)` — Stage 4
  and 5's dedupe key

Partitioning by `created_at` would force `created_at` into both, which stops
either from guaranteeing anything. Partitioning by `conversation_id` keeps both
exactly as they are, and both hot queries filter on `conversation_id`, so every
read touches **one** partition. That is partition pruning, and it is the whole
benefit.

The cost, accepted openly: deleting old data is no longer one `DROP TABLE`. Time
partitioning buys that and this does not. It is the wrong trade here, because
correctness is not negotiable and cheap deletes are not yet a problem.

Migration `00008` cannot `ALTER` a table into a partitioned one. It must:

```sql
CREATE TABLE messages_new (...) PARTITION BY HASH (conversation_id);
-- 8 partitions
INSERT INTO messages_new SELECT * FROM messages;
-- swap names, recreate every index and both FKs, reset the id sequence
```

All inside one transaction, following migration `00002`'s precedent that a
migration is allowed to refuse when the data does not fit.

Nothing points a foreign key **at** `messages` today — migration `00005`
deliberately left `consumed_messages` without one — and that is what makes this
migration possible at all. It must stay that way.

### 9. Every chaos run has a written expectation before it is run

The point is not "nothing broke". The point is "it broke the way I said it
would". k6 runs throughout, so each one has a latency cost, not just a verdict.

| Run | Expected |
| --- | --- |
| `kill api1` | nginx moves traffic. api1's sockets die, clients reconnect and repair with `?after_seq=`. A p99 spike for a few seconds, then normal. |
| `stop psql_replica` | History reads fall back to the primary. Latency goes up. **Zero errors.** |
| `stop redis` | Presence goes dark. Chat is untouched. (Proved in Stage 3, re-proved under load.) |
| `stop psql_bp` (primary) | Sends fail **fast**, not after a hang. Scroll-back history still works from the replica — that is the new answer this stage buys. |
| Fill the disk | The primary refuses writes. The API returns a clean error and stays up. |

---

## What to build

| Area | Files |
| --- | --- |
| Load test | `k6/read.js`, `k6/lib/`, `docker-compose.yml` (`load` profile), `make k6` |
| Index | `internal/database/migrations/00007_message_history_index.sql` |
| Second pool | `internal/database/database.go` (`service.read`, `readDB()`, env vars, `Health`, `Close`, `PoolStats`) |
| Read routing | `internal/database/conversations.go` (`ListMessages`, `ListConversationsForUser`), `internal/database/unread.go` (`UnreadForUser`) |
| Fallback | `internal/database/read.go` (new: fallback wrapper + `db_read_fallback_total`) |
| Cache | `internal/cache/` (new: bounded LRU with TTL), `internal/server/conversations.go` (read side), `internal/server/server.go` (`deliverMessage` drop) |
| Partitions | `internal/database/migrations/00008_message_partitions.sql` |
| Metrics | `internal/metrics/metrics.go` (pool label, cache counters, replica lag gauge) |
| Replica | `docker-compose.yml` (`psql_replica`), `docker/replica-entrypoint.sh`, `docker/primary-init.sql` |
| Dashboard | `grafana/dashboards/` (cache hit ratio, replica lag, pool waits per pool) |
| Proof | `cmd/lagcheck/main.go`, `make lagcheck` |
| Infra | `docker-compose.yml`, `Makefile`, `.env` docs |

New direct dependencies: none in Go. k6 and the replica are container images.

---

## The proof tool: `cmd/lagcheck`

A table can show a p99. It cannot show how stale a replica is from a user's
point of view, or that a dead replica costs latency and not errors. That is what
this tool is for.

It follows [`cmd/tracecheck`](../cmd/tracecheck)'s shape — the same flag
vocabulary, `report()` then `verdict()`, and a non-zero exit when the claim
fails.

1. Log in as two seeded users, create a room, and send enough messages to make
   several pages of history.
2. **Cache.** Read the first page twice and print both times, plus the delta in
   `cache_hits_total` scraped from `/metrics`. Then send one message and read
   again — the hit counter must not move, because NATS dropped the entry.
3. **Lag.** Connect to the primary and the replica directly. Send a message,
   then poll `SELECT max(seq) FROM messages WHERE conversation_id = $1` on the
   replica until it appears. Repeat 20 times. Print p50 / p95 / max.
4. **Routing.** Read an older page and confirm `db_read_total{pool="read"}`
   moved, and that a gap read did **not** move it.
5. With `-pause`, stop for the operator to run
   `docker compose stop psql_replica`, then prove history reads still return 200
   and print what the fallback costs.

The verdict fails if: a cached read missed after an invalidation, a gap read
touched the replica, or any read returned non-200 while the replica was down.

---

## The baseline

Recorded before any change, with `make k6`, on 50 users and 20 rooms of 200
messages. Server, database and load tool all on one Windows laptop, so these
are relative numbers, not a benchmark.

| Read | 100 req/s | 600 req/s |
| --- | --- | --- |
| First page | p50 **4.07 ms**, p95 **6.22 ms**, p99 **7.95 ms** | p50 **3.75 ms**, p95 **6.30 ms**, p99 **30.72 ms** |
| Scroll back (`before_id`) | p50 **3.61 ms**, p95 **6.05 ms**, p99 **7.96 ms** | p50 **3.76 ms**, p95 **6.34 ms**, p99 **30.66 ms** |
| Gap read (`after_seq`) | p50 **3.89 ms**, p95 **5.97 ms**, p99 **8.85 ms** | p50 **3.49 ms**, p95 **6.03 ms**, p99 **27.86 ms** |
| Room list | p50 **4.62 ms**, p95 **7.10 ms**, p99 **9.34 ms** | p50 **4.24 ms**, p95 **6.91 ms**, p99 **30.61 ms** |
| Send | p50 **6.99 ms**, p95 **10.80 ms**, p99 **14.04 ms** | p50 **6.01 ms**, p95 **9.27 ms**, p99 **52.67 ms** |

Zero failed requests and zero 429s in both runs.

**The median does not move and the p99 quadruples.** Six times the load leaves
p50 exactly where it was and takes the p99 from 8 ms to 31 ms. That shape is
queueing: nothing got slower, some requests started waiting. It is the same
story Stage 1 told with sockets, and it is why every later step in this stage
is judged on the p99 and not on the average.

**Three things this baseline says the plan got wrong.** They are written here
rather than in the retro because they change the order of the work:

1. **The dataset is too small to measure anything.** 10,715 messages is
   2.6 MB against 128 MB of `shared_buffers`. `EXPLAIN (ANALYZE, BUFFERS)`
   reports `shared hit=901`, `read=0` — every page is already in memory and
   nothing ever touches a disk. A read replica, a cache and partitions are all
   answers to a database that has to fetch something. On this dataset they
   would each measure noise and any one of them could be "proved" to help.
2. **Scroll-back is not slower than the first page.** Decision 2 predicted it
   would be the worst read. It is the fastest, by a hair. The prediction
   assumed a query that is not the query Postgres runs.
3. **There is no sort, and the `(conversation_id)` index is not used.** The
   plan said Postgres "reads every row of the room and sorts them". It does
   not. It walks the primary key **backwards** and filters as it goes:

   ```
   Limit (actual time=0.103..0.369 rows=50)
     ->  Index Scan Backward using messages_pkey on messages
           Filter: ((deleted_at IS NULL) AND (conversation_id = 20))
           Rows Removed by Filter: 1050
   ```

   `id DESC` and "newest first" are the same order, so the primary key already
   provides it. That is why the p99 did not care.

   The index is still worth adding, for a reason the plan did not name. That
   backward walk is fast only while the room is **dense** near the end of the
   id space. Reading 50 rows already costs 1100 here, with 20 rooms. A big but
   quiet room in a system with 500 busy ones is sparse up there, and the same
   plan walks tens of thousands of rows to find 50. The failure is not slow
   today and gets slowly worse; it is fine until the room mix changes, and then
   it is a cliff.

So step 2 is no longer "add the index and re-run". It is **make the dataset
real first** — more rooms than fit in cache, and rooms of very different sizes —
because that is what turns all three of these from arguments into numbers.

---

## The order of work

Each step ends with a k6 run and a number written down.

1. `k6/read.js` + `make k6`. **Record the baseline.** Nothing else changes.
   Done — see [The baseline](#the-baseline).
2. **Grow the dataset until it stops fitting in memory**, with rooms of mixed
   sizes. The baseline proved this has to come before anything else: on 2.6 MB
   of data every change in this stage measures noise.
3. The `(conversation_id, id DESC)` index. Re-run k6. Compare.
4. The replica in compose, the second pool, the routing, the fallback. Re-run.
5. `cmd/lagcheck`. Measure the real lag.
6. The cache. Re-run. Watch the hit ratio, not just the p99.
7. Partitioning. Re-run.
8. The five chaos runs, each with its expectation written first.

You can stop after any step and still have finished, measured work.

---

## What Stage 8 does NOT do

- **No write scaling.** One primary takes every write. Sharding writes is a
  different project and this one does not need it.
- **No automatic failover.** No Patroni, no repmgr. A dead primary stays dead
  until someone starts it. Adding failover would mean testing failover, and that
  is a stage of its own.
- **No online re-partitioning.** The partition count is fixed when the table is
  created. Changing it later is a rewrite.
- **No cache for anything user-specific.** Unread counts stay a database read.
  A cache keyed by user times room is a cache nobody can invalidate.
- **No k6 WebSocket tests.** `cmd/wsload` owns that and is better at it.
- **No CI.** Running k6 on every push is Stage 9.

---

## Verification

Cheapest first.

1. `go build ./... && go vet ./...`
2. `make test`
3. `make itest` — the database tests, with `BLUEPRINT_DB_READ_HOST` unset, so
   one pool serves both roles.
4. `make test-race`
5. `make run` with no replica configured. Everything must work exactly as
   before. This is the no-op path.
6. `docker compose up --build -d`, then `make migrate-status` — `00007` and
   `00008` applied.
7. `docker compose exec psql_replica psql -c "SELECT pg_is_in_recovery()"` → `t`.
8. `make seed ARGS="-n 50"`, then `make k6` — the baseline shape.
9. `make lagcheck` — must PASS.
10. `make lagcheck ARGS="-pause 30s"`, and during the pause
    `docker compose stop psql_replica`. Zero errors.
11. `make k6` while running each chaos command from decision 9, one at a time.
12. `make observability`, then Grafana on :3000 — cache hit ratio, replica lag,
    and both pools' wait counts.

---

## After it works

Update, in this order:

- **`README.md`** — replace the "Still missing" paragraph in `## Status`, which
  currently names this stage. Add a `### Measured` section in the house format:
  a `| Case | Result |` table with the before and after p50, p95 and p99 for
  each step, then paragraphs whose first sentence is a bolded claim. Keep the
  standing disclaimer that this is one laptop.
- **`docs/architecture.md`** — the replica, the cache and the partitions in the
  picture.
- **`ROADMAP.md`** — tick Stage 8 and write the "What the build taught that the
  plan did not say" block. Expect at least one of: the index mattered more than
  the replica, the lag was smaller or larger than assumed, or the cache hit
  ratio was disappointing because rooms are colder than they feel.
- **This file** — add the retro blockquote at the top, past tense, with real
  numbers, the way stages 5 and 7 did.

# Stage 5 — Outbox and a message broker

> **Three things came out different from this plan.**
>
> 1. **The idempotency guard was wrong.** The plan used a high water mark on
>    the counter row: only apply a `seq` higher than the last one applied. It
>    passed every test written for it and lost 2 messages out of 14 on the
>    first two-node run. Two nodes share one consumer, so they commit in
>    whatever order they finish, and a message that lands after a higher one is
>    not a duplicate — it is late. Replaced by an **inbox**: one row per message
>    in `consumed_messages`, written in the same transaction as the work. See
>    [Design decision 7](#7-the-unread-consumer-is-idempotent-through-an-inbox).
>
> 2. **The relay had no backoff.** With NATS stopped it retried the first row
>    545 times in one second, each attempt writing a failure row and a log line
>    — so a broker outage made the database busier. `Drain` now reports rows
>    fetched as well as rows published, and a batch that moved nothing waits 5
>    seconds instead of 100 ms.
>
> 3. **Redis Pub/Sub was deleted, not kept alongside.** The plan said this, but
>    it is worth recording that it really happened: `internal/cluster` is now
>    presence only, and about 120 lines went.

Decisions taken before the build:

- Broker: **NATS JetStream**.
- The broker **takes over live fan-out** from Redis Pub/Sub. Redis keeps
  presence only.
- The consumer does **unread counters**.

---

## Context

### Where we were

`SendMessageHandler` did two writes:

1. `db.CreateMessage(...)` — one transaction: bump `conversations.last_seq`,
   insert the message row.
2. `broadcastMessage(...)` — read the member list, then `cluster.Publish` to
   the Redis channel `chat:fanout`.

The `201` was written to the client **before** step 2.

### The problem Stage 5 fixes

Two writes to two systems with no transaction around them: the **dual write**.

- The process dies after the commit and before the publish → the message is in
  Postgres and nobody is told. Stage 4's `?after_seq=` only helps a client that
  reconnects; a client that stays connected sees nothing, ever.
- Redis is down for two seconds → same result, `Publish` just logs an error.

Redis Pub/Sub also has no memory. A message published while a node was
restarting was gone, with no ack, no retry, and no way to ask what failed. That
was acceptable while the only consumer was a socket. It stops being acceptable
the moment something else has to react to a message.

### The outcome

- The handler does **one** write. Message row and outbox row commit together.
- A relay reads the outbox and publishes. It can crash at any point and start
  again from the same row.
- Every node gets messages from NATS and pushes to its own sockets.
- A second consumer keeps unread counters, with retries, redelivery, and a
  dead-letter queue.

---

## Design decisions

### 1. The outbox row is written inside `CreateMessage`'s transaction

Third statement in the existing closure, after `tx.Create(msg)`.

The `client_msg_id` retry path rolls the transaction back and re-reads the
existing row. That rollback undoes the outbox insert too, so **a retry queues
no second delivery**. It falls out for free, and it has a test, because it is
what stops a client with a flaky connection posting the same line to everyone's
screen five times.

### 2. The outbox payload does not store member ids

The row stores the message envelope. The relay resolves members with
`ListConversationMemberIDs` at publish time:

- members resolved after the commit are fresher than members frozen at write
  time;
- one query per message in one place, which is the same cost as before.
  Resolving per node would be one query per message *per node*.

### 3. One relay at a time, chosen by a Postgres advisory lock

Every node starts a relay; they fight over `pg_try_advisory_lock` on a
dedicated `*sql.Conn`. The winner drains, the losers retry every 5 seconds.

| Rejected | Why |
|---|---|
| `SELECT ... FOR UPDATE SKIP LOCKED`, many relays | Loses order. Two relays can publish seq 5 before seq 4, throwing away Stage 4. |
| A lease row renewed on a timer | More code and more state, same result. |
| A separate `cmd/relay` process | One more thing to run and shut down, no benefit at this size. |

The lock dies with its connection, so a node killed with `-9` frees it with no
lease to expire and no stale row to clean up. goose already uses the same trick
so two nodes can migrate at once.

If the leader's connection dies quietly and two relays run for a moment,
decision 5 and decision 7 both cover it.

### 4. The relay polls; it does not listen

`WHERE published_at IS NULL ORDER BY id LIMIT 100`. A full batch loops again
with no sleep; anything less sleeps `OUTBOX_POLL_INTERVAL` (100 ms).

That adds up to 100 ms to live push — measured at 306 ms end to end against
8 ms in Stage 3. It is the honest price of never losing a message.
`LISTEN/NOTIFY` would remove it and is a named follow-up.

**Added during the build:** a batch that fetched rows and published none of
them waits `leaderRetry` (5 s) instead. See the note at the top.

### 5. NATS dedupes on a message id

Every publish carries `Nats-Msg-Id: outbox-<row id>`, and the stream sets
`duplicates: 5m`. A relay that publishes and dies before marking the row done
republishes on restart, and JetStream drops the copy.

It is a window, not a promise. Consumers still have to be idempotent — two
layers, because either alone has a hole.

### 6. Two consumers on one stream, with opposite shapes

| | `fanout` | `unread` |
| --- | --- | --- |
| Who needs it | every node | exactly one node |
| Consumer | one per node, ephemeral, `InactiveThreshold` 5m | one durable, shared by name |
| Starts at | `DeliverNew` | `DeliverAll` |
| Acks | `AckNone` | `AckExplicit`, `AckWait` 30 s, `MaxDeliver` 5 |
| On failure | nothing | `NakWithDelay`, then dead-letter |

Fan-out does not replay after a restart, and that is correct rather than lazy:
the sockets that would have received those messages are closed, and their
clients repair with `?after_seq=`. Replaying would push at sockets that no
longer exist and deliver twice to the ones that do.

### 7. The unread consumer is idempotent through an inbox

**This is the decision that changed during the build.** The plan had:

```sql
ON CONFLICT (conversation_id, user_id) DO UPDATE
   SET unread_count = unread_counters.unread_count + 1
 WHERE unread_counters.applied_seq < EXCLUDED.applied_seq
```

Wrong, and quietly so. Two nodes share the `unread` consumer, so they work on
different messages at the same instant and commit in whatever order they
finish. Node A commits seq 9, node B commits seq 8, and the mark says 8 has
"already been applied". It had not — it was late.

The replacement is one row per message, written in the same transaction as the
counters:

```sql
INSERT INTO consumed_messages (consumer, message_id, consumed_at)
VALUES ('unread', $1, now())
ON CONFLICT (consumer, message_id) DO NOTHING;   -- 0 rows: already done, stop
```

"Have I seen this message" has the same answer whenever it is asked, so order
stops mattering. It is the mirror of the outbox: the outbox stops a message
being published zero times, the inbox stops it being applied twice.

`consumed_messages.message_id` is deliberately **not** a foreign key. The row
records something that happened; nothing joins the two tables, and a key would
lock the message row on every insert and tie the two cleanups together.

`unread_counters.last_seq` survives as an informational column, written with
`GREATEST` so a late message cannot pull it backwards.

### 8. Dead-letter queue

At `NumDelivered >= 5` the message is copied to `chat.dead.unread` on a second
stream `CHAT_DLQ` (30-day retention) and then `Term`'d. The copy happens
**first**: `Term` is final, so terminating before the evidence is safe would
throw it away. If the copy fails, the message is `Nak`'d instead — stuck is
better than silently gone.

### 9. No NATS still works, through the same relay

`broker.FromEnv` returns nil when `NATS_URL` is unset, like `cluster.FromEnv`.
The relay always runs; only its `Publisher` changes:

- `*broker.Broker` when NATS is configured;
- `outbox.LocalPublisher` when it is not — applies the unread work and hands
  the event to this node's hub.

So `make run` and every handler test exercise the same relay with a different
last step, rather than a second code path that only exists in development.

---

## What was built

| Area | Files |
| --- | --- |
| Schema | `internal/database/migrations/00005_outbox_and_unread.sql` |
| Models + store | `internal/database/models.go`, `outbox.go`, `unread.go`, `conversations.go` |
| Shared event | `internal/event/event.go` |
| Broker | `internal/broker/` — `broker.go`, `consume.go`, `stats.go` |
| Relay | `internal/outbox/` — `relay.go`, `local.go`, `stats.go` |
| HTTP | `internal/server/conversations.go`, `ws.go`, `routes.go`, `health.go` |
| Retired | `internal/cluster/cluster.go` — Pub/Sub removed, presence kept |
| Wiring | `internal/server/server.go`, `internal/metrics/metrics.go` |
| Proof | `cmd/outboxcheck/`, `make outboxcheck` |
| Infra | `docker-compose.yml` (nats service + volume), `Makefile` |

---

## What Stage 5 does NOT do

- **`LISTEN/NOTIFY`.** The relay polls. ~100 ms of extra latency, measured.
- **Exactly-once.** Delivery stays at-least-once; the message id and the inbox
  make duplicates harmless.
- **A scaled relay.** One leader. Sharding the outbox is a Stage 8 problem.
- **Tracing through the broker.** Stage 7.
- **A circuit breaker.** Stage 6.
- **DLQ replay.** You can read dead messages; replaying them is not built.
- **A second service.** Everything still runs in one binary — that is Stage 6.

---

## Verification

Cheapest first.

1. `go build ./... && go vet ./...`
2. `make test` — handler and unit tests, no containers.
3. `make itest` — the Postgres tests: the outbox row in the transaction, the
   retry that queues nothing, the inbox that counts a late message.
4. `make test-race`
5. `make run` with no `NATS_URL` and no `REDIS_ADDR`. `/health` should say
   `nats: not configured` and messages should still reach sockets — that is the
   local publisher path.
6. `docker compose up --build -d`, then
   `BLUEPRINT_DB_HOST=127.0.0.1 BLUEPRINT_DB_PORT=5434 make seed ARGS="-n 3"`
   and `make outboxcheck`.
7. `docker compose stop nats`, then `make outboxcheck` again. Sends must still
   answer 201. Start NATS and confirm nothing was lost.
8. `curl localhost:8081/metrics | grep outbox` — `pending` 0 at rest, climbing
   while NATS is down; `relay_leader` 1 on exactly one node across the cluster.
9. `http://localhost:8222/jsz?streams=1` — the `CHAT` stream, the `unread`
   consumer's `num_pending` and `num_redelivered`, and an empty `CHAT_DLQ`.
10. `docker compose kill api2` (the leader) mid-run and confirm the other node
    picks up the relay within ~5 seconds with nothing lost.

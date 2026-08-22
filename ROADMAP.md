# Roadmap

This file is the plan for growing `go-chat-backend` from one service into a
small distributed system.

The goal is not to collect technologies. The goal is to meet each problem in
real code, feel it break, and then fix it. A tool added that way is a tool you
can defend in an interview.

## The rule for every stage

Four moves, in this order:

1. **Build** the smallest version that works.
2. **Break** it on purpose — kill a node, stop Redis, open 5000 sockets.
3. **Fix** what broke.
4. **Write the numbers down** in the README.

Move 4 is the one that is easy to skip, and it is the one that turns knowledge
into experience. "I ran 10k sockets across 2 nodes, p99 fan-out was 40ms, and
killing Redis stopped delivery but not sends" is a different sentence from
"I know Redis pub/sub".

Each stage ends with something running. No stage is only reading.

---

## Stage 0 — Graceful shutdown

**Status:** done. Signal handling, drain and ordered close live in
[`cmd/api/main.go`](cmd/api/main.go) and `App.Shutdown` in
[`internal/server/server.go`](internal/server/server.go). Measured numbers are in the
README.

- Listen for `SIGINT` and `SIGTERM`.
- Stop accepting new requests, let open ones finish, then close the database.
- Exit with a timeout, so a stuck request cannot hang the process forever.

**Why first:** every later stage kills a process on purpose — a rolling deploy,
a chaos test, a node restart. A dirty stop makes all of those unreadable,
because you cannot tell a real bug from a rude exit.

**Size:** a few hours.

---

## Stage 1 — WebSocket delivery, one node

**Status:** done. The hub is in [`internal/ws`](internal/ws), the handler in
[`internal/server/ws.go`](internal/server/ws.go), and the load tool in
[`cmd/wsload`](cmd/wsload). Measured numbers are in the README.

Writes keep going over REST. The socket only pushes out rows that
`POST /conversations/:id/messages` already stored.

- `GET /ws`, authenticated with the access token.
- One goroutine reading and one goroutine writing per client.
- A hub with channels for register, unregister and broadcast.
- Ping/pong heartbeat, so a dead TCP connection is noticed.
- A **slow client** must never block the hub: if its send channel is full,
  drop the client instead of waiting.
- On shutdown, close every socket cleanly.

**Learn:** real Go concurrency — goroutines, channels, `select`, contexts,
backpressure, races. Go interviews dig here hardest.

**Break it:** connect 5000 clients. Attach a client that never reads. Kill the
network on one client without closing the socket.

**What broke, and what it taught:**

- 5000 sockets connected fine — but only at 64 dials at a time. Opening them
  256 at a time overflowed the accept queue and 4384 of 5000 were refused.
- The fan-out was never the bottleneck. The hub's queue never filled once; the
  database pool sat at 25 of 25 and piled up 17.8 s of waiting in four seconds
  of traffic. The p99 was a message waiting for a database handle.
- A client that never reads took about 0.8 MB before the hub noticed. The
  kernel's own buffers hide it until they are full.

**Size:** 1–2 weeks.

---

## Stage 2 — Make it observable and safe

One node still, but now you can see inside it.

- Structured logs with `log/slog`.
- A request id on every request, carried into the logs.
- Prometheus `/metrics`: request count, latency histogram, open sockets,
  messages sent per second.
- `/readyz` separate from `/health` — "alive" and "ready for traffic" are two
  different questions.
- Rate limiting per user.
- Panic recovery that logs and keeps the process up.
- **Close a socket whose token has died.** Stage 1 checks the token only at the
  handshake, so an open socket survives both expiry and logout — proved by
  `TestSocketOutlivesItsExpiredToken` and `TestSocketOutlivesLogout`. Decide
  between re-checking on a timer and closing at the known expiry time.

**Learn:** how to answer "how do you know your service is healthy?" — and how
to prove it instead of guessing.

**Size:** about 1 week.

---

## Stage 3 — Two nodes: the first real distributed problem

Run two API instances behind nginx.

**It breaks.** User A is connected to node 1, user B to node 2. B never sees the
message, because node 1's hub only knows its own sockets. Reproduce this bug and
watch it before you fix it. This is the moment the project becomes distributed.

- Fix with Redis Pub/Sub: the write path publishes, every node subscribes, each
  node fans out to its own local sockets only.
- Add presence (who is online) in Redis, with a TTL and a heartbeat, so a node
  that dies does not leave ghosts online forever.

**Learn:** why in-process state does not scale, sticky sessions, shared state,
what happens when the shared thing goes down.

**Break it:** stop Redis while both nodes run. What still works? Sends should
still succeed and history should still read. Only live delivery should stop.
Write down what you saw.

**Size:** about 2 weeks.

---

## Stage 4 — Delivery guarantees

Right now a message can be missed while a client reconnects.

- Give each conversation a sequence number, so messages have an order that does
  not depend on clocks.
- The client remembers `last_seq` and asks for the gap after reconnecting.
- Delivery becomes at-least-once, so duplicates are possible — the existing
  `client_msg_id` unique index is already the dedupe key.

**Learn:** ordering, idempotency, at-least-once versus exactly-once. This is one
of the most common distributed-systems interview questions, and here you will
have your own code as the answer.

**Size:** about 2 weeks.

---

## Stage 5 — Outbox and a message broker

Add NATS or Kafka. Pick one and learn it properly.

The problem to feel first: the handler writes to Postgres and then publishes to
the broker. If the process dies between the two, the message exists but nobody
is told. That is the **dual-write problem**.

- Fix with the **transactional outbox**: in one database transaction, write the
  message row *and* an outbox row. A separate relay reads outbox rows and
  publishes them.
- Then write a consumer that does real work: unread counters, or push
  notifications.
- Add retries, a dead-letter queue, and an idempotent consumer.

**Learn:** dual writes, consumer groups, redelivery, poison messages. This is
what most job ads mean by "distributed systems".

**Size:** 2–3 weeks.

---

## Stage 6 — Split a second service

Extract one service for real — presence, or notifications — over **gRPC**.

- Define the contract in protobuf.
- Add timeouts, deadlines, retries with backoff, and a circuit breaker.
- Decide what happens when the other service is down. Degrade, do not crash.

**Learn:** service boundaries, contracts, partial failure. You will also feel
why splitting too early hurts. That lesson is worth as much as the code.

**Size:** about 2 weeks.

---

## Stage 7 — Tracing across services

OpenTelemetry, so one trace id follows a single message the whole way:

HTTP request → outbox → broker → consumer → WebSocket push.

Jaeger for traces, Grafana for the Prometheus metrics from Stage 2.

**Learn:** finding a slow step when the work crosses four processes.

**Size:** about 1 week.

---

## Stage 8 — Scale the data, then prove it

- A read replica for history reads.
- Partition `messages` by conversation or by time.
- Cache hot conversations, and decide how the cache is invalidated.
- Load test with k6. Record p50, p95, p99.
- Chaos: kill a node mid-test, stop Redis, stop Postgres, fill a disk.

**Learn:** where the real limit is. Guessing is not engineering; a graph is.

**Size:** about 2 weeks.

---

## Stage 9 — Kubernetes and CI

- GitHub Actions: test, build, push the image.
- Kubernetes: liveness and readiness probes, rolling deploy with no dropped
  sockets, HPA, secrets and config.

**Learn:** how the thing actually ships, and why Stage 0 mattered.

**Size:** 1–2 weeks.

---

## Why this order

Each stage creates the problem that the next stage solves.

Redis is not added because a tutorial said so. It is added because two of your
own nodes stopped talking to each other. The outbox is not a pattern from a
blog post; it is the fix for a message you personally lost.

That is the difference between having read about distributed systems and having
worked on one.

## Progress

- [x] Stage 0 — Graceful shutdown
- [x] Stage 1 — WebSocket delivery, one node
- [ ] Stage 2 — Observability and safety
- [ ] Stage 3 — Two nodes, Redis Pub/Sub, presence
- [ ] Stage 4 — Delivery guarantees
- [ ] Stage 5 — Outbox and a broker
- [ ] Stage 6 — A second service over gRPC
- [ ] Stage 7 — Tracing
- [ ] Stage 8 — Data scale, load and chaos tests
- [ ] Stage 9 — Kubernetes and CI

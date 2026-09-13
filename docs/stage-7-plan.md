# Stage 7 — Tracing across services

> **Two things came out different from this plan.**
>
> 1. **The sampler rule was in the wrong place, and it did nothing.** Design
>    decision 5 dropped heartbeat traces by matching the gRPC method name,
>    `presence.v1.PresenceService/Heartbeat`. A root sampler only ever sees the
>    span that STARTS a trace, and that is the presence client's own wrapper
>    span, not the gRPC one underneath it. So Jaeger filled with heartbeat
>    traces anyway. The fix was to name the wrapper span after the operation —
>    `presence.Online`, `presence.Heartbeat` — and drop that. The unit test
>    passed the whole time, because it asserted the rule rather than asking
>    which span came first.
>
> 2. **The poll gap is a range, not a number.** The plan said this stage would
>    measure the ~100 ms Stage 5 estimated. It measured **92 ms**, then
>    **40 ms**, then **6 ms** — which is what a uniform draw across a 100 ms
>    poll interval looks like. Reporting one figure would have been wrong every
>    time.

Decisions taken before the build:

- **OpenTelemetry** SDK, **OTLP over gRPC**, **Jaeger** all-in-one as the collector and the UI.
- **Grafana** on top of the Prometheus from Stage 2, in the same compose profile.
- The trace crosses the outbox in a **new column**, not in the event payload.
- Tracing off when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, like every other optional dependency in this repo.
- A proof tool, `cmd/tracecheck`, like every stage before it.

---

## Context

### Where we are

There are seven processes. One message crosses four of them:

`POST /conversations/:id/messages` → `outbox` row in Postgres → the relay on the leader node → NATS → the fan-out consumer on **another** node → a WebSocket frame.

Every step already logs. But the logs do not join.

- `X-Request-Id` ([`internal/server/logging.go:52`](../internal/server/logging.go#L52)) stops at the `201`. The relay runs later, on a different node, with no idea the request existed.
- The `envelope` in [`internal/broker/broker.go:156`](../internal/broker/broker.go#L156) carries `From`, the node name, and nothing else about where the message came from.
- The gRPC call to `presenced` has metadata that could carry an id, and carries none.

So "this message took 300 ms" is a sentence you can only prove by opening four logs side by side and matching timestamps. The README already admits this and names it the next stage.

### The problem Stage 7 fixes

Two questions that cannot be answered today:

1. **Where did the time go?** Stage 5 measured 306 ms end to end and guessed that ~100 ms of it was the relay poll. Guessed, not measured.
2. **Which request caused this?** A message stuck in the outbox, a dead-letter row, a slow presence call — none of them can be traced back to the user action that started it.

### The outcome

One trace id follows one message from the HTTP request to the WebSocket push, through Postgres, the relay, NATS, and the consumer on a different process. Jaeger draws it as one waterfall. Every log line in every process carries the same `trace_id`, so a trace and a log can be joined.

---

## Design decisions

### 1. The trace context rides in its own column, not in the event

`message_outbox.payload` is the product event ([`internal/event/event.go`](../internal/event/event.go)). It is what a message *is*. A trace id is not part of a message; it is how we watched one being sent.

New migration `00006_outbox_trace.sql`:

```sql
ALTER TABLE outbox ADD COLUMN trace_context jsonb;
```

Nullable, on purpose. Rows written before this migration, and rows written while tracing is off, simply have no trace and the relay handles that as the normal case.

`CreateMessage` ([`internal/database/conversations.go:232`](../internal/database/conversations.go#L232)) injects the current span context into a `propagation.MapCarrier` and stores it beside the payload. The `ctx` it needs is already a parameter.

| Rejected | Why |
|---|---|
| A field on `event.MessageCreated` | That struct is the contract for the frame the client receives. A `traceparent` would leak into the WebSocket JSON. |
| A `traceparent text` column | The W3C carrier is two keys (`traceparent`, `tracestate`). `jsonb` holds the carrier as it is, and matches `payload` next to it. |

### 2. The relay span is a **child** of the request span, not a new root

The HTTP span ends at the `201`. The relay publishes up to 100 ms later, on a different node. The parent is already finished.

That is fine, and it is the whole point. A span context stays valid after its span ends. The relay extracts the carrier and starts `outbox.publish` as a child, so Jaeger shows one trace with a visible gap between the commit and the publish — **the poll delay, drawn to scale, for the first time**.

Rejected: a span *Link* instead of a parent. Links are for fan-in (many causes, one span). This is one cause and one effect, so a parent is the honest shape.

### 3. NATS carries the context in headers, so `Publish` becomes `PublishMsg`

[`internal/broker/broker.go:285`](../internal/broker/broker.go#L285) calls `js.Publish(ctx, subject, data, jetstream.WithMsgID(...))`. It becomes `js.PublishMsg` with a `*nats.Msg` that carries `Header` plus `Nats-Msg-Id`.

The propagator writes into a `nats.Header` through a small adapter (`nats.Header` is a `map[string][]string`, so the adapter is ~15 lines in `internal/broker`). The consumers in [`consume.go`](../internal/broker/consume.go) read it back with `msg.Headers()`.

Headers, not the body. The body is the envelope that both consumers decode; putting transport metadata inside it would mean a decode failure also loses the trace of the decode failure.

### 4. The two consumers get spans with opposite shapes, matching Stage 5

- `SubscribeFanout` → span `fanout.deliver`, a child, ending when `deliver` returns. It records how many sockets on **this** node got the frame. This is the last hop we can see; the browser is not instrumented.
- `handleUnread` → span `unread.apply`, a child, with `messaging.nats.delivered` set from `meta.NumDelivered`. A redelivery is then visible as a second span on the same trace, and a dead-letter is a span with an error status. **This is the thing that was hardest to see in Stage 5.**

### 5. gRPC to `presenced` uses stats handlers, and the heartbeat is sampled out

`grpc.WithStatsHandler(otelgrpc.NewClientHandler())` on the client ([`internal/presence/client.go:126`](../internal/presence/client.go#L126)) and `otelgrpc.NewServerHandler()` on the server in [`cmd/presenced/main.go`](../cmd/presenced/main.go). gRPC metadata carries `traceparent` with no code of ours.

But the heartbeat loop ([`server.go:295`](../internal/server/server.go#L295)) runs every 10 s on every node with no parent span. That is a new root trace 12 times a minute that nobody will ever read, and it would bury the traces that matter.

So `internal/tracing` ships a small sampler: `ParentBased` with a root sampler that drops spans named `presence.v1.Presence/Heartbeat`. One place, one rule, and `Online` — the call a user actually waits for — still traces.

### 6. `internal/presence/client.go` adds span events for the breaker

`call()` ([line 257](../internal/presence/client.go#L257)) gets a span with events for `retry`, `short_circuited`, and `breaker_open`.

This is where the stage pays for itself. Stage 6's worst bug was confusing "did this fail" with "did the service reply", and every unit test passed while it was broken. On a trace, a call that pays the full 1 s deadline and a call refused in 5 ms by an open breaker are two visibly different shapes — the bug would have been obvious in one screenshot.

### 7. Logs carry `trace_id`, so the two views join

`logFrom(c)` ([`logging.go:101`](../internal/server/logging.go#L101)) already returns a per-request logger. The `requestID` middleware adds `trace_id` and `span_id` from the span in `c.Request.Context()` when there is one.

`X-Request-Id` **stays**. The browser client and the check tools read it, and it works with tracing off. The two ids sit side by side rather than one replacing the other.

### 8. `/ws`, `/metrics`, `/livez` and `/readyz` are not traced

`/ws` is the important one: a WebSocket handshake becomes a connection that lives for hours. Tracing it makes one span that never ends and never exports. The probes are the same reason `quietWhenHealthy` exists in the request logger — a scrape every 5 s is not a trace anybody wants.

The filter reuses the existing `quietWhenHealthy` idea, extended with `/ws`.

### 9. No endpoint means no tracing, and that is a supported mode

`tracing.Setup(ctx, serviceName)` returns a shutdown func. With `OTEL_EXPORTER_OTLP_ENDPOINT` unset it installs a no-op provider and returns immediately.

Same rule as `NATS_URL`, `PRESENCE_ADDR` and `REDIS_ADDR`: unset is single-node development, not a failure. `make run` and every test keep working with no collector. A collector that is **set but unreachable** must also not stop the process — the OTLP exporter batches and drops, which is the correct behaviour for telemetry.

### 10. Jaeger and Grafana go in the existing `observability` profile

`docker compose up` stays the database, the nodes, the broker, presence and nginx. Watching graphs is a thing you ask for:

```
docker compose --profile observability up -d
```

| Service | Host port | Why |
|---|---|---|
| `jaeger` (all-in-one) | 16686 UI, 4317 OTLP gRPC | Accepts OTLP natively; no separate Collector is needed at this size. |
| `grafana` | 3000 | Prometheus datasource provisioned from a file in the repo, plus Jaeger as a second datasource so a slow metric links to a trace. |

Provisioning lives in `grafana/provisioning/`, committed, for the same reason `prometheus.yml` is committed: a dashboard typed into a UI is lost the first time the volume is removed.

---

## What to build

| Area | Files |
|---|---|
| New: setup, sampler, propagator, no-op mode | `internal/tracing/tracing.go`, `sampler.go` |
| Schema | `internal/database/migrations/00006_outbox_trace.sql` |
| Inject at write | `internal/database/models.go` (`Outbox.TraceContext`), `internal/database/conversations.go` |
| Extract and re-parent | `internal/outbox/relay.go` (`Drain`), `internal/database/outbox.go` (`FetchOutbox` selects the column) |
| NATS headers + spans | `internal/broker/broker.go` (`PublishMsg`, header carrier), `internal/broker/consume.go` |
| HTTP ingress | `internal/server/routes.go` (otelgin, with the skip list), `internal/server/logging.go` (`trace_id` on the logger) |
| gRPC both ends | `internal/presence/client.go`, `cmd/presenced/main.go` |
| Breaker events | `internal/presence/client.go` (`call`) |
| Wiring + shutdown | `internal/server/server.go` (`New`, `App.Shutdown`), `cmd/api/main.go`, `cmd/presenced/main.go` |
| Proof | `cmd/tracecheck/main.go`, `make tracecheck` |
| Infra | `docker-compose.yml`, `grafana/provisioning/**`, `Makefile`, `.env` docs |

New direct dependencies (the tree already has `go.opentelemetry.io/otel v1.44.0` indirect, so versions line up):
`otel`, `otel/sdk`, `otel/trace`, `exporters/otlp/otlptrace/otlptracegrpc`, `contrib/instrumentation/github.com/gin-gonic/gin/otelgin`, `contrib/instrumentation/google.golang.org/grpc/otelgrpc`.

### Shutdown order

`tracing.Setup` returns a `shutdown(ctx)` that flushes the batch exporter. It must run **last** in `App.Shutdown` ([`server.go:339`](../internal/server/server.go#L339)) — after the background goroutines and the database — or the spans describing the shutdown itself are dropped. Give it its own short timeout; a collector that is down must not delay the exit.

---

## The proof tool: `cmd/tracecheck`

Every stage has one, and this stage's claim is exactly the kind a table cannot show.

1. Log in as two seeded users on **two different nodes** (`:8081` and `:8082`), the way `splitcheck` does.
2. Open a socket for user B on `api2`. Send a message as user A on `api1`, reading the `X-Request-Id` and the `traceparent` the client generated.
3. Wait for the frame on B's socket.
4. `GET http://localhost:16686/api/traces/<traceID>` from Jaeger.
5. Assert the trace contains, in order: the HTTP server span on `api1`, `outbox.publish` on the leader node, `fanout.deliver` on `api2`, `unread.apply` on either — **and that at least two different `service.name`/node values appear**. One trace crossing two processes is the whole claim.
6. Print the span durations as a small table, so the poll gap is a number and not a story.

`make tracecheck ARGS="-pause 20s"` for the interesting run: kill `jaeger` during the pause and confirm sends still answer `201`. Telemetry going down must cost nothing.

---

## What Stage 7 does NOT do

- **Metrics or logs over OTLP.** Prometheus stays the metrics path, `slog` stays the log path. Only traces move.
- **Browser tracing.** The trace ends at the WebSocket write.
- **A Collector.** Apps export straight to Jaeger. A real deployment puts a Collector in between; at this size it is one more process to run for no lesson.
- **Sampling policy beyond "all, minus heartbeats".** Head sampling at 100 % is right for a laptop and wrong for production, and that is a Stage 8 problem.
- **Database spans.** `otelgorm` would add a span per query. Tempting, and it would triple the span count while the thing being investigated is the cross-process gap.
- **Tracing `LISTEN/NOTIFY`.** The relay still polls. Stage 7 *measures* that 100 ms; removing it stays a named follow-up.

---

## Verification

Cheapest first.

1. `go build ./... && go vet ./...`
2. `make test` — including a new test that the relay re-parents a stored carrier, and one that an outbox row with a `NULL` `trace_context` still publishes.
3. `make itest` — the migration applies, `CreateMessage` writes a carrier, `FetchOutbox` reads it back.
4. `make test-race`
5. `make run` with **no** `OTEL_EXPORTER_OTLP_ENDPOINT`. Everything must behave exactly as before, and no span code may run.
6. `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 make run` with `docker compose --profile observability up -d jaeger`. Send one message and find the trace at http://localhost:16686.
7. Full cluster: `docker compose --profile observability up --build -d`, then `make seed ARGS="-n 3"` and `make tracecheck`.
8. In Jaeger, open one message trace and check the shape: request → commit → *gap* → publish → fan-out on the other node. Write the gap down; it is the number Stage 5 estimated.
9. `docker compose kill presenced`, then `curl localhost:8081/conversations/1/presence` five or six times. The traces should show the shape change — ~1 s spans with an error, then 5 ms spans carrying the `short_circuited` event.
10. `docker compose kill jaeger`, then send messages. `201`s must stay at ~10 ms and nothing may be lost. Start Jaeger again and confirm the nodes reconnect with no restart.
11. Grafana at http://localhost:3000 — the Prometheus datasource answers, and the Jaeger datasource is listed.

---

## After it works

Update, in this order:

- `README.md` — a "Tracing" section with the measured waterfall, replacing the "Still missing" paragraph that names this stage.
- `docs/architecture.md` — a fourth path diagram, "one message's trace", and Jaeger/Grafana in the process table.
- `ROADMAP.md` — Stage 7 marked done, with a **"What the build taught that the plan did not say"** block. Stages 5 and 6 both have one, and both times it was the most valuable part of the file.

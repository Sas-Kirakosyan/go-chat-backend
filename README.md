# go-chat-backend

A Go HTTP backend for a chat application: JWT-authenticated accounts on top of
Postgres, with conversations and messages stored per user.

Built with [Gin](https://gin-gonic.com/), [GORM](https://gorm.io/) and Postgres.
CORS is configured for a frontend on `http://localhost:5173`.

## Status

Authentication with refresh-token sessions, the conversation REST API, the
WebSocket delivery layer, and the observability and safety work around them are
implemented and tested.

Delivery runs across **two nodes behind nginx**, with presence in Redis and a
per-node view on `/metrics` — see [Two nodes](#two-nodes).

Every message carries a **per-room sequence number**, so a client that loses
its socket can see exactly what it missed and ask for it. Delivery is
at-least-once: a message may arrive twice, never zero times — see
[Delivery guarantees](#delivery-guarantees).

The send path now does **one write**. The message row and an outbox row commit
together, and a relay publishes them to **NATS JetStream** afterwards. Kill the
broker and sends still answer 201; the messages queue in Postgres and go out
when it comes back. A consumer on the same stream keeps unread counters, with
retries and a dead-letter queue — see [The outbox and the broker](#the-outbox-and-the-broker).

Still missing: the relay polls, so live delivery costs up to 100ms more than it
did. Postgres `LISTEN/NOTIFY` would remove that, and there is no second service
yet — everything still runs in one binary. That split is the next stage.

## Endpoints

| Method | Path                          | Auth   | Description                                 |
| ------ | ----------------------------- | ------ | ------------------------------------------- |
| `GET`  | `/livez`                      | no     | Is the process alive? Restart it if not     |
| `GET`  | `/readyz`                     | no     | Should this node be sent traffic right now? |
| `GET`  | `/health`                     | no     | Database connectivity and pool stats        |
| `GET`  | `/metrics`                    | no     | Prometheus metrics                          |
| `GET`  | `/ws`                         | query  | Live delivery socket (see below)            |
| `POST` | `/register`                   | no     | Create an account                           |
| `POST` | `/login`                      | no     | Exchange credentials for a JWT + session    |
| `POST` | `/auth/refresh`               | cookie | A new access token, without logging in      |
| `POST` | `/auth/logout`                | cookie | End the session                             |
| `GET`  | `/auth/profile`               | bearer | The current user                            |
| `POST` | `/conversations`              | bearer | Create a room                               |
| `GET`  | `/conversations`              | bearer | Rooms I am a member of, each with its `unread_count` |
| `POST` | `/conversations/:id/members`  | bearer | Add someone to a room                       |
| `POST` | `/conversations/:id/messages` | bearer | Send a message                              |
| `GET`  | `/conversations/:id/messages` | bearer | History newest first, or the gap after `?after_seq=` |
| `POST` | `/conversations/:id/read`     | bearer | Set my unread count for this room to zero   |
| `GET`  | `/conversations/:id/presence` | bearer | Who in the room is online, cluster-wide     |

Tokens are HS256, valid for 15 minutes by default, and sent as
`Authorization: Bearer <token>`. Set `ACCESS_TOKEN_TTL=2h` to make development
less tiring; production leaves it alone.
The token carries the user id as well as the name, so a room handler does not
need an extra `SELECT` to find out who is calling.

## Sessions

A login gives you two things with two different jobs:

| | Access token | Refresh token |
| --- | --- | --- |
| What it is | HS256 JWT | 32 random bytes, base64 |
| Lives for | 15 minutes (`ACCESS_TOKEN_TTL`) | 7 days |
| Sent as | `Authorization: Bearer` | httpOnly cookie, path `/auth` |
| Checked against the database | never | on every use |
| Job | prove who you are | get a new access token |

The client keeps working while the access token is alive, calls
`POST /auth/refresh` when it expires, and only sees a login screen after seven
idle days.

### Why not just a 24-hour access token?

Because the access token is never looked up, it cannot be taken back. Stretching
it to 24 hours would make a stolen token useful for a day and would still leave
no way to log anyone out. The refresh token is a **row in a table**, so ending a
session is a single `UPDATE` — and that is what makes logout mean something.

### The cookie

`httpOnly`, so page JavaScript cannot read it: an XSS bug cannot walk off with a
week-long login. `SameSite=Lax` is enough because `:5173` and `:8080` are the
same site — a port is not part of a site — while a genuine cross-site request
still gets no cookie. `Secure` is switched off only when `APP_ENV=local`, since
a browser refuses a `Secure` cookie over plain http.

The path is `/auth`, not `/`, so the browser never attaches a week-long
credential to a message send or a history read.

### Only the hash is stored

The database keeps the SHA-256 of the token, never the token itself, so a
leaked backup hands an attacker nothing usable. bcrypt is deliberately not used
here: it is slow by design to make guessing a human password expensive, and this
input is 256 bits of randomness with nothing to guess — the slowness would only
tax every refresh.

### Two trade-offs worth knowing

**The refresh token does not rotate.** A refresh returns a new access token and
leaves the cookie alone. Two browser tabs can therefore refresh at the same
moment without one invalidating the other. The cost is that a stolen refresh
token stays useful for its whole life, and nothing detects the theft. Seven days
rather than the more common thirty is the counterweight to that choice.

**Logout is not instant.** Ending the session stops any *new* access token being
minted, but the one the caller already holds keeps working until it expires — at
most 15 more minutes. Closing that gap would mean checking a revocation list on
every single request, which puts a database lookup on the busiest path in the
app. The 15-minute window is the price of a stateless access token.

Sessions are per login, not per user, so logging out on your phone leaves your
laptop signed in. Dead rows are cleared at the next login by that user, which
keeps the table bounded without a background job.

## Design decisions

**Writes go over REST. The WebSocket is delivery-only.**

Every message is created by `POST /conversations/:id/messages` and nowhere
else. The hub only pushes out rows this endpoint already stored.

One write path means one place for validation, persistence and idempotency, and
it leaves the hub as a pure fan-out with no database of its own. Accepting
writes over the socket as well would mean two code paths that must stay in step
forever — two validators, two ways to fail halfway, two things to fix when a
rule changes — and it buys nothing, because a client that can open a socket can
also send a POST.

**Rooms live at `/conversations`, not `/auth/conversations`.**

Auth is the mechanism that guards a room, not the thing a room belongs to. The
routes use the same middleware in a separate route group.

**A room you are not in returns 404, not 403.**

403 confirms the room is real. Repeated over a range of ids, that lets an
outsider map which rooms exist. A missing room, a malformed id and a real room
the caller is not in all answer the same way. The check lives in one helper,
`memberOnly`, which every room handler calls first.

**Wire types are separate from the GORM models.**

The models carry `DeletedAt`, a password hash and association slices that must
never reach a client, and renaming a column must not silently change the public
API. The `*DTO` types in `internal/server/conversations.go` are the contract.

### Membership and messages

A room owns no user of its own. Membership lives in `conversation_members`,
with a unique index on `(conversation_id, user_id)`, so a room holds any number
of people. `created_by_id` is kept for audit only — it grants no extra rights,
and any member may add anyone else.

### Paging history

`GET /conversations/:id/messages` serves two different reads, because both
answer "give me messages from this room" and both need the same membership
check.

**Backwards, for a person scrolling up.** This is the default.

- `?before_id=` returns only messages older than that id.
- `?limit=` defaults to 50 and is capped at 100 rather than refused.
- The response carries `next_before_id`: the cursor for the next, older page,
  or `null` at the start of the room.

**Forwards, for a client catching up.** See
[Delivery guarantees](#delivery-guarantees).

- `?after_seq=` returns messages that came after that sequence number, **oldest
  first**, because a client applies missed messages in the order they were sent.
- The response carries `next_after_seq` when the gap is bigger than one page.
- Sending both cursors at once is a `400`. They run in opposite directions, so
  there is no single sensible answer.

Paging on a cursor instead of `OFFSET` keeps the pages stable. `OFFSET` has to
count and discard every earlier row, and a message written between two requests
shifts the whole window, so the reader sees a line twice or misses it.

### Idempotent sends

`POST /conversations/:id/messages` accepts an optional `client_msg_id`. Sending
the same one again returns the first message with `200` instead of writing a
second one with `201`, so a retry after a network timeout cannot double-post.
The unique index covers `(conversation_id, sender_id, client_msg_id)`, so the
key only has to be unique per sender and two clients picking the same string
never collide.

## WebSocket delivery

`GET /ws` opens the live feed. It carries messages out; it never takes any in.

```
ws://localhost:8080/ws?token=<access token>
```

Every frame is an envelope with a `type`, so a client can switch on one field
instead of guessing what arrived:

```json
{"type": "connected",   "data": {"user_id": 7}}
{"type": "message.new", "data": { ...the same shape GET /messages returns... }}
```

`connected` is sent once, right after the upgrade. `message.new` is sent to
every member of a room when `POST /conversations/:id/messages` stores a new
row. A repeat of a `client_msg_id` stored nothing, so it delivers nothing — a
retry after a timeout must not put the same line on the screen twice.

The hub lives in [`internal/ws`](internal/ws). It knows nothing about Gin, JWT
or the database: it moves bytes to user ids. When Stage 3 adds a second node,
Redis will call the same `Broadcast` from the outside rather than being wired
through the middle of it.

### The token is in the query string

The browser's WebSocket API cannot set request headers, so there is no way to
send `Authorization: Bearer` on the handshake. The token goes in the URL
instead. Clients that *can* set headers — tests, Go clients, `wscat` — still
use the header, and the handler accepts either.

A token in a URL is a real cost: URLs are written to access logs, to proxy
logs, and sometimes to a `Referer`. It is accepted on this one route and
nowhere else, and the access token lives 15 minutes. Widening it to every route
would trade a small, bounded leak for a large one.

Our own log is the part we control, and the rule there is simple: **the query
string is never logged.** Gin's own logger prints path and query together,
which would write a live token to disk on every connect — one of the reasons
`RegisterRoutes` builds the middleware by hand instead of calling
`gin.Default()`. Ours logs `URL.Path` and nothing else, so `/ws` is logged like
any other route and the token never reaches the file. There is a test that
fails if that ever changes.

### The socket dies with its token

The token is checked **once**, at the handshake. Rather than re-check it, the
socket carries the token's own `exp` and closes at that moment:

| | Result |
| --- | --- |
| Dial with an expired token | refused, `401` (`TestWSRejectsAnExpiredToken`) |
| Token expires **while** a socket is open | closed with code **4401**, `"token expired"` |
| User logs out while a socket is open | keeps delivering, until the token expires |

Close code 4401 is in the range reserved for the application, and echoes HTTP
401 on purpose: it tells a client *get a new token and reconnect*, which is a
different instruction from `1001 going away` (*the server is stopping, come
back later*). Without the distinction a client cannot tell a deploy from an
expired login, and would reconnect forever with the same dead token.

**Why a deadline and not a re-check.** The other options were a timer that
re-parses the token, or a session lookup in the database every so often. Both
put a clock — and one of them a query — inside the socket's own goroutine,
times five thousand sockets, to learn something already known: `exp` is inside
the token, so the moment it dies is known at connect time. One timer per
socket, no database, no polling.

**What it does not fix.** Logout is still not instant for a socket. Ending a
session stops new access tokens being minted; it does not reach inside a
connection that is already open. The gap is now *bounded* by the token's life
instead of being unbounded — a socket is no worse than a REST call with the
same token, which is the 15-minute window the session design already accepts.
Closing it completely means a revocation check on every use, which is the exact
database lookup a stateless access token exists to avoid.

Every row above is a test in [`ws_test.go`](internal/server/ws_test.go), and
each one first proves the credential is really dead — `/auth/profile` answers
`401`, `/auth/refresh` answers `401` — before drawing any conclusion from the
socket.

### CORS does not protect a socket

The handshake is a plain `GET` with an `Upgrade` header. The browser sends no
preflight for it and ignores `Access-Control-Allow-Origin` in the answer, so
the CORS middleware in front of the REST routes does nothing here. Any page on
the internet may open a socket to this server and the browser will attach
cookies to it.

The only thing standing in the way is the `CheckOrigin` function on the
upgrader. It allows the configured frontend origin, and it allows a **missing**
`Origin` header, because that means the caller is not a browser — `curl`,
`wscat`, the load tool — and there is no other site to protect them from.

### Two goroutines per client, and one for the hub

Each socket gets a reader and a writer, because gorilla allows exactly one
concurrent writer and because a blocked read must not stop a write.

- **readPump** throws away everything it reads. It exists so the connection
  keeps processing pongs and close frames, and so a dead socket is noticed.
- **writePump** owns every write: queued messages, pings, and the close frame.

The hub itself is a third goroutine, and it is the **only** one that touches
the client map. There is no mutex anywhere in the package. Register,
unregister and broadcast are channels; the map is owned, not shared. That is
also what makes closing a client's send channel safe — a channel must be closed
by its only sender, and the hub is its only sender.

### A slow client is dropped, never waited for

Every client has a small buffer, 16 messages. When it is full, the hub does not
wait:

```go
select {
case c.send <- payload:
default:
    h.drop(c) // remove from the map, close the channel
}
```

Waiting there would block the hub goroutine, and the hub goroutine is the one
serving every other socket on the node. One phone on a bad train connection
would freeze the whole server. Dropping costs that one client a reconnect; the
alternative costs everybody everything.

A bigger buffer would not fix this. It only moves the moment we notice, while
holding more memory per socket — and there are thousands of sockets.

The same rule applies one level up: `Broadcast` never blocks the HTTP handler
either. If the hub's own queue is full, the fan-out is dropped and counted. The
message is already committed to Postgres, so the cost is a missed live push,
not a lost message, and the client can still read it from history. Stage 4 is
where that gap gets closed with sequence numbers.

### A dead network is caught by the heartbeat

The server pings every 54 seconds and expects a pong within 60. Every pong
pushes the read deadline forward; no pong, and the next read fails on its own.

This is the only thing that catches a pulled cable. TCP does not report it:
nothing is being sent, so nothing fails, and a socket from a laptop that closed
its lid would sit there looking healthy until the process restarts.

### Measured

[`cmd/wsload`](cmd/wsload) logs in seeded users, builds rooms, opens a socket
per user, sends messages over REST and times how long each one takes to come
back out of a socket. Server, database and load tool all on one Windows laptop,
so these are relative numbers, not a benchmark.

```bash
make seed ARGS="-n 5000"
make wsload ARGS="-n 5000 -messages 20"
```

| Case | Result |
| ---- | ------ |
| 5000 sockets, 100 rooms of 50 | **5000/5000** connected in **1.7 s** |
| 100 k frames fanned out (500 messages/s in) | p50 **23 ms**, p95 **447 ms**, p99 **787 ms** |
| 25 k frames fanned out (125 messages/s in) | p50 **14 ms**, p95 **216 ms**, p99 **332 ms** |
| 5 clients that never read, 4 KB messages | all **5 dropped** by the server, the other 95 unaffected |
| Interrupt with 5000 sockets open | exit **0** in **201 ms**, 5000 clean close frames |

**The fan-out was never the bottleneck; the database pool was.** The hub's queue
never filled once — not a single fan-out was shed. Meanwhile `/health` showed
the pool pinned at 25 of 25 connections, and during the four seconds of sending
its counters moved by **2420 waits totalling 17.8 s** of queueing for a
connection. The 787 ms p99 is a message waiting for a database handle, not for
a socket. That is a Stage 8 problem, and now it is a measured one.

**A client that never reads is not noticed for a surprisingly long time.** It
took about **0.8 MB** — 202 messages of 4 KB — before one was dropped. The
kernel keeps a send buffer on our side and a receive buffer on theirs, and on
loopback both are large and grow on demand. Until they are full, every write
returns instantly and the client looks healthy. So the 16-message buffer is not
the whole story: the real memory a dead reader holds sits in the kernel, where
no Go counter can see it. Nothing breaks, but "we drop slow clients" deserves
the footnote.

**5000 sockets is easy; opening 5000 sockets at once is not.** The first
attempt dialled 256 at a time and got 616 connections, with the rest refused
outright. That is not the server dying — it is the listening socket's accept
queue overflowing, and the kernel refusing what will not fit. The load tool now
dials 64 at a time and retries with backoff, which is what a real client does
anyway.

## Seeing inside it

One node, but now you can watch it work: structured logs with a request id,
Prometheus metrics, and health endpoints that answer different questions.

### The logs are structured

Every line is key/value pairs, written by `log/slog`. JSON in production,
because a log shipper wants JSON; plain text when `APP_ENV=local`, because a
person watching a terminal does not.

```
level=INFO msg="request" request_id=a1b2c3 method=POST path=/conversations/7/messages status=201 duration_ms=4.2 bytes=133 ip=10.0.0.4 user_id=12
level=INFO msg="ws connected" request_id=d4e5f6 user_id=12 expires_in_s=899
level=WARN msg="rate limited" request_id=99aa88 scope=auth path=/login retry_after_s=1
```

"A request was slow" is a sentence a human reads one of. `status=500
route=/login` is something a machine can count, filter and alert on. Once there
is more than one node, reading logs by eye stops working, and this is what
replaces it.

`slog.SetDefault` also redirects the old `log` package, so a stray `log.Printf`
anywhere in the tree comes out in the same format instead of bypassing all of
this.

The level follows the status: 5xx is `ERROR`, 4xx is `WARN`, everything else is
`INFO`. So "show me the failures" is a filter, not a grep for words.

**Quiet when healthy, loud when not.** `/livez`, `/readyz` and `/metrics` are
asked by machines every few seconds. They are not logged while they answer
`2xx` — and they are logged like everything else the moment they do not,
because a probe that starts failing is one of the most interesting lines in the
file.

### One id per request

Every request gets a 16-character id. It goes on every log line the request
writes, and back to the caller in `X-Request-Id`, so a user who reports a
failed call gives you the exact rows to look at.

An id sent by a proxy is reused, so a trace that started at the edge is not cut
in half here — but it is **checked first**. It arrives from the network and
goes straight into a log line, and a caller who could put a newline in it would
be writing our logs for us: one crafted header and a convincing fake `request
status=200` line appears in the file, which is how an audit trail stops being
evidence. Only short strings of letters, digits, `-`, `_` and `.` are accepted;
anything else is replaced with one of ours.

In Stage 5 and 6 the same id will follow a message into a broker and into
another service.

### Metrics

`GET /metrics` in the Prometheus text format.

| Metric | Type | What it answers |
| ------ | ---- | --------------- |
| `chat_http_requests_total{method,route,status}` | counter | throughput, and the error rate as `status=~"5.."` |
| `chat_http_request_duration_seconds{method,route}` | histogram | p50/p95/p99 latency |
| `chat_http_requests_in_flight` | gauge | requests being served right now |
| `chat_messages_stored_total` | counter | the real write rate |
| `chat_ws_connections_open` | gauge | sockets on this node |
| `chat_ws_frames_sent_total` | counter | fan-out volume — one message to 50 people counts 50 |
| `chat_ws_clients_dropped_total` | counter | sockets dropped for reading too slowly |
| `chat_ws_broadcasts_shed_total` | counter | fan-outs thrown away because the hub was behind |
| `chat_ws_sockets_expired_total` | counter | sockets closed because their token ran out |
| `chat_rate_limited_total{scope}` | counter | requests refused with 429 |
| `chat_panics_recovered_total` | counter | should be flat at zero |
| `chat_db_pool_*` | gauges + counters | the pool that Stage 1 found was the real bottleneck |
| `chat_gap_messages_total`, `chat_gap_size` | counter + histogram | how much live delivery is being missed, and by whom |
| `chat_outbox_lag_seconds` | gauge | **how far behind delivery is.** The one to alert on |
| `chat_outbox_pending` | gauge | outbox rows not yet published. 0 at rest |
| `chat_outbox_relay_leader` | gauge | 1 on the node draining the outbox. Must sum to exactly 1 |
| `chat_outbox_published_total`, `chat_outbox_publish_failures_total` | counters | what the relay got out, and what it could not |
| `chat_broker_consuming` | gauge | 1 while this node holds its fan-out subscription |
| `chat_broker_published_total`, `chat_broker_fanout_received_total` | counters | published once, received by every node |
| `chat_broker_redelivered_total` | counter | at-least-once delivery, visible |
| `chat_broker_dead_lettered_total` | counter | should be flat at zero |
| `chat_unread_applied_total`, `chat_unread_duplicates_total` | counters | consumer work done, and redeliveries correctly ignored |

Two of those deserve an alert, and neither has an HTTP symptom — a broken
cluster still answers `201` to every send:

- `chat_outbox_lag_seconds` above a few seconds means messages are written and
  not being delivered. A pending count alone says nothing: 900 rows could be
  one busy second, while 900 rows whose oldest has waited four minutes means
  the relay has stopped.
- `sum(chat_outbox_relay_leader)` other than 1. Zero means nobody is draining.
  Two means the advisory lock is broken and one room's messages are going out
  in the wrong order.

The Go runtime and process collectors come free with the default registry.
`go_goroutines` is the one to watch here: this service runs **two goroutines
per socket**, so a leak shows up there before it shows up anywhere else.

**Labels are the part that is easy to get wrong.** Prometheus stores one time
series per unique label combination, so a label whose value comes from the
caller is a memory leak with an open door in front of it. The HTTP metrics are
therefore labelled with the *route template*:

```
chat_http_requests_total{method="POST",route="/conversations/:id/messages",status="201"} 400
```

Ten thousand rooms are one series, not ten thousand. Anything unrouted — a
scanner asking for `/wp-login.php`, `/.env`, and whatever it tries next — is
labelled `other`, because the path is chosen by a stranger and our series names
must not be. Both rules have a test.

The Stage 1 finding is now a graph rather than a lucky glance at `/health`:

```
chat_db_pool_waits_total 44
chat_db_pool_wait_seconds_total 0.331
chat_db_pool_open_connections 25
chat_db_pool_max_open_connections 25
```

Waits climbing while `open_connections` sits at the maximum *is* the picture of
a message queueing for a database handle.

To look at it as graphs:

```bash
docker compose --profile observability up -d
# Prometheus on http://localhost:9090
```

`/metrics` is open because everything here runs on one machine. On a cluster it
belongs on the internal network only: the numbers say how many people are
online and how the service is coping, which is not something to hand to the
internet.

### Three endpoints, three questions

|  |  |
| --- | --- |
| `/livez` | Is this process alive? **Restart me if not.** |
| `/readyz` | Should traffic come to me right now? **Take me out if not.** |
| `/health` | What is going on in there? For a person. |

Mixing the first two is the classic mistake, and it is expensive. If liveness
checked the database, then a database outage would make Kubernetes kill every
API pod, over and over, in a restart loop — and not one of those restarts would
help, because the broken thing is the database. The pods were fine.

Split, the behaviour is right: the database goes down, `/readyz` starts
failing, the nodes are taken out of the load balancer, nothing is restarted,
and when the database comes back they are put in again on their own.

`/livez` deliberately checks nothing. Its answer arriving *is* the check: the
process is up, the accept loop works, and a goroutine got scheduled to write
it.

`/readyz` also fails at the **start** of shutdown, before the drain, so a load
balancer stops sending new requests to a node that is about to close. Without
that, every deploy drops a handful of requests into a dying process. Liveness
stays true throughout — the process is alive, it is just not taking new work,
and failing liveness there would ask the platform to `SIGKILL` a clean
shutdown.

## Staying up

### A panic does not take the node down

A panic in one handler would otherwise kill the process, and with it every
other request in flight and every open socket. One nil map in one rare branch
must not be able to do that. The recovery middleware logs the panic with its
stack and the request id, counts it in `chat_panics_recovered_total`, and
answers `500`.

The stack is logged, never returned: a stack trace in a response body hands a
stranger the file layout and library versions of the server.

A *broken pipe* is treated differently — that is the client hanging up
mid-response, not a bug in us. The connection is already gone, so writing a 500
into it would only panic a second time.

### Rate limits

Two limiters, because the two groups of routes are attacked in completely
different ways.

| | Key | Default | Guards |
| --- | --- | --- | --- |
| `auth` | client IP | 5/s, burst 20 | `/register`, `/login`, `/auth/refresh`, `/auth/logout` |
| `api` | user id | 20/s, burst 40 | everything behind the access token |
| `api` | client IP | 20/s, burst 40 | `/ws` — nobody has proved who they are yet |

`/login` is guessed at: a thousand passwords against one account, or one
password against a thousand accounts. There is no user id yet — that is the
part being guessed — so the only key available is the address, and the limit is
low because a real person logs in a handful of times a day.

Behind `AuthMiddleware` the caller is known, so the key is the user id and one
noisy client cannot spend the allowance of everyone else in the same office.
The limit is higher because a chat client legitimately bursts: opening the app
fires a room list plus history for the room you were last in.

Each caller gets a **token bucket**: it holds `burst` tokens, refills at `rate`
per second, and a request costs one. It is a number and a timestamp, not a
timer, so a caller that goes quiet costs nothing until it comes back. A fixed
window ("100 per minute") was the alternative and is worse: it lets a client
spend everything in the last second of one window and again in the first second
of the next.

Idle buckets are swept once a minute. Without that the map grows by one entry
per address that ever arrived, which is a slow memory leak an attacker can
steer by rotating IPs. A *full* bucket is identical to no bucket at all — a new
caller starts full — so deleting it takes nothing away from anyone.

A refused request gets `429` and a `Retry-After` in whole seconds, never zero,
because a client told to wait zero comes straight back.

The probes are not limited at all. A refused probe looks exactly like a dead
node, so the monitoring would take a healthy server out of service.

**One caveat worth knowing.** `c.ClientIP()` reads `X-Forwarded-For`, and gin
trusts every proxy by default, so a client can currently claim any address it
likes. This is a speed bump against a plain script, not a defence against a
determined attacker. The fix is `SetTrustedProxies` with the real proxy's
address, and that address is only known once there is an nginx in front — Stage
3.

For a load test, raise the limits rather than adding a back door that skips
them; a server with the limiter disabled is not the server that ships:

```bash
RATE_LIMIT_AUTH_RPS=2000 RATE_LIMIT_API_RPS=5000 make run
```

### Measured

| Case | Result |
| ---- | ------ |
| 40 wrong-password logins, one at a time with `curl` | **0 refused** — 7.1 s for 40, which is 5.6/s |
| 60 wrong-password logins, 20 in parallel | **28 refused**, `Retry-After: 1`, in 2.7 s |
| `chat_rate_limited_total{scope="auth"}` after that | 28, matching the 28 `status="429"` rows |
| 1000 sockets, limits raised, 20 msg/room | 1000/1000 connected in **206 ms**, p50 **6 ms**, p99 **206 ms**, 20 000 frames, 0 shed |
| Same run, pool pressure | 44 waits totalling **0.33 s** |

**A serial attacker never trips the limit — and does not need to.** Forty
attempts through one `curl` at a time took 7.1 seconds, which is 5.6 per
second, right at the refill rate. The thing throttling it was not the limiter:
it was bcrypt, deliberately slow, costing about 90 ms of server CPU per
attempt. The rate limit is what catches the *parallel* attacker, and against 20
at once it refused 28 of 60.

**The safety feature broke the tooling, twice.** `cmd/wsload` is one machine
pretending to be a thousand people, so it is the first thing the limiter
punishes:

1. 80 of 100 logins failed with `429`. Exactly 20 got through — the burst.
2. Teaching the tool to honour `Retry-After` fixed the logins, and then 68 of
   100 **sockets** failed instead. The handshake was wearing the login limit,
   which exists because bcrypt is expensive; a handshake only parses a JWT and
   costs microseconds. `/ws` was moved to the API limit, and the same run then
   connected 100 of 100.

The second one is the more useful bug: an office behind one NAT address would
have seen exactly what the load tool saw. Charging a cheap route the price of
an expensive one is a limit that looks fine until real users share an address.

The fix on the client side is what a real client does anyway — back off for as
long as the server asked, double the wait each attempt, and add jitter. Jitter
matters as much as the wait: every caller was refused by the same bucket at the
same moment, so returning after exactly one second sends them all back
together.

## Two nodes

One node is a program. Two nodes is a system, and the difference shows up in
the first minute.

Run two API instances behind nginx and the chat breaks. User A is connected to
node 1, user B to node 2. A sends a message; B never sees it. Nothing errors —
the message is committed to Postgres, the sender gets a 201, and history shows
it on both nodes. Only the live push is missing, because node 1's hub knows
node 1's sockets and nothing else.

[`cmd/splitcheck`](cmd/splitcheck) puts two members of one room on two named
nodes and asks the only question that matters:

```bash
docker compose up --build -d
make splitcheck
```

Before the fix:

```
A sends on node A
  socket on localhost:8081  received it
  socket on localhost:8082  NOTHING after 3s

B sends on node B
  socket on localhost:8081  NOTHING after 3s
  socket on localhost:8082  received it
```

Through nginx it is worse, not better: round robin decides where each socket
lands, so the same command fails differently on every run. Once, both sockets
landed on one node and the message went to the other, and **nobody** received
it. A bug that changes shape each time is the one that survives a whole sprint.

### One delivery path

The fix was Redis Pub/Sub: the write path published to one channel, every node
subscribed, and each fanned out to its own local sockets. The hub did not
change at all — the transport plugs in beside it, which is what the Stage 1
note in [`internal/ws`](internal/ws) was written for.

That transport is now NATS, for reasons that are the whole of
[the next section](#the-outbox-and-the-broker), and the plug-in point held: the
hub still did not change, and neither did the decision below.

The decision worth defending: **a node does not deliver its own message
locally.** The relay publishes, and every node — the sending one included —
receives it back through its subscription. One path in, so a member on the
sending node cannot get the message twice, and the cross-node path is exercised
by every single message rather than only by the ones that happen to cross a
boundary.

The counters say it works. After two messages, one sent on each node:

| Metric | api1 | api2 |
| ------ | ---- | ---- |
| `chat_broker_published_total` | 2 | 0 |
| `chat_broker_fanout_received_total` | 2 | 2 |
| `chat_ws_frames_sent_total` | 2 | 2 |

Publishing is no longer split between the nodes, because only the relay
publishes and only one node is the relay — see
[One relay, chosen by a lock](#one-relay-chosen-by-a-lock). Both nodes still
receive both messages, and each sends one frame per message per socket. No
duplicates.

One subject tree for the whole system, not a subscription per room. A consumer
per room would mean creating and deleting consumers as people come and go, and
a node holding sockets in 5000 rooms would carry 5000 of them. Every node
reading everything is more traffic and far less bookkeeping, and the filter is
cheap: a node drops any fan-out whose users it does not hold.

### Presence, and why it is a timer

`GET /conversations/:id/presence` answers who in a room is online, anywhere in
the cluster. It is a Redis sorted set: member `userID:nodeID`, score the unix
time that node last saw them. Online means "score newer than 30 seconds ago".

Everything about that shape is chosen for one case — **a node that dies without
saying goodbye**:

- The score, not a TTL per key, because one key holds the whole cluster and a
  room of fifty is one command instead of fifty.
- `userID:nodeID`, not `userID`, because a phone on node 1 and a laptop on node
  2 are two entries. As one entry, the node that lost the user would erase the
  node that still has them.
- Written on a **timer**, not when a socket opens or closes. Events are cheaper
  and would be wrong: the one moment a node cannot send an event is the moment
  it is killed.

Which is exactly the test. Hold two sockets open, kill the node one of them is
on, and watch from the survivor:

```bash
make splitcheck ARGS="-hold 60s"
docker compose kill api1      # in another terminal
```

```
  + 50s  node B says: user A online, user B online
  + 55s  node B says: user A OFFLINE, user B online
```

Nobody told Redis that user A had gone. Their entry simply stopped being
refreshed and fell out of the 30-second window on its own.

### Trusted proxies

Putting nginx in front created a hole that had to be closed in the same stage.
Gin trusts every proxy by default, which means it trusts every client: anyone
could send `X-Forwarded-For: 1.2.3.4` and the per-IP rate limit from Stage 2
would count a brand new caller on every request. `TRUSTED_PROXIES` names the
addresses whose header is believed. Unset, it means *trust nobody* — the client
IP is the TCP address — which is the right answer for `make run`.

### Measured: what happens when Redis stops

The interesting run is the failure one. Both nodes up, `docker compose stop
redis`, then measure every path.

| Path | Redis up | Redis down |
| ---- | -------- | ---------- |
| `POST /conversations/:id/messages` | 201 in **12 ms** | 201 in **10 ms** |
| `GET /conversations/:id/messages` | 200 in **9 ms** | 200 in **8 ms** |
| `GET /conversations` | 200 in **8 ms** | 200 in **11 ms** |
| `GET /conversations/:id/presence` | 200 in **6 ms** | **503** in 1.0 s |
| `GET /readyz` | 200 | **200** |
| Live delivery | both nodes | **both nodes** |

Only presence breaks now. Since Stage 5 moved the fan-out to NATS, Redis holds
nothing that delivery needs, and the blast radius of losing it shrank to one
endpoint.

Three things had to be fixed to get here, and all three were found by running
this, not by thinking about it.

**Presence hung for over 15 seconds.** With Redis stopped, the handler waited
on a dead dependency until the *client* gave up. The per-connection timeouts
were not enough — go-redis retries, and a DNS lookup for a container that no
longer exists is slow by itself, so the waits stack. Any handler that touches a
shared service needs its own deadline. One second, and presence now fails fast
with a 503.

**Every send took 2.009 seconds.** Back when Redis carried the fan-out, the
handler published inside the request, and the publish timeout was not
protecting the write path — it *was* the write path's problem. Dropping it to
500 ms made an outage survivable rather than painless, and publishing in a
goroutine was rejected because two messages sent in order could then reach
Redis out of order.

That whole trade-off is gone. The handler does not publish at all any more; it
commits an outbox row and returns, and a relay publishes afterwards. The number
in the table above is the proof: a send during a **broker** outage is 10 ms,
the same as a healthy one. The answer to "how slow is a send when the messaging
system is down" turned out to be "it does not touch it".

**The nodes crash-looped.** The first version pinged Redis at startup and
called `os.Exit` if it failed. With Redis down, restarting the nodes put them
in a restart loop: exit 1, restart, fail, exit 1. A Redis outage had become a
total outage — no login, no history, no sending — for a service that is
supposed to lose only its live push. Now it logs one line and starts anyway.
`broker.FromEnv` follows the same rule for NATS.

That last one is the same rule as `/readyz`, which deliberately checks
**neither** Redis nor NATS. Both are shared by every node, so an outage of
either would fail the probe on all of them at once and the load balancer would
take the whole service out. A shared dependency in a readiness probe turns one
broken thing into an outage. `/health` reports `redis: down` and `nats: down`
for the human, and the status code stays 200.

Recovery needs no restart in either case. `docker compose start redis`, and
presence answers again within a couple of seconds.

`chat_broker_consuming` is the metric to alert on. A node stuck at 0 is storing
messages and pushing none of them, and nothing in the HTTP metrics shows it:
every send still answers 201.

## Delivery guarantees

Live push is best effort, and it always will be. A socket dies mid-message, a
node loses its Redis subscription, a phone goes through a tunnel. The message
is stored and the sender gets its `201`, but one screen never shows it.

Before this stage that loss was **silent**. A client had no way to ask "did I
miss anything?", because the only order was `id`, and `id` is one global
counter shared by every room. A client that last saw id 100 and now sees id 140
cannot tell whether 39 messages went to other rooms or 39 of its own were lost.

### The sequence number

Every message carries `seq`: its place in its **own** room, running 1, 2, 3
with no holes. Now "I have 42, the server says 47" means exactly five missing
messages.

The number comes from `conversations.last_seq`, and the allocation happens in
the same transaction as the insert:

```sql
UPDATE conversations SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq;
INSERT INTO messages (..., seq) VALUES (..., $seq);
```

The `UPDATE` takes a row lock, so a second sender in the same room waits and
cannot be handed the same number. Both statements commit together, so a crash
in between leaves no hole. A unique index on `(conversation_id, seq)` is the
safety net: if the allocation is ever wrong the insert fails loudly, instead of
quietly giving two messages the same place and making one invisible.

Three alternatives were rejected:

| Idea | Why not |
| ---- | ------- |
| Use the global `id` | Has holes per room, so a client can never prove it is up to date |
| One Postgres sequence per room | `CREATE SEQUENCE` on every new room, and it leaks numbers on rollback |
| Redis `INCR` | Fast, but the counter and the row are two systems — the dual-write problem, one stage early |

A retried `client_msg_id` uses up **no** number. The duplicate rolls the
transaction back, which undoes the `UPDATE` too. Without that, a client
retrying five times over a flaky connection would leave four holes behind, and
every other client in the room would sit forever asking for messages that were
never written.

### Two dedupe keys, two directions

These are easy to confuse and they solve different problems.

| Key | Direction | Stops |
| --- | --------- | ----- |
| `client_msg_id` | client → server | a retried `POST` writing the message twice |
| `seq` | server → client | a re-delivered push showing the line twice |

Delivery is **at-least-once**. A message may arrive twice — live and again in a
gap read — and the client drops any `seq` it already holds.

### The reconnect order

This is the part that is easy to get wrong, and getting it wrong opens a
second, smaller hole that is even harder to see, because it only appears when
the room is busy at the wrong moment.

1. Open the socket **first**, and buffer every frame that arrives.
2. Then call `?after_seq=<last seq you hold>`.
3. Apply the gap, then apply the buffer.
4. Drop any `seq` you already have.

Fetching the gap before connecting loses everything sent between the two calls.
`cmd/gapcheck` sends messages *while* the gap read is in flight on purpose, so
that mistake cannot pass.

### Measured

`make gapcheck` breaks a socket while messages keep being sent, reconnects, and
counts. B is disconnected for the middle five:

```
1. B is connected, A sends 3
   seq [1 2 3], B saw 3 live
2. B's socket is dead, A sends 5 into the dark
   seq [4 5 6 7 8], B saw none of them
   B's last known seq is 3
3. B reconnects — socket first, then ?after_seq=3
   the gap read returned seq [4 5 6 7 8]
4. A sent 3 more while B was catching up: seq [9 10 11]

  sent           11
  seen live       6
  recovered       5   (through ?after_seq=)
  duplicates      0   (arrived twice, dropped by seq)
  MISSING         0
```

The run worth doing twice is the one with the broker killed in the middle:

```
make gapcheck ARGS="-pause 30s"      # then: docker compose kill nats
```

Live push stops completely — not for one socket, for every socket on every
node — and the numbers change shape while the verdict does not:

| | Socket dropped | Redis killed |
| --- | --- | --- |
| sent | 11 | 11 |
| seen live | 6 | **3** (only the ones before the kill) |
| recovered by `?after_seq=` | 5 | **8** |
| **missing** | **0** | **0** |

That is the whole point of the stage. The transport can fail entirely, and the
client still ends up holding every message in order, because the gap read never
touches Redis at all — it is one indexed query against Postgres.

`chat_gap_messages_total` is the metric this creates. It is the honest measure
of how much live delivery is being missed, and nothing else shows it: a message
that never reaches a socket is still stored, still answered with `201`, and
still counted by `chat_messages_stored_total`. `chat_gap_size` says whether a
rising rate is many clients missing one message each or one client that was
away for an hour — two completely different causes.

### The cost

Writes into **one** room now serialise on one row lock. Different rooms never
touch the same row, so this is a per-room ceiling and not a service-wide one.

Measured at the store, 20 concurrent writers, 500 messages, no HTTP and no rate
limiter in the way:

| Where the writes go | Throughput |
| ------------------- | ---------- |
| One hot room | **372 writes/sec** |
| Spread over 20 rooms | **1410 writes/sec** |
| One hot room, repeated | 393 writes/sec |

So contention on one room costs about **3.7×**, and rooms scale past it
independently. That is the right shape for a chat: a room where twenty people
type at the same instant is nowhere near 372 messages a second, and a service
with thousands of rooms gets the second number, not the first.

If one room ever did need more, the fix is not to drop the lock — it is to stop
sharing a counter, and that means moving the allocation somewhere it can be
sharded. That trade is worth making only with a real room that needs it.

### What this does not fix

- The live push is still best effort. This stage lets a client *notice and
  repair* a miss; it does not stop the miss.
- The client has to actually ask. A client that never reconnects never learns
  anything, and a client that ignores `seq` is exactly as broken as before.

## The outbox and the broker

Up to here the send path did **two** writes:

```
commit the message to Postgres   →   publish to Redis
```

Two systems, one after the other, with nothing holding them together. If the
process died in between, the message existed and **nobody was ever told**. The
sender already had its `201`. That is the **dual-write problem**, and no amount
of retrying inside the handler fixes it, because the process that would do the
retry is the one that died.

Redis Pub/Sub had a second problem with the same root: it stores nothing. A
node that was restarting when a message was published never learned about it,
and there was nobody left to ask. Fine while the only consumer was a socket and
history was the real record. Not fine the moment something else has to react to
a message.

### One write

`CreateMessage` now writes the message row **and** an outbox row in the same
transaction:

```go
tx.Create(msg)                                  // the message
tx.Create(&Outbox{Topic: ..., Payload: ...})    // the instruction to deliver it
```

Both land or neither does. The instruction to publish lives in the same
database as the thing it describes, so it survives everything the message
survives, and a relay can crash at any point and start again from the same row.

The handler no longer knows that sockets exist.

One thing falls out for free and is worth naming: a repeated `client_msg_id`
fails the unique index, the transaction rolls back, and the outbox row goes
with it. A client retrying a send five times produces one message, one
delivery, and no burned sequence numbers. The Stage 4 dedupe key now protects
the broker too.

### One relay, chosen by a lock

Every node starts a relay; they fight over a Postgres advisory lock, and the
winner drains. `pg_try_advisory_lock` on a dedicated connection, which is the
same trick goose already uses so two nodes can migrate at once.

The lock dies with its connection. So a node that is killed with `-9` frees it
without a lease to expire or a stale row to clean up, and another node takes
over within five seconds.

**One** relay, not many, because order is worth more than throughput here. Two
relays working the same table with `SELECT ... FOR UPDATE SKIP LOCKED` would
publish seq 5 before seq 4 and throw away what Stage 4 built.

`chat_outbox_relay_leader` must sum to exactly 1 across the cluster. Zero means
nothing is being delivered anywhere; two means the lock is broken.

Measured: `docker compose kill api2` while it held the lock, and api1 reported
itself leader **4 seconds** later. `make outboxcheck` straight afterwards
delivered 8 of 8 with nothing missing. No lease to expire, no repair step.

### The price: latency

The relay polls, so a message waits up to `OUTBOX_POLL_INTERVAL` — 100 ms by
default — before it goes out. Measured end to end, POST to frame on the wire,
the slowest of three was **306 ms** against **8 ms** in Stage 3.

That is the honest cost of never losing one, and it is a cost worth naming out
loud rather than burying. Postgres `LISTEN/NOTIFY` would cut it to nearly
nothing and is deliberately not in this stage — the trade is documented, and it
can be paid down later without changing the design.

### Two consumers, two opposite shapes

The same stream feeds two things that want opposite delivery rules, and the
contrast is the most useful thing in the stage.

| | `fanout` | `unread` |
| --- | --- | --- |
| Who needs it | **every** node | exactly **one** node |
| Consumer | one per node, ephemeral | one durable, shared by name |
| Starts at | new messages only | the beginning |
| Acks | none | explicit, `AckWait` 30 s |
| On failure | nothing | redeliver, then dead-letter |

Fan-out is per node because each node owns different sockets; a shared consumer
would hand each message to one node and every member connected elsewhere would
see nothing. It does not replay after a restart, and that is correct rather
than lazy: the sockets that would have received those messages are closed, and
their clients repair themselves with `?after_seq=`. Replaying would push at
sockets that no longer exist and deliver twice to the ones that do.

Unread is shared because the work is a **write**, and it must happen once for
the cluster and not once per node. That is where acks, redelivery and the
dead-letter queue live.

### Idempotency, and the bug that proved it was needed

Delivery is at-least-once, so the unread consumer **will** be handed the same
message twice — a consumer that did its work and died before its ack sees it
again on restart.

The first version guarded that with a high water mark on the counter row: only
apply a `seq` higher than the last one applied. It passed every test written
for it, because every test applied messages in order.

Then `make outboxcheck` ran against two nodes:

```
C unread       12   (want 14: C never connected, so this is the consumer's work)
FAIL  C's unread count is 12, want 14
```

Two messages of fourteen, silently uncounted. Both nodes share one consumer, so
they work on different messages at the same time and commit in whatever order
they finish. Node A commits seq 9, node B then commits seq 8 — and seq 8 was
not a duplicate, it was **late**. A high water mark cannot tell those apart.

The fix is an **inbox**: one row per message in `consumed_messages`, written in
the same transaction as the counters.

```sql
INSERT INTO consumed_messages (consumer, message_id)
VALUES ('unread', $1)
ON CONFLICT DO NOTHING;          -- zero rows means: already done, stop here
```

"Have I seen this message" gives the same answer no matter when it is asked.
Order stops mattering. It is the mirror of the outbox — the outbox stops a
message being published zero times, the inbox stops it being applied twice —
and both halves are needed because delivery is at-least-once at both ends.

There are two more layers behind it. JetStream drops a repeat of a message id
inside a five-minute window, which covers a relay that published and died
before it could mark the row done. And the inbox covers everything after that
window closes.

### Poison messages

A message that fails five times is not going to work on the sixth. It is
copied to a second stream, `CHAT_DLQ`, and then `Term`'d so JetStream never
hands it out again. The copy happens **first**: `Term` is final, so
terminating before the evidence is safe would throw it away.

Without that step one bad message is retried forever and the whole queue waits
behind it.

`chat_broker_dead_lettered_total` should be flat at zero. Any other value is a
message that needs a person. Read them with `nats stream view CHAT_DLQ`, or the
monitoring page on <http://localhost:8222/jsz?streams=1>.

### Measured: what happens when NATS stops

`docker compose stop nats`, then measure every path:

| Path | NATS up | NATS down |
| ---- | ------- | --------- |
| `POST /conversations/:id/messages` | 201 in **12 ms** | 201 in **10 ms** |
| `GET /conversations/:id/messages` | 200 in **9 ms** | 200 in **8 ms** |
| `GET /conversations` | 200 in **8 ms** | 200 in **9 ms** |
| `GET /conversations/:id/presence` | 200 in **6 ms** | 200 in **7 ms** |
| `GET /readyz` | 200 | **200** |
| Live delivery | both nodes | **none** |

Not one path is slower. Sends are entirely unaffected by the messaging system
being gone, which is the whole point of the stage: the messages queue in
Postgres and `chat_outbox_pending` climbs while `chat_outbox_lag_seconds` says
how far behind delivery has fallen. `docker compose start nats` and the relay
drains it with no restart and no repair step.

`make outboxcheck` is the proof, with NATS killed before the run:

```
phase 1  4 messages with the broker up
         0 of 4 arrived live
         outbox: 4 pending, oldest 9.6s old
phase 2  8 messages, broker expected down
         8 accepted, 0 refused
phase 3  12 recovered after the broker returned

result
  sent           15
  refused        0
  delivered      15   live 0, recovered 12, live again 3
  missing        0    []
  in history     15 of 15
  C unread       15   (want 15)
```

Twelve messages sent with the broker completely gone, every one accepted, every
one delivered afterwards, and each counted exactly once.

### A second bug the same run found

With NATS stopped, the relay retried the first row **545 times in one second**.
Every attempt was a failed publish, a log line, and an `UPDATE` to record the
failure — so a broker outage made the *database* busier, which is the opposite
of what a queue is for.

The loop could not tell "nothing was waiting" from "everything was waiting and
none of it moved". `Drain` now reports both numbers, and the second case backs
off for five seconds instead of 100 ms. The same outage now produces **8
attempts in 97 seconds**.

## Shutdown

`SIGINT` or `SIGTERM` starts an orderly stop, and the order matters:

1. `http.Server.Shutdown` refuses new connections and **waits** for the
   requests already running.
2. The hub closes every WebSocket.
3. Only then is the database closed.

The other way round breaks exactly the requests the shutdown was trying to
protect: they are still reading and writing rows, and they would fail with
`sql: database is closed`. Stop the traffic first, then close what the traffic
needed.

Step 2 has to be its own step, because `Shutdown` does not know the sockets
exist. An upgraded connection is **hijacked**: the HTTP server hands the raw
connection over and stops tracking it, so it is not counted, not waited for,
and not closed. Without the hub step, 5000 clients would be cut off mid-frame
when the process exits instead of receiving a close frame.

The database is closed even when step 1 fails or runs out of time. A request
that never returns makes `Shutdown` give back `context.DeadlineExceeded`, and
the process is exiting either way, so holding the pool open helps nobody.
`errors.Join` keeps both errors instead of hiding one.

A second `Ctrl+C` kills at once. `signal.NotifyContext` is stopped as soon as
the first signal is handled, which puts the default behaviour back, so a
shutdown that hangs is never a trap.

`ListenAndServe` always returns a non-nil error, and after `Shutdown` that
error is `http.ErrServerClosed`. That is the healthy path. Logging it as a
crash would make every clean stop look like a failure.

### Measured

| Case | Result |
| ---- | ------ |
| `Ctrl+C`, nothing in flight | clean, immediate |
| `docker stop`, nothing in flight | clean, 525 ms, exit code 0 |
| `docker stop -t 30`, a 5 s request in flight | request finished **200**, container exited **0** after 5.26 s |
| `docker stop` (default grace), same 5 s request | request cut at 3.4 s, exit code **137** (SIGKILL) |
| Interrupt with **5000 open sockets** | exited **0** after **201 ms**; all 5000 clients got a close frame |

The `docker stop` row with the 5 s request is the useful one. `Shutdown` waited
for the request exactly as it should — the platform gave up first and killed the
container.

The 5000-socket row looks surprisingly fast next to it, and the reason is worth
knowing: `Shutdown` returns immediately because it is not waiting for the
sockets at all. Closing them is the hub's job, and closing a channel 5000 times
is cheap. A long shutdown here would mean a REST request was still running, not
that the sockets were slow.

**The platform's grace period is a hard cap on your shutdown timeout.** Ours is
10 seconds, but a plain `docker stop` here killed the container after about
3.4 seconds, so those 10 seconds could never be used. Kubernetes allows 30
seconds by default (`terminationGracePeriodSeconds`), which leaves room for a
10-second drain. A shutdown timeout longer than the grace period is not a
promise, it is a `SIGKILL` waiting to happen.

## Configuration

Create a `.env` file in the project root (it is gitignored):

```
PORT=8080
APP_ENV=local
JWT_SECRET=<a long random string>

BLUEPRINT_DB_HOST=localhost
BLUEPRINT_DB_PORT=5432
BLUEPRINT_DB_DATABASE=chat
BLUEPRINT_DB_USERNAME=chat
BLUEPRINT_DB_PASSWORD=<password>
BLUEPRINT_DB_SCHEMA=public
```

`JWT_SECRET` is required — the server refuses to start without it. Setting
`APP_ENV=local` echoes every SQL statement to the log, and prints logs as plain
text instead of JSON.

Everything else is optional and has a working default:

| Variable | Default | What it does |
| -------- | ------- | ------------ |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `ACCESS_TOKEN_TTL` | `15m` | how long an access token lives. Longer is easier in development and worse in production: a logout only really bites once the token expires |
| `RATE_LIMIT_AUTH_RPS` | `5` | logins and registrations per second, per IP |
| `RATE_LIMIT_AUTH_BURST` | `20` | how many may arrive at once |
| `RATE_LIMIT_API_RPS` | `20` | authenticated requests per second, per user |
| `RATE_LIMIT_API_BURST` | `40` | how many may arrive at once |
| `REDIS_ADDR` | unset | `host:port` of Redis. Unset means presence is not tracked. Delivery does not use it |
| `REDIS_PASSWORD` | unset | only if your Redis needs one |
| `NATS_URL` | unset | `nats://host:port`. Unset means single-node: messages still reach this node's own sockets, and no others |
| `OUTBOX_POLL_INTERVAL` | `100ms` | how long the relay waits when the queue is empty. This is the delivery latency the outbox costs |
| `OUTBOX_RETENTION` | `1h` | how long a published outbox row is kept before it is deleted |
| `INBOX_RETENTION` | `48h` | how long `consumed_messages` remembers a handled message. Must stay above the stream's own 24h retention, or a redelivery could be counted twice |
| `NODE_ID` | hostname | the name this node uses in its logs, in presence, and in its fan-out consumer |
| `TRUSTED_PROXIES` | unset | addresses or CIDRs whose `X-Forwarded-For` is believed. Unset means trust nobody |

`NATS_URL` is the switch between one node and many. Compose sets it; `make run`
does not, so local development needs no broker at all — the relay still runs
and still drains, straight into this node's hub. That is deliberate: the
single-node path and the clustered path are the same program with a different
last step, so the one that gets tested most is the one that runs in production.

The limits are configurable because a limit that fits real users does not fit a
load test — see [Rate limits](#rate-limits).

## Migrations

The schema lives in [`internal/database/migrations`](internal/database/migrations)
as plain `.sql` files, run by [goose](https://github.com/pressly/goose). The
server applies anything pending when it starts, so a fresh clone plus a running
Postgres is all you need.

The files are embedded into the binary with `//go:embed`, so a built server
carries its own schema and needs no extra files beside it.

| Command | What it does |
| ------- | ------------ |
| `make migrate-status` | which migrations have run, which are pending |
| `make migrate-up` | apply everything pending |
| `make migrate-down` | undo the newest migration, one step |
| `make migration NAME=add_read_receipts` | scaffold a new pair of Up/Down files |

`migrate-down` is one step on purpose. A down migration can drop a column and
lose the data in it, so it should be a decision each time, not one command that
walks the database back to nothing.

Two servers starting together take a Postgres advisory lock first, so only one
of them applies a migration.

### Why not GORM's AutoMigrate?

This project used `AutoMigrate` before. It reads the Go structs and adds
whatever the database is missing. That is convenient and it is not enough:

- It only ever **adds**. It cannot drop a column, so the dead `conversations.user_id`
  and `messages.role` had to be removed by hand.
- It cannot **move data**. Renaming a column gives you a new empty one and
  leaves the old values behind, silently.
- It keeps **no history**. There is no record of what ran, so it re-guesses the
  whole schema on every start and you cannot read the SQL before it runs.
- There is **no way back**. No rollback, no down step.

The migration that introduced rooms shows the difference:
[`00002_rooms_and_members.sql`](internal/database/migrations/00002_rooms_and_members.sql)
carries each old conversation's single owner into `created_by_id` *and* inserts
that person into `conversation_members`, so nobody loses a room they already
had. `AutoMigrate` could never have done that.

The same file adds `messages.sender_id` as `NOT NULL` with no default. On a
table that already holds messages Postgres refuses and the whole migration
rolls back. That is deliberate: the old `role` column cannot tell us who wrote
an old message, and refusing is better than inventing a sender.

The GORM structs in [`models.go`](internal/database/models.go) no longer carry
`size`, `index` or `not null` tags. Those only ever fed `AutoMigrate`. The SQL
files are the one source of truth for the schema now; the structs only say how
rows are read and written.

### Adopting goose on a database you already have

`00001_init.sql` is the baseline. It is the schema exactly as `AutoMigrate`
built it, and it is the one file that uses `IF NOT EXISTS`, so a database made
by the old code adopts goose without being rebuilt and keeps its rows. Every
migration after it is plain, exact SQL.

## Getting started

```bash
make docker-run   # start Postgres
make run          # start the API — it applies any pending migrations first
```

If you are coming from an older checkout, `make run` is enough: goose adopts the
existing tables and brings them up to date. `make migrate-status` shows you what
it will do before you run it.

The two `api` services in `docker-compose.yml` ship the same binary. They
migrate on startup too — safely, both at once, because goose takes a Postgres
advisory lock — so after pulling schema changes rebuild them rather than only
restarting them:

```bash
docker compose up --build -d
```

A stale container is worth avoiding: it still holds the old code, and old code
migrates the database its old way.

### The cluster

`docker compose up` starts Postgres, Redis, NATS, two API nodes and nginx:

| Address | What |
| ------- | ---- |
| `localhost:8080` | nginx — the address a client should use |
| `localhost:8081` | `api1` directly |
| `localhost:8082` | `api2` directly |
| `localhost:5434` | Postgres |
| `localhost:6380` | Redis |
| `localhost:4222` | NATS |
| `localhost:8222` | the NATS monitoring page — try `/jsz?streams=1` |

The two node ports are not how a client is meant to connect. They exist so a
test can say "put this socket on that node", which is what `make splitcheck`,
`make gapcheck` and `make outboxcheck` do. The database and Redis are published
on unusual ports on purpose: a natively installed Postgres or Redis takes the
normal one, and the failure that causes is nasty — on this machine, Docker held
`5433` on IPv6 while a native Postgres held it on IPv4, so a host tool reached
one or the other depending on which address family it picked, and reported a
wrong password.

Redis has no volume and NATS has one, and the difference is the point. Redis
holds claims about right now that are stale within 30 seconds. NATS holds
messages somebody still has to act on, and a restart that lost them would lose
exactly the unread counts this stage was built to keep.

The container database is its own database. To seed it, point the tool at the
published port:

```bash
BLUEPRINT_DB_HOST=127.0.0.1 BLUEPRINT_DB_PORT=5434 make seed ARGS="-n 3"
```

Forgetting that is an easy hour to lose: plain `make seed` reads `.env`, writes
to the database on `localhost:5432`, and reports success — into a database the
cluster is not using. The logins then fail with `401` and nothing explains why.

## MakeFile

Run build make command with tests
```bash
make all
```

Build the application
```bash
make build
```

Run the application
```bash
make run
```
Create DB container
```bash
make docker-run
```

Shutdown DB Container
```bash
make docker-down
```

DB Integrations Test:
```bash
make itest
```

Load test the WebSocket hub against a running server (seed the users first):
```bash
make seed ARGS="-n 5000"
make wsload ARGS="-n 5000 -messages 20"
```

Check that two nodes share their fan-out (start the cluster first):
```bash
make splitcheck
make splitcheck ARGS="-hold 60s"    # then kill a node and watch presence
```

Check that a client which loses its socket loses no messages:
```bash
make gapcheck
make gapcheck ARGS="-pause 30s"     # then `docker compose kill nats`
```

Check that killing the broker delays messages and loses none (seed three users
first — the third never connects, so their unread count is the consumer's work
and nothing else):
```bash
make seed ARGS="-n 3"
make outboxcheck
make outboxcheck ARGS="-pause 25s"  # then kill and restart nats during the pause
```

Live reload the application:
```bash
make watch
```

Run the tests (see [Testing](#testing) for the whole list):
```bash
make test
```

Clean up binary from the last build:
```bash
make clean
```

Migrations (see [Migrations](#migrations) above):
```bash
make migrate-status
make migrate-up
make migrate-down
make migration NAME=add_read_receipts
```

## Testing

| Command | What it runs |
| ------- | ------------ |
| `make test` | Everything, quietly. One line per package. **Start here.** |
| `make test-v` | Everything, loudly: every test name, and everything the server logged |
| `make test-one NAME=TestX` | One test, or every test whose name matches. Add `PKG=./internal/server` to look in one package only |
| `make itest` | Only the database tests — the slow ones |
| `make test-race` | Everything, with the race detector |
| `make cover` | Which lines the tests reach, as a coloured page in your browser |

Every one of them passes `-count=1`. Go caches test results, so without it a
second run prints `(cached)` and tests nothing — helpful in CI, misleading on a
laptop, where you re-run a test exactly *because* you just changed something.

**Docker must be running** for the database tests. They start a real Postgres
with [testcontainers](https://testcontainers.com/), so without a Docker daemon
they stop with `cannot connect to the Docker API`. That is the environment
talking, not the code. The rest of the suite needs nothing.

**`make test-race` is the valuable one here, and the one most likely to refuse
to start.** It finds two goroutines touching the same memory at the same
moment, which is the bug this project can have — there are two goroutines per
socket, and every stage since has added more: the presence heartbeat, the
relay, and two broker consumers. It needs cgo and a C compiler, and stops with
`-race requires cgo` when there is no `gcc` on `PATH`.

On Windows, this is the one that works:

```powershell
winget install BrechtSanders.WinLibs.POSIX.UCRT
```

winget does **not** put it on `PATH`, so add it by hand and then restart every
terminal — and VS Code itself, because its terminals inherit the environment
VS Code started with:

```powershell
$bin = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe\mingw64\bin"
[Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path","User") + ";$bin", "User")
```

TDM-GCC also works but is stuck on GCC 10 from 2021. There is no reason to
prefer it.

### What the race detector found

Nothing, in the concurrency. Every package came back clean, including the hub,
the per-socket goroutines, the relay and the broker consumers.

The bugs this stage had were not races, and neither of them could have been
found by a unit test — both needed two real nodes and a broker that was really
gone. They are written up in
[The outbox and the broker](#the-outbox-and-the-broker): a counter that dropped
messages arriving out of order, and a relay that retried a stuck queue 545
times a second. Tests were added for both **after** the run found them, which
is the honest order and worth admitting.

What it did find was **three flaky tests**, which is worth writing down because
the lesson generalises. The rate-limit tests over real routes spent their
tokens and expected the next request to be refused. But `-race` makes bcrypt
take two seconds instead of sixty milliseconds, and a token bucket refills with
the wall clock — so the bucket quietly refilled mid-test and the request was
allowed.

Two runs on two machines failed on *different* tests. That is the tell: a test
whose answer depends on how fast the machine is, is not a test.

The bucket tests always used a fake clock. The fix was to carry that clock
through `rateLimits` so the middleware tests can freeze time too. Five tests
now use `frozen(...)`, and one of them was passing for the wrong reason before:
`TestOperationsRoutesAreNotRateLimited` could have been rescued by a refill,
hiding a real limiter accidentally wrapped around `/readyz`.

# go-chat-backend

A Go HTTP backend for a chat application: JWT-authenticated accounts on top of
Postgres, with conversations and messages stored per user.

Built with [Gin](https://gin-gonic.com/), [GORM](https://gorm.io/) and Postgres.
CORS is configured for a frontend on `http://localhost:5173`.

## Status

Authentication with refresh-token sessions, the conversation REST API, and the
WebSocket delivery layer are implemented and tested. Delivery runs on a single
node: a second instance would not see the first one's sockets. That is the next
stage.

## Endpoints

| Method | Path                          | Auth   | Description                                 |
| ------ | ----------------------------- | ------ | ------------------------------------------- |
| `GET`  | `/health`                     | no     | Database connectivity and pool stats        |
| `GET`  | `/ws`                         | query  | Live delivery socket (see below)            |
| `POST` | `/register`                   | no     | Create an account                           |
| `POST` | `/login`                      | no     | Exchange credentials for a JWT + session    |
| `POST` | `/auth/refresh`               | cookie | A new access token, without logging in      |
| `POST` | `/auth/logout`                | cookie | End the session                             |
| `GET`  | `/auth/profile`               | bearer | The current user                            |
| `POST` | `/conversations`              | bearer | Create a room                               |
| `GET`  | `/conversations`              | bearer | Rooms I am a member of                      |
| `POST` | `/conversations/:id/members`  | bearer | Add someone to a room                       |
| `POST` | `/conversations/:id/messages` | bearer | Send a message                              |
| `GET`  | `/conversations/:id/messages` | bearer | History, newest first, keyset paginated     |

Tokens are HS256, valid for 15 minutes, and sent as `Authorization: Bearer <token>`.
The token carries the user id as well as the name, so a room handler does not
need an extra `SELECT` to find out who is calling.

## Sessions

A login gives you two things with two different jobs:

| | Access token | Refresh token |
| --- | --- | --- |
| What it is | HS256 JWT | 32 random bytes, base64 |
| Lives for | 15 minutes | 7 days |
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

`GET /conversations/:id/messages` returns messages newest first.

- `?before_id=` returns only messages older than that id.
- `?limit=` defaults to 50 and is capped at 100 rather than refused.
- The response carries `next_before_id`: the cursor for the next, older page,
  or `null` at the start of the room.

Paging on the id instead of `OFFSET` keeps the pages stable. `OFFSET` has to
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

Our own log is the part we control, so `/ws` is left out of gin's request
logger — otherwise the server would write a live token to disk on every
connect. That is why `RegisterRoutes` builds the middleware by hand instead of
calling `gin.Default()`.

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
`APP_ENV=local` echoes every SQL statement to the log.

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

The `api` service in `docker-compose.yml` ships the same binary. It migrates on
startup too, so after pulling schema changes rebuild it rather than only
restarting it:

```bash
docker compose up --build -d
```

A stale container is worth avoiding: it still holds the old code, and old code
migrates the database its old way.

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

Live reload the application:
```bash
make watch
```

Run the test suite:
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

The database tests use [testcontainers](https://testcontainers.com/) and need a
running Docker daemon.

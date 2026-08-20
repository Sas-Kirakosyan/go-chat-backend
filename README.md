# go-chat-backend

A Go HTTP backend for a chat application: JWT-authenticated accounts on top of
Postgres, with conversations and messages stored per user.

Built with [Gin](https://gin-gonic.com/), [GORM](https://gorm.io/) and Postgres.
CORS is configured for a frontend on `http://localhost:5173`.

## Status

Authentication with refresh-token sessions, and the conversation REST API, are
implemented and tested. The WebSocket delivery layer is not built yet.

## Endpoints

| Method | Path                          | Auth   | Description                                 |
| ------ | ----------------------------- | ------ | ------------------------------------------- |
| `GET`  | `/health`                     | no     | Database connectivity and pool stats        |
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
else. When the hub arrives it will only push out rows this endpoint already
stored.

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

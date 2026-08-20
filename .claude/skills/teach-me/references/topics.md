# Syllabus

One topic per session. Each entry has the files to read, lab cases to run, and
the five drill questions.

Suggested order: 1 to 9. It goes from the request a client sends, inward to the
database, then out to how the thing runs.

Setup for every lab (the user runs these, not you):

```
make docker-run
make run
make seed
curl.exe -s -X POST http://localhost:8080/login -H "Content-Type: application/json" -d '{"username":"alice","password":"password123"}'
```

Keep the `token` from the login answer. Most cases need it.

---

## 1. Auth basics: password and access token

**Files:** [auth.go](internal/server/auth.go), [auth_test.go](internal/server/auth_test.go)

**Lab**

1. Register a new user. Predict the status code and the body.
2. Register the same username again. Predict before you run it.
3. Register with the password `short`. Where does that fail: in the handler, or
   in the database?
4. Log in with the wrong password. Read the error text closely. Does it tell an
   attacker whether the user exists?
5. Call `GET /auth/profile` three times: with no header, with `Bearer nonsense`,
   and with a real token.
6. Paste the token into jwt.io. What is inside it? Is it encrypted?

**Questions**

1. What happens to a password between the request and the database row?
2. How does the server check a token on a later request, step by step?
3. Why does the token carry the username and not only the user id?
4. Anyone can read a JWT payload. Why is that safe here, and what must never go
   inside it?
5. At 100x scale, what would you change about a 15-minute HS256 token?
   Push back: "how do you log a user out right now?"

---

## 2. Sessions: the refresh token

**Files:** [refresh.go](internal/server/refresh.go),
[refresh_tokens.go](internal/database/refresh_tokens.go),
the Sessions part of [README.md](README.md)

**Lab**

1. Log in with `-c cookies.txt -i`. Find the `Set-Cookie` line. Name every flag
   on it and say what each one does.
2. Run `curl.exe -s -i -X POST http://localhost:8080/auth/refresh -b cookies.txt`.
   Predict the status and the body first.
3. Refresh twice. Is the cookie the same both times? Is the token the same?
4. Refresh with no cookie at all.
5. Log out, then refresh again with the same cookie file.
6. Look in the database: `select * from refresh_tokens;` Is your token in there
   in a form you can read?

**Questions**

1. What are the two tokens, and what job does each one do?
2. What happens on the server when `/auth/refresh` is called?
3. Why is the refresh token hashed with SHA-256 and not with bcrypt?
4. The refresh token does not rotate. What does that cost you?
   Push back: "so how would you notice a stolen token?"
5. Logout does not kill the access token right away. Why was that accepted, and
   what would it take to close the gap?

---

## 3. Access control: 404, not 403

**Files:** [conversations.go](internal/server/conversations.go), starting at the
`memberOnly` helper

**Lab**

1. As alice, create a room. Keep the id.
2. Log in as a second user, bob. As bob, read alice's room messages.
3. As bob, read room `999999`, which does not exist.
4. As bob, read room `abc`, which is not even a number.
5. Compare cases 2, 3 and 4. Are the answers the same? Should they be?
6. Now add bob as a member and repeat case 2.

**Questions**

1. What does `memberOnly` check?
2. Walk me through what happens when bob asks for a room he is not in.
3. Why 404 and not 403?
4. What is the cost of hiding the difference? Think about debugging.
5. A room with 50,000 members: what does this check cost per request, and how
   would you fix it? Push back: "what if your membership cache is stale?"

---

## 4. Idempotent sends: `client_msg_id`

**Files:** [conversations.go](internal/server/conversations.go),
[00002_rooms_and_members.sql](internal/database/migrations/00002_rooms_and_members.sql)

**Lab**

1. Send a message with body `{"content":"hi","client_msg_id":"abc-1"}`. Note the
   status code.
2. Send the exact same body again. Predict: 201 or 200? A new row, or the old
   one?
3. Send `{"content":"different text","client_msg_id":"abc-1"}`. Which content
   comes back, the new one or the first one? Is that the right behaviour?
4. Send twice with no `client_msg_id` at all. What happens now?
5. As bob, in the same room, send with the same `client_msg_id` value. Does it
   collide with the one alice sent?
6. Count the rows in `messages` and check your prediction.

**Questions**

1. What problem does `client_msg_id` solve?
2. How does the server decide it has seen this one before?
3. Why is the unique index on `(conversation_id, sender_id, client_msg_id)` and
   not on `client_msg_id` alone?
4. What does this cost: in writes, in storage, in work for the client?
5. This protects one retry against one server. What still breaks with three
   servers behind a load balancer? Push back: "what if the row is written but
   the response is lost?"

---

## 5. Paging history: keyset, not OFFSET

**Files:** [conversations.go](internal/server/conversations.go),
[conversations.go](internal/database/conversations.go)

**Lab**

1. Send 10 messages into one room.
2. Call `GET /conversations/1/messages?limit=3`. What is `next_before_id`?
3. Put that value into `?before_id=`. Do you get the next 3, with no repeats?
4. Page all the way back to the start. What is `next_before_id` on the last page?
5. Ask for `?limit=500`. Refused, or quietly capped? Find that line in the code.
6. The real test: start paging, then send a new message between page 1 and
   page 2. Did you see any message twice? Now imagine the same test with
   `OFFSET 3`.

**Questions**

1. What does this endpoint return, and in what order?
2. How does a client walk back through the whole history?
3. Why page on an id instead of using `OFFSET`?
4. What can keyset paging not do that `OFFSET` can? Hint: jump to page 50.
5. Which index makes this fast, and what happens without it?
   Push back: "the id is one sequence. What breaks if you shard the table?"

---

## 6. Migrations: goose, and why not AutoMigrate

**Files:** [migrate.go](internal/database/migrate.go),
[migrations/](internal/database/migrations/), the Migrations part of the README

**Lab**

1. Run `make migrate-status`. Read the output line by line.
2. Run `make migrate-down`, then `make migrate-status` again. What moved?
3. Run `make migrate-up` to get back.
4. Open `00002_rooms_and_members.sql`. Find the part that moves data, not only
   the part that changes the shape.
5. Start two servers at the same time. Both try to migrate. Why does the
   database stay safe? Find the lock in the code.
6. Run `make migration NAME=test_me`, look at the two files it made, then
   delete them.

**Questions**

1. What runs when the server starts up?
2. How does goose know which files already ran?
3. Why was `AutoMigrate` dropped? Give one case it cannot handle.
4. `migrate-down` goes one step only. Why not all the way back?
5. In a zero-downtime deploy, old code and new code run at the same time for a
   minute. What does that rule out inside a migration? Push back: "so how do
   you rename a column with no downtime?"

---

## 7. DTOs: the wire is not the table

**Files:** [conversations.go](internal/server/conversations.go), the `*DTO`
types, and [models.go](internal/database/models.go)

**Lab**

1. Read `database.User`. List every field a client must never see.
2. Call `GET /auth/profile`. Which fields came back? Which stayed behind?
3. Imagine adding a field to a GORM model. Which JSON answers would change if
   there were no DTO?
4. Find where a model becomes a DTO. Is it in one place, or many?

**Questions**

1. What is a DTO here, and what is it for?
2. How does a `database.Message` become a `messageDTO`?
3. Why not add `json:"-"` tags to the model and send the model?
4. What does the extra layer cost you?
5. Two clients need different fields from the same room. What now?

---

## 8. Tests: testcontainers and a real database

**Files:** [database_test.go](internal/database/database_test.go),
[conversations_test.go](internal/server/conversations_test.go)

**Lab**

1. Run `make itest`. Watch Docker while it runs. What appears, and what
   disappears?
2. Stop Docker and run it again. Read the failure message.
3. Find where the test container is created, and where it is thrown away.
4. Find one test that checks an error case, not a happy path.
5. Break one assertion on purpose. Read the message. Could you fix the bug
   without opening the test file?

**Questions**

1. What do these tests catch that a mock would not?
2. How does each test get a clean database?
3. Why a real Postgres instead of sqlite or a fake?
4. What does it cost: time, Docker, CI minutes?
5. The suite takes 10 minutes once there are 100x more tests. What do you do?
   Push back: "which tests would you be willing to lose?"

---

## 9. The server itself: timeouts and shutdown

**Files:** [server.go](internal/server/server.go), [main.go](cmd/api/main.go),
[docker-compose.yml](docker-compose.yml)

**Lab**

1. Read the timeouts in `NewServer`. Say what each one covers.
2. `WriteTimeout` is 0 on purpose. Read the comment. What would break if it
   were 30 seconds?
3. Start the server and press Ctrl+C during a slow request. What happens to
   that request today?
4. Open `main.go`. The graceful shutdown is commented out. Predict exactly what
   changes once it is switched on.
5. In `docker-compose.yml`, find `healthcheck` and `depends_on`. Take the health
   condition away in your head: what breaks on a cold start?
6. Why is Postgres on port 5433 outside but 5432 inside?

**Questions**

1. What do `ReadTimeout` and `IdleTimeout` protect you from?
2. What should happen, step by step, when the process gets `SIGTERM`?
3. Why must `WriteTimeout` stay 0 in a server that will hold WebSockets?
4. What does a slow shutdown cost during a rolling deploy?
5. Kubernetes sends `SIGTERM` and kills the pod after 30 seconds. Design the
   shutdown. Push back: "what about the 4,000 open sockets?"

---

## Later topics

Add one entry here for each finished stage in [ROADMAP.md](ROADMAP.md):

- Stage 1, the hub: one goroutine per client, slow clients, ping and pong.
- Stage 2: metrics, request ids, readiness.
- Stage 3: two nodes, Redis Pub/Sub, presence with a TTL.
- Stage 4: sequence numbers, resume after reconnect, at-least-once delivery.
- Stage 5: outbox, broker, dead-letter queue.

A topic is only ready to teach when it has at least one case that fails on
purpose.

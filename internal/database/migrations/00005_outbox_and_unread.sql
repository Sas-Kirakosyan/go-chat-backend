-- +goose Up

-- The outbox: one row per message that still has to reach the broker.
--
-- Until now the send path wrote twice. First the message row committed to
-- Postgres, then the handler published to Redis. Two systems, one after the
-- other, with nothing holding them together. If the process died in between,
-- the message existed and nobody was ever told. That is the dual write, and it
-- is what this table removes.
--
-- Now the handler writes the message row AND the outbox row in the same
-- transaction. Both land or neither does. A separate relay reads this table
-- and publishes. The relay can crash, restart, and read the same row again;
-- the row is still here, so nothing is lost.
CREATE TABLE outbox (
    id bigserial PRIMARY KEY,

    -- What kind of event this is, so a future stage can add a second kind
    -- without a second table. Today it is always 'message.created'.
    topic text NOT NULL,

    -- The event itself. jsonb and not text, so you can read it with SQL while
    -- debugging instead of guessing what is inside a string.
    --
    -- It holds the message, not the member list. Who is in the room is looked
    -- up by the relay at publish time, so a user added between the commit and
    -- the publish still gets the message. A frozen list would miss them.
    payload jsonb NOT NULL,

    created_at timestamptz NOT NULL DEFAULT now(),

    -- NULL means "still waiting". This is the only state that matters: the
    -- relay asks for NULL rows, publishes them, and stamps the time. A boolean
    -- would answer "was it sent?" but not "how long did it wait?", and the
    -- waiting time is the number that tells you the relay is falling behind.
    published_at timestamptz,

    -- Kept for the humans, not for the code. When a row is stuck, these two
    -- say how many times the relay has tried and what the broker said last.
    attempts int NOT NULL DEFAULT 0,
    last_error text
);

-- A partial index: only unpublished rows are in it.
--
-- This matters more than it looks. The table grows with every message ever
-- sent, but the relay only ever asks for the rows that are still waiting, and
-- at rest there are none. So the index stays a few pages wide forever, and
-- "find the next batch" stays a cheap scan no matter how big the table gets.
-- A plain index on (id) would grow with the whole table and index millions of
-- rows nobody will ever look at again.
CREATE INDEX idx_outbox_pending ON outbox (id) WHERE published_at IS NULL;


-- The inbox: one row per message a consumer has already handled.
--
-- This is the mirror of the outbox above, and it is what makes the consumer
-- idempotent. The broker delivers at-least-once, so the same message WILL
-- arrive twice: a consumer that did its work and then died before its ack sees
-- it again on the next start. Doing the work twice must not count a message
-- twice.
--
-- A first attempt used a high water mark on the counter row instead — "only
-- apply a seq higher than the last one I applied". That is smaller and it is
-- wrong, and a two-node run found it in a minute: eleven messages sent,
-- nine counted. Both nodes share one consumer, so they work on different
-- messages at the same time and finish in whatever order they finish in. Node
-- A commits seq 9, node B then commits seq 8, and the mark says 8 has "already
-- been applied". It had not. It was simply late.
--
-- Order is not something a shared consumer has, so the guard must not need it.
-- One row per message does not: it asks "have I seen THIS message", which has
-- the same answer whenever it is asked.
CREATE TABLE consumed_messages (
    -- Which consumer. Two different consumers must both be able to handle the
    -- same message, so the name is part of the key. Today there is only
    -- 'unread'; the day a second one is added, this column is why it does not
    -- silently skip everything.
    consumer text NOT NULL,

    -- Deliberately NOT a foreign key to messages.
    --
    -- This row is a record of something that happened, not a pointer to
    -- something that exists. Nothing ever joins the two tables: the consumer
    -- only ever asks "have I seen this id", which is answered by this table
    -- alone. A foreign key would take a lock on the message row on every
    -- insert, and would make the two cleanups depend on each other's order,
    -- for integrity nobody reads.
    message_id bigint NOT NULL,

    consumed_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (consumer, message_id)
);

-- The cleanup index. Rows are deleted by age, and without this that DELETE
-- would scan the whole table every time it ran.
CREATE INDEX idx_consumed_at ON consumed_messages (consumed_at);


-- Unread counters: how many messages each member of a room has not seen.
--
-- This is the first piece of work in this project done by a consumer instead
-- of by the handler that caused it. The sender's request does not pay for it,
-- and if the counter is a second late, nothing breaks.
CREATE TABLE unread_counters (
    conversation_id bigint NOT NULL REFERENCES conversations (id),
    user_id bigint NOT NULL REFERENCES users (id),

    unread_count bigint NOT NULL DEFAULT 0,

    -- The highest message seq counted into this row. Nothing depends on it —
    -- the guard is consumed_messages above — and it is kept because it is the
    -- first thing a person wants when a badge looks wrong: it says how far
    -- through the room this counter has got.
    --
    -- It is written with GREATEST, so a message that arrives late cannot pull
    -- it backwards.
    last_seq bigint NOT NULL DEFAULT 0,

    updated_at timestamptz NOT NULL DEFAULT now(),

    -- One row per member per room, and the primary key says so. The consumer
    -- writes with ON CONFLICT on exactly these columns.
    PRIMARY KEY (conversation_id, user_id)
);

-- No backfill. Every existing room starts at zero unread.
--
-- Counting the old messages would be wrong, not just extra work: nobody was
-- keeping this number before, so there is no honest answer to "how many did
-- this user miss last month". Zero is the honest answer.

-- +goose Down

DROP TABLE unread_counters;
DROP INDEX idx_consumed_at;
DROP TABLE consumed_messages;
DROP INDEX idx_outbox_pending;
DROP TABLE outbox;

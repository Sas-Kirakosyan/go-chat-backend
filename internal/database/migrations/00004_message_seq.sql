-- +goose Up

-- Messages get a per-room sequence number.
--
-- Until now the only order was `id`, which is one global counter shared by
-- every room. That is enough to sort one room's history, and useless for the
-- question a reconnecting client actually asks: "did I miss anything?". A
-- client that last saw id 100 and now sees id 140 cannot tell whether 39
-- messages went to other rooms or 39 of its own were lost.
--
-- seq answers it. Inside one room it runs 1, 2, 3 with no holes, so
-- "I have 42, the server says 47" means exactly five missing messages.

-- last_seq is the allocator. CreateMessage bumps it and inserts the message in
-- ONE transaction, so the row lock this UPDATE takes is what stops two senders
-- in the same room from getting the same number.
--
-- It lives on conversations, not in a counter table, because the row is
-- already there and the foreign key already keeps it honest.
ALTER TABLE conversations ADD COLUMN last_seq bigint NOT NULL DEFAULT 0;

-- DEFAULT 0 exists only so this ALTER can run on a table that already holds
-- rows. Every real message gets a number below, and after that the application
-- always supplies one. A stored 0 would mean a bug.
ALTER TABLE messages ADD COLUMN seq bigint NOT NULL DEFAULT 0;

-- Backfill. Existing messages already have an order — their own id — so number
-- them inside each room in that order. This is the part AutoMigrate could
-- never do: it moves data, it does not only reshape columns.
UPDATE messages m
SET seq = n.rn
FROM (
    SELECT id, row_number() OVER (PARTITION BY conversation_id ORDER BY id) AS rn
    FROM messages
) n
WHERE m.id = n.id;

-- Every room's allocator starts where its history stopped. An empty room, and
-- a room whose messages were all soft-deleted, both stay at 0.
--
-- max(seq) is used rather than count(*) on purpose: soft-deleted rows are
-- still physically here and were numbered above, so counting would hand out a
-- number that is already taken.
UPDATE conversations c
SET last_seq = COALESCE((SELECT max(m.seq) FROM messages m WHERE m.conversation_id = c.id), 0);

-- The safety net. If the allocator is ever wrong, the second insert fails
-- loudly instead of quietly giving two different messages the same place in
-- the room, which would make one of them invisible to every client.
--
-- It is also the index that serves gap reads: ListMessagesAfterSeq asks for
-- (conversation_id, seq > N) ordered by seq, which is a range scan on exactly
-- these columns in exactly this order.
CREATE UNIQUE INDEX idx_msg_seq ON messages (conversation_id, seq);

-- +goose Down

DROP INDEX idx_msg_seq;
ALTER TABLE messages DROP COLUMN seq;
ALTER TABLE conversations DROP COLUMN last_seq;

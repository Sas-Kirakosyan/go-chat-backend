-- +goose Up

-- Rooms get members.
--
-- Until now a conversation belonged to exactly one user, so two people could
-- never share one. Membership moves into its own table.

CREATE TABLE conversation_members (
    id              bigserial PRIMARY KEY,
    conversation_id bigint NOT NULL REFERENCES conversations (id),
    user_id         bigint NOT NULL REFERENCES users (id),
    created_at      timestamptz
);

-- The membership rule lives here, in the database, not in Go. Two requests
-- arriving at the same instant cannot both add the same person: one insert
-- wins and the other is rejected, and AddMember turns that rejection into
-- ErrAlreadyMember.
--
-- There is no deleted_at column on this table on purpose. With soft deletes,
-- removing a member and adding them back would collide with this index,
-- because the old row would still physically be there.
CREATE UNIQUE INDEX idx_conv_member ON conversation_members (conversation_id, user_id);
CREATE INDEX idx_conversation_members_user_id ON conversation_members (user_id);

-- The old single owner becomes the creator, and also becomes a member, so
-- nobody loses a room they already had. This is the part AutoMigrate could
-- never do: it moves data, it does not only reshape columns.
ALTER TABLE conversations ADD COLUMN created_by_id bigint;

UPDATE conversations SET created_by_id = user_id;

INSERT INTO conversation_members (conversation_id, user_id, created_at)
SELECT id, user_id, now() FROM conversations;

ALTER TABLE conversations ALTER COLUMN created_by_id SET NOT NULL;
ALTER TABLE conversations DROP COLUMN user_id;

-- created_by_id is kept for audit only. It grants no rights: any member may
-- add anyone, and the check is membership, never ownership.
CREATE INDEX idx_conversations_created_by_id ON conversations (created_by_id);

UPDATE conversations SET title = '' WHERE title IS NULL;
ALTER TABLE conversations ALTER COLUMN title SET NOT NULL;

-- Messages need to say who wrote them.
--
-- "role" only ever held "user" or "model", which is the shape for a chat with
-- an AI. In a room with five people it cannot name the writer.
--
-- sender_id is added NOT NULL with no default on purpose. On an empty table
-- that is fine. On a table that already holds messages Postgres refuses, and
-- goose rolls the whole migration back, because there is no honest way to
-- guess who sent an old row. Refusing is the correct outcome, not a bug.
ALTER TABLE messages ADD COLUMN sender_id bigint NOT NULL REFERENCES users (id);
ALTER TABLE messages DROP COLUMN role;

-- An optional key that the sending client invents, so that a retry after a
-- network timeout cannot post the same message twice.
--
-- The unique index covers three columns, not two, so the key only has to be
-- unique per sender: two clients that pick the same string never collide.
-- NULL is allowed many times over, because Postgres does not treat two NULLs
-- as duplicates, so messages sent without a key are unaffected.
ALTER TABLE messages ADD COLUMN client_msg_id varchar(64);

CREATE UNIQUE INDEX idx_msg_client ON messages (conversation_id, sender_id, client_msg_id);
CREATE INDEX idx_messages_sender_id ON messages (sender_id);

-- +goose Down

DROP INDEX idx_messages_sender_id;
DROP INDEX idx_msg_client;
ALTER TABLE messages DROP COLUMN client_msg_id;
ALTER TABLE messages DROP COLUMN sender_id;
ALTER TABLE messages ADD COLUMN role varchar(16) NOT NULL DEFAULT 'user';

ALTER TABLE conversations ALTER COLUMN title DROP NOT NULL;
DROP INDEX idx_conversations_created_by_id;
ALTER TABLE conversations ADD COLUMN user_id bigint;
UPDATE conversations SET user_id = created_by_id;
ALTER TABLE conversations ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE conversations DROP COLUMN created_by_id;
CREATE INDEX idx_conversations_user_id ON conversations (user_id);

DROP TABLE conversation_members;

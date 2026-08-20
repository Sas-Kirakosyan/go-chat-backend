-- +goose Up

-- Baseline.
--
-- This is the schema exactly as GORM's AutoMigrate built it, written out by
-- hand so that goose can take over from here. It is the only migration that
-- uses IF NOT EXISTS, and it does so on purpose: a database that was already
-- built by AutoMigrate adopts goose without being rebuilt and keeps its rows,
-- while a brand new database gets the same tables. Every migration after this
-- one is plain, exact SQL.

CREATE TABLE IF NOT EXISTS users (
    id            bigserial PRIMARY KEY,
    created_at    timestamptz,
    updated_at    timestamptz,
    deleted_at    timestamptz,
    username      varchar(64) NOT NULL,
    password_hash text NOT NULL
);

-- Note that the unique index does not know about soft deletes: a deleted user
-- still holds its username. That is the behaviour we already had.
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users (username);
CREATE INDEX IF NOT EXISTS idx_users_deleted_at ON users (deleted_at);

CREATE TABLE IF NOT EXISTS conversations (
    id         bigserial PRIMARY KEY,
    created_at timestamptz,
    updated_at timestamptz,
    deleted_at timestamptz,
    user_id    bigint NOT NULL REFERENCES users (id),
    title      varchar(200)
);

CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations (user_id);
CREATE INDEX IF NOT EXISTS idx_conversations_deleted_at ON conversations (deleted_at);

CREATE TABLE IF NOT EXISTS messages (
    id              bigserial PRIMARY KEY,
    created_at      timestamptz,
    updated_at      timestamptz,
    deleted_at      timestamptz,
    conversation_id bigint NOT NULL REFERENCES conversations (id),
    role            varchar(16) NOT NULL,
    content         text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages (conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_deleted_at ON messages (deleted_at);

-- +goose Down

DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS conversations;
DROP TABLE IF EXISTS users;

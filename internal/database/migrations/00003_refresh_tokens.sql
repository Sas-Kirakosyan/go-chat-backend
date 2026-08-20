-- +goose Up

-- One row per login session.
--
-- The access token stays a short JWT that the server never looks up. This
-- table is what makes logging out possible at all: an access token cannot be
-- taken back once it is signed, but a session can be deleted, and after that
-- no new access token can be minted from it.
CREATE TABLE refresh_tokens (
    id         bigserial PRIMARY KEY,
    user_id    bigint NOT NULL REFERENCES users (id),

    -- The SHA-256 of the token, never the token itself. A stolen database
    -- backup then hands an attacker nothing they can use: they would have to
    -- reverse the hash, and the token is 256 bits of randomness, so there is
    -- nothing to guess. bcrypt is not used here on purpose — it is slow by
    -- design, and that slowness would sit on the refresh path for no gain
    -- against an input that cannot be brute-forced.
    token_hash bytea NOT NULL,

    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,

    -- NULL means the session is live. A time means the user logged out.
    -- Keeping the row rather than deleting it separates "this token was never
    -- real" from "this token was ended", which is worth having in a log.
    revoked_at timestamptz
);

CREATE UNIQUE INDEX idx_refresh_token_hash ON refresh_tokens (token_hash);

-- For "log out everywhere", and for clearing a user's dead rows at login.
CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens (user_id);
CREATE INDEX idx_refresh_tokens_expires_at ON refresh_tokens (expires_at);

-- +goose Down

DROP TABLE refresh_tokens;

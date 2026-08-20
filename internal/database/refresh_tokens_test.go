package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func hashOf(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

func TestRefreshTokenStore(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	user := mustUser(t, srv, "session-user")
	live := hashOf("live-token")

	session, err := srv.CreateRefreshToken(ctx, user.ID, live, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("CreateRefreshToken() returned %v", err)
	}
	if session.ID == 0 || session.RevokedAt != nil {
		t.Fatalf("new session looks wrong: %+v", session)
	}

	// Reading it back must bring the user along, because the refresh handler
	// needs the username to sign the next access token.
	got, err := srv.GetValidRefreshToken(ctx, live)
	if err != nil {
		t.Fatalf("GetValidRefreshToken() returned %v", err)
	}
	if got.ID != session.ID || got.User.Username != "session-user" {
		t.Fatalf("GetValidRefreshToken() = %+v, want session %d with its user loaded", got, session.ID)
	}

	// A token nobody ever issued.
	if _, err := srv.GetValidRefreshToken(ctx, hashOf("never-issued")); !errors.Is(err, ErrRefreshTokenNotFound) {
		t.Fatalf("unknown token returned %v, want ErrRefreshTokenNotFound", err)
	}

	// An expired one is refused without any help from Go: the query decides.
	expired := hashOf("expired-token")
	if _, err := srv.CreateRefreshToken(ctx, user.ID, expired, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("CreateRefreshToken(expired) returned %v", err)
	}
	if _, err := srv.GetValidRefreshToken(ctx, expired); !errors.Is(err, ErrRefreshTokenNotFound) {
		t.Fatalf("expired token returned %v, want ErrRefreshTokenNotFound", err)
	}

	// Logging out.
	if err := srv.RevokeRefreshToken(ctx, live); err != nil {
		t.Fatalf("RevokeRefreshToken() returned %v", err)
	}
	if _, err := srv.GetValidRefreshToken(ctx, live); !errors.Is(err, ErrRefreshTokenNotFound) {
		t.Fatalf("revoked token returned %v, want ErrRefreshTokenNotFound", err)
	}

	// Twice is not a failure, and neither is revoking something imaginary.
	if err := srv.RevokeRefreshToken(ctx, live); err != nil {
		t.Fatalf("second RevokeRefreshToken() returned %v", err)
	}
	if err := srv.RevokeRefreshToken(ctx, hashOf("never-issued")); err != nil {
		t.Fatalf("RevokeRefreshToken(unknown) returned %v, want nil", err)
	}
}

// The same token hash must never exist twice. The unique index is what
// guarantees a token names exactly one session.
func TestRefreshTokenHashIsUnique(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	user := mustUser(t, srv, "unique-session-user")
	hash := hashOf("one-and-only")

	if _, err := srv.CreateRefreshToken(ctx, user.ID, hash, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("first CreateRefreshToken() returned %v", err)
	}
	if _, err := srv.CreateRefreshToken(ctx, user.ID, hash, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("the same token hash was stored twice; the unique index is missing")
	}
}

// Purging keeps the table from growing forever, and must not touch a session
// somebody is still using.
func TestPurgeExpiredRefreshTokens(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	user := mustUser(t, srv, "purge-user")
	other := mustUser(t, srv, "purge-bystander")

	good := hashOf("purge-good")
	stale := hashOf("purge-stale")
	loggedOut := hashOf("purge-logged-out")
	someoneElse := hashOf("purge-other-user")

	for _, tc := range []struct {
		owner   uint
		hash    []byte
		expires time.Time
	}{
		{user.ID, good, time.Now().Add(time.Hour)},
		{user.ID, stale, time.Now().Add(-time.Hour)},
		{user.ID, loggedOut, time.Now().Add(time.Hour)},
		{other.ID, someoneElse, time.Now().Add(-time.Hour)},
	} {
		if _, err := srv.CreateRefreshToken(ctx, tc.owner, tc.hash, tc.expires); err != nil {
			t.Fatalf("CreateRefreshToken() returned %v", err)
		}
	}
	if err := srv.RevokeRefreshToken(ctx, loggedOut); err != nil {
		t.Fatalf("RevokeRefreshToken() returned %v", err)
	}

	if err := srv.PurgeExpiredRefreshTokens(ctx, user.ID); err != nil {
		t.Fatalf("PurgeExpiredRefreshTokens() returned %v", err)
	}

	s := srv.(*service)
	count := func(hash []byte) int64 {
		var n int64
		if err := s.db.WithContext(ctx).Model(&RefreshToken{}).
			Where("token_hash = ?", hash).Count(&n).Error; err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	if count(good) != 1 {
		t.Error("the live session was purged")
	}
	if count(stale) != 0 {
		t.Error("the expired session survived the purge")
	}
	if count(loggedOut) != 0 {
		t.Error("the logged-out session survived the purge")
	}
	// The purge is scoped to one user, so nobody else's rows move.
	if count(someoneElse) != 1 {
		t.Error("the purge deleted another user's session")
	}
}

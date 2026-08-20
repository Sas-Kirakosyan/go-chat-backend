package database

import (
	"context"
	"errors"
	"testing"
)

// mustUser creates a user for a test and fails the test if it cannot.
func mustUser(t *testing.T, srv Service, username string) *User {
	t.Helper()
	u, err := srv.CreateUser(context.Background(), username, "hashed-password")
	if err != nil {
		t.Fatalf("CreateUser(%s) returned %v", username, err)
	}
	return u
}

func TestConversationStore(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	owner := mustUser(t, srv, "conv-owner")
	invited := mustUser(t, srv, "conv-invited")
	outsider := mustUser(t, srv, "conv-outsider")

	conv, err := srv.CreateConversation(ctx, "release plan", owner.ID, []uint{invited.ID, owner.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}
	// owner.ID was passed in twice on purpose: it must still be one row.
	if len(conv.Members) != 2 {
		t.Fatalf("new conversation has %d members, want 2", len(conv.Members))
	}
	for _, m := range conv.Members {
		if m.User.Username == "" {
			t.Fatalf("member %d came back without its user preloaded", m.UserID)
		}
	}

	// A member id with no user must write nothing at all.
	if _, err := srv.CreateConversation(ctx, "ghosts", owner.ID, []uint{999999}); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("CreateConversation(bad member) returned %v, want ErrUserNotFound", err)
	}

	// The access check: members pass, everyone else gets "not found", and a
	// room that never existed is not distinguishable from one you are not in.
	if err := srv.EnsureMember(ctx, conv.ID, invited.ID); err != nil {
		t.Fatalf("EnsureMember(member) returned %v", err)
	}
	if err := srv.EnsureMember(ctx, conv.ID, outsider.ID); !errors.Is(err, ErrConversationNotFound) {
		t.Fatalf("EnsureMember(outsider) returned %v, want ErrConversationNotFound", err)
	}
	if err := srv.EnsureMember(ctx, conv.ID+999999, owner.ID); !errors.Is(err, ErrConversationNotFound) {
		t.Fatalf("EnsureMember(missing room) returned %v, want ErrConversationNotFound", err)
	}

	// Listing only returns rooms you are in.
	rooms, err := srv.ListConversationsForUser(ctx, invited.ID)
	if err != nil {
		t.Fatalf("ListConversationsForUser() returned %v", err)
	}
	if len(rooms) != 1 || rooms[0].ID != conv.ID {
		t.Fatalf("ListConversationsForUser(member) = %+v, want just conversation %d", rooms, conv.ID)
	}
	if rooms, err := srv.ListConversationsForUser(ctx, outsider.ID); err != nil || len(rooms) != 0 {
		t.Fatalf("ListConversationsForUser(outsider) = %+v, %v; want no rooms and no error", rooms, err)
	}

	// Adding a member is once only — the unique index has to hold.
	if _, err := srv.AddMember(ctx, conv.ID, outsider.ID); err != nil {
		t.Fatalf("AddMember() returned %v", err)
	}
	if _, err := srv.AddMember(ctx, conv.ID, outsider.ID); !errors.Is(err, ErrAlreadyMember) {
		t.Fatalf("AddMember(twice) returned %v, want ErrAlreadyMember", err)
	}
	if err := srv.EnsureMember(ctx, conv.ID, outsider.ID); err != nil {
		t.Fatalf("EnsureMember() after AddMember returned %v", err)
	}
}

func TestMessageStore(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "msg-sender")
	other := mustUser(t, srv, "msg-other")

	conv, err := srv.CreateConversation(ctx, "history", sender.ID, []uint{other.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	// Five messages, ids going up.
	ids := make([]uint, 0, 5)
	for _, text := range []string{"m1", "m2", "m3", "m4", "m5"} {
		msg, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, text, nil)
		if err != nil || !created {
			t.Fatalf("CreateMessage(%s) = created %v, err %v", text, created, err)
		}
		ids = append(ids, msg.ID)
	}

	// Newest first, capped at the limit, with the sender loaded.
	page, err := srv.ListMessages(ctx, conv.ID, 0, 2)
	if err != nil {
		t.Fatalf("ListMessages() returned %v", err)
	}
	if len(page) != 2 || page[0].ID != ids[4] || page[1].ID != ids[3] {
		t.Fatalf("first page = %v, want the two newest ids %v", messageIDs(page), ids[3:])
	}
	if page[0].Sender.Username != "msg-sender" {
		t.Fatalf("first page sender = %q, want msg-sender", page[0].Sender.Username)
	}

	// Walking back with before_id must not repeat or skip anything.
	next, err := srv.ListMessages(ctx, conv.ID, page[1].ID, 2)
	if err != nil {
		t.Fatalf("ListMessages(before_id) returned %v", err)
	}
	if len(next) != 2 || next[0].ID != ids[2] || next[1].ID != ids[1] {
		t.Fatalf("second page = %v, want %v", messageIDs(next), ids[1:3])
	}

	// The same client_msg_id twice is a retry, not a second message.
	key := "client-key-1"
	first, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, "once", &key)
	if err != nil || !created {
		t.Fatalf("CreateMessage(with key) = created %v, err %v", created, err)
	}
	again, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, "once again", &key)
	if err != nil {
		t.Fatalf("CreateMessage(same key) returned %v", err)
	}
	if created || again.ID != first.ID || again.Content != "once" {
		t.Fatalf("CreateMessage(same key) = %+v, created %v; want the first message back", again, created)
	}

	// The key only has to be unique per sender, so another user may reuse it.
	if _, created, err := srv.CreateMessage(ctx, conv.ID, other.ID, "mine", &key); err != nil || !created {
		t.Fatalf("CreateMessage(same key, other sender) = created %v, err %v; want a new message", created, err)
	}

	// Two nil keys must both be stored: NULL is not a duplicate.
	if _, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, "no key", nil); err != nil || !created {
		t.Fatalf("CreateMessage(nil key) = created %v, err %v", created, err)
	}
}

// Migrate must leave the database at the newest version, and running it again
// must change nothing. This is what goose buys over AutoMigrate: it writes
// down what it applied, so a second start is a no-op instead of a fresh guess.
func TestMigrateIsVersionedAndRepeatable(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}
	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() returned %v", err)
	}

	s := srv.(*service)
	provider, err := newProvider(s)
	if err != nil {
		t.Fatalf("newProvider() returned %v", err)
	}
	version, err := provider.GetDBVersion(ctx)
	if err != nil {
		t.Fatalf("GetDBVersion() returned %v", err)
	}
	// Bump this when a migration is added. It is a deliberate speed bump: a
	// new .sql file should be a conscious act, not something that slips in.
	if want := int64(3); version != want {
		t.Fatalf("database is at version %d, want %d", version, want)
	}

	// The old single-owner columns must be gone, and the new shape present.
	m := s.db.WithContext(ctx).Migrator()
	for _, c := range []struct {
		model any
		name  string
		want  bool
	}{
		{&Conversation{}, "user_id", false},      // replaced by conversation_members
		{&Conversation{}, "created_by_id", true}, // audit only
		{&Message{}, "role", false},              // replaced by sender_id
		{&Message{}, "sender_id", true},
		{&Message{}, "client_msg_id", true},
	} {
		if got := m.HasColumn(c.model, c.name); got != c.want {
			t.Errorf("column %s present = %v, want %v", c.name, got, c.want)
		}
	}
	if !m.HasTable(&ConversationMember{}) {
		t.Error("conversation_members table is missing")
	}
}

// The down direction has to work too, and 00002 has to carry data across, not
// only reshape columns: the single owner of an old conversation must come out
// the other side as the creator *and* as a member. A broken Down is only ever
// discovered at the worst possible moment, so it is exercised here.
func TestMigrateDownAndUpMovesOwnerToMember(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	s := srv.(*service)
	provider, err := newProvider(s)
	if err != nil {
		t.Fatalf("newProvider() returned %v", err)
	}

	// Earlier tests left rows behind. 00002 adds messages.sender_id as NOT
	// NULL with no default, which is exactly what should stop a migration that
	// cannot name the sender of an old row, so clear the two tables first.
	// Users are left alone.
	if err := s.db.WithContext(ctx).Exec(
		`TRUNCATE messages, conversation_members, conversations RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Go back to the old single-owner schema.
	if _, err := provider.DownTo(ctx, 1); err != nil {
		t.Fatalf("DownTo(1) returned %v", err)
	}
	owner := mustUser(t, srv, "migration-owner")
	if err := s.db.WithContext(ctx).Exec(
		`INSERT INTO conversations (created_at, updated_at, user_id, title) VALUES (now(), now(), ?, 'old room')`,
		owner.ID).Error; err != nil {
		t.Fatalf("insert old-shape conversation: %v", err)
	}

	// Forward again. The owner must survive the trip.
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("Up() returned %v", err)
	}

	var got struct {
		CreatedByID uint
		Members     int64
	}
	if err := s.db.WithContext(ctx).Raw(
		`SELECT c.created_by_id,
		        (SELECT count(*) FROM conversation_members m
		          WHERE m.conversation_id = c.id AND m.user_id = c.created_by_id) AS members
		   FROM conversations c WHERE c.title = 'old room'`).Scan(&got).Error; err != nil {
		t.Fatalf("read migrated conversation: %v", err)
	}
	if got.CreatedByID != owner.ID {
		t.Errorf("created_by_id = %d, want %d", got.CreatedByID, owner.ID)
	}
	if got.Members != 1 {
		t.Errorf("the old owner has %d membership rows, want 1", got.Members)
	}

	// Leave a clean table for whatever runs next.
	if err := s.db.WithContext(ctx).Exec(
		`TRUNCATE messages, conversation_members, conversations RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("cleanup truncate: %v", err)
	}
}

func messageIDs(msgs []Message) []uint {
	out := make([]uint, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.ID)
	}
	return out
}

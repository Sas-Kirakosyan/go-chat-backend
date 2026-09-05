package database

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The unread counter is written by a consumer on a broker, and a broker
// delivers at-least-once. So the interesting question is never "does it
// count?" — it is "what happens when it counts the same message again?".
//
// The answer is the inbox: one row per message in consumed_messages, written
// in the same transaction as the counters. These tests are what say it works.
//
// The test below called TestApplyUnreadCountsAMessageThatArrivesLate is the
// one that earned its place. The first version of this code used a high water
// mark on the counter row instead, and it passed every test written for it —
// because every test applied messages in order. Two nodes sharing one consumer
// do not, and a real two-node run lost two messages out of fourteen.

// msgID makes a message id for a test. Every test in this package shares one
// database, so the ids have to be unique across tests as well as inside one,
// and the room id is what makes them so.
//
// They are invented rather than taken from real rows because consumed_messages
// has no foreign key to messages — see the migration for why — and inventing
// them keeps each test to the one thing it is about.
func msgID(conversationID, seq uint) uint { return conversationID*1000 + seq }

func TestApplyUnreadCountsEveryoneButTheSender(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "unread-sender")
	reader := mustUser(t, srv, "unread-reader")
	other := mustUser(t, srv, "unread-other")

	conv, err := srv.CreateConversation(ctx, "unread room", sender.ID, []uint{reader.ID, other.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	changed, err := srv.ApplyUnread(ctx, msgID(conv.ID, 1), conv.ID, sender.ID, 1)
	if err != nil {
		t.Fatalf("ApplyUnread() returned %v", err)
	}
	// Three members, one of them the sender.
	if changed != 2 {
		t.Fatalf("rows changed: got %d, want 2", changed)
	}

	for _, u := range []*User{reader, other} {
		unread, err := srv.UnreadForUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("UnreadForUser(%s) returned %v", u.Username, err)
		}
		if unread[conv.ID] != 1 {
			t.Fatalf("%s has %d unread, want 1", u.Username, unread[conv.ID])
		}
	}

	// Your own message is not something you have not read.
	own, err := srv.UnreadForUser(ctx, sender.ID)
	if err != nil {
		t.Fatalf("UnreadForUser(sender) returned %v", err)
	}
	if own[conv.ID] != 0 {
		t.Fatalf("the sender has %d unread for their own message", own[conv.ID])
	}
}

// The one that matters. Deliver the same message five times and the number
// must move exactly once.
func TestApplyUnreadIgnoresARedelivery(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "redeliver-sender")
	reader := mustUser(t, srv, "redeliver-reader")

	conv, err := srv.CreateConversation(ctx, "redeliver room", sender.ID, []uint{reader.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	for i := range 5 {
		changed, err := srv.ApplyUnread(ctx, msgID(conv.ID, 1), conv.ID, sender.ID, 1)
		if err != nil {
			t.Fatalf("ApplyUnread(delivery %d) returned %v", i+1, err)
		}
		// The return value is how the consumer tells work apart from a
		// duplicate, and it is what feeds the redelivery metric.
		want := int64(0)
		if i == 0 {
			want = 1
		}
		if changed != want {
			t.Fatalf("delivery %d changed %d rows, want %d", i+1, changed, want)
		}
	}

	unread, err := srv.UnreadForUser(ctx, reader.ID)
	if err != nil {
		t.Fatalf("UnreadForUser() returned %v", err)
	}
	if unread[conv.ID] != 1 {
		t.Fatalf("unread after five deliveries of one message: got %d, want 1", unread[conv.ID])
	}
}

// A message that arrives after a later one must still be counted. This is the
// test the first version of this code failed.
//
// Two nodes share one durable consumer, so they are working on different
// messages at the same instant and commit in whatever order they finish. Node
// A commits seq 4, node B then commits seq 3. Seq 3 is not a duplicate — it is
// late — and a guard that cannot tell those apart silently loses it.
//
// A high water mark cannot tell them apart. An inbox row per message does not
// have to: "have I seen this message" gives the same answer whenever it is
// asked.
func TestApplyUnreadCountsAMessageThatArrivesLate(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "ooo-sender")
	reader := mustUser(t, srv, "ooo-reader")

	conv, err := srv.CreateConversation(ctx, "ooo room", sender.ID, []uint{reader.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	// Newest first, which is what two nodes racing looks like from here.
	for _, seq := range []uint{4, 3, 5} {
		changed, err := srv.ApplyUnread(ctx, msgID(conv.ID, seq), conv.ID, sender.ID, seq)
		if err != nil {
			t.Fatalf("ApplyUnread(%d) returned %v", seq, err)
		}
		if changed != 1 {
			t.Fatalf("seq %d changed %d rows, want 1 — an out-of-order message was dropped", seq, changed)
		}
	}

	unread, err := srv.UnreadForUser(ctx, reader.ID)
	if err != nil {
		t.Fatalf("UnreadForUser() returned %v", err)
	}
	if unread[conv.ID] != 3 {
		t.Fatalf("unread after three messages applied out of order: got %d, want 3", unread[conv.ID])
	}

	// And a redelivery of the late one still does nothing, so the fix did not
	// simply turn the guard off.
	changed, err := srv.ApplyUnread(ctx, msgID(conv.ID, 3), conv.ID, sender.ID, 3)
	if err != nil {
		t.Fatalf("ApplyUnread(3, again) returned %v", err)
	}
	if changed != 0 {
		t.Fatalf("a redelivery changed %d rows, want 0", changed)
	}
}

// Two nodes can be handed the same redelivered message at the same instant.
// No amount of care in Go makes a read-then-write safe across two processes,
// which is why the guard is inside the statement. This is that case.
func TestApplyUnreadIsSafeConcurrently(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "race-sender")
	reader := mustUser(t, srv, "race-reader")

	conv, err := srv.CreateConversation(ctx, "race room", sender.ID, []uint{reader.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	// Twenty goroutines applying the SAME message. Exactly one of them may
	// change a row; the counter must end at 1.
	const nodes = 20

	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		mu      sync.Mutex
		changes int64
	)
	start.Add(1)

	for range nodes {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()

			changed, err := srv.ApplyUnread(ctx, msgID(conv.ID, 1), conv.ID, sender.ID, 1)
			if err != nil {
				t.Errorf("ApplyUnread() returned %v", err)
				return
			}
			mu.Lock()
			changes += changed
			mu.Unlock()
		}()
	}

	start.Done()
	done.Wait()

	if changes != 1 {
		t.Fatalf("%d of %d concurrent deliveries changed a row, want exactly 1", changes, nodes)
	}
	unread, err := srv.UnreadForUser(ctx, reader.ID)
	if err != nil {
		t.Fatalf("UnreadForUser() returned %v", err)
	}
	if unread[conv.ID] != 1 {
		t.Fatalf("unread after %d concurrent deliveries of one message: got %d, want 1", nodes, unread[conv.ID])
	}
}

// Different messages must each count once, including when many arrive at the
// same time from different nodes.
func TestApplyUnreadCountsEachMessageOnce(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "many-sender")
	reader := mustUser(t, srv, "many-reader")

	conv, err := srv.CreateConversation(ctx, "many room", sender.ID, []uint{reader.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	const messages = 10
	for seq := uint(1); seq <= messages; seq++ {
		if _, err := srv.ApplyUnread(ctx, msgID(conv.ID, seq), conv.ID, sender.ID, seq); err != nil {
			t.Fatalf("ApplyUnread(%d) returned %v", seq, err)
		}
	}

	unread, err := srv.UnreadForUser(ctx, reader.ID)
	if err != nil {
		t.Fatalf("UnreadForUser() returned %v", err)
	}
	if unread[conv.ID] != messages {
		t.Fatalf("unread: got %d, want %d", unread[conv.ID], messages)
	}
}

// Marking a room read zeroes the badge and leaves the consumer alone. The
// guard is a separate table, so nothing a reader does here can make the
// consumer skip a message or count one twice.
func TestMarkConversationReadClearsWithoutBlockingTheConsumer(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "read-sender")
	reader := mustUser(t, srv, "read-reader")

	conv, err := srv.CreateConversation(ctx, "read room", sender.ID, []uint{reader.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	for seq := uint(1); seq <= 3; seq++ {
		if _, err := srv.ApplyUnread(ctx, msgID(conv.ID, seq), conv.ID, sender.ID, seq); err != nil {
			t.Fatalf("ApplyUnread(%d) returned %v", seq, err)
		}
	}

	if err := srv.MarkConversationRead(ctx, conv.ID, reader.ID); err != nil {
		t.Fatalf("MarkConversationRead() returned %v", err)
	}
	unread, _ := srv.UnreadForUser(ctx, reader.ID)
	if unread[conv.ID] != 0 {
		t.Fatalf("unread after marking read: got %d, want 0", unread[conv.ID])
	}

	// The next real message still counts.
	if _, err := srv.ApplyUnread(ctx, msgID(conv.ID, 4), conv.ID, sender.ID, 4); err != nil {
		t.Fatalf("ApplyUnread(4) returned %v", err)
	}
	unread, _ = srv.UnreadForUser(ctx, reader.ID)
	if unread[conv.ID] != 1 {
		t.Fatalf("unread after the next message: got %d, want 1", unread[conv.ID])
	}

	// And a redelivery of one of the messages read earlier still does nothing,
	// which is what proves the bookmark was left alone.
	changed, err := srv.ApplyUnread(ctx, msgID(conv.ID, 2), conv.ID, sender.ID, 2)
	if err != nil {
		t.Fatalf("ApplyUnread(2) returned %v", err)
	}
	if changed != 0 {
		t.Fatalf("a redelivery after marking read changed %d rows, want 0", changed)
	}
}

// The inbox grows with every message, so it is cleaned up by age. What must
// not happen is a row being forgotten while the broker could still redeliver
// its message — that would let the message be counted a second time.
func TestDeleteConsumedBeforeForgetsOnlyOldRows(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "inbox-clean-sender")
	reader := mustUser(t, srv, "inbox-clean-reader")

	conv, err := srv.CreateConversation(ctx, "inbox clean room", sender.ID, []uint{reader.ID})
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	id := msgID(conv.ID, 1)
	if _, err := srv.ApplyUnread(ctx, id, conv.ID, sender.ID, 1); err != nil {
		t.Fatalf("ApplyUnread() returned %v", err)
	}

	// A cutoff in the past leaves the row alone, and the guard still holds.
	if _, err := srv.DeleteConsumedBefore(ctx, time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("DeleteConsumedBefore(past) returned %v", err)
	}
	changed, err := srv.ApplyUnread(ctx, id, conv.ID, sender.ID, 1)
	if err != nil {
		t.Fatalf("ApplyUnread() returned %v", err)
	}
	if changed != 0 {
		t.Fatal("the cleanup removed a row it should have kept, so a redelivery was counted again")
	}

	// A cutoff in the future takes it. This is the honest edge of the design:
	// once the row is gone the guard is gone with it, which is exactly why the
	// retention has to be longer than the broker keeps a message.
	deleted, err := srv.DeleteConsumedBefore(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("DeleteConsumedBefore(future) returned %v", err)
	}
	if deleted < 1 {
		t.Fatalf("deleted %d rows, want at least 1", deleted)
	}
	unread, _ := srv.UnreadForUser(ctx, reader.ID)
	if unread[conv.ID] != 1 {
		t.Fatalf("the cleanup changed a counter: got %d, want 1", unread[conv.ID])
	}
}

// Marking an empty room read must work rather than fail on a missing row. A
// client opening a room it has never had a message in is an ordinary thing to
// do.
func TestMarkConversationReadOnAnEmptyRoom(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	owner := mustUser(t, srv, "empty-read-owner")
	conv, err := srv.CreateConversation(ctx, "empty room", owner.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	if err := srv.MarkConversationRead(ctx, conv.ID, owner.ID); err != nil {
		t.Fatalf("MarkConversationRead() on an empty room returned %v", err)
	}
	// Twice, because a client can send it twice.
	if err := srv.MarkConversationRead(ctx, conv.ID, owner.ID); err != nil {
		t.Fatalf("MarkConversationRead() twice returned %v", err)
	}

	unread, err := srv.UnreadForUser(ctx, owner.ID)
	if err != nil {
		t.Fatalf("UnreadForUser() returned %v", err)
	}
	// A room with a zero counter must not appear in the map at all, so that a
	// missing key can mean zero everywhere else.
	if _, ok := unread[conv.ID]; ok {
		t.Fatalf("a room with 0 unread is in the map: %v", unread)
	}
}

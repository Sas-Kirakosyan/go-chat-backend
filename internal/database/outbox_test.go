package database

import (
	"context"
	"sync"
	"testing"
	"time"

	"go-chat-backend/internal/event"
)

// The outbox has one promise, and it is the reason Stage 5 exists: a message
// row and an instruction to deliver it are written together or not at all.
//
// These tests are the proof. They run against a real Postgres, because the
// promise is a property of the transaction, and a fake would only prove that
// the fake was written to agree.

func TestOutboxRowIsWrittenWithTheMessage(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "outbox-sender")
	conv, err := srv.CreateConversation(ctx, "outbox room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)

	msg, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "hello", nil)
	if err != nil || !created {
		t.Fatalf("CreateMessage() = created %v, err %v", created, err)
	}

	rows := pendingSince(t, srv, before)
	if len(rows) != 1 {
		t.Fatalf("one message wrote %d outbox rows, want 1", len(rows))
	}

	row := rows[0]
	if row.Topic != event.TopicMessageCreated {
		t.Fatalf("topic: got %q, want %q", row.Topic, event.TopicMessageCreated)
	}
	if row.PublishedAt != nil {
		t.Fatal("a brand new outbox row is already marked published")
	}

	// The payload has to describe the message completely. Anything missing
	// here is missing from every delivery, and there is no second chance to
	// look it up: the relay never reads the messages table.
	ev, err := event.DecodeMessageCreated([]byte(row.Payload))
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if ev.MessageID != msg.ID || ev.Seq != msg.Seq {
		t.Fatalf("payload describes a different message: %+v, want id %d seq %d", ev, msg.ID, msg.Seq)
	}
	if ev.ConversationID != conv.ID || ev.SenderID != sender.ID {
		t.Fatalf("payload has the wrong room or sender: %+v", ev)
	}
	if ev.SenderName != sender.Username {
		t.Fatalf("payload sender name: got %q, want %q", ev.SenderName, sender.Username)
	}
	if ev.Content != "hello" {
		t.Fatalf("payload content: got %q, want %q", ev.Content, "hello")
	}
	if ev.CreatedAt.IsZero() {
		t.Fatal("payload carries no created_at, so every delivered message would be dated zero")
	}
}

// A retry of a client_msg_id must queue nothing.
//
// This falls out of the design rather than being coded: the duplicate insert
// fails, the transaction rolls back, and the outbox row goes with it. It is
// still worth a test, because it is the property that stops a client with a
// flaky connection from posting the same line to everyone's screen five times.
//
// It is the same rollback that stops a retry burning a sequence number, so
// this test also re-checks that from the other side.
func TestRetryQueuesNoSecondOutboxRow(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "outbox-retry-sender")
	conv, err := srv.CreateConversation(ctx, "retry room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)
	key := "retry-key"

	first, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "once", &key)
	if err != nil || !created {
		t.Fatalf("CreateMessage(first) = created %v, err %v", created, err)
	}

	for i := range 4 {
		again, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "once", &key)
		if err != nil {
			t.Fatalf("CreateMessage(retry %d) returned %v", i+1, err)
		}
		if created || again.ID != first.ID {
			t.Fatalf("CreateMessage(retry %d) = id %d created %v; want the first message back", i+1, again.ID, created)
		}
	}

	rows := pendingSince(t, srv, before)
	if len(rows) != 1 {
		t.Fatalf("one message and four retries wrote %d outbox rows, want 1", len(rows))
	}

	// And no sequence number was burned, so the next message has no hole in
	// front of it.
	next, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "after", nil)
	if err != nil || !created {
		t.Fatalf("CreateMessage(after) = created %v, err %v", created, err)
	}
	if next.Seq != first.Seq+1 {
		t.Fatalf("seq after four retries: got %d, want %d", next.Seq, first.Seq+1)
	}
}

// The relay reads in id order and marks in bulk. Order is the whole reason
// there is only one relay: out of order here is a client seeing line 3 before
// line 2.
func TestFetchOutboxIsOrderedAndMarkable(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "outbox-order-sender")
	conv, err := srv.CreateConversation(ctx, "order room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)
	for range 5 {
		if _, _, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "m", nil); err != nil {
			t.Fatalf("CreateMessage() returned %v", err)
		}
	}

	rows := pendingSince(t, srv, before)
	if len(rows) != 5 {
		t.Fatalf("pending rows: got %d, want 5", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].ID <= rows[i-1].ID {
			t.Fatalf("FetchOutbox is not in id order: %d came after %d", rows[i].ID, rows[i-1].ID)
		}
	}

	// The limit has to be honoured, or a burst would be pulled into memory all
	// at once.
	small, err := srv.FetchOutbox(ctx, 2)
	if err != nil {
		t.Fatalf("FetchOutbox(2) returned %v", err)
	}
	if len(small) != 2 {
		t.Fatalf("FetchOutbox(2) returned %d rows", len(small))
	}

	ids := []uint64{rows[0].ID, rows[1].ID, rows[2].ID}
	if err := srv.MarkOutboxPublished(ctx, ids); err != nil {
		t.Fatalf("MarkOutboxPublished() returned %v", err)
	}

	left := pendingSince(t, srv, before)
	if len(left) != 2 {
		t.Fatalf("after publishing 3 of 5, %d rows are still pending, want 2", len(left))
	}
	for _, row := range left {
		for _, id := range ids {
			if row.ID == id {
				t.Fatalf("row %d was marked published and came back anyway", id)
			}
		}
	}

	// An empty list must be a no-op and not "update every row".
	if err := srv.MarkOutboxPublished(ctx, nil); err != nil {
		t.Fatalf("MarkOutboxPublished(nil) returned %v", err)
	}
	if len(pendingSince(t, srv, before)) != 2 {
		t.Fatal("MarkOutboxPublished(nil) changed rows")
	}
}

// A failed publish must leave the row pending and say what went wrong. If it
// marked the row done, the message would be lost — which is the exact failure
// the outbox was built to remove.
func TestMarkOutboxFailedKeepsTheRow(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "outbox-fail-sender")
	conv, err := srv.CreateConversation(ctx, "fail room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)
	if _, _, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "m", nil); err != nil {
		t.Fatalf("CreateMessage() returned %v", err)
	}
	row := pendingSince(t, srv, before)[0]

	for i := range 3 {
		if err := srv.MarkOutboxFailed(ctx, row.ID, "broker is down"); err != nil {
			t.Fatalf("MarkOutboxFailed(%d) returned %v", i+1, err)
		}
	}

	after := pendingSince(t, srv, before)
	if len(after) != 1 {
		t.Fatalf("a failed row is no longer pending; got %d rows", len(after))
	}
	if after[0].Attempts != 3 {
		t.Fatalf("attempts: got %d, want 3", after[0].Attempts)
	}
	if after[0].LastError == nil || *after[0].LastError != "broker is down" {
		t.Fatalf("last_error: got %v, want %q", after[0].LastError, "broker is down")
	}
}

// Published rows are kept for a while and then deleted. What must never happen
// is the cleanup taking a row that has not gone out yet.
func TestCleanupOnlyTakesPublishedRows(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "outbox-clean-sender")
	conv, err := srv.CreateConversation(ctx, "clean room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)
	for range 3 {
		if _, _, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "m", nil); err != nil {
			t.Fatalf("CreateMessage() returned %v", err)
		}
	}
	rows := pendingSince(t, srv, before)
	if err := srv.MarkOutboxPublished(ctx, []uint64{rows[0].ID}); err != nil {
		t.Fatalf("MarkOutboxPublished() returned %v", err)
	}

	// A cutoff in the future, so everything old enough to go, goes.
	deleted, err := srv.DeleteOutboxPublishedBefore(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("DeleteOutboxPublishedBefore() returned %v", err)
	}
	if deleted < 1 {
		t.Fatalf("deleted %d rows, want at least the one published row", deleted)
	}

	left := pendingSince(t, srv, before)
	if len(left) != 2 {
		t.Fatalf("the cleanup took a row that had not been published; %d left, want 2", len(left))
	}
}

// The stats are what /metrics reports, and lag is the number that says whether
// the relay is alive. An empty queue must report no lag rather than a lag
// measured from the zero time, which would read as fifty-six years.
func TestOutboxStatsReportPendingAndAge(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "outbox-stats-sender")
	conv, err := srv.CreateConversation(ctx, "stats room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	// Drain whatever earlier tests left behind, so this one can talk about
	// absolute numbers.
	drainOutbox(t, srv)

	pending, oldest, err := srv.OutboxStats(ctx)
	if err != nil {
		t.Fatalf("OutboxStats() returned %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending on an empty queue: got %d, want 0", pending)
	}
	if !oldest.IsZero() {
		t.Fatalf("an empty queue reported an oldest row at %v; the lag gauge would read as decades", oldest)
	}

	for range 2 {
		if _, _, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "m", nil); err != nil {
			t.Fatalf("CreateMessage() returned %v", err)
		}
	}

	pending, oldest, err = srv.OutboxStats(ctx)
	if err != nil {
		t.Fatalf("OutboxStats() returned %v", err)
	}
	if pending != 2 {
		t.Fatalf("pending: got %d, want 2", pending)
	}
	if oldest.IsZero() {
		t.Fatal("two rows are waiting and the oldest is the zero time")
	}
	if age := time.Since(oldest); age < 0 || age > time.Minute {
		t.Fatalf("the oldest row is %v old, which cannot be right", age)
	}
}

// Only one node may be the relay. The lock is what guarantees it, and if it
// ever handed itself out twice, two relays would publish the same room's
// messages in whatever order they happened to interleave.
func TestOutboxLockIsExclusive(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	first, err := srv.TryOutboxLock(ctx)
	if err != nil {
		t.Fatalf("TryOutboxLock() returned %v", err)
	}
	if first == nil {
		t.Fatal("the first caller did not get the lock")
	}

	// A second ask, on a different connection, must be refused — and refused
	// without an error, because "somebody else is the relay" is the normal
	// answer on every node but one.
	second, err := srv.TryOutboxLock(ctx)
	if err != nil {
		t.Fatalf("TryOutboxLock() while held returned %v", err)
	}
	if second != nil {
		t.Fatal("two callers hold the relay lock at the same time")
	}

	if err := first.Release(ctx); err != nil {
		t.Fatalf("Release() returned %v", err)
	}

	// Released, so the next node can take over. This is the handover a crashed
	// leader gets for free, because the lock dies with its connection.
	third, err := srv.TryOutboxLock(ctx)
	if err != nil {
		t.Fatalf("TryOutboxLock() after release returned %v", err)
	}
	if third == nil {
		t.Fatal("the lock was released and nobody could take it")
	}
	if err := third.Release(ctx); err != nil {
		t.Fatalf("Release() returned %v", err)
	}

	// Releasing twice must not be an error: shutdown paths do not want to keep
	// track of whether they already did it.
	if err := third.Release(ctx); err != nil {
		t.Fatalf("Release() twice returned %v", err)
	}
}

// Twenty nodes starting at the same moment must produce exactly one relay. A
// race here is not theoretical: every node in a rolling deploy asks within the
// same second.
func TestOutboxLockUnderConcurrentStart(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	const nodes = 20

	var (
		mu    sync.Mutex
		won   []*OutboxLock
		start sync.WaitGroup
		done  sync.WaitGroup
	)
	start.Add(1)

	for range nodes {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait() // all twenty ask at once

			lock, err := srv.TryOutboxLock(ctx)
			if err != nil {
				t.Errorf("TryOutboxLock() returned %v", err)
				return
			}
			if lock == nil {
				return
			}
			mu.Lock()
			won = append(won, lock)
			mu.Unlock()
		}()
	}

	start.Done()
	done.Wait()

	if len(won) != 1 {
		t.Fatalf("%d of %d nodes became the relay, want exactly 1", len(won), nodes)
	}
	if err := won[0].Release(ctx); err != nil {
		t.Fatalf("Release() returned %v", err)
	}
}

// ---------------------------------------------------------------------------
// Helpers
//
// Every test in this package shares one container and one database, so a test
// cannot assume the outbox starts empty. These read "what is pending now" and
// "what appeared since then" instead of counting rows outright.
// ---------------------------------------------------------------------------

// mustPending returns the highest outbox id that already exists, so a test can
// ignore whatever earlier tests left behind.
func mustPending(t *testing.T, srv Service) uint64 {
	t.Helper()

	rows, err := srv.FetchOutbox(context.Background(), 10000)
	if err != nil {
		t.Fatalf("FetchOutbox() returned %v", err)
	}
	var highest uint64
	for _, row := range rows {
		if row.ID > highest {
			highest = row.ID
		}
	}
	return highest
}

// pendingSince returns the pending rows written after the given id, in id
// order.
func pendingSince(t *testing.T, srv Service, after uint64) []Outbox {
	t.Helper()

	rows, err := srv.FetchOutbox(context.Background(), 10000)
	if err != nil {
		t.Fatalf("FetchOutbox() returned %v", err)
	}
	out := []Outbox{}
	for _, row := range rows {
		if row.ID > after {
			out = append(out, row)
		}
	}
	return out
}

// drainOutbox marks everything pending as published, so a test that needs
// absolute numbers can start from an empty queue.
func drainOutbox(t *testing.T, srv Service) {
	t.Helper()
	ctx := context.Background()

	rows, err := srv.FetchOutbox(ctx, 10000)
	if err != nil {
		t.Fatalf("FetchOutbox() returned %v", err)
	}
	ids := make([]uint64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	if err := srv.MarkOutboxPublished(ctx, ids); err != nil {
		t.Fatalf("MarkOutboxPublished() returned %v", err)
	}
}

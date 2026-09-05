package database

import (
	"context"
	"sync"
	"testing"
)

// The promise seq makes is narrow and strict: inside one room the numbers run
// 1, 2, 3 with no holes and no repeats. Everything a reconnecting client does
// rests on it. If a number is ever skipped, that client sees a gap that is not
// there and asks for messages that do not exist; if one is ever handed out
// twice, one of the two messages is invisible to everybody.
//
// These tests are the proof.

func TestMessageSeqStartsAtOneAndIsPerRoom(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "seq-sender")

	roomA, err := srv.CreateConversation(ctx, "room a", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation(a) returned %v", err)
	}
	roomB, err := srv.CreateConversation(ctx, "room b", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation(b) returned %v", err)
	}

	for i, want := range []uint{1, 2, 3} {
		msg, created, err := srv.CreateMessage(ctx, roomA.ID, sender.ID, sender.Username, "hello", nil)
		if err != nil || !created {
			t.Fatalf("CreateMessage(#%d) = created %v, err %v", i, created, err)
		}
		if msg.Seq != want {
			t.Fatalf("message #%d in room A has seq %d, want %d", i, msg.Seq, want)
		}
	}

	// The counter belongs to the room, not to the database. A busy room next
	// door must not push room B's first message to seq 4 — that is exactly the
	// hole a client would try to fill and never could.
	msg, created, err := srv.CreateMessage(ctx, roomB.ID, sender.ID, sender.Username, "hello", nil)
	if err != nil || !created {
		t.Fatalf("CreateMessage(room B) = created %v, err %v", created, err)
	}
	if msg.Seq != 1 {
		t.Fatalf("first message in room B has seq %d, want 1", msg.Seq)
	}
}

// The reason the allocation sits inside the transaction, and the reason it is
// an UPDATE rather than a read followed by a write: twenty senders at once must
// still produce 1..20 exactly. Read-then-write would let two of them read the
// same value and both write the next one.
func TestMessageSeqUnderConcurrentSenders(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "seq-race-sender")
	conv, err := srv.CreateConversation(ctx, "busy room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	const senders = 20

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seqs []uint
		errs []error
	)
	for range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "at once", nil)

			mu.Lock()
			defer mu.Unlock()
			if err != nil || !created {
				errs = append(errs, err)
				return
			}
			seqs = append(seqs, msg.Seq)
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("%d of %d concurrent sends failed, first error: %v", len(errs), senders, errs[0])
	}

	// Every number from 1 to 20, each exactly once. Sorting is not needed —
	// the set is what matters, because the order goroutines finish in is not
	// something the database promises.
	seen := make(map[uint]bool, senders)
	for _, s := range seqs {
		if seen[s] {
			t.Fatalf("seq %d was handed out twice", s)
		}
		seen[s] = true
	}
	for want := uint(1); want <= senders; want++ {
		if !seen[want] {
			t.Fatalf("seq %d is missing — the sequence has a hole in it", want)
		}
	}
}

// A retried send must not burn a number. This is the quiet one: without the
// rollback, a client retrying five times over a flaky connection would leave
// four holes behind, and every other client in the room would sit forever
// asking for messages that were never written.
func TestMessageSeqNotBurnedByRetry(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "seq-retry-sender")
	conv, err := srv.CreateConversation(ctx, "retry room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	key := "retry-key"
	first, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "once", &key)
	if err != nil || !created {
		t.Fatalf("CreateMessage(first) = created %v, err %v", created, err)
	}
	if first.Seq != 1 {
		t.Fatalf("first message has seq %d, want 1", first.Seq)
	}

	// Three retries of the same key. Each one rolls its transaction back, so
	// each one gives its number back too.
	for i := range 3 {
		again, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "once", &key)
		if err != nil {
			t.Fatalf("CreateMessage(retry %d) returned %v", i, err)
		}
		if created {
			t.Fatalf("CreateMessage(retry %d) wrote a second message", i)
		}
		if again.Seq != first.Seq {
			t.Fatalf("retry %d came back with seq %d, want the original %d", i, again.Seq, first.Seq)
		}
	}

	// The proof: the next real message is 2, not 5.
	next, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "after the retries", nil)
	if err != nil || !created {
		t.Fatalf("CreateMessage(next) = created %v, err %v", created, err)
	}
	if next.Seq != 2 {
		t.Fatalf("the message after three retries has seq %d, want 2 — the retries burned %d numbers", next.Seq, next.Seq-2)
	}
}

func TestListMessagesAfterSeq(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "gap-sender")
	conv, err := srv.CreateConversation(ctx, "gap room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	for _, text := range []string{"m1", "m2", "m3", "m4", "m5"} {
		if _, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, text, nil); err != nil || !created {
			t.Fatalf("CreateMessage(%s) = created %v, err %v", text, created, err)
		}
	}

	// The client says "I have 2". It gets 3, 4, 5, in that order — oldest
	// first, the order they were sent in.
	got, err := srv.ListMessagesAfterSeq(ctx, conv.ID, 2, 50)
	if err != nil {
		t.Fatalf("ListMessagesAfterSeq() returned %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("gap after seq 2 has %d messages, want 3", len(got))
	}
	for i, want := range []uint{3, 4, 5} {
		if got[i].Seq != want {
			t.Fatalf("gap[%d] has seq %d, want %d — the gap must come back oldest first", i, got[i].Seq, want)
		}
	}
	if got[0].Content != "m3" || got[2].Content != "m5" {
		t.Fatalf("gap content = %q..%q, want m3..m5", got[0].Content, got[2].Content)
	}
	if got[0].Sender.Username != "gap-sender" {
		t.Fatalf("gap[0] sender = %q, want the sender preloaded", got[0].Sender.Username)
	}

	// A limit stops at the oldest end of the gap, so the next request can
	// carry on from there. Cutting from the newest end instead would leave a
	// hole the client could never notice.
	page, err := srv.ListMessagesAfterSeq(ctx, conv.ID, 0, 2)
	if err != nil {
		t.Fatalf("ListMessagesAfterSeq(limit) returned %v", err)
	}
	if len(page) != 2 || page[0].Seq != 1 || page[1].Seq != 2 {
		t.Fatalf("first page = %v, want seq 1 and 2", seqNumbers(page))
	}

	// A client that is already up to date gets nothing, and that is a normal
	// answer, not an error. It is also the common case.
	caughtUp, err := srv.ListMessagesAfterSeq(ctx, conv.ID, 5, 50)
	if err != nil {
		t.Fatalf("ListMessagesAfterSeq(caught up) returned %v", err)
	}
	if len(caughtUp) != 0 {
		t.Fatalf("a caught-up client got %d messages, want 0", len(caughtUp))
	}

	// A number from the future is the same story. It should not happen, but a
	// client with a stale local database must not crash the read.
	if ahead, err := srv.ListMessagesAfterSeq(ctx, conv.ID, 9999, 50); err != nil || len(ahead) != 0 {
		t.Fatalf("ListMessagesAfterSeq(ahead) = %d messages, %v; want 0 and no error", len(ahead), err)
	}
}

// 00004 has to number the messages that were already there, not only add the
// column. A backfill that leaves every old row at seq 0 would look fine until
// the first client asked for a gap and was handed the whole room.
func TestMigrateBackfillsMessageSeq(t *testing.T) {
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

	// Start from a clean slate: other tests leave rows behind, and this one
	// counts exact numbers.
	if err := s.db.WithContext(ctx).Exec(
		`TRUNCATE unread_counters, messages, conversation_members, conversations RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Back to the schema before sequence numbers existed.
	if _, err := provider.DownTo(ctx, 3); err != nil {
		t.Fatalf("DownTo(3) returned %v", err)
	}

	sender := mustUser(t, srv, "backfill-sender")

	// Two rooms, written the old way — no seq column to write to.
	var roomIDs []uint
	for _, title := range []string{"old room one", "old room two"} {
		var id uint
		if err := s.db.WithContext(ctx).Raw(
			`INSERT INTO conversations (created_at, updated_at, created_by_id, title)
			 VALUES (now(), now(), ?, ?) RETURNING id`, sender.ID, title).Scan(&id).Error; err != nil {
			t.Fatalf("insert old-shape conversation: %v", err)
		}
		roomIDs = append(roomIDs, id)
	}

	// Three messages in the first room, two in the second. They interleave, so
	// a backfill that numbered by global id instead of per room would produce
	// 1, 3, 5 here and be caught.
	for _, m := range []struct {
		room uint
		text string
	}{
		{roomIDs[0], "a1"}, {roomIDs[1], "b1"}, {roomIDs[0], "a2"},
		{roomIDs[1], "b2"}, {roomIDs[0], "a3"},
	} {
		if err := s.db.WithContext(ctx).Exec(
			`INSERT INTO messages (created_at, updated_at, conversation_id, sender_id, content)
			 VALUES (now(), now(), ?, ?, ?)`, m.room, sender.ID, m.text).Error; err != nil {
			t.Fatalf("insert old-shape message: %v", err)
		}
	}

	// Forward again. Now the numbering has to happen.
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("Up() returned %v", err)
	}

	for i, roomID := range roomIDs {
		want := []uint{3, 2}[i] // three messages in the first room, two in the second

		var rows []struct {
			Seq     uint
			Content string
		}
		if err := s.db.WithContext(ctx).Raw(
			`SELECT seq, content FROM messages WHERE conversation_id = ? ORDER BY seq`, roomID).
			Scan(&rows).Error; err != nil {
			t.Fatalf("read backfilled messages: %v", err)
		}
		if len(rows) != int(want) {
			t.Fatalf("room %d has %d messages, want %d", roomID, len(rows), want)
		}
		for j, row := range rows {
			if row.Seq != uint(j+1) {
				t.Errorf("room %d message %q has seq %d, want %d", roomID, row.Content, row.Seq, j+1)
			}
		}

		// The allocator has to start where the backfill stopped. If it stayed
		// at 0, the very next message would reuse seq 1 and the unique index
		// would refuse the send.
		var lastSeq uint
		if err := s.db.WithContext(ctx).Raw(
			`SELECT last_seq FROM conversations WHERE id = ?`, roomID).Scan(&lastSeq).Error; err != nil {
			t.Fatalf("read last_seq: %v", err)
		}
		if lastSeq != want {
			t.Errorf("room %d has last_seq %d, want %d", roomID, lastSeq, want)
		}

		// And prove it by sending one.
		next, created, err := srv.CreateMessage(ctx, roomID, sender.ID, sender.Username, "after the migration", nil)
		if err != nil || !created {
			t.Fatalf("CreateMessage() after backfill = created %v, err %v", created, err)
		}
		if next.Seq != want+1 {
			t.Errorf("the first message after the backfill has seq %d, want %d", next.Seq, want+1)
		}
	}

	// Leave a clean table for whatever runs next.
	if err := s.db.WithContext(ctx).Exec(
		`TRUNCATE unread_counters, messages, conversation_members, conversations RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("cleanup truncate: %v", err)
	}
}

func seqNumbers(msgs []Message) []uint {
	out := make([]uint, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Seq)
	}
	return out
}

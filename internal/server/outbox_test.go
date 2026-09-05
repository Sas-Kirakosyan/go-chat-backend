package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/database"
	"go-chat-backend/internal/event"
	"go-chat-backend/internal/outbox"
)

// ---------------------------------------------------------------------------
// fakeDB: the outbox and the unread counters
//
// These mirror the real SQL closely enough for the two rules that matter:
//
//   - FetchOutbox returns unpublished rows in id order, which is what keeps a
//     room's messages in the order they were written.
//   - ApplyUnread refuses a seq it has already applied, which is what makes
//     the consumer safe to run twice.
//
// Everything else is a map.
// ---------------------------------------------------------------------------

func (f *fakeDB) TryOutboxLock(context.Context) (*database.OutboxLock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lockHeld {
		return nil, nil
	}
	f.lockHeld = true
	// A zero-value lock, whose Release is a no-op. The real one carries a
	// database connection; there is nothing here to hold.
	return &database.OutboxLock{}, nil
}

func (f *fakeDB) FetchOutbox(_ context.Context, limit int) ([]database.Outbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := []database.Outbox{}
	for _, row := range f.outbox { // appended in id order
		if row.PublishedAt != nil {
			continue
		}
		out = append(out, row)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeDB) MarkOutboxPublished(_ context.Context, ids []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()
	for _, id := range ids {
		for i := range f.outbox {
			if f.outbox[i].ID == id {
				f.outbox[i].PublishedAt = &now
			}
		}
	}
	return nil
}

func (f *fakeDB) MarkOutboxFailed(_ context.Context, id uint64, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range f.outbox {
		if f.outbox[i].ID == id {
			f.outbox[i].Attempts++
			f.outbox[i].LastError = &reason
		}
	}
	return nil
}

func (f *fakeDB) DeleteOutboxPublishedBefore(_ context.Context, t time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	kept := make([]database.Outbox, 0, len(f.outbox))
	var deleted int64
	for _, row := range f.outbox {
		if row.PublishedAt != nil && row.PublishedAt.Before(t) {
			deleted++
			continue
		}
		kept = append(kept, row)
	}
	f.outbox = kept
	return deleted, nil
}

func (f *fakeDB) OutboxStats(context.Context) (int64, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var pending int64
	var oldest time.Time
	for _, row := range f.outbox {
		if row.PublishedAt != nil {
			continue
		}
		pending++
		if oldest.IsZero() || row.CreatedAt.Before(oldest) {
			oldest = row.CreatedAt
		}
	}
	return pending, oldest, nil
}

func (f *fakeDB) ApplyUnread(_ context.Context, messageID, conversationID, senderID, seq uint) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// The inbox row, which is the whole guard. It is per message and not per
	// counter, so it gives the same answer however out of order the messages
	// arrive.
	if f.consumed[messageID] {
		return 0, nil
	}
	f.consumed[messageID] = true

	if f.unread[conversationID] == nil {
		f.unread[conversationID] = map[uint]*database.UnreadCounter{}
	}

	var changed int64
	for _, userID := range f.memberIDs[conversationID] {
		if userID == senderID {
			continue
		}
		row := f.unread[conversationID][userID]
		if row == nil {
			row = &database.UnreadCounter{ConversationID: conversationID, UserID: userID}
			f.unread[conversationID][userID] = row
		}
		row.UnreadCount++
		row.LastSeq = max(row.LastSeq, seq)
		changed++
	}
	return changed, nil
}

func (f *fakeDB) DeleteConsumedBefore(context.Context, time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n := int64(len(f.consumed))
	clear(f.consumed)
	return n, nil
}

func (f *fakeDB) UnreadForUser(_ context.Context, userID uint) (map[uint]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := map[uint]int64{}
	for convID, rows := range f.unread {
		if row := rows[userID]; row != nil && row.UnreadCount > 0 {
			out[convID] = row.UnreadCount
		}
	}
	return out, nil
}

func (f *fakeDB) MarkConversationRead(_ context.Context, conversationID, userID uint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.unread[conversationID] == nil {
		f.unread[conversationID] = map[uint]*database.UnreadCounter{}
	}
	row := f.unread[conversationID][userID]
	if row == nil {
		row = &database.UnreadCounter{ConversationID: conversationID, UserID: userID}
		f.unread[conversationID][userID] = row
	}
	row.UnreadCount = 0
	return nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// The handler must write exactly one outbox row per stored message, and none
// at all for a retry. That is the Stage 5 contract at the HTTP edge: one
// write, and no way to queue the same message twice.
func TestSendWritesOneOutboxRow(t *testing.T) {
	_, r, db := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)

	path := fmt.Sprintf("/conversations/%d/messages", roomID)
	body := `{"content":"hello","client_msg_id":"same-key"}`

	if rr := do(t, r, "POST", path, body, alice); rr.Code != http.StatusCreated {
		t.Fatalf("first send: got %d (body %s)", rr.Code, rr.Body)
	}
	// The same key again. The real store rolls the whole transaction back, so
	// there is no second message row and no second outbox row.
	if rr := do(t, r, "POST", path, body, alice); rr.Code != http.StatusOK {
		t.Fatalf("retry: got %d (body %s)", rr.Code, rr.Body)
	}

	rows, err := db.FetchOutbox(t.Context(), 100)
	if err != nil {
		t.Fatalf("FetchOutbox: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("outbox rows: got %d, want 1 — a retry must not queue a second delivery", len(rows))
	}
	if rows[0].Topic != event.TopicMessageCreated {
		t.Fatalf("topic: got %q, want %q", rows[0].Topic, event.TopicMessageCreated)
	}

	// The payload must carry the sender's name. It is copied at write time so
	// that nothing on the delivery path has to go back to the database, and a
	// missing name would reach every client as an empty username.
	ev, err := event.DecodeMessageCreated([]byte(rows[0].Payload))
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if ev.SenderName != "alice" {
		t.Fatalf("sender name: got %q, want %q", ev.SenderName, "alice")
	}
	if ev.Seq != 1 || ev.ConversationID != roomID || ev.Content != "hello" {
		t.Fatalf("the event does not describe the message: %+v", ev)
	}
}

// The handler must not deliver anything itself any more. If it did, a member
// on this node would get every message twice: once from the handler and once
// from the relay.
func TestSendDoesNotDeliverByItself(t *testing.T) {
	_, r, db := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)

	sendN(t, r, alice, roomID, 1)

	rows, _ := db.FetchOutbox(t.Context(), 10)
	if len(rows) != 1 || rows[0].PublishedAt != nil {
		t.Fatalf("the send path published on its own; outbox: %+v", rows)
	}
}

// The relay is what turns a row into a delivery. This runs the real relay over
// the fake store with a local publisher, which is exactly how server.New wires
// a single node.
func TestRelayDrainsInOrder(t *testing.T) {
	s, r, db := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)

	sendN(t, r, alice, roomID, 3)

	var got []event.MessageCreated
	pub := outbox.NewLocalPublisher(
		func(ev event.MessageCreated, _ []uint) { got = append(got, ev) },
		s.applyUnread,
	)

	batch, err := outbox.New(db, pub, quietLogger()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if batch.Published != 3 || len(got) != 3 {
		t.Fatalf("drained %d rows and delivered %d events, want 3 and 3", batch.Published, len(got))
	}

	// Order is the point of having a single relay. Out of order here means a
	// client would see line 3 before line 2.
	for i, ev := range got {
		if want := fmt.Sprintf("m%d", i+1); ev.Content != want {
			t.Fatalf("event %d: got %q, want %q — the relay reordered the room", i, ev.Content, want)
		}
		if ev.Seq != uint(i+1) {
			t.Fatalf("event %d has seq %d, want %d", i, ev.Seq, i+1)
		}
	}

	if rows, _ := db.FetchOutbox(t.Context(), 10); len(rows) != 0 {
		t.Fatalf("%d rows still pending after a full drain", len(rows))
	}
}

// A publish that fails must leave the row where it is, so the next poll tries
// again. The row staying is what turns a broker outage into a delay instead of
// a loss, and it is the reason the send path can answer 201 with NATS down.
func TestRelayKeepsRowsWhenPublishFails(t *testing.T) {
	_, r, db := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)

	sendN(t, r, alice, roomID, 2)

	broken := publisherFunc(func(context.Context, uint64, event.MessageCreated, []uint) error {
		return errors.New("broker is down")
	})

	batch, err := outbox.New(db, broken, quietLogger()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if batch.Published != 0 {
		t.Fatalf("the relay reported %d published while the broker was down", batch.Published)
	}
	// Rows were read and none went out, which is what tells the loop to back
	// off for five seconds instead of retrying ten times a second.
	if batch.Fetched != 2 {
		t.Fatalf("Drain() fetched %d rows, want 2 — the loop cannot see that it is stuck", batch.Fetched)
	}

	rows, _ := db.FetchOutbox(t.Context(), 10)
	if len(rows) != 2 {
		t.Fatalf("rows still pending: got %d, want 2 — a failed publish must not lose a message", len(rows))
	}
	if rows[0].Attempts != 1 || rows[0].LastError == nil {
		t.Fatalf("the failure was not recorded on the row: %+v", rows[0])
	}
	// The second row was never tried. The relay stops at the first failure so
	// that a later message cannot overtake an earlier one on the retry.
	if rows[1].Attempts != 0 {
		t.Fatalf("the relay carried on past a failure; row 2 has %d attempts", rows[1].Attempts)
	}
}

// Only one node may drain at a time. Two relays on the same table would
// publish one room's messages out of order.
func TestOnlyOneRelayHoldsTheLock(t *testing.T) {
	_, _, db := newTestServer(t)

	first, err := db.TryOutboxLock(t.Context())
	if err != nil || first == nil {
		t.Fatalf("first lock: got %v, %v; want a lock", first, err)
	}

	second, err := db.TryOutboxLock(t.Context())
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if second != nil {
		t.Fatal("two nodes hold the relay lock at the same time")
	}
}

// The consumer's work must be safe to run twice, because at-least-once
// delivery means it will be.
func TestApplyUnreadIsIdempotent(t *testing.T) {
	s, r, db := newTestServer(t)
	alice, aliceID := signUp(t, r, "alice")
	_, bobID := signUp(t, r, "bob")
	roomID := newRoom(t, r, alice, bobID)

	ev := event.MessageCreated{MessageID: 1, ConversationID: roomID, SenderID: aliceID, Seq: 1}
	for i := range 3 {
		if err := s.applyUnread(t.Context(), ev); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	unread, err := db.UnreadForUser(t.Context(), bobID)
	if err != nil {
		t.Fatalf("UnreadForUser: %v", err)
	}
	if unread[roomID] != 1 {
		t.Fatalf("unread after three deliveries of one message: got %d, want 1", unread[roomID])
	}

	// The sender never counts their own message.
	own, _ := db.UnreadForUser(t.Context(), aliceID)
	if own[roomID] != 0 {
		t.Fatalf("the sender has %d unread for their own message", own[roomID])
	}
}

// The badge is on the conversation list, and marking a room read clears it.
func TestUnreadCountOnTheConversationList(t *testing.T) {
	s, r, _ := newTestServer(t)
	alice, aliceID := signUp(t, r, "alice")
	bob, bobID := signUp(t, r, "bob")
	roomID := newRoom(t, r, alice, bobID)

	// Applied newest first, on purpose. Two nodes share one consumer and
	// finish in whatever order they finish in, so a badge that only counts
	// messages arriving in order is a badge that is quietly wrong.
	for seq := uint(2); seq >= 1; seq-- {
		ev := event.MessageCreated{MessageID: seq, ConversationID: roomID, SenderID: aliceID, Seq: seq}
		if err := s.applyUnread(t.Context(), ev); err != nil {
			t.Fatalf("apply seq %d: %v", seq, err)
		}
	}

	if got := unreadOnList(t, r, bob, roomID); got != 2 {
		t.Fatalf("unread_count: got %d, want 2", got)
	}
	// Alice sent them, so hers was never anything but zero.
	if got := unreadOnList(t, r, alice, roomID); got != 0 {
		t.Fatalf("the sender's unread_count is %d, want 0", got)
	}

	path := fmt.Sprintf("/conversations/%d/read", roomID)
	if rr := do(t, r, "POST", path, "", bob); rr.Code != http.StatusOK {
		t.Fatalf("mark read: got %d (body %s)", rr.Code, rr.Body)
	}

	if got := unreadOnList(t, r, bob, roomID); got != 0 {
		t.Fatalf("unread_count after marking read: got %d, want 0", got)
	}
}

// Only a member may mark a room read, and an outsider must not learn from the
// status code whether the room exists.
func TestMarkReadNeedsMembership(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	mallory, _ := signUp(t, r, "mallory")
	roomID := newRoom(t, r, alice)

	path := fmt.Sprintf("/conversations/%d/read", roomID)
	if rr := do(t, r, "POST", path, "", mallory); rr.Code != http.StatusNotFound {
		t.Fatalf("outsider marking a room read: got %d, want %d", rr.Code, http.StatusNotFound)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// publisherFunc lets a test be a Publisher without declaring a struct.
type publisherFunc func(ctx context.Context, outboxID uint64, ev event.MessageCreated, userIDs []uint) error

func (f publisherFunc) Publish(ctx context.Context, id uint64, ev event.MessageCreated, userIDs []uint) error {
	return f(ctx, id, ev, userIDs)
}

// quietLogger keeps the relay's own lines out of the test output. What is
// being checked is what it did, not what it said about it.
func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// unreadOnList reads one room's badge off GET /conversations, which is where a
// client actually sees it.
func unreadOnList(t *testing.T, r *gin.Engine, authHeader string, roomID uint) int64 {
	t.Helper()

	rr := do(t, r, "GET", "/conversations", "", authHeader)
	if rr.Code != http.StatusOK {
		t.Fatalf("list conversations: got %d (body %s)", rr.Code, rr.Body)
	}

	var body struct {
		Conversations []conversationDTO `json:"conversations"`
	}
	decode(t, rr.Body.Bytes(), &body)

	for _, c := range body.Conversations {
		if c.ID == roomID {
			return c.UnreadCount
		}
	}
	t.Fatalf("room %d is not in the list", roomID)
	return 0
}

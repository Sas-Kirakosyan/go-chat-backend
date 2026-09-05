package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go-chat-backend/internal/event"
)

// These tests need no NATS. What they check is the part that would be wrong on
// every machine equally: the subject a message is published on, the id that
// makes a duplicate harmless, and the shape of what travels between nodes.
// Anything that needs a real server is checked with cmd/outboxcheck.
//
// A nil broker is a supported mode — single node, no NATS_URL — so the
// counters are checked on one too. A metric that panicked when NATS was absent
// would take out the whole /metrics page.

func TestMessageSubject(t *testing.T) {
	if got, want := messageSubject(7), "chat.message.7"; got != want {
		t.Fatalf("subject: got %q, want %q", got, want)
	}

	// The consumers filter on chat.message.> and would silently receive
	// nothing if the prefix and the filter ever drifted apart.
	if got := messageSubject(7); got[:len(messageSubjectPrefix)] != messageSubjectPrefix {
		t.Fatalf("subject %q is not under the filtered prefix %q", got, messageSubjectPrefix)
	}
}

// The message id is what JetStream dedupes on. Two different outbox rows must
// never produce the same one, or the second message would be silently dropped
// as a duplicate of the first.
func TestMsgIDIsUniquePerRow(t *testing.T) {
	if got, want := msgID(42), "outbox-42"; got != want {
		t.Fatalf("msg id: got %q, want %q", got, want)
	}
	if msgID(1) == msgID(11) {
		t.Fatal("two different outbox rows produced the same message id")
	}
}

// The envelope carries the event and the member list together. If a field
// stopped surviving the round trip, the message would still arrive and would
// reach nobody, or would arrive with an empty sender name.
func TestEnvelopeRoundTrip(t *testing.T) {
	clientID := "abc-123"
	sent := envelope{
		Event: event.MessageCreated{
			MessageID:      7,
			ConversationID: 3,
			SenderID:       1,
			SenderName:     "alice",
			Seq:            9,
			Content:        "hello",
			ClientMsgID:    &clientID,
			CreatedAt:      time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC),
		},
		UserIDs: []uint{1, 2},
		From:    "api1",
	}

	raw, err := json.Marshal(sent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var back envelope
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if back.Event.MessageID != 7 || back.Event.Seq != 9 {
		t.Fatalf("ids changed: %+v", back.Event)
	}
	if back.Event.SenderName != "alice" {
		t.Fatalf("sender name changed: %q", back.Event.SenderName)
	}
	if back.Event.ClientMsgID == nil || *back.Event.ClientMsgID != clientID {
		t.Fatalf("client_msg_id changed: %v", back.Event.ClientMsgID)
	}
	if !back.Event.CreatedAt.Equal(sent.Event.CreatedAt) {
		t.Fatalf("created_at changed: %v", back.Event.CreatedAt)
	}
	if len(back.UserIDs) != 2 || back.UserIDs[0] != 1 || back.UserIDs[1] != 2 {
		t.Fatalf("user ids changed: %v", back.UserIDs)
	}
	if back.From != "api1" {
		t.Fatalf("from: got %q, want %q", back.From, "api1")
	}
}

// A message with no client_msg_id must not put a null in the envelope: the
// field is omitempty so that the common case stays small on the wire.
func TestEnvelopeOmitsEmptyClientMsgID(t *testing.T) {
	raw, err := json.Marshal(envelope{Event: event.MessageCreated{MessageID: 1}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); strings.Contains(got, "client_msg_id") {
		t.Fatalf("empty client_msg_id was encoded: %s", got)
	}
}

// Single-node mode has no broker at all. Every counter must answer on nil,
// because metrics.RegisterBroker reads them on a scrape and a panic there
// takes out the whole /metrics page.
func TestNilBrokerIsSafe(t *testing.T) {
	var b *Broker

	if b.Published() != 0 || b.PublishFailed() != 0 || b.FanoutReceived() != 0 {
		t.Fatal("nil broker reported non-zero publish counters")
	}
	if b.UnreadHandled() != 0 || b.Redelivered() != 0 || b.DeadLettered() != 0 {
		t.Fatal("nil broker reported non-zero consumer counters")
	}
	if b.Consuming() != 0 {
		t.Fatal("nil broker claims to be consuming")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("closing a nil broker: %v", err)
	}
	if err := b.Ping(t.Context()); err == nil {
		t.Fatal("nil broker answered a ping")
	}

	// These block on ctx.Done for a real broker, and must return at once for a
	// nil one, or App.Shutdown would wait forever in single-node mode.
	b.SubscribeFanout(t.Context(), nil)
	b.ConsumeUnread(t.Context(), nil)
}

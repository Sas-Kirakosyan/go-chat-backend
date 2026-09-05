// Package event holds the shapes that travel through the outbox and the
// broker.
//
// It exists so that four packages can agree on one struct without any of them
// importing another: the store writes it into the outbox row, the relay reads
// it back, the broker carries it between nodes, and the server turns it into a
// WebSocket frame. If this lived in any of those four, one of them would have
// to import a package that already imports it.
//
// It imports nothing but the standard library, on purpose. That is what keeps
// it free of cycles.
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// TopicMessageCreated is the only topic today. It is stored in the outbox row
// so that a later stage can add a second kind of event without a second table
// and without guessing what an old row meant.
const TopicMessageCreated = "message.created"

// MessageCreated says that one message was committed to the database.
//
// It carries everything a receiver needs, including the sender's username, so
// that nothing on the delivery path has to go back to Postgres for a name. The
// name is copied at write time and never refreshed: a message shows the name
// its sender had when they sent it, which is also what a chat log should do.
//
// It does NOT carry the member list. Who is in the room is looked up when the
// relay publishes, so a user added to the room after the commit still gets the
// message. See the relay in internal/outbox.
type MessageCreated struct {
	MessageID      uint      `json:"message_id"`
	ConversationID uint      `json:"conversation_id"`
	SenderID       uint      `json:"sender_id"`
	SenderName     string    `json:"sender_name"`
	Seq            uint      `json:"seq"`
	Content        string    `json:"content"`
	ClientMsgID    *string   `json:"client_msg_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// Encode turns the event into the JSON stored in outbox.payload.
func (m MessageCreated) Encode() (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode message event: %w", err)
	}
	return string(b), nil
}

// DecodeMessageCreated reads a payload back.
//
// A payload that cannot be decoded is a poison message: retrying it will fail
// the same way forever, so callers must send it to the dead-letter queue
// instead of asking for it again.
func DecodeMessageCreated(payload []byte) (MessageCreated, error) {
	var m MessageCreated
	if err := json.Unmarshal(payload, &m); err != nil {
		return MessageCreated{}, fmt.Errorf("decode message event: %w", err)
	}
	return m, nil
}

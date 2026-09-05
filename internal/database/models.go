package database

import (
	"time"

	"gorm.io/gorm"
)

// The SQL files in migrations/ own the schema. These structs only describe how
// GORM reads and writes rows, so they carry no size, index or NOT NULL tags:
// those would build nothing and would only mislead the next reader. What stays
// is what GORM needs at run time — the primary key and the association keys.
//
// gorm.Model embeds ID, CreatedAt, UpdatedAt and DeletedAt. DeletedAt makes a
// model soft-deleted: GORM stamps the column instead of removing the row, and
// adds "WHERE deleted_at IS NULL" to every query. Note that a soft-deleted
// user still occupies its username, because the unique index on username does
// not know about soft deletes.

// User is a registered account.
type User struct {
	gorm.Model
	Username     string
	PasswordHash string

	// Memberships is a has-many association. It is only populated when you
	// ask for it, with .Preload("Memberships").
	Memberships []ConversationMember
}

// RefreshToken is one login session.
//
// It does not embed gorm.Model: a soft-deleted session is a contradiction, and
// the unique index on token_hash does not know about soft deletes anyway.
// Ending a session sets RevokedAt instead.
type RefreshToken struct {
	ID     uint `gorm:"primarykey"`
	UserID uint

	// TokenHash is the SHA-256 of the token that was handed to the client.
	// The token itself is never stored anywhere on the server.
	TokenHash []byte

	ExpiresAt time.Time
	CreatedAt time.Time

	// RevokedAt is nil while the session is live, and set when the user logs
	// out. A session is usable only when it is neither revoked nor expired.
	RevokedAt *time.Time

	// User is a belongs-to association, populated with .Preload("User").
	User User `gorm:"foreignKey:UserID"`
}

// Conversation is one chat room. The room itself owns no user: who is inside
// lives in ConversationMember, so a room can hold two people or twenty.
// CreatedByID is kept for audit only — it grants no extra rights.
type Conversation struct {
	gorm.Model
	Title       string
	CreatedByID uint

	// LastSeq is the highest sequence number handed out in this room, and the
	// allocator for the next one. CreateMessage bumps it and writes the message
	// in one transaction, so the row lock on this row is what stops two senders
	// from getting the same number.
	LastSeq uint

	Members  []ConversationMember
	Messages []Message
}

// ConversationMember says that one user is inside one room. It is the join
// table between User and Conversation.
//
// It does not embed gorm.Model on purpose. With a soft-delete column, removing
// a member and adding them back would collide with the unique index on
// (conversation_id, user_id), because the old row is still physically there.
type ConversationMember struct {
	ID             uint `gorm:"primarykey"`
	ConversationID uint
	UserID         uint
	CreatedAt      time.Time

	// User is a belongs-to association, populated with .Preload("User").
	User User `gorm:"foreignKey:UserID"`
}

// Message is a single message written by one member of a room.
type Message struct {
	gorm.Model
	ConversationID uint
	SenderID       uint

	// ClientMsgID is the id the sender's own client invented for this message.
	// It is optional. When it is set, sending it twice returns the first
	// message instead of writing a second one, so a retry after a network
	// timeout cannot double-post.
	//
	// It is a pointer so that "no key" stores SQL NULL, and Postgres allows
	// any number of NULLs inside a unique index. A plain string would store ""
	// for every keyless message, and the second one would be rejected.
	ClientMsgID *string

	// Seq is this message's place in its own room: 1, 2, 3, with no holes. It
	// is what lets a client that reconnects say "I have 42" and be handed
	// exactly what came after.
	//
	// The global ID cannot do that job. It counts every room at once, so a jump
	// in it proves nothing about this room.
	Seq uint

	Content string

	// Sender is a belongs-to association, populated with .Preload("Sender").
	Sender User `gorm:"foreignKey:SenderID"`
}

// Outbox is one event waiting to reach the broker.
//
// The row is written in the same transaction as the message it describes, so
// the two commit together. Before this table the handler wrote the message and
// then published, and a crash between the two lost the publish for good.
//
// It does not embed gorm.Model: a soft-deleted outbox row is a contradiction.
// The relay stamps PublishedAt, and old rows are deleted for real.
type Outbox struct {
	ID uint64 `gorm:"primarykey"`

	// Topic is the kind of event. Today always OutboxTopicMessageCreated.
	Topic string

	// Payload is the event as JSON text. The column is jsonb, so Postgres
	// checks that it really is JSON and you can query inside it while
	// debugging.
	Payload string

	CreatedAt time.Time

	// PublishedAt is nil while the row is still waiting. The relay asks for
	// exactly these rows, which is why the index on them is partial.
	PublishedAt *time.Time

	// Attempts and LastError are for a human reading the table, not for the
	// code. They say how hard the relay has tried and what went wrong last.
	Attempts  int
	LastError *string
}

// TableName pins the table to "outbox".
//
// GORM names tables by pluralising the struct, which turns Outbox into
// "outboxes" — a word that is technically correct and that nobody would ever
// type at a psql prompt. The migration creates "outbox", so this is what makes
// the two agree.
//
// It is the one place a struct in this file overrides the schema, and it is
// only a name. The columns still come from the SQL.
func (Outbox) TableName() string { return "outbox" }

// ConsumedMessage says that one consumer has already handled one message.
//
// It is the inbox, and it is the mirror of Outbox above: the outbox stops a
// message being published zero times, and this stops it being applied twice.
// Delivery is at-least-once, so both halves are needed.
//
// The row is written in the same transaction as the work it guards, so a
// consumer that crashes half way leaves neither behind and gets the message
// again.
type ConsumedMessage struct {
	Consumer   string `gorm:"primarykey"`
	MessageID  uint   `gorm:"primarykey"`
	ConsumedAt time.Time
}

// UnreadCounter is how many messages one user has not read in one room.
//
// It is maintained by the broker consumer, not by the send handler, so the
// sender's request does not pay for it.
//
// It does not embed gorm.Model: the primary key is (ConversationID, UserID),
// and a soft-deleted counter would still be counted by the unique key.
type UnreadCounter struct {
	ConversationID uint `gorm:"primarykey"`
	UserID         uint `gorm:"primarykey"`

	UnreadCount int64

	// LastSeq is the highest message seq counted into this row. Nothing
	// depends on it — ConsumedMessage is the guard — and it is kept because it
	// is the first thing a person wants when a badge looks wrong.
	LastSeq uint

	UpdatedAt time.Time
}

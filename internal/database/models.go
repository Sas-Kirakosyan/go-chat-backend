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

// Conversation is one chat room. The room itself owns no user: who is inside
// lives in ConversationMember, so a room can hold two people or twenty.
// CreatedByID is kept for audit only — it grants no extra rights.
type Conversation struct {
	gorm.Model
	Title       string
	CreatedByID uint

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

	Content string

	// Sender is a belongs-to association, populated with .Preload("Sender").
	Sender User `gorm:"foreignKey:SenderID"`
}

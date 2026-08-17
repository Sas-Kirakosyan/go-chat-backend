package database

import "gorm.io/gorm"

// gorm.Model embeds ID, CreatedAt, UpdatedAt and DeletedAt. DeletedAt makes
// every model soft-deleted by default: GORM stamps the column instead of
// removing the row, and adds "WHERE deleted_at IS NULL" to every query.
// Note that a soft-deleted user still occupies its username, because the
// unique index below does not know about soft deletes.

// User is a registered account.
type User struct {
	gorm.Model
	Username     string `gorm:"size:64;uniqueIndex;not null"`
	PasswordHash string `gorm:"not null"`

	// Conversations is a has-many association. It is only populated when you
	// ask for it, with .Preload("Conversations").
	Conversations []Conversation
}

// Conversation is one chat thread belonging to a user.
type Conversation struct {
	gorm.Model
	UserID uint   `gorm:"index;not null"`
	Title  string `gorm:"size:200"`

	Messages []Message
}

// Message is a single turn in a conversation.
type Message struct {
	gorm.Model
	ConversationID uint   `gorm:"index;not null"`
	Role           string `gorm:"size:16;not null"` // "user" or "model"
	Content        string `gorm:"type:text;not null"`
}

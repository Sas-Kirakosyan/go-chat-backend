package database

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

//this file content (database) talk to Postgres

// CreateConversation makes a room and puts the creator inside it, together
// with everyone in memberIDs. It returns ErrUserNotFound if any of those ids
// has no user, and writes nothing in that case.
func (s *service) CreateConversation(ctx context.Context, title string, creatorID uint, memberIDs []uint) (*Conversation, error) {
	// The creator is always a member, and a repeated id must not become two
	// rows and trip the unique index.
	ids := []uint{creatorID}
	seen := map[uint]bool{creatorID: true}
	for _, id := range memberIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}

	conv := &Conversation{Title: title, CreatedByID: creatorID}

	// One transaction, so a bad member id leaves no half-built room behind.
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var found int64
		if err := tx.Model(&User{}).Where("id IN ?", ids).Count(&found).Error; err != nil {
			return fmt.Errorf("count members: %w", err)
		}
		if int(found) != len(ids) {
			return ErrUserNotFound
		}

		if err := tx.Create(conv).Error; err != nil {
			return fmt.Errorf("insert conversation: %w", err)
		}

		members := make([]ConversationMember, 0, len(ids))
		for _, id := range ids {
			members = append(members, ConversationMember{ConversationID: conv.ID, UserID: id})
		}
		if err := tx.Create(&members).Error; err != nil {
			return fmt.Errorf("insert members: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Read the room back so the caller gets usernames, not bare ids.
	if err := s.db.WithContext(ctx).Preload("Members.User").First(conv, conv.ID).Error; err != nil {
		return nil, fmt.Errorf("reload conversation: %w", err)
	}
	return conv, nil
}

// ListConversationsForUser returns every room the user is a member of, newest
// first, with the member list loaded.
func (s *service) ListConversationsForUser(ctx context.Context, userID uint) ([]Conversation, error) {
	var convs []Conversation

	err := s.db.WithContext(ctx).
		Joins("JOIN conversation_members ON conversation_members.conversation_id = conversations.id AND conversation_members.user_id = ?", userID).
		Preload("Members.User").
		Order("conversations.id DESC").
		Find(&convs).Error
	if err != nil {
		return nil, fmt.Errorf("select conversations: %w", err)
	}
	return convs, nil
}

// EnsureMember returns nil when the user is inside the room. It returns
// ErrConversationNotFound both when the room is missing and when the user is
// not in it.
//
// It counts rows instead of loading the room, because it runs on every send
// and every history read and the callers only need the yes or no. The lookup
// hits the unique index on (conversation_id, user_id) directly, so it stays
// one small query.
func (s *service) EnsureMember(ctx context.Context, conversationID, userID uint) error {
	var count int64

	err := s.db.WithContext(ctx).
		Model(&ConversationMember{}).
		// The join skips rooms that were soft-deleted: the membership row
		// survives a deleted room, and it must not keep granting access.
		Joins("JOIN conversations ON conversations.id = conversation_members.conversation_id AND conversations.deleted_at IS NULL").
		Where("conversation_members.conversation_id = ? AND conversation_members.user_id = ?", conversationID, userID).
		Count(&count).Error
	if err != nil {
		return fmt.Errorf("count membership: %w", err)
	}
	if count == 0 {
		return ErrConversationNotFound
	}
	return nil
}

// AddMember puts a user in a room. It returns ErrAlreadyMember if the user is
// already there.
func (s *service) AddMember(ctx context.Context, conversationID, userID uint) (*ConversationMember, error) {
	member := &ConversationMember{ConversationID: conversationID, UserID: userID}

	err := s.db.WithContext(ctx).Create(member).Error
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return nil, ErrAlreadyMember
	}
	if err != nil {
		return nil, fmt.Errorf("insert member: %w", err)
	}

	if err := s.db.WithContext(ctx).Preload("User").First(member, member.ID).Error; err != nil {
		return nil, fmt.Errorf("reload member: %w", err)
	}
	return member, nil
}

// ListConversationMemberIDs returns the ids of everyone in a room.
//
// It runs on the send path, once per message, so it reads as little as
// possible: Pluck asks Postgres for one column and skips building user
// structs. The lookup uses the same (conversation_id, user_id) index that
// EnsureMember hits.
//
// A room that does not exist is not an error here. It answers an empty list,
// and the caller has already checked membership with EnsureMember anyway.
func (s *service) ListConversationMemberIDs(ctx context.Context, conversationID uint) ([]uint, error) {
	var ids []uint

	err := s.db.WithContext(ctx).
		Model(&ConversationMember{}).
		Where("conversation_id = ?", conversationID).
		Pluck("user_id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("select member ids: %w", err)
	}
	return ids, nil
}

// CreateMessage stores one message. When clientMsgID is not nil and this
// sender already used it in this room, nothing is written: the first message
// comes back with created set to false.
func (s *service) CreateMessage(ctx context.Context, conversationID, senderID uint, content string, clientMsgID *string) (*Message, bool, error) {
	msg := &Message{
		ConversationID: conversationID,
		SenderID:       senderID,
		Content:        content,
		ClientMsgID:    clientMsgID,
	}

	err := s.db.WithContext(ctx).Create(msg).Error
	if errors.Is(err, gorm.ErrDuplicatedKey) && clientMsgID != nil {
		// The unique index rejected the insert, so this sender already used
		// this key in this room. That is a retry, not an error: hand back the
		// row that got there first. The index covers sender_id, so the row is
		// always this sender's own message.
		var existing Message
		err := s.db.WithContext(ctx).
			Where("conversation_id = ? AND sender_id = ? AND client_msg_id = ?", conversationID, senderID, *clientMsgID).
			First(&existing).Error
		if err != nil {
			return nil, false, fmt.Errorf("load duplicate message: %w", err)
		}
		return &existing, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("insert message: %w", err)
	}
	return msg, true, nil
}

// ListMessages returns up to limit messages from a room, newest first, with
// the sender loaded. A beforeID above zero returns only messages older than
// that id.
//
// Paging on the id instead of an offset keeps the pages stable: OFFSET has to
// count and throw away every earlier row, and a message written between two
// requests shifts everything, so the reader sees a line twice or not at all.
func (s *service) ListMessages(ctx context.Context, conversationID, beforeID uint, limit int) ([]Message, error) {
	q := s.db.WithContext(ctx).
		Where("conversation_id = ?", conversationID).
		Preload("Sender").
		Order("id DESC").
		Limit(limit)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}

	var msgs []Message
	if err := q.Find(&msgs).Error; err != nil {
		return nil, fmt.Errorf("select messages: %w", err)
	}
	return msgs, nil
}

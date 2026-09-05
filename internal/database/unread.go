package database

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// UnreadConsumer is the name this work is recorded under in consumed_messages.
// A second consumer would use a different one and would then be free to handle
// the same messages without either of them skipping the other's.
const UnreadConsumer = "unread"

// ApplyUnread adds one to the unread count of every member of a room except
// the sender, and does nothing at all if this message was already counted.
//
// It returns how many counter rows really changed, so the caller can tell "I
// did the work" apart from "this was a duplicate". That is the only way
// redelivery becomes visible on a graph.
//
// # How it is made safe to run twice
//
// Delivery is at-least-once, so this WILL be called twice for the same
// message. The guard is a row in consumed_messages, written in the same
// transaction as the counters. If the insert conflicts, this message has been
// handled and the transaction ends without touching a counter.
//
// The guard is per message, not per counter, and that is the whole lesson. A
// high water mark on the counter row — "only apply a seq higher than the last
// one" — is smaller and is wrong: two nodes share one consumer, so they finish
// messages in whatever order they finish in, and a message that commits after
// a higher one is not a duplicate, it is late. A two-node run lost two
// messages out of fourteen that way. "Have I seen this message" has the same
// answer no matter when it is asked; "is this newer than the last one" does
// not.
//
// The two statements are one transaction on purpose. A crash between them
// would otherwise either count without recording, or record without counting,
// and one of those two is silent and permanent.
//
// The member list is not a parameter. The statement reads
// conversation_members itself, so the room's membership is read at the same
// instant as the counters are written, and a member added a moment ago is
// counted from their first message.
func (s *service) ApplyUnread(ctx context.Context, messageID, conversationID, senderID, seq uint) (int64, error) {
	var changed int64

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The guard. ON CONFLICT DO NOTHING makes this "claim this message, if
		// nobody else has": exactly one caller gets a row, and the rest see
		// zero rows affected.
		claim := tx.Exec(`
			INSERT INTO consumed_messages (consumer, message_id, consumed_at)
			VALUES (?, ?, now())
			ON CONFLICT (consumer, message_id) DO NOTHING`,
			UnreadConsumer, messageID)
		if claim.Error != nil {
			return fmt.Errorf("claim message: %w", claim.Error)
		}
		if claim.RowsAffected == 0 {
			// Already handled. This is the redelivery path, and doing nothing
			// is the correct and complete response to it.
			return nil
		}

		// One statement for the whole room. A fifty-person room is one round
		// trip, not fifty. The SELECT hits the (conversation_id, user_id)
		// index that EnsureMember already uses.
		//
		// There is no condition on the update now: reaching this line already
		// means the message is new, so every member's count moves.
		//
		// last_seq uses GREATEST so that a message which arrives late cannot
		// drag the marker backwards. Nothing depends on it; it is there to be
		// read by a person.
		res := tx.Exec(`
			INSERT INTO unread_counters (conversation_id, user_id, unread_count, last_seq, updated_at)
			SELECT cm.conversation_id, cm.user_id, 1, ?, now()
			  FROM conversation_members cm
			 WHERE cm.conversation_id = ?
			   AND cm.user_id <> ?
			ON CONFLICT (conversation_id, user_id) DO UPDATE
			   SET unread_count = unread_counters.unread_count + 1,
			       last_seq     = GREATEST(unread_counters.last_seq, EXCLUDED.last_seq),
			       updated_at   = now()`,
			seq, conversationID, senderID)
		if res.Error != nil {
			return fmt.Errorf("bump unread counters: %w", res.Error)
		}
		changed = res.RowsAffected
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("apply unread: %w", err)
	}
	return changed, nil
}

// DeleteConsumedBefore removes inbox rows older than t and returns how many
// went. The relay's janitor calls it.
//
// The table only has to remember a message for as long as the broker could
// still hand it out again, which is the stream's own retention — 24 hours. The
// default keeps them for twice that, because the cost of one extra day of
// small rows is nothing and the cost of forgetting too early is a message
// counted twice.
func (s *service) DeleteConsumedBefore(ctx context.Context, t time.Time) (int64, error) {
	res := s.db.WithContext(ctx).
		Where("consumed_at < ?", t).
		Delete(&ConsumedMessage{})
	if res.Error != nil {
		return 0, fmt.Errorf("delete consumed messages: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// UnreadForUser returns the unread count per room for one user. Rooms with
// nothing unread are left out, so the caller treats a missing key as zero.
//
// One query for the whole conversation list, because the list page needs a
// number for every room at once and a query per room would turn one screen
// into twenty round trips.
func (s *service) UnreadForUser(ctx context.Context, userID uint) (map[uint]int64, error) {
	var rows []struct {
		ConversationID uint
		UnreadCount    int64
	}

	err := s.db.WithContext(ctx).
		Model(&UnreadCounter{}).
		Select("conversation_id, unread_count").
		Where("user_id = ? AND unread_count > 0", userID).
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("select unread counters: %w", err)
	}

	out := make(map[uint]int64, len(rows))
	for _, r := range rows {
		out[r.ConversationID] = r.UnreadCount
	}
	return out, nil
}

// MarkConversationRead sets one user's unread count in one room back to zero.
//
// It touches nothing the consumer relies on. The consumer's guard lives in
// consumed_messages, which this does not go near, so a message counted a
// moment ago is still recorded as counted and cannot be counted a second time
// later.
//
// There is a small race, and it is worth naming rather than hiding. A message
// counted between the moment the user opened the room and the moment this runs
// is wiped along with the rest, so the user sees zero for a message they did
// not read. The next message they receive brings the count back to 1. For a
// badge on a room list that is the right trade: simple, cheap, and wrong for at
// most a second. A client that cannot accept that would send the seq it has
// actually seen, which is a bigger API and a Stage 8 problem.
//
// The row is created if it is missing, so that marking an empty room read is
// not an error.
func (s *service) MarkConversationRead(ctx context.Context, conversationID, userID uint) error {
	err := s.db.WithContext(ctx).Exec(`
		INSERT INTO unread_counters (conversation_id, user_id, unread_count, last_seq, updated_at)
		VALUES (?, ?, 0, 0, now())
		ON CONFLICT (conversation_id, user_id) DO UPDATE
		   SET unread_count = 0,
		       updated_at   = now()`,
		conversationID, userID).Error
	if err != nil {
		return fmt.Errorf("mark conversation read: %w", err)
	}
	return nil
}

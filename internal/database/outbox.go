package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// outboxLockKey is the advisory lock every node fights over to become the
// relay. Any number works as long as every node picks the same one; this is
// "5" for Stage 5 followed by the year, which is memorable and is not going to
// collide with goose's own lock.
const outboxLockKey int64 = 52026

// OutboxLock is the right to be the relay, held for as long as its database
// connection lives.
//
// A Postgres advisory session lock is tied to the connection that took it, not
// to a transaction and not to a row. That is exactly the property we want: if
// the node crashes, or the network drops, or the process is killed with -9,
// the connection dies and Postgres frees the lock. There is no lease to renew
// and no stale row to clean up, and a new leader can start within seconds.
//
// The connection is held out of the pool for the life of the lock, which costs
// one connection out of twenty-five on one node. That is the whole price.
type OutboxLock struct {
	conn *sql.Conn
}

// Release gives the lock back and returns the connection to the pool.
//
// Unlocking explicitly is not strictly needed — closing the connection would
// do it — but it makes the handover fast instead of waiting for Postgres to
// notice a dead connection.
func (l *OutboxLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	_, unlockErr := l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", outboxLockKey)
	closeErr := l.conn.Close()
	l.conn = nil

	if unlockErr != nil {
		return fmt.Errorf("release outbox lock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close outbox lock connection: %w", closeErr)
	}
	return nil
}

// TryOutboxLock asks to be the relay. It returns nil, nil when another node
// already holds the lock — that is the normal answer on every node but one,
// and it is not an error.
//
// pg_try_advisory_lock does not wait. A blocking pg_advisory_lock would leave
// every follower parked on a connection forever, and a follower that is parked
// cannot notice that its own context was cancelled.
func (s *service) TryOutboxLock(ctx context.Context) (*OutboxLock, error) {
	sqlDB, err := s.db.DB()
	if err != nil {
		return nil, fmt.Errorf("outbox lock: get pool: %w", err)
	}

	// A dedicated connection, not the pool. The lock belongs to whichever
	// connection took it, and a pooled query would hand that connection to
	// somebody else the moment it returned.
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("outbox lock: open connection: %w", err)
	}

	var got bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", outboxLockKey).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("outbox lock: %w", err)
	}
	if !got {
		_ = conn.Close()
		return nil, nil
	}
	return &OutboxLock{conn: conn}, nil
}

// FetchOutbox returns the oldest unpublished rows, in id order, at most limit
// of them.
//
// The order is not decoration. Messages inside one room must reach sockets in
// the order they were written, and id order is that order because the ids come
// from one sequence. This is also why there is a single relay: two relays
// working the same table in parallel would publish 5 before 4.
//
// The query reads the partial index from migration 00005, so it stays cheap
// however big the table grows.
func (s *service) FetchOutbox(ctx context.Context, limit int) ([]Outbox, error) {
	var rows []Outbox

	err := s.db.WithContext(ctx).
		Where("published_at IS NULL").
		Order("id ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("select outbox: %w", err)
	}
	return rows, nil
}

// MarkOutboxPublished stamps rows as done, in one statement.
//
// This runs AFTER the publish, and that order is deliberate. If the process
// dies in between, the row is still unpublished and the relay publishes it
// again on the next start. A duplicate is safe — the broker drops it by
// message id, and the consumer is idempotent anyway — while a lost message is
// not. When in doubt, send it twice.
func (s *service) MarkOutboxPublished(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}

	err := s.db.WithContext(ctx).
		Model(&Outbox{}).
		Where("id IN ?", ids).
		Update("published_at", time.Now()).Error
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

// MarkOutboxFailed records that a publish did not work. The row stays
// unpublished, so it will be tried again.
//
// Nothing reads these two columns at run time. They are here for the person
// looking at a stuck queue at two in the morning, who needs to know whether
// the relay has tried once or four hundred times, and what the broker said.
func (s *service) MarkOutboxFailed(ctx context.Context, id uint64, reason string) error {
	// Truncated, because an error string is not always short and this column
	// is a note to a human, not data.
	if len(reason) > 500 {
		reason = reason[:500]
	}

	err := s.db.WithContext(ctx).
		Model(&Outbox{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"attempts":   gorm.Expr("attempts + 1"),
			"last_error": reason,
		}).Error
	if err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}
	return nil
}

// DeleteOutboxPublishedBefore removes rows that were published before t, and
// returns how many went.
//
// Published rows are kept for a while and not deleted on the spot. An empty
// table tells you nothing when you are trying to work out what happened five
// minutes ago, and the partial index means the leftovers cost the relay
// nothing. They are deleted eventually because the table would otherwise grow
// forever with rows that no longer answer any question.
func (s *service) DeleteOutboxPublishedBefore(ctx context.Context, t time.Time) (int64, error) {
	res := s.db.WithContext(ctx).
		Where("published_at IS NOT NULL AND published_at < ?", t).
		Delete(&Outbox{})
	if res.Error != nil {
		return 0, fmt.Errorf("delete published outbox rows: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// OutboxStats reports how many rows are waiting and how long the oldest one
// has waited. oldest is the zero time when nothing is waiting.
//
// The waiting time is the number that matters. A pending count of 900 means
// nothing on its own — it could be one busy second. Nine hundred rows whose
// oldest is four minutes old means the relay has stopped.
func (s *service) OutboxStats(ctx context.Context) (pending int64, oldest time.Time, err error) {
	var row struct {
		Pending int64
		Oldest  *time.Time
	}

	err = s.db.WithContext(ctx).
		Model(&Outbox{}).
		Select("count(*) AS pending, min(created_at) AS oldest").
		Where("published_at IS NULL").
		Scan(&row).Error
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("outbox stats: %w", err)
	}
	if row.Oldest != nil {
		oldest = *row.Oldest
	}
	return row.Pending, oldest, nil
}

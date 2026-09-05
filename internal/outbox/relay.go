// Package outbox drains the outbox table into the broker.
//
// This is the second half of the transactional outbox. The first half is one
// line in database.CreateMessage: the message row and the outbox row commit
// together. This half reads those rows and publishes them.
//
// # Why that split fixes anything
//
// Before Stage 5 the handler committed the message and then published. Two
// systems, no transaction across them, so a process that died in between left
// a message that existed and that nobody was ever told about. Retrying the
// publish in the handler does not fix it — the process that would do the retry
// is the one that died.
//
// Splitting the two makes the failure recoverable instead of silent. The
// instruction to publish is a row in the same database as the message, so it
// survives everything the message survives. The relay can crash at any point
// and start again from the same row.
//
// # What it costs
//
// Delivery is no longer instant. A message waits for the relay's next poll, so
// live push gains up to OUTBOX_POLL_INTERVAL, 100ms by default. That is the
// honest price of never losing one, and it is a price worth naming out loud
// rather than hiding.
//
// # Only one relay runs
//
// Every node starts one, and they fight over a Postgres advisory lock. The
// winner drains; the losers wait and take over if it dies. One relay, because
// two of them working the same table in parallel would publish seq 5 before
// seq 4, and Stage 4 exists to stop exactly that.
package outbox

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go-chat-backend/internal/database"
	"go-chat-backend/internal/event"
)

// Store is the slice of database.Service the relay needs. It is an interface
// so the tests can drive the loop with a fake instead of a container.
type Store interface {
	TryOutboxLock(ctx context.Context) (*database.OutboxLock, error)
	FetchOutbox(ctx context.Context, limit int) ([]database.Outbox, error)
	MarkOutboxPublished(ctx context.Context, ids []uint64) error
	MarkOutboxFailed(ctx context.Context, id uint64, reason string) error
	DeleteOutboxPublishedBefore(ctx context.Context, t time.Time) (int64, error)
	DeleteConsumedBefore(ctx context.Context, t time.Time) (int64, error)
	OutboxStats(ctx context.Context) (pending int64, oldest time.Time, err error)
	ListConversationMemberIDs(ctx context.Context, conversationID uint) ([]uint, error)
}

// Publisher is where a drained row goes. *broker.Broker is one; LocalPublisher
// is the other, used when there is no broker at all.
//
// outboxID is passed separately from the event because it is not part of the
// event — it is the delivery attempt's identity, and the broker uses it as a
// dedupe key.
type Publisher interface {
	Publish(ctx context.Context, outboxID uint64, ev event.MessageCreated, userIDs []uint) error
}

const (
	// leaderRetry is how long a node that lost the lock waits before asking
	// again. It is also the worst case handover time when the leader dies, so
	// it is short enough to not be felt and long enough that four idle nodes
	// are not hammering Postgres.
	leaderRetry = 5 * time.Second

	// janitorEvery is how often published rows older than the retention are
	// deleted. Once a minute: the table is not urgent, and a DELETE competing
	// with the drain loop for the same table is worth keeping rare.
	janitorEvery = time.Minute

	// statsEvery is how often the pending count and the lag are refreshed for
	// /metrics. Prometheus scrapes every 15 seconds by default, so five is
	// fresh enough, and it keeps a scrape from turning into a database query.
	statsEvery = 5 * time.Second
)

// Relay drains the outbox.
type Relay struct {
	store Store
	pub   Publisher
	log   *slog.Logger

	pollInterval   time.Duration
	retention      time.Duration
	inboxRetention time.Duration
	batchSize      int

	// Read by Prometheus at scrape time.
	leader    atomic.Bool
	published atomic.Int64
	failed    atomic.Int64
	pending   atomic.Int64
	lagMillis atomic.Int64
}

// New builds a relay. Everything tunable is read from the environment here,
// with defaults that are right for one machine.
func New(store Store, pub Publisher, log *slog.Logger) *Relay {
	return &Relay{
		store: store,
		pub:   pub,
		log:   log,
		// 100ms is the added delivery latency, and it is a straight trade
		// against how often an idle cluster queries an empty index. Lower it
		// and delivery feels instant again at the cost of more queries;
		// LISTEN/NOTIFY would remove the trade entirely and is a later stage.
		pollInterval: envDuration("OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
		// Published rows are kept for an hour. Long enough to look at what
		// happened during an incident, short enough that the table does not
		// become an accidental second copy of every message ever sent.
		retention: envDuration("OUTBOX_RETENTION", time.Hour),
		// The inbox is kept for two days, and that is not a taste. The broker
		// keeps a message for 24 hours, so that is the longest one can still
		// be redelivered; forgetting sooner would let a redelivery be counted
		// a second time. Twice the window is the margin.
		inboxRetention: envDuration("INBOX_RETENTION", 48*time.Hour),
		// 100 rows per round trip. Big enough that a burst drains in a few
		// passes, small enough that one slow publish does not hold a hundred
		// others behind it for long.
		batchSize: 100,
	}
}

// Run drains the outbox until ctx ends. It blocks, so callers give it a
// goroutine.
//
// The shape is: try to become the leader, drain while you are, and go back to
// trying when you stop. A node that is not the leader does almost nothing —
// one small query every five seconds — which is what makes it safe to start
// this on every node and never think about it again.
func (r *Relay) Run(ctx context.Context) {
	// The stats loop runs on every node, leader or not, so `outbox_pending` is
	// readable from whichever node you happen to scrape.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.runStats(ctx)
	}()
	defer wg.Wait()

	for {
		if ctx.Err() != nil {
			return
		}

		lock, err := r.store.TryOutboxLock(ctx)
		if err != nil {
			r.log.Warn("could not ask for the outbox lock", "err", err)
			if !sleep(ctx, leaderRetry) {
				return
			}
			continue
		}
		if lock == nil {
			// Another node is the relay. Normal, and not worth a log line
			// every five seconds.
			if !sleep(ctx, leaderRetry) {
				return
			}
			continue
		}

		r.leader.Store(true)
		r.log.Info("this node is now the outbox relay")

		r.runLeader(ctx)

		r.leader.Store(false)
		// A background context, because ctx is usually already cancelled by
		// the time we get here and releasing the lock is the last useful thing
		// this node can do for the next leader.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := lock.Release(releaseCtx); err != nil {
			r.log.Warn("could not release the outbox lock", "err", err)
		}
		cancel()
	}
}

// runLeader is the loop this node runs while it holds the lock. It returns
// when ctx ends.
func (r *Relay) runLeader(ctx context.Context) {
	janitor := time.NewTicker(janitorEvery)
	defer janitor.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		select {
		case <-janitor.C:
			r.clean(ctx)
		default:
		}

		batch, err := r.Drain(ctx)
		if err != nil {
			r.log.Error("outbox poll failed", "err", err)
			if !sleep(ctx, leaderRetry) {
				return
			}
			continue
		}

		switch {
		// Rows are waiting and not one of them went out. The broker is down,
		// or the work behind it is failing, and neither gets better by being
		// asked again in 100 milliseconds.
		//
		// This wait is not politeness, it is the difference between a queue
		// that waits and a queue that melts. Without it, a broker outage turns
		// the relay into ten failed publishes a second, each one writing an
		// UPDATE to record the failure and a line to the log — so the moment
		// the broker goes down, the database gets busier. A run with NATS
		// stopped reached 545 attempts on one row inside a second before this
		// case existed.
		case batch.Fetched > 0 && batch.Published == 0:
			if !sleep(ctx, leaderRetry) {
				return
			}

		// A full batch means there is probably more waiting, so go straight
		// round again. This is what keeps a burst from draining at one batch
		// per poll interval: 10,000 queued messages leave in a few seconds
		// rather than in a hundred separate ticks.
		case batch.Published == r.batchSize:

		// The normal case: nothing waiting, or a small batch that went out.
		default:
			if !sleep(ctx, r.pollInterval) {
				return
			}
		}
	}
}

// Batch says what one call to Drain did.
//
// Both numbers are needed to know what to do next, and that is the whole
// reason this is not a plain int. "Nothing was published" means two completely
// different things depending on whether anything was waiting: an empty queue
// is healthy and should be polled again soon, while a full queue that produced
// nothing is a broker that is refusing, and asking it again straight away only
// makes an outage more expensive.
type Batch struct {
	// Fetched is how many rows the relay read.
	Fetched int
	// Published is how many of them reached the broker. It is lower than
	// Fetched when a publish failed, because the batch stops at the first
	// failure to keep a room in order.
	Published int
}

// Drain publishes one batch and reports what it managed.
//
// It is exported because it is the whole relay in one call, with no timers and
// no lock around it. A test can run it once and check exactly what came out;
// Run is only this in a loop.
//
// It stops at the first row it cannot publish, on purpose. Skipping ahead
// would deliver a later message before an earlier one in the same room, and
// the failed row would then arrive out of order on the retry. Order is worth
// more than throughput here, and a broker that refuses one message is usually
// refusing all of them anyway.
func (r *Relay) Drain(ctx context.Context) (Batch, error) {
	rows, err := r.store.FetchOutbox(ctx, r.batchSize)
	if err != nil {
		return Batch{}, err
	}
	if len(rows) == 0 {
		return Batch{}, nil
	}

	done := make([]uint64, 0, len(rows))

	for _, row := range rows {
		ev, err := event.DecodeMessageCreated([]byte(row.Payload))
		if err != nil {
			// The bytes will not get better. Retrying forever would stop the
			// whole queue behind one broken row, so it is recorded, marked
			// done, and shouted about. This is the outbox's own poison
			// message, and it should never happen: the payload was written by
			// this same program.
			r.log.Error("outbox row is not readable and was skipped",
				"outbox_id", row.ID, "err", err)
			r.note(ctx, row.ID, "undecodable payload: "+err.Error())
			done = append(done, row.ID)
			continue
		}

		// Who is in the room is read now, after the commit, not at write time.
		// So somebody added to the room a second ago receives this message,
		// and a frozen list stored in the payload would have missed them.
		memberIDs, err := r.store.ListConversationMemberIDs(ctx, ev.ConversationID)
		if err != nil {
			r.note(ctx, row.ID, "list members: "+err.Error())
			break
		}

		if err := r.pub.Publish(ctx, row.ID, ev, memberIDs); err != nil {
			r.failed.Add(1)
			r.note(ctx, row.ID, err.Error())
			r.log.Warn("could not publish an outbox row, will retry",
				"outbox_id", row.ID, "attempts", row.Attempts+1, "err", err)
			break
		}
		done = append(done, row.ID)
	}

	// Marked published only after the broker has acknowledged them.
	//
	// If the process dies on this line, the rows are already on the stream and
	// still unpublished in Postgres, so the next relay sends them again. That
	// duplicate is caught by the message id inside the broker's duplicate
	// window, and by the idempotent consumer after it. Sending twice is a
	// problem with two answers; sending never is a problem with none.
	batch := Batch{Fetched: len(rows), Published: len(done)}

	if err := r.store.MarkOutboxPublished(ctx, done); err != nil {
		return batch, err
	}
	r.published.Add(int64(len(done)))
	return batch, nil
}

// note records why a row did not go out. It never fails the caller: this is a
// message to a human, and losing it must not change what the relay does.
func (r *Relay) note(ctx context.Context, id uint64, reason string) {
	if err := r.store.MarkOutboxFailed(ctx, id, reason); err != nil {
		r.log.Warn("could not record an outbox failure", "outbox_id", id, "err", err)
	}
}

// clean deletes the two kinds of row that would otherwise grow forever: outbox
// rows that have gone out, and inbox rows for messages that can no longer be
// redelivered.
//
// The leader does both, because it is already the one node doing table
// maintenance and two nodes deleting the same rows would only compete.
func (r *Relay) clean(ctx context.Context) {
	n, err := r.store.DeleteOutboxPublishedBefore(ctx, time.Now().Add(-r.retention))
	if err != nil {
		r.log.Warn("could not clean up published outbox rows", "err", err)
	} else if n > 0 {
		r.log.Info("cleaned up published outbox rows", "deleted", n)
	}

	// The inbox is kept far longer than the outbox, and for a different
	// reason. An outbox row that has gone out answers no question after an
	// hour. An inbox row is a promise not to count a message twice, and it has
	// to outlive the broker's own retention — forget too early and a
	// redelivery is applied again.
	n, err = r.store.DeleteConsumedBefore(ctx, time.Now().Add(-r.inboxRetention))
	if err != nil {
		r.log.Warn("could not clean up consumed message rows", "err", err)
	} else if n > 0 {
		r.log.Info("cleaned up consumed message rows", "deleted", n)
	}
}

// runStats keeps the /metrics numbers fresh without making a Prometheus scrape
// wait on a database query.
func (r *Relay) runStats(ctx context.Context) {
	ticker := time.NewTicker(statsEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		read, cancel := context.WithTimeout(ctx, statsEvery)
		pending, oldest, err := r.store.OutboxStats(read)
		cancel()
		if err != nil {
			r.log.Warn("could not read outbox stats", "err", err)
			continue
		}

		r.pending.Store(pending)
		// Nothing waiting means no lag, not "lag since the epoch".
		if oldest.IsZero() {
			r.lagMillis.Store(0)
			continue
		}
		r.lagMillis.Store(time.Since(oldest).Milliseconds())
	}
}

// sleep waits for d, and returns false if ctx ended first. It exists so the
// loops above read as "wait, unless we are shutting down".
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// envDuration reads a duration like "250ms" or "2s". Anything missing,
// unreadable or zero falls back, so a typo in the environment degrades to the
// default instead of stopping the relay.
func envDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		slog.Warn("bad duration in environment, using the default",
			"key", key, "value", raw, "default", fallback)
		return fallback
	}
	return d
}

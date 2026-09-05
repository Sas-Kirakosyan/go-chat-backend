package outbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go-chat-backend/internal/database"
	"go-chat-backend/internal/event"
)

// These tests need no Postgres and no broker. What they check is the relay's
// own decisions: what order it publishes in, what it does when a publish
// fails, and whether it stops when it is told to. The store's real behaviour
// is checked against a container in internal/database.

// ---------------------------------------------------------------------------
// A fake store
// ---------------------------------------------------------------------------

type fakeStore struct {
	mu sync.Mutex

	rows     []database.Outbox
	members  []uint
	lockHeld bool

	// failLock makes TryOutboxLock return an error, so the "cannot even ask"
	// branch can be exercised.
	failLock bool

	// fetchErr fails the read, which is the other way the drain loop can go
	// wrong without any publish being involved.
	fetchErr error

	marked          []uint64
	notes           map[uint64]string
	deleted         int
	consumedDeleted int
}

func newFakeStore(payloads ...event.MessageCreated) *fakeStore {
	s := &fakeStore{members: []uint{1, 2}, notes: map[uint64]string{}}
	for i, ev := range payloads {
		body, err := ev.Encode()
		if err != nil {
			panic(err)
		}
		s.rows = append(s.rows, database.Outbox{
			ID:        uint64(i + 1),
			Topic:     event.TopicMessageCreated,
			Payload:   body,
			CreatedAt: time.Now(),
		})
	}
	return s
}

func (s *fakeStore) TryOutboxLock(context.Context) (*database.OutboxLock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failLock {
		return nil, errors.New("postgres is down")
	}
	if s.lockHeld {
		return nil, nil
	}
	s.lockHeld = true
	return &database.OutboxLock{}, nil
}

func (s *fakeStore) FetchOutbox(_ context.Context, limit int) ([]database.Outbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fetchErr != nil {
		return nil, s.fetchErr
	}

	out := []database.Outbox{}
	for _, row := range s.rows {
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

func (s *fakeStore) MarkOutboxPublished(_ context.Context, ids []uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, id := range ids {
		s.marked = append(s.marked, id)
		for i := range s.rows {
			if s.rows[i].ID == id {
				s.rows[i].PublishedAt = &now
			}
		}
	}
	return nil
}

func (s *fakeStore) MarkOutboxFailed(_ context.Context, id uint64, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notes[id] = reason
	for i := range s.rows {
		if s.rows[i].ID == id {
			s.rows[i].Attempts++
		}
	}
	return nil
}

func (s *fakeStore) DeleteOutboxPublishedBefore(context.Context, time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted++
	return 0, nil
}

func (s *fakeStore) DeleteConsumedBefore(context.Context, time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumedDeleted++
	return 0, nil
}

func (s *fakeStore) OutboxStats(context.Context) (int64, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var pending int64
	var oldest time.Time
	for _, row := range s.rows {
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

func (s *fakeStore) ListConversationMemberIDs(context.Context, uint) ([]uint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint(nil), s.members...), nil
}

func (s *fakeStore) pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int
	for _, row := range s.rows {
		if row.PublishedAt == nil {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// A fake publisher
// ---------------------------------------------------------------------------

type capturePublisher struct {
	mu sync.Mutex

	got  []event.MessageCreated
	ids  []uint64
	err  error
	fail map[uint64]bool
}

func (p *capturePublisher) Publish(_ context.Context, id uint64, ev event.MessageCreated, _ []uint) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return p.err
	}
	if p.fail[id] {
		return errors.New("refused")
	}
	p.ids = append(p.ids, id)
	p.got = append(p.got, ev)
	return nil
}

func (p *capturePublisher) events() []event.MessageCreated {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event.MessageCreated(nil), p.got...)
}

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

func messages(n int) []event.MessageCreated {
	out := make([]event.MessageCreated, 0, n)
	for i := range n {
		out = append(out, event.MessageCreated{
			MessageID:      uint(i + 1),
			ConversationID: 1,
			SenderID:       1,
			SenderName:     "alice",
			Seq:            uint(i + 1),
			Content:        "m",
			CreatedAt:      time.Now(),
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// A clean batch goes out in id order and the rows are marked done. Order is
// the reason there is only one relay at a time.
func TestDrainPublishesInOrderAndMarks(t *testing.T) {
	store := newFakeStore(messages(3)...)
	pub := &capturePublisher{}

	batch, err := New(store, pub, quiet()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Published != 3 {
		t.Fatalf("Drain() published %d, want 3", batch.Published)
	}

	got := pub.events()
	for i, ev := range got {
		if ev.Seq != uint(i+1) {
			t.Fatalf("event %d has seq %d, want %d — the relay reordered the room", i, ev.Seq, i+1)
		}
	}
	if store.pending() != 0 {
		t.Fatalf("%d rows still pending after a clean drain", store.pending())
	}
	if len(store.marked) != 3 {
		t.Fatalf("marked %d rows, want 3", len(store.marked))
	}
}

// A row is marked published only after the broker took it. Marking first would
// mean a crash between the two lost the message for good, which is the exact
// failure the outbox exists to remove.
func TestDrainMarksNothingWhenThePublishFails(t *testing.T) {
	store := newFakeStore(messages(3)...)
	pub := &capturePublisher{err: errors.New("broker is down")}

	batch, err := New(store, pub, quiet()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Published != 0 {
		t.Fatalf("Drain() published %d, want 0", batch.Published)
	}
	if store.pending() != 3 {
		t.Fatalf("%d rows pending after a failed drain, want 3 — a failed publish must not lose a message", store.pending())
	}
	if len(store.marked) != 0 {
		t.Fatalf("%d rows were marked published while the broker was down", len(store.marked))
	}
	if store.notes[1] == "" {
		t.Fatal("the failure was not recorded on the row")
	}
}

// The batch stops at the first failure, so a later message cannot overtake an
// earlier one in the same room when the retry comes round.
func TestDrainStopsAtTheFirstFailure(t *testing.T) {
	store := newFakeStore(messages(4)...)
	pub := &capturePublisher{fail: map[uint64]bool{2: true}}

	batch, err := New(store, pub, quiet()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Published != 1 {
		t.Fatalf("Drain() published %d, want 1 — only the row before the failure", batch.Published)
	}
	if store.pending() != 3 {
		t.Fatalf("%d rows pending, want 3", store.pending())
	}
	// Rows 3 and 4 must not have been tried at all.
	if got := pub.events(); len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("the relay carried on past the failure: %+v", got)
	}
	if store.rows[2].Attempts != 0 || store.rows[3].Attempts != 0 {
		t.Fatal("rows after the failure were attempted")
	}
}

// A payload that cannot be decoded is poison. Retrying it forever would stop
// the whole queue behind one broken row, so it is recorded, marked done, and
// the batch carries on.
func TestDrainSkipsAnUnreadableRow(t *testing.T) {
	store := newFakeStore(messages(2)...)
	store.rows[0].Payload = "{not json"
	pub := &capturePublisher{}

	batch, err := New(store, pub, quiet()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Published != 2 {
		t.Fatalf("Drain() published %d, want 2 — the bad row counts as done", batch.Published)
	}
	if store.pending() != 0 {
		t.Fatal("a poison row was left to block the queue")
	}
	if store.notes[1] == "" {
		t.Fatal("the bad row was skipped with no reason recorded")
	}
	// The good row still went out.
	if got := pub.events(); len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("the good row did not go out: %+v", got)
	}
}

// An empty queue is the common case, and it must not publish, mark, or fail.
func TestDrainOnAnEmptyQueue(t *testing.T) {
	store := newFakeStore()
	pub := &capturePublisher{}

	batch, err := New(store, pub, quiet()).Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Fetched != 0 || batch.Published != 0 || len(pub.events()) != 0 || len(store.marked) != 0 {
		t.Fatalf("an empty queue did something: fetched %d, published %d, delivered %d, marked %d",
			batch.Fetched, batch.Published, len(pub.events()), len(store.marked))
	}
}

// A read that fails is reported rather than swallowed, so the loop backs off
// instead of spinning.
func TestDrainReportsAFetchError(t *testing.T) {
	store := newFakeStore(messages(1)...)
	store.fetchErr = errors.New("postgres is down")

	if _, err := New(store, &capturePublisher{}, quiet()).Drain(t.Context()); err == nil {
		t.Fatal("Drain() hid a failed read")
	}
}

// Run must drain, and must stop when its context ends. A relay that did not
// stop would hold its lock into the next process and delay the handover.
func TestRunDrainsAndStops(t *testing.T) {
	store := newFakeStore(messages(2)...)
	pub := &capturePublisher{}
	relay := New(store, pub, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()

	waitFor(t, "the queue to drain", func() bool { return store.pending() == 0 })
	if !waitForBool(func() bool { return relay.IsLeader() == 1 }) {
		t.Fatal("the draining node does not report itself as the leader")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop when its context ended")
	}

	if relay.IsLeader() != 0 {
		t.Fatal("a stopped relay still claims to be the leader")
	}
	if relay.Published() != 2 {
		t.Fatalf("published counter: got %d, want 2", relay.Published())
	}
}

// A queue that is going nowhere must be left alone for a while, not retried
// ten times a second.
//
// This is the bug a real run found. With NATS stopped, the relay retried the
// first row 545 times inside one second — each attempt a failed publish, a log
// line, and an UPDATE to record the failure. A broker outage made the database
// busier, which is the opposite of what a queue is for. The fix is to tell
// "nothing was waiting" apart from "everything was waiting and none of it
// moved", and to back off on the second.
func TestRunBacksOffWhenNothingCanBePublished(t *testing.T) {
	store := newFakeStore(messages(3)...)
	pub := &capturePublisher{err: errors.New("broker is down")}
	relay := New(store, pub, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()

	// A second is ten poll intervals, and one leaderRetry is five seconds, so
	// a relay that backs off correctly tries once here and a relay that spins
	// tries about ten times.
	waitFor(t, "the first failed attempt", func() bool { return relay.Failed() > 0 })
	time.Sleep(time.Second)

	cancel()
	<-done

	if got := relay.Failed(); got > 2 {
		t.Fatalf("the relay made %d failed publishes in a second; it is spinning on a queue it cannot drain", got)
	}
	if store.pending() != 3 {
		t.Fatalf("%d rows pending, want 3 — nothing should have gone out", store.pending())
	}
}

// A node that does not hold the lock publishes nothing. Two relays draining
// the same table would publish one room's messages out of order.
func TestRunDoesNothingWithoutTheLock(t *testing.T) {
	store := newFakeStore(messages(2)...)
	store.lockHeld = true // somebody else is the relay
	pub := &capturePublisher{}
	relay := New(store, pub, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	if got := len(pub.events()); got != 0 {
		t.Fatalf("a follower published %d events", got)
	}
	if relay.IsLeader() != 0 {
		t.Fatal("a follower reports itself as the leader")
	}

	cancel()
	<-done
}

// A store that cannot even be asked for the lock must not stop the relay for
// good. Postgres comes back, and the relay has to still be there when it does.
func TestRunSurvivesALockError(t *testing.T) {
	store := newFakeStore(messages(1)...)
	store.failLock = true
	relay := New(store, &capturePublisher{}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after a lock error")
	}
}

// The relay is only useful if its numbers are honest, because outbox_lag is
// what an alert would fire on.
func TestStatsAreZeroOnANilRelay(t *testing.T) {
	var r *Relay

	if r.IsLeader() != 0 || r.Published() != 0 || r.Failed() != 0 {
		t.Fatal("a nil relay reported non-zero counters")
	}
	if r.Pending() != 0 || r.LagSeconds() != 0 {
		t.Fatal("a nil relay reported a backlog")
	}
}

// The local publisher is the single-node path: no broker, so the work is done
// and the event handed straight to this node's hub. The durable work goes
// first, and a failure there must stop the delivery and be reported, so the
// row stays pending and is retried.
func TestLocalPublisherAppliesBeforeDelivering(t *testing.T) {
	var order []string

	pub := NewLocalPublisher(
		func(event.MessageCreated, []uint) { order = append(order, "deliver") },
		func(context.Context, event.MessageCreated) error {
			order = append(order, "apply")
			return nil
		},
	)
	if err := pub.Publish(t.Context(), 1, event.MessageCreated{}, []uint{1}); err != nil {
		t.Fatalf("Publish() returned %v", err)
	}
	if len(order) != 2 || order[0] != "apply" || order[1] != "deliver" {
		t.Fatalf("order was %v, want [apply deliver]", order)
	}

	delivered := false
	failing := NewLocalPublisher(
		func(event.MessageCreated, []uint) { delivered = true },
		func(context.Context, event.MessageCreated) error { return errors.New("database is down") },
	)
	if err := failing.Publish(t.Context(), 1, event.MessageCreated{}, []uint{1}); err == nil {
		t.Fatal("Publish() hid a failed apply, so the row would be marked done")
	}
	if delivered {
		t.Fatal("the event was delivered even though the work failed")
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	if !waitForBool(ok) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitForBool(ok func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return ok()
}

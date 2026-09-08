package presence

import (
	"sync"
	"testing"
	"time"
)

// Every test here uses a fake clock. A breaker test that sleeps is slow and
// flaky at the same time, and the thing being tested is a state machine driven
// by time — so the clock is exactly the input, and an input should be chosen,
// not waited for.

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestBreakerOpensOnlyAtTheThreshold(t *testing.T) {
	clock := newClock()
	b := newBreaker(3, time.Second, clock.Now)

	for i := 1; i < 3; i++ {
		b.failure()
		if !b.allow() {
			t.Fatalf("breaker opened after %d failures, the threshold is 3", i)
		}
	}

	b.failure()
	if b.allow() {
		t.Fatal("breaker stayed closed after 3 failures")
	}
	if got := b.State(); got != "open" {
		t.Fatalf("state: got %q, want open", got)
	}
}

// The failures have to be consecutive. One success in the middle means the
// dependency is answering, and a breaker that opened anyway would cut off a
// service that is merely flaky — which is a service that is still useful.
func TestBreakerCountsConsecutiveFailures(t *testing.T) {
	clock := newClock()
	b := newBreaker(3, time.Second, clock.Now)

	b.failure()
	b.failure()
	b.success()
	b.failure()
	b.failure()

	if !b.allow() {
		t.Fatal("breaker opened on 4 failures split by a success")
	}
}

// The point of the whole thing: once open, a call costs nothing and reaches
// nothing.
func TestOpenBreakerRefusesWithoutWaiting(t *testing.T) {
	clock := newClock()
	b := newBreaker(1, 5*time.Second, clock.Now)

	b.failure()

	for i := 0; i < 100; i++ {
		if b.allow() {
			t.Fatalf("call %d got through an open breaker", i)
		}
	}

	// Not one second in. Time on the fake clock has not moved at all, which is
	// the honest way to say "these calls did not wait".
	clock.advance(4 * time.Second)
	if b.allow() {
		t.Fatal("breaker let a call through before openFor had passed")
	}
}

func TestBreakerProbesOnceAndClosesOnSuccess(t *testing.T) {
	clock := newClock()
	b := newBreaker(1, 5*time.Second, clock.Now)

	b.failure()
	clock.advance(5 * time.Second)

	if !b.allow() {
		t.Fatal("no probe was allowed after openFor had passed")
	}
	// Exactly one. A recovering service must not be hit by every waiting caller
	// the moment it answers — that is how a service that just came back goes
	// down again.
	if b.allow() {
		t.Fatal("a second call got through while the probe was in flight")
	}
	if got := b.State(); got != "half-open" {
		t.Fatalf("state: got %q, want half-open", got)
	}

	b.success()

	if !b.allow() || !b.allow() {
		t.Fatal("breaker did not close after a successful probe")
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("state: got %q, want closed", got)
	}
}

// A failed probe must reopen the circuit for a full openFor. Without this, a
// service that is down for an hour is probed by every single request after the
// first five seconds — which is the retry storm the breaker exists to stop.
func TestFailedProbeReopensForAnotherFullWait(t *testing.T) {
	clock := newClock()
	b := newBreaker(1, 5*time.Second, clock.Now)

	b.failure()
	clock.advance(5 * time.Second)

	if !b.allow() {
		t.Fatal("no probe was allowed")
	}
	b.failure()

	if b.allow() {
		t.Fatal("a call got through immediately after the probe failed")
	}
	clock.advance(4 * time.Second)
	if b.allow() {
		t.Fatal("the new open window was shorter than openFor")
	}
	clock.advance(time.Second)
	if !b.allow() {
		t.Fatal("no second probe after the new openFor had passed")
	}
}

// The metric must not say a service is cut off when the very next call will in
// fact try it.
func TestStateReportsHalfOpenOnceTheWaitIsOver(t *testing.T) {
	clock := newClock()
	b := newBreaker(1, 5*time.Second, clock.Now)

	b.failure()
	if got := b.State(); got != "open" {
		t.Fatalf("state: got %q, want open", got)
	}

	clock.advance(5 * time.Second)
	if got := b.State(); got != "half-open" {
		t.Fatalf("state: got %q, want half-open", got)
	}
}

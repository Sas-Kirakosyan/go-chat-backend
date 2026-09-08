package presence

import (
	"sync"
	"time"
)

// A circuit breaker, and why a timeout is not enough on its own.
//
// The presence service goes down. Every call now waits for its deadline and
// then fails. Say the deadline is one second and the room page asks on every
// load: each of those requests holds a goroutine, a database connection it
// already took, and a client's browser tab for a second longer than it should,
// to produce an error that was certain from the start. Multiply by the request
// rate and a small dependency being down becomes this service being slow — for
// routes that never needed presence at all.
//
// Worse, the retries make it a load test. Three attempts per request against a
// service that is struggling is how a service that was struggling becomes a
// service that is dead. That is the classic retry storm, and it is the reason
// the breaker sits OUTSIDE the retry loop rather than inside it.
//
// The breaker's job is to make the failure cheap and to stop hitting the thing
// that is down:
//
//	closed     normal. Calls go through, failures are counted.
//	open       the last `threshold` calls failed. Calls return at once, with
//	           no connection and no wait, until `openFor` has passed.
//	half-open  one call is let through as a probe. If it works the breaker
//	           closes and everything resumes; if it fails the breaker opens
//	           again for another `openFor`.
//
// The half-open state is the part people leave out, and without it the breaker
// is just an outage that lasts as long as you configured. One probe is what
// makes recovery automatic, and exactly one is what stops a recovering service
// from being hit by everything at once the moment it answers.
type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

func (s breakerState) String() string {
	switch s {
	case open:
		return "open"
	case halfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

type breaker struct {
	// threshold is how many failures in a row open the circuit.
	//
	// It counts consecutive failures, not a rate. A rate needs a window and a
	// minimum sample size to avoid opening on the first unlucky call of the
	// day; consecutive failures need neither, and for a dependency that is
	// either up or down — which is what a dead process is — they say the same
	// thing.
	threshold int

	// openFor is how long the circuit stays open before a probe is allowed.
	//
	// Five seconds is short. It is deliberately shorter than the presence TTL,
	// so a service that comes back is used again before anybody has fallen out
	// of the online window.
	openFor time.Duration

	// now is time.Now in production and a fake clock in tests. A breaker test
	// that sleeps is a test that is slow and flaky at the same time.
	now func() time.Time

	mu       sync.Mutex
	state    breakerState
	failures int
	openedAt time.Time

	// probing is true while the one half-open call is in flight, so a second
	// caller does not also get through.
	probing bool
}

func newBreaker(threshold int, openFor time.Duration, now func() time.Time) *breaker {
	if now == nil {
		now = time.Now
	}
	return &breaker{threshold: threshold, openFor: openFor, now: now}
}

// allow reports whether a call may go out right now.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case closed:
		return true

	case open:
		if b.now().Sub(b.openedAt) < b.openFor {
			return false
		}
		// Time is up. Move to half-open and let this one caller be the probe.
		b.state = halfOpen
		b.probing = true
		return true

	default: // halfOpen
		// Exactly one probe at a time. Everyone else is refused as if the
		// circuit were still open, because it effectively is until the probe
		// says otherwise.
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

// success records a call that reached the service and got an answer.
//
// "Got an answer" is the test, not "got the answer the caller wanted". A
// request the service rejected as invalid proves the service is alive, and a
// breaker that opened on those would take a working dependency out of service
// because of a bug in the caller.
func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures = 0
	b.probing = false
	b.state = closed
}

// failure records a call that could not be completed.
func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.probing = false

	// A failed probe reopens the circuit for a fresh openFor, rather than
	// letting the next caller through immediately. Without this reset, a
	// service that is down for an hour is probed by every single request after
	// the first five seconds, which is the retry storm the breaker exists to
	// prevent.
	if b.state == halfOpen {
		b.state = open
		b.openedAt = b.now()
		return
	}

	b.failures++
	if b.failures >= b.threshold {
		b.state = open
		b.openedAt = b.now()
	}
}

// State is the current state, for the log line and the metric. A gauge for this
// is worth having: "presence is slow" and "presence has been cut off for the
// last four minutes" look identical from the outside otherwise.
func (b *breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	// An open circuit whose time is up is reported as half-open, because that
	// is what the next caller will find. Reporting "open" here would make the
	// metric say a service is cut off when the next request will in fact try
	// it.
	if b.state == open && b.now().Sub(b.openedAt) >= b.openFor {
		return halfOpen.String()
	}
	return b.state.String()
}

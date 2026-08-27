package server

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// The bucket is tested through a fake clock rather than by sleeping. Real
// sleeps would make a test that is slow and, worse, one that fails on a busy
// machine for reasons that have nothing to do with the code.

// fakeClock is a clock a test moves by hand.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestLimiter(rate, burst float64) (*rateLimiter, *fakeClock) {
	clock := &fakeClock{now: time.Now()}
	l := newRateLimiter(rate, burst)
	l.now = clock.Now
	return l, clock
}

func TestBucketAllowsTheBurstThenRefuses(t *testing.T) {
	l, _ := newTestLimiter(1, 3)

	for i := 1; i <= 3; i++ {
		if ok, _ := l.allow("someone"); !ok {
			t.Fatalf("request %d of the burst was refused", i)
		}
	}

	ok, wait := l.allow("someone")
	if ok {
		t.Fatal("the fourth request went through, but the burst was 3")
	}
	// At one token per second, the next one is a second away.
	if wait <= 0 || wait > time.Second {
		t.Fatalf("retry-after: got %v, want something up to a second", wait)
	}
}

func TestBucketRefillsOverTime(t *testing.T) {
	l, clock := newTestLimiter(2, 2) // two per second

	l.allow("someone")
	l.allow("someone")
	if ok, _ := l.allow("someone"); ok {
		t.Fatal("the bucket was not empty")
	}

	clock.advance(500 * time.Millisecond) // exactly one token
	if ok, _ := l.allow("someone"); !ok {
		t.Fatal("half a second bought no token at two per second")
	}
	if ok, _ := l.allow("someone"); ok {
		t.Fatal("half a second bought two tokens")
	}
}

// A bucket must not fill past its burst. Otherwise a client that was quiet for
// an hour would come back with an hour's worth of allowance and could empty
// the whole thing at once — exactly the spike the limit exists to stop.
func TestAnIdleBucketDoesNotSaveUpTokens(t *testing.T) {
	l, clock := newTestLimiter(1, 3)

	clock.advance(time.Hour)

	for i := 1; i <= 3; i++ {
		if ok, _ := l.allow("someone"); !ok {
			t.Fatalf("request %d was refused after an idle hour", i)
		}
	}
	if ok, _ := l.allow("someone"); ok {
		t.Fatal("an idle hour saved up more than the burst")
	}
}

func TestOneCallerDoesNotSpendAnothersAllowance(t *testing.T) {
	l, _ := newTestLimiter(1, 1)

	if ok, _ := l.allow("user:1"); !ok {
		t.Fatal("the first caller was refused")
	}
	if ok, _ := l.allow("user:1"); ok {
		t.Fatal("the first caller got two tokens out of a burst of one")
	}
	if ok, _ := l.allow("user:2"); !ok {
		t.Fatal("a second caller was refused because of the first one")
	}
}

// Without the sweep the map grows by one entry per key that ever arrived,
// which is a slow memory leak an attacker can steer by rotating IPs.
func TestFullBucketsAreSweptAway(t *testing.T) {
	l, clock := newTestLimiter(1, 1)

	l.allow("comes-back")
	l.allow("never-comes-back")
	if got := len(l.buckets); got != 2 {
		t.Fatalf("buckets: got %d, want 2", got)
	}

	// Long enough for both to refill completely, and for a sweep to be due.
	clock.advance(2 * sweepEvery)
	l.allow("comes-back")

	if got := len(l.buckets); got != 1 {
		t.Fatalf("buckets after the sweep: got %d, want only the one that came back", got)
	}
	if _, ok := l.buckets["never-comes-back"]; ok {
		t.Fatal("an idle bucket survived the sweep")
	}

	// Being swept must not punish the caller: a bucket that was full and a
	// caller with no bucket at all are the same thing.
	if ok, _ := l.allow("never-comes-back"); !ok {
		t.Fatal("a swept caller was refused on its next request")
	}
}

// ---------------------------------------------------------------------------
// The middleware, over real routes
// ---------------------------------------------------------------------------

// Guessing a password is the attack this limit exists for. The route is
// limited by IP, because the user id is the part being guessed.
func TestLoginIsRateLimitedByIP(t *testing.T) {
	_, r, _ := newTestServerWith(t, rateLimits{authRPS: 1, authBurst: 2, apiRPS: 100, apiBurst: 100})
	const creds = `{"username":"alice","password":"wrong-password-here"}`

	for i := 1; i <= 2; i++ {
		if rr := do(t, r, "POST", "/login", creds, ""); rr.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: got %d, want %d", i, rr.Code, http.StatusUnauthorized)
		}
	}

	rr := do(t, r, "POST", "/login", creds, "")
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("the third attempt: got %d, want %d", rr.Code, http.StatusTooManyRequests)
	}

	// A client that is told to wait must be told how long, or it comes
	// straight back and the limit achieves nothing.
	retry := rr.Header().Get("Retry-After")
	seconds, err := strconv.Atoi(retry)
	if err != nil || seconds < 1 {
		t.Fatalf("Retry-After: got %q, want whole seconds above zero", retry)
	}
}

// Behind the auth check the caller is known, so the limit is per user. Two
// people in the same office must not share an allowance.
func TestTheAPILimitIsPerUserNotPerIP(t *testing.T) {
	// A burst of 3: signUp ends with a /auth/profile call, which is behind the
	// same limiter, so each user arrives here having already spent one token.
	_, r, _ := newTestServerWith(t, rateLimits{apiRPS: 1, apiBurst: 3})
	alice, _ := signUp(t, r, "alice")
	bob, _ := signUp(t, r, "bob")

	for i := 1; i <= 2; i++ {
		if rr := do(t, r, "GET", "/conversations", "", alice); rr.Code != http.StatusOK {
			t.Fatalf("alice's request %d: got %d", i, rr.Code)
		}
	}
	if rr := do(t, r, "GET", "/conversations", "", alice); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("alice's third request: got %d, want %d", rr.Code, http.StatusTooManyRequests)
	}

	// Same IP, different user, untouched allowance.
	if rr := do(t, r, "GET", "/conversations", "", bob); rr.Code != http.StatusOK {
		t.Fatalf("bob was refused because alice was noisy: got %d", rr.Code)
	}
}

// The probes must never be limited. A refused probe looks exactly like a dead
// node, so the monitoring would take a healthy server out of service.
func TestOperationsRoutesAreNotRateLimited(t *testing.T) {
	_, r, _ := newTestServerWith(t, rateLimits{authRPS: 1, authBurst: 1, apiRPS: 1, apiBurst: 1})

	for _, path := range []string{"/livez", "/readyz", "/metrics", "/health"} {
		for i := 1; i <= 5; i++ {
			if rr := do(t, r, "GET", path, "", ""); rr.Code != http.StatusOK {
				t.Fatalf("%s call %d: got %d, want 200", path, i, rr.Code)
			}
		}
	}
}

// A limited request is refused before the handler runs, so nothing is written.
func TestARateLimitedSendStoresNothing(t *testing.T) {
	// Four tokens, and the setup spends two of them: /auth/profile at the end
	// of signUp, then the room. That leaves exactly the two sends below.
	_, r, db := newTestServerWith(t, rateLimits{apiRPS: 1, apiBurst: 4})
	alice, _ := signUp(t, r, "alice")
	room := createRoom(t, r, alice)

	path := fmt.Sprintf("/conversations/%d/messages", room)
	for i := 1; i <= 2; i++ {
		if rr := do(t, r, "POST", path, `{"content":"hello"}`, alice); rr.Code != http.StatusCreated {
			t.Fatalf("send %d: got %d (body %s)", i, rr.Code, rr.Body)
		}
	}

	if rr := do(t, r, "POST", path, `{"content":"one too many"}`, alice); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("the third send: got %d, want %d", rr.Code, http.StatusTooManyRequests)
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if got := len(db.messages); got != 2 {
		t.Fatalf("messages stored: got %d, want 2 — the refused send wrote a row", got)
	}
}

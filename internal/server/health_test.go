package server

import (
	"context"
	"net/http"
	"testing"
)

// Liveness and readiness answer two different questions, and the whole point
// of splitting them is that a broken database must not restart healthy pods.
// These tests are what stops the two drifting back together.

// testHTTPServer is an http.Server that was never started. Shutdown on one
// returns straight away, which is what these tests want: what is under test is
// the readiness flag and the order of the steps, not the listener.
func testHTTPServer(h http.Handler) *http.Server {
	return &http.Server{Handler: h}
}

func TestLivezDoesNotDependOnTheDatabase(t *testing.T) {
	_, r, db := newTestServer(t)
	db.healthy = false

	if rr := do(t, r, "GET", "/livez", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("livez with a dead database: got %d, want 200.\n"+
			"A liveness probe that checks the database restarts every pod when the "+
			"database goes down, and none of those restarts help.", rr.Code)
	}
}

func TestReadyzFailsWhenTheDatabaseIsDown(t *testing.T) {
	_, r, db := newTestServer(t)

	if rr := do(t, r, "GET", "/readyz", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("readyz on a healthy server: got %d, want 200", rr.Code)
	}

	db.healthy = false
	if rr := do(t, r, "GET", "/readyz", "", ""); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with a dead database: got %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}

	// And it comes back on its own, with nothing restarted.
	db.healthy = true
	if rr := do(t, r, "GET", "/readyz", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("readyz after the database came back: got %d, want 200", rr.Code)
	}
}

// The reason /readyz exists at all: a node that is about to close must be
// taken out of the load balancer BEFORE it stops accepting, or every deploy
// drops a handful of requests into a dying process.
func TestReadyzFailsWhileShuttingDown(t *testing.T) {
	s, r, _ := newTestServer(t)

	if rr := do(t, r, "GET", "/readyz", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("readyz before shutdown: got %d, want 200", rr.Code)
	}

	app := &App{HTTP: testHTTPServer(r), srv: s, hub: s.hub, db: s.db}
	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if rr := do(t, r, "GET", "/readyz", "", ""); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz while draining: got %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}

	// Liveness stays true throughout. The process is alive; it is just not
	// taking new work. Failing liveness here would ask the platform to kill a
	// process that is in the middle of a clean shutdown.
	if rr := do(t, r, "GET", "/livez", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("livez while draining: got %d, want 200", rr.Code)
	}
}

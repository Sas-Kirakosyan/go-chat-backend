package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// This file uses a real socket, because the thing under test is the heartbeat,
// and a heartbeat only exists on a real connection.
//
// The hub's timings are fields, not constants, so the wait here is
// milliseconds instead of the minute a real client gets.

// serveHub puts the hub behind a real listener that upgrades and registers
// whatever connects.
func serveHub(t *testing.T, h *Hub) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		h.Add(conn, 1)
	}))

	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// waitFor polls until the condition holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

// The case the roadmap calls "kill the network on one client": the socket is
// never closed, the client simply stops answering.
//
// TCP will not report this. Nothing is being sent, so nothing fails, and the
// connection would sit there looking healthy until the process restarts. Only
// the ping/pong pair turns that silence into an error: no pong inside pongWait
// and the read deadline fires.
func TestASilentClientIsClosedByTheHeartbeat(t *testing.T) {
	h := New()
	h.pongWait = 300 * time.Millisecond
	h.pingPeriod = 100 * time.Millisecond
	go h.Run()
	t.Cleanup(h.Close)

	srv := serveHub(t, h)
	conn := dial(t, srv)

	waitFor(t, time.Second, "the socket to register", func() bool { return h.Open() == 1 })

	// The client goes quiet. It never reads, so gorilla never sends the
	// automatic pong, and it never writes anything of its own either. From the
	// outside it looks exactly like a laptop that lost its wifi.
	_ = conn

	waitFor(t, 3*time.Second, "the hub to close the silent socket", func() bool { return h.Open() == 0 })
}

// The opposite case, and the one that must not be a false alarm: a client that
// is idle but healthy answers the pings, so it stays connected.
func TestAnAnsweringClientStaysConnected(t *testing.T) {
	h := New()
	h.pongWait = 300 * time.Millisecond
	h.pingPeriod = 100 * time.Millisecond
	go h.Run()
	t.Cleanup(h.Close)

	srv := serveHub(t, h)
	conn := dial(t, srv)

	// Reading is what answers a ping: gorilla replies with a pong from inside
	// ReadMessage. A client that never reads never answers, which is exactly
	// the trap the test above falls into on purpose.
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	waitFor(t, time.Second, "the socket to register", func() bool { return h.Open() == 1 })

	// Several pongWait windows go by. A healthy idle client must survive them.
	time.Sleep(1500 * time.Millisecond)

	if got := h.Open(); got != 1 {
		t.Fatalf("open sockets: got %d, want 1 — a healthy idle client was dropped", got)
	}
}

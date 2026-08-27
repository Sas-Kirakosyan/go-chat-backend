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
// whatever connects. A zero expiresAt means the socket has no expiry.
func serveHub(t *testing.T, h *Hub, expiresAt time.Time) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		h.Add(conn, 1, expiresAt)
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

	srv := serveHub(t, h, time.Time{})
	conn := dial(t, srv)

	waitFor(t, time.Second, "the socket to register", func() bool { return h.Open() == 1 })

	// The client goes quiet. It never reads, so gorilla never sends the
	// automatic pong, and it never writes anything of its own either. From the
	// outside it looks exactly like a laptop that lost its wifi.
	_ = conn

	waitFor(t, 3*time.Second, "the hub to close the silent socket", func() bool { return h.Open() == 0 })
}

// A socket must not outlive the token that opened it.
//
// The token is checked once, at the handshake. Without a deadline on the
// socket itself, a connection opened one second before its token expired would
// keep delivering for as long as the process lived.
//
// The close code matters as much as the close: 4401 tells the client "get a
// new token and reconnect", which is a different instruction from "this server
// is going away".
func TestSocketIsClosedWhenItsTokenExpires(t *testing.T) {
	h := New()
	go h.Run()
	t.Cleanup(h.Close)

	srv := serveHub(t, h, time.Now().Add(200*time.Millisecond))
	conn := dial(t, srv)

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("the socket was still delivering after its token expired")
	}
	if !websocket.IsCloseError(err, CloseTokenExpired) {
		t.Fatalf("close: got %v, want close code %d", err, CloseTokenExpired)
	}

	waitFor(t, time.Second, "the hub to forget the expired socket", func() bool { return h.Open() == 0 })
	if got := h.Expired(); got != 1 {
		t.Fatalf("expired count: got %d, want 1", got)
	}
}

// A token that is already dead at the handshake closes the socket at once. The
// server refuses such a token before the upgrade, so this only happens to a
// clock that jumped — but "wait for a deadline in the past" must not mean
// "wait forever".
func TestAnAlreadyExpiredTokenClosesTheSocketAtOnce(t *testing.T) {
	h := New()
	go h.Run()
	t.Cleanup(h.Close)

	srv := serveHub(t, h, time.Now().Add(-time.Minute))
	conn := dial(t, srv)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, CloseTokenExpired) {
		t.Fatalf("close: got %v, want close code %d", err, CloseTokenExpired)
	}
}

// The opposite case, and the one that must not be a false alarm: a client that
// is idle but healthy answers the pings, so it stays connected.
func TestAnAnsweringClientStaysConnected(t *testing.T) {
	h := New()
	h.pongWait = 300 * time.Millisecond
	h.pingPeriod = 100 * time.Millisecond
	go h.Run()
	t.Cleanup(h.Close)

	srv := serveHub(t, h, time.Time{})
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

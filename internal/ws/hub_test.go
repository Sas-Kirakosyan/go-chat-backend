package ws

import (
	"testing"
	"time"
)

// These tests never open a socket. A Client is a send channel plus a
// connection, and everything the hub decides — who gets a message, who is too
// slow, what happens at shutdown — is decided on the channel side. Building
// the clients by hand keeps the tests fast and free of timing luck.
//
// The real socket is covered end to end in internal/server/ws_test.go.

// startHub returns a running hub that is stopped when the test ends.
func startHub(t *testing.T) *Hub {
	t.Helper()
	h := New()
	go h.Run()
	t.Cleanup(h.Close)
	return h
}

// join registers a hand-made client and waits until the hub has it.
func join(t *testing.T, h *Hub, userID uint, buffer int) *Client {
	t.Helper()
	c := &Client{hub: h, userID: userID, send: make(chan []byte, buffer)}

	select {
	case h.register <- c:
	case <-time.After(time.Second):
		t.Fatal("hub did not accept the registration")
	}
	return c
}

// wantFrame fails unless one message arrives soon with the expected body.
func wantFrame(t *testing.T, c *Client, want string) {
	t.Helper()
	select {
	case got, ok := <-c.send:
		if !ok {
			t.Fatalf("user %d: channel was closed, wanted %q", c.userID, want)
		}
		if string(got) != want {
			t.Fatalf("user %d: got %q, want %q", c.userID, got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("user %d: no message arrived, wanted %q", c.userID, want)
	}
}

// wantNothing fails if anything arrives. It has to wait, because "nothing
// happened" can only be shown by giving it time to happen.
func wantNothing(t *testing.T, c *Client) {
	t.Helper()
	select {
	case got := <-c.send:
		t.Fatalf("user %d: got %q, wanted nothing", c.userID, got)
	case <-time.After(100 * time.Millisecond):
	}
}

// wantClosed fails unless the hub closed the channel, which is how a client
// learns it was dropped.
func wantClosed(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-c.send:
			if !ok {
				return // closed, which is what we wanted
			}
			// A buffered message was still in there; keep draining.
		case <-deadline:
			t.Fatalf("user %d: channel was never closed", c.userID)
		}
	}
}

func TestBroadcastReachesOnlyTheListedUsers(t *testing.T) {
	h := startHub(t)
	alice := join(t, h, 1, sendBuffer)
	bob := join(t, h, 2, sendBuffer)
	carol := join(t, h, 3, sendBuffer)

	h.Broadcast([]uint{1, 2}, []byte(`{"type":"message.new"}`))

	wantFrame(t, alice, `{"type":"message.new"}`)
	wantFrame(t, bob, `{"type":"message.new"}`)
	wantNothing(t, carol)
}

func TestBroadcastReachesEverySocketOfOneUser(t *testing.T) {
	h := startHub(t)
	laptop := join(t, h, 1, sendBuffer)
	phone := join(t, h, 1, sendBuffer)

	h.Broadcast([]uint{1}, []byte("hello"))

	wantFrame(t, laptop, "hello")
	wantFrame(t, phone, "hello")
	if got := h.Open(); got != 2 {
		t.Fatalf("open sockets: got %d, want 2", got)
	}
}

func TestBroadcastToAnAbsentUserIsHarmless(t *testing.T) {
	h := startHub(t)
	alice := join(t, h, 1, sendBuffer)

	// User 99 is a member of the room but is not connected. That is the normal
	// case, not an error.
	h.Broadcast([]uint{1, 99}, []byte("hello"))

	wantFrame(t, alice, "hello")
}

// The rule the whole stage turns on: a client that does not read is dropped,
// never waited for.
//
// The fast client is also the clock here. Broadcast is asynchronous, so the
// test must not look at the slow client until the hub has really handled both
// fan-outs. Reading the fast client's frames in order is that proof — and it
// does not touch the slow client's buffer, which has to stay full.
func TestSlowClientIsDroppedInsteadOfBlockingTheHub(t *testing.T) {
	h := startHub(t)
	slow := join(t, h, 1, 1) // room for exactly one message
	fast := join(t, h, 2, sendBuffer)

	h.Broadcast([]uint{1, 2}, []byte("first"))  // fills the slow client's buffer
	h.Broadcast([]uint{1, 2}, []byte("second")) // no room left: slow is dropped
	h.Broadcast([]uint{2}, []byte("third"))

	// The important half: the hub kept serving everyone else, in order.
	wantFrame(t, fast, "first")
	wantFrame(t, fast, "second")
	wantFrame(t, fast, "third")

	wantClosed(t, slow)

	if got := h.Dropped(); got != 1 {
		t.Fatalf("dropped count: got %d, want 1", got)
	}
	if got := h.Open(); got != 1 {
		t.Fatalf("open sockets after the drop: got %d, want 1", got)
	}
}

// A dropped client is unregistered again by its own readPump a moment later.
// The second unregister must be a no-op, not a second close of the channel.
func TestUnregisterTwiceIsSafe(t *testing.T) {
	h := startHub(t)
	c := join(t, h, 1, 1)
	other := join(t, h, 2, sendBuffer)

	h.Broadcast([]uint{1, 2}, []byte("first"))
	h.Broadcast([]uint{1, 2}, []byte("second")) // c is dropped here

	wantFrame(t, other, "first")
	wantFrame(t, other, "second")
	wantClosed(t, c)

	// This is what readPump does when it notices the closed socket.
	h.unregister <- c
	h.unregister <- c

	// The hub is still alive and serving, which a double close would have
	// prevented by panicking.
	h.Broadcast([]uint{2}, []byte("still here"))
	wantFrame(t, other, "still here")

	if got := h.Open(); got != 1 {
		t.Fatalf("open sockets: got %d, want 1", got)
	}
}

func TestCloseClosesEverySocket(t *testing.T) {
	h := New()
	go h.Run()

	alice := join(t, h, 1, sendBuffer)
	bob := join(t, h, 2, sendBuffer)

	h.Close()

	wantClosed(t, alice)
	wantClosed(t, bob)
	if got := h.Open(); got != 0 {
		t.Fatalf("open sockets after Close: got %d, want 0", got)
	}
}

// Shutdown order is not something a caller should have to get right. A message
// stored just before the hub closed must not block the request or panic.
func TestBroadcastAfterCloseIsANoOp(t *testing.T) {
	h := New()
	go h.Run()
	h.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Broadcast([]uint{1}, []byte("too late"))
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Broadcast blocked after Close")
	}
}

func TestCloseTwiceIsSafe(t *testing.T) {
	h := New()
	go h.Run()

	h.Close()
	h.Close() // a second stop must not panic on close of a closed channel
}

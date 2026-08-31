package ws

import (
	"sort"
	"testing"
)

// UserIDs is what the presence heartbeat reads every ten seconds. It has to be
// right about two things: one user with several sockets is one person, and a
// user whose last socket closed is gone.

func TestUserIDsCountsPeopleNotSockets(t *testing.T) {
	h := startHub(t)

	// User 1 has three tabs open. User 2 has one.
	join(t, h, 1, 1)
	join(t, h, 1, 1)
	tab := join(t, h, 1, 1)
	join(t, h, 2, 1)

	if got := want(t, h); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}

	// Closing one of user 1's three tabs changes nothing: they are still here.
	leave(t, h, tab)
	if got := want(t, h); len(got) != 2 {
		t.Fatalf("after one tab closed: got %v, want both users", got)
	}
}

func TestUserIDsIsEmptyWhenNobodyIsConnected(t *testing.T) {
	h := startHub(t)

	if got := want(t, h); len(got) != 0 {
		t.Fatalf("got %v, want nothing", got)
	}
}

// A hub that is closing reports nobody. That is the truthful answer, and it is
// what stops the heartbeat from keeping a draining node's users online.
func TestUserIDsAfterCloseIsEmpty(t *testing.T) {
	h := New()
	go h.Run()
	join(t, h, 1, 1)
	h.Close()

	if got := h.UserIDs(); len(got) != 0 {
		t.Fatalf("got %v, want nothing", got)
	}
}

// want reads the ids in a stable order. The hub keeps them in a map, so the
// order they come out in is deliberately random in Go.
func want(t *testing.T, h *Hub) []uint {
	t.Helper()
	ids := h.UserIDs()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// leave unregisters a client and waits for the hub to have let go of it.
func leave(t *testing.T, h *Hub, c *Client) {
	t.Helper()
	h.unregister <- c
	// The next UserIDs call is answered by the same goroutine that just handled
	// the unregister, so by the time it returns the removal has happened.
	h.UserIDs()
}

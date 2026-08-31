package server

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// These run in single-node mode, where "online" means "has a socket on this
// node". The cluster answer goes through Redis and is checked by hand with
// cmd/splitcheck; what is checked here is everything around it — the member
// check, the two lists, and that opening and closing a socket really moves a
// user between them.

func TestPresenceListsMembersAsOffline(t *testing.T) {
	_, _, r := newWSTestServer(t)
	alice, aliceID := signUp(t, r, "alice")
	_, bobID := signUp(t, r, "bob")
	roomID := newRoom(t, r, alice, bobID)

	got := presence(t, r, roomID, alice)

	if len(got.Online) != 0 {
		t.Fatalf("online: got %v, want nobody — no socket is open", got.Online)
	}
	if len(got.Offline) != 2 {
		t.Fatalf("offline: got %v, want both members (%d and %d)", got.Offline, aliceID, bobID)
	}
}

func TestPresenceFollowsTheSocket(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, aliceID := signUp(t, r, "alice")
	_, bobID := signUp(t, r, "bob")
	roomID := newRoom(t, r, alice, bobID)

	conn := connect(t, srv, alice)

	got := presence(t, r, roomID, alice)
	if len(got.Online) != 1 || got.Online[0] != aliceID {
		t.Fatalf("online: got %v, want [%d]", got.Online, aliceID)
	}
	if len(got.Offline) != 1 || got.Offline[0] != bobID {
		t.Fatalf("offline: got %v, want [%d]", got.Offline, bobID)
	}

	// And gone again once the socket closes. This is the half that matters:
	// anyone can mark a user online, and the bug is always in taking them off.
	conn.Close()
	waitUntilOffline(t, r, roomID, alice)
}

// An outsider must not learn who is online in a room they are not in. It is the
// same 404 as every other route on a room: 403 would confirm the room exists.
func TestPresenceHidesRoomsYouAreNotIn(t *testing.T) {
	_, _, r := newWSTestServer(t)
	alice, _ := signUp(t, r, "alice")
	mallory, _ := signUp(t, r, "mallory")
	roomID := newRoom(t, r, alice)

	rr := do(t, r, "GET", fmt.Sprintf("/conversations/%d/presence", roomID), "", mallory)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404 (body %s)", rr.Code, rr.Body)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newRoom(t *testing.T, r *gin.Engine, authHeader string, memberIDs ...uint) uint {
	t.Helper()

	members := ""
	for i, id := range memberIDs {
		if i > 0 {
			members += ","
		}
		members += fmt.Sprint(id)
	}
	body := fmt.Sprintf(`{"title":"presence","member_ids":[%s]}`, members)

	rr := do(t, r, "POST", "/conversations", body, authHeader)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create room: got %d (body %s)", rr.Code, rr.Body)
	}

	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)
	return room.ID
}

func presence(t *testing.T, r *gin.Engine, roomID uint, authHeader string) presenceDTO {
	t.Helper()

	rr := do(t, r, "GET", fmt.Sprintf("/conversations/%d/presence", roomID), "", authHeader)
	if rr.Code != http.StatusOK {
		t.Fatalf("presence: got %d (body %s)", rr.Code, rr.Body)
	}

	var out presenceDTO
	decode(t, rr.Body.Bytes(), &out)
	return out
}

// waitUntilOffline polls until nobody is online, or gives up.
//
// Closing a socket is not instant from the server's side: the read pump has to
// notice, unregister, and the hub has to handle it. Polling is what a real
// client would see, and it is honest about the delay instead of hiding it in a
// sleep long enough to always pass.
func waitUntilOffline(t *testing.T, r *gin.Engine, roomID uint, authHeader string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(presence(t, r, roomID, authHeader).Online) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a user stayed online after their socket closed")
}

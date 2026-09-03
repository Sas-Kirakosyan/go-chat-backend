package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

// sendN posts n messages into a room and fails the test if any of them does not
// store. It returns the seq numbers, in the order they were sent.
func sendN(t *testing.T, r *gin.Engine, auth string, roomID uint, n int) []uint {
	t.Helper()

	path := fmt.Sprintf("/conversations/%d/messages", roomID)
	seqs := make([]uint, 0, n)
	for i := range n {
		body := fmt.Sprintf(`{"content":"m%d"}`, i+1)
		rr := do(t, r, "POST", path, body, auth)
		if rr.Code != http.StatusCreated {
			t.Fatalf("send %d: got %d, want %d (body %s)", i+1, rr.Code, http.StatusCreated, rr.Body)
		}
		var msg messageDTO
		decode(t, rr.Body.Bytes(), &msg)
		seqs = append(seqs, msg.Seq)
	}
	return seqs
}

// The reply to a send has to carry the seq, because that is the number the
// sender's own client stores. Without it the sender would have to re-read
// history to learn where it is.
func TestSendReturnsSeq(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)

	seqs := sendN(t, r, alice, roomID, 3)
	for i, got := range seqs {
		if want := uint(i + 1); got != want {
			t.Fatalf("message %d came back with seq %d, want %d", i+1, got, want)
		}
	}
}

// A retry of a client_msg_id must give back the FIRST message, with the first
// message's seq. Handing back a new number would make every client think a
// message had appeared that never existed.
func TestRetriedSendKeepsItsSeq(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	path := fmt.Sprintf("/conversations/%d/messages", roomID)

	body := `{"content":"once","client_msg_id":"key-1"}`

	rr := do(t, r, "POST", path, body, alice)
	if rr.Code != http.StatusCreated {
		t.Fatalf("first send: got %d, want %d (body %s)", rr.Code, http.StatusCreated, rr.Body)
	}
	var first messageDTO
	decode(t, rr.Body.Bytes(), &first)

	rr = do(t, r, "POST", path, body, alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("retry: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	var again messageDTO
	decode(t, rr.Body.Bytes(), &again)

	if again.Seq != first.Seq {
		t.Fatalf("retry came back with seq %d, want the original %d", again.Seq, first.Seq)
	}

	// And the retry used up no number: the next real message is 2.
	next := sendN(t, r, alice, roomID, 1)
	if next[0] != first.Seq+1 {
		t.Fatalf("the message after a retry has seq %d, want %d", next[0], first.Seq+1)
	}
}

// The gap read: what a client asks for after its socket comes back.
func TestGapReadReturnsMissedMessagesOldestFirst(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 5)

	path := fmt.Sprintf("/conversations/%d/messages?after_seq=2", roomID)
	rr := do(t, r, "GET", path, "", alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("gap read: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}

	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != 3 {
		t.Fatalf("gap after seq 2 has %d messages, want 3", len(page.Messages))
	}
	// Oldest first. History reads backwards; a gap replays forwards, because
	// the client applies the missed messages in the order they were sent.
	for i, want := range []uint{3, 4, 5} {
		if page.Messages[i].Seq != want {
			t.Fatalf("gap[%d] has seq %d, want %d", i, page.Messages[i].Seq, want)
		}
	}
	if page.NextAfterSeq != nil {
		t.Fatalf("next_after_seq = %d, want null when the gap fitted in one page", *page.NextAfterSeq)
	}
}

// after_seq=0 is a real request, not an empty one: it means "I have nothing
// yet, start me at the beginning".
func TestGapReadFromZeroReturnsWholeRoom(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 3)

	path := fmt.Sprintf("/conversations/%d/messages?after_seq=0", roomID)
	rr := do(t, r, "GET", path, "", alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("gap read: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}

	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != 3 || page.Messages[0].Seq != 1 {
		t.Fatalf("after_seq=0 returned %d messages starting at seq %d, want 3 starting at 1",
			len(page.Messages), page.Messages[0].Seq)
	}
}

// A client that was away for a long time misses more than one page holds. It
// has to be told where to carry on from, or it silently keeps the hole.
func TestGapReadPagesThroughABigGap(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 5)

	var (
		seen   []uint
		cursor uint
	)
	for range 10 { // a stop, so a bug cannot spin forever
		path := fmt.Sprintf("/conversations/%d/messages?after_seq=%d&limit=2", roomID, cursor)
		rr := do(t, r, "GET", path, "", alice)
		if rr.Code != http.StatusOK {
			t.Fatalf("gap page: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
		}

		var page messagePageDTO
		decode(t, rr.Body.Bytes(), &page)
		for _, m := range page.Messages {
			seen = append(seen, m.Seq)
		}
		if page.NextAfterSeq == nil {
			break
		}
		cursor = *page.NextAfterSeq
	}

	// Every message exactly once, in order. No repeats, no holes — that is the
	// whole promise of a cursor.
	if len(seen) != 5 {
		t.Fatalf("paging through the gap saw %v, want 5 messages", seen)
	}
	for i, got := range seen {
		if want := uint(i + 1); got != want {
			t.Fatalf("paging saw %v, want 1..5 in order", seen)
		}
	}
}

// The two cursors run in opposite directions, so a request with both has no
// sensible answer. Refusing beats quietly picking one.
func TestGapReadRejectsBothCursors(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 2)

	for _, query := range []string{
		"?after_seq=1&before_id=2",
		"?after_seq=abc",
		"?after_seq=-1",
	} {
		path := fmt.Sprintf("/conversations/%d/messages%s", roomID, query)
		if rr := do(t, r, "GET", path, "", alice); rr.Code != http.StatusBadRequest {
			t.Errorf("GET %s: got %d, want %d (body %s)", query, rr.Code, http.StatusBadRequest, rr.Body)
		}
	}
}

// A caught-up client is the common case, and it must be cheap and boring: an
// empty list, not an error.
func TestGapReadIsEmptyWhenCaughtUp(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 2)

	path := fmt.Sprintf("/conversations/%d/messages?after_seq=2", roomID)
	rr := do(t, r, "GET", path, "", alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("gap read: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}

	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != 0 {
		t.Fatalf("a caught-up client got %d messages, want 0", len(page.Messages))
	}
}

// The gap read is still a room read, so it obeys the same rule as every other
// one: a non-member cannot tell a real room from an imaginary one.
func TestGapReadHiddenFromNonMembers(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	mallory, _ := signUp(t, r, "mallory")

	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 2)

	path := fmt.Sprintf("/conversations/%d/messages?after_seq=0", roomID)
	if rr := do(t, r, "GET", path, "", mallory); rr.Code != http.StatusNotFound {
		t.Fatalf("outsider gap read: got %d, want %d (body %s)", rr.Code, http.StatusNotFound, rr.Body)
	}
}

// A big limit must be capped, not obeyed. Otherwise one request drags the whole
// room into memory, and ?limit=10000000 is a free denial of service.
func TestGapReadCapsTheLimit(t *testing.T) {
	// This test has to store more than one page of messages, which is more
	// sends than the default per-user burst allows. The limiter is not what is
	// under test here, so give it room.
	_, r, _ := newTestServerWith(t, rateLimits{
		authRPS: 1000, authBurst: 1000,
		apiRPS: 1000, apiBurst: 1000,
	})
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, maxPageSize+5)

	path := fmt.Sprintf("/conversations/%d/messages?after_seq=0&limit=100000", roomID)
	rr := do(t, r, "GET", path, "", alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("gap read: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}

	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != maxPageSize {
		t.Fatalf("gap read returned %d messages, want the cap of %d", len(page.Messages), maxPageSize)
	}
	if page.NextAfterSeq == nil {
		t.Fatal("next_after_seq is null on a full page, so the client has no way to ask for the rest")
	}
}

// before_id must keep working exactly as it did. The gap read is a second door
// into the same room, not a replacement for scrolling up through history.
func TestHistoryStillReadsNewestFirst(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	roomID := newRoom(t, r, alice)
	sendN(t, r, alice, roomID, 3)

	path := fmt.Sprintf("/conversations/%d/messages", roomID)
	rr := do(t, r, "GET", path, "", alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("history: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}

	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != 3 {
		t.Fatalf("history has %d messages, want 3", len(page.Messages))
	}
	for i, want := range []uint{3, 2, 1} {
		if page.Messages[i].Seq != want {
			t.Fatalf("history[%d] has seq %d, want %d — history reads newest first", i, page.Messages[i].Seq, want)
		}
	}
	if page.NextAfterSeq != nil {
		t.Fatalf("next_after_seq = %d on a history read, want null", *page.NextAfterSeq)
	}
}

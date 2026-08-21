package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// These tests use a real listening server and a real WebSocket client. The
// hub's own rules are covered with plain channels in internal/ws; what is
// checked here is the wiring: the token, the upgrade, and the path from a POST
// to a frame on the wire.

const wsReadTimeout = 2 * time.Second

// wsFrame is one frame as a client sees it. Data stays raw so each test can
// decode only what it cares about.
type wsFrame struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// newWSTestServer is newTestServer plus a real TCP listener, which a socket
// needs and httptest.NewRecorder cannot give.
func newWSTestServer(t *testing.T) (*httptest.Server, *Server, *gin.Engine) {
	t.Helper()
	s, r, _ := newTestServer(t)

	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, s, r
}

// dialWS opens a socket. It returns the handshake response too, because a
// refused upgrade is an ordinary HTTP answer and that status is the thing
// under test.
func dialWS(t *testing.T, srv *httptest.Server, query string, header http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws" + query

	conn, resp, err := websocket.DefaultDialer.Dial(url, header)
	if conn != nil {
		t.Cleanup(func() { conn.Close() })
	}
	return conn, resp, err
}

// token strips the "Bearer " that signUp puts in front, because the query
// string carries the bare token.
func token(authHeader string) string {
	return strings.TrimPrefix(authHeader, "Bearer ")
}

// connect opens an authenticated socket and swallows the "connected" frame, so
// each test starts from a clean line.
func connect(t *testing.T, srv *httptest.Server, authHeader string) *websocket.Conn {
	t.Helper()
	conn, resp, err := dialWS(t, srv, "?token="+token(authHeader), nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial: %v (status %d)", err, status)
	}

	if got := readFrame(t, conn); got.Type != "connected" {
		t.Fatalf("first frame: got %q, want \"connected\"", got.Type)
	}
	return conn
}

func readFrame(t *testing.T, conn *websocket.Conn) wsFrame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}

	var frame wsFrame
	decode(t, raw, &frame)
	return frame
}

// wantNoFrame fails if anything arrives before the deadline. "Nothing was
// delivered" can only be shown by waiting for it.
func wantNoFrame(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))

	_, raw, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("got a frame that should never have been sent: %s", raw)
	}
	if !isTimeout(err) {
		t.Fatalf("read: want a timeout, got %v", err)
	}
}

func isTimeout(err error) bool {
	type timeouter interface{ Timeout() bool }
	te, ok := err.(timeouter)
	return ok && te.Timeout()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestWSRejectsBadTokens(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, _ := signUp(t, r, "alice")

	cases := map[string]string{
		"no token":      "",
		"empty token":   "?token=",
		"nonsense":      "?token=not-a-jwt",
		"wrong secret":  "?token=" + signedWithOtherSecret(t),
		"chopped token": "?token=" + token(alice)[:len(token(alice))-4],
	}
	for name, query := range cases {
		t.Run(name, func(t *testing.T) {
			_, resp, err := dialWS(t, srv, query, nil)
			if err == nil {
				t.Fatal("the socket opened, want it refused")
			}
			if resp == nil || resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status: got %v, want %d", resp, http.StatusUnauthorized)
			}
		})
	}
}

// The browser cannot set headers on a socket, but everything else can, and the
// header must keep working for them.
func TestWSAcceptsTheAuthorizationHeader(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, aliceID := signUp(t, r, "alice")

	conn, _, err := dialWS(t, srv, "", http.Header{"Authorization": {alice}})
	if err != nil {
		t.Fatalf("dial with header: %v", err)
	}

	frame := readFrame(t, conn)
	var hello struct {
		UserID uint `json:"user_id"`
	}
	decode(t, frame.Data, &hello)
	if frame.Type != "connected" || hello.UserID != aliceID {
		t.Fatalf("hello frame: got %s %s, want connected for user %d", frame.Type, frame.Data, aliceID)
	}
}

// CORS does not cover a WebSocket handshake, so this check is the only thing
// standing between us and any page on the internet opening a socket.
func TestWSRejectsAForeignOrigin(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, _ := signUp(t, r, "alice")

	header := http.Header{"Origin": {"http://evil.example"}}
	_, resp, err := dialWS(t, srv, "?token="+token(alice), header)
	if err == nil {
		t.Fatal("the socket opened for a foreign origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status: got %v, want %d", resp, http.StatusForbidden)
	}
}

func TestWSDeliversNewMessagesToMembersOnly(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, _ := signUp(t, r, "alice")
	bob, bobID := signUp(t, r, "bob")
	carol, _ := signUp(t, r, "carol")

	room := createRoom(t, r, alice, bobID)
	aliceConn := connect(t, srv, alice)
	carolConn := connect(t, srv, carol)

	path := fmt.Sprintf("/conversations/%d/messages", room)
	if rr := do(t, r, "POST", path, `{"content":"hello"}`, bob); rr.Code != http.StatusCreated {
		t.Fatalf("send: got %d (body %s)", rr.Code, rr.Body)
	}

	frame := readFrame(t, aliceConn)
	if frame.Type != wsMessageEvent {
		t.Fatalf("type: got %q, want %q", frame.Type, wsMessageEvent)
	}
	var msg messageDTO
	decode(t, frame.Data, &msg)
	if msg.Content != "hello" || msg.ConversationID != room || msg.Sender.Username != "bob" {
		t.Fatalf("pushed message: got %+v", msg)
	}

	// Carol is not in the room, so nothing reaches her.
	wantNoFrame(t, carolConn)
}

// The sender sees their own message too. Their other tabs and their phone are
// showing the same room.
func TestWSDeliversTheSendersOwnMessage(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, _ := signUp(t, r, "alice")

	room := createRoom(t, r, alice)
	conn := connect(t, srv, alice)

	path := fmt.Sprintf("/conversations/%d/messages", room)
	do(t, r, "POST", path, `{"content":"note to self"}`, alice)

	if frame := readFrame(t, conn); frame.Type != wsMessageEvent {
		t.Fatalf("type: got %q, want %q", frame.Type, wsMessageEvent)
	}
}

// A retry of the same client_msg_id stores nothing, so it must deliver nothing.
// Pushing twice would be the double-post the key exists to prevent.
func TestWSDoesNotDeliverARepeatedSend(t *testing.T) {
	srv, _, r := newWSTestServer(t)
	alice, _ := signUp(t, r, "alice")
	bob, bobID := signUp(t, r, "bob")

	room := createRoom(t, r, alice, bobID)
	conn := connect(t, srv, alice)

	path := fmt.Sprintf("/conversations/%d/messages", room)
	const body = `{"content":"hello","client_msg_id":"retry-1"}`

	if rr := do(t, r, "POST", path, body, bob); rr.Code != http.StatusCreated {
		t.Fatalf("first send: got %d (body %s)", rr.Code, rr.Body)
	}
	if rr := do(t, r, "POST", path, body, bob); rr.Code != http.StatusOK {
		t.Fatalf("retry: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}

	readFrame(t, conn) // the first send
	wantNoFrame(t, conn)
}

// The roadmap's hardest case, over a real socket: a client that connects and
// then never reads a byte.
//
// It takes a lot of traffic to get there, and that is worth knowing. The
// kernel holds a send buffer on our side and a receive buffer on theirs, and
// on loopback both are large and grow on demand. Until those are full, every
// write returns straight away and the client looks perfectly healthy. Only
// after they fill does writePump block, the send channel back up, and the hub
// drop the client.
//
// So the server survives a dead reader — but it holds real memory for it
// first, in the kernel, where no Go counter can see it.
func TestARealClientThatNeverReadsIsDropped(t *testing.T) {
	if testing.Short() {
		t.Skip("this test pushes megabytes through a socket")
	}
	srv, s, r := newWSTestServer(t)
	alice, aliceID := signUp(t, r, "alice")

	connect(t, srv, alice) // opened, and never read from again

	hub := s.hub
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = 'x'
	}

	const maxRounds = 20000 // 80 MB is far past any socket buffer
	deadline := time.Now().Add(30 * time.Second)

	var sent int
	for hub.Dropped() == 0 && sent < maxRounds && time.Now().Before(deadline) {
		hub.Broadcast([]uint{aliceID}, payload)
		sent++
	}

	if hub.Dropped() == 0 {
		t.Fatalf("client was never dropped after %d messages (%d MB)", sent, sent*len(payload)/(1<<20))
	}
	t.Logf("dropped after %d messages (about %d MB of buffer)", sent, sent*len(payload)/(1<<20))

	// The counter goes up a moment before the map entry goes away, so give the
	// hub goroutine time to finish the removal rather than reading mid-step.
	closing := time.Now().Add(time.Second)
	for hub.Open() != 0 && time.Now().Before(closing) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := hub.Open(); got != 0 {
		t.Fatalf("open sockets after the drop: got %d, want 0", got)
	}
}

// A closed hub must not take the write path down with it. The message is still
// stored and the sender still gets a 201; only the live push is missed.
func TestSendStillWorksAfterTheHubIsClosed(t *testing.T) {
	s, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	room := createRoom(t, r, alice)

	s.hub.Close()

	path := fmt.Sprintf("/conversations/%d/messages", room)
	if rr := do(t, r, "POST", path, `{"content":"hello"}`, alice); rr.Code != http.StatusCreated {
		t.Fatalf("send after shutdown: got %d (body %s)", rr.Code, rr.Body)
	}
}

// ---------------------------------------------------------------------------
// Helpers used only here
// ---------------------------------------------------------------------------

// createRoom makes a room owned by the caller and returns its id.
func createRoom(t *testing.T, r *gin.Engine, authHeader string, memberIDs ...uint) uint {
	t.Helper()

	ids, err := json.Marshal(memberIDs)
	if err != nil {
		t.Fatalf("encode member ids: %v", err)
	}
	body := fmt.Sprintf(`{"title":"standup","member_ids":%s}`, ids)

	rr := do(t, r, "POST", "/conversations", body, authHeader)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create room: got %d (body %s)", rr.Code, rr.Body)
	}

	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)
	return room.ID
}

// signedWithOtherSecret mints a token that is well formed but signed by
// somebody else. It must be refused exactly like a random string.
func signedWithOtherSecret(t *testing.T) string {
	t.Helper()
	other := &Server{jwtKey: []byte("not-the-test-secret")}

	tok, err := other.signAccessToken(1, "mallory")
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return tok
}

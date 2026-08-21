package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"go-chat-backend/internal/database"
)

// ---------------------------------------------------------------------------
// The room half of fakeDB. It mirrors what the Postgres store guarantees:
// the unique index on (conversation_id, user_id), the one on
// (conversation_id, sender_id, client_msg_id), and newest-first history.
// ---------------------------------------------------------------------------

func (f *fakeDB) GetUserByID(_ context.Context, id uint) (*database.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.usersByID[id]
	if !ok {
		return nil, database.ErrUserNotFound
	}
	return u, nil
}

func (f *fakeDB) CreateConversation(_ context.Context, title string, creatorID uint, memberIDs []uint) (*database.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ids := []uint{creatorID}
	seen := map[uint]bool{creatorID: true}
	for _, id := range memberIDs {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		if _, ok := f.usersByID[id]; !ok {
			return nil, database.ErrUserNotFound
		}
	}

	f.nextConvID++
	conv := &database.Conversation{
		Model:       gorm.Model{ID: f.nextConvID, CreatedAt: time.Now()},
		Title:       title,
		CreatedByID: creatorID,
	}
	f.conversations[conv.ID] = conv
	f.memberIDs[conv.ID] = ids
	return f.loadConversation(conv.ID), nil
}

func (f *fakeDB) ListConversationsForUser(_ context.Context, userID uint) ([]database.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := []database.Conversation{}
	for id := f.nextConvID; id >= 1; id-- { // newest first
		if _, ok := f.conversations[id]; !ok || !f.isMember(id, userID) {
			continue
		}
		out = append(out, *f.loadConversation(id))
	}
	return out, nil
}

func (f *fakeDB) EnsureMember(_ context.Context, conversationID, userID uint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.conversations[conversationID]; !ok || !f.isMember(conversationID, userID) {
		return database.ErrConversationNotFound
	}
	return nil
}

func (f *fakeDB) AddMember(_ context.Context, conversationID, userID uint) (*database.ConversationMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isMember(conversationID, userID) {
		return nil, database.ErrAlreadyMember
	}
	f.memberIDs[conversationID] = append(f.memberIDs[conversationID], userID)
	f.nextMemberID++
	return &database.ConversationMember{
		ID:             f.nextMemberID,
		ConversationID: conversationID,
		UserID:         userID,
		User:           *f.usersByID[userID],
	}, nil
}

func (f *fakeDB) ListConversationMemberIDs(_ context.Context, conversationID uint) ([]uint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// A copy, not the slice itself: the caller hands it to the hub, which
	// reads it on another goroutine while AddMember may still append here.
	return append([]uint(nil), f.memberIDs[conversationID]...), nil
}

func (f *fakeDB) CreateMessage(_ context.Context, conversationID, senderID uint, content string, clientMsgID *string) (*database.Message, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if clientMsgID != nil {
		for _, m := range f.messages {
			if m.ConversationID == conversationID && m.SenderID == senderID &&
				m.ClientMsgID != nil && *m.ClientMsgID == *clientMsgID {
				dup := m
				return &dup, false, nil
			}
		}
	}

	f.nextMsgID++
	msg := database.Message{
		Model:          gorm.Model{ID: f.nextMsgID, CreatedAt: time.Now()},
		ConversationID: conversationID,
		SenderID:       senderID,
		Content:        content,
		ClientMsgID:    clientMsgID,
	}
	f.messages = append(f.messages, msg)
	return &msg, true, nil
}

func (f *fakeDB) ListMessages(_ context.Context, conversationID, beforeID uint, limit int) ([]database.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := []database.Message{}
	for i := len(f.messages) - 1; i >= 0; i-- { // appended in id order, so this is newest first
		m := f.messages[i]
		if m.ConversationID != conversationID {
			continue
		}
		if beforeID > 0 && m.ID >= beforeID {
			continue
		}
		m.Sender = *f.usersByID[m.SenderID]
		out = append(out, m)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// isMember and loadConversation both expect f.mu to be held already.
func (f *fakeDB) isMember(conversationID, userID uint) bool {
	for _, id := range f.memberIDs[conversationID] {
		if id == userID {
			return true
		}
	}
	return false
}

func (f *fakeDB) loadConversation(id uint) *database.Conversation {
	out := *f.conversations[id]
	out.Members = nil
	for _, userID := range f.memberIDs[id] {
		out.Members = append(out.Members, database.ConversationMember{
			ConversationID: id,
			UserID:         userID,
			User:           *f.usersByID[userID],
		})
	}
	return &out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// signUp registers a user, logs in, and returns the Authorization header value
// plus the user id the server put in the token.
func signUp(t *testing.T, r *gin.Engine, username string) (authHeader string, userID uint) {
	t.Helper()
	creds := fmt.Sprintf(`{"username":%q,"password":"correct-horse"}`, username)

	if rr := do(t, r, "POST", "/register", creds, ""); rr.Code != http.StatusCreated {
		t.Fatalf("register %s: got %d (body %s)", username, rr.Code, rr.Body)
	}
	rr := do(t, r, "POST", "/login", creds, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("login %s: got %d (body %s)", username, rr.Code, rr.Body)
	}
	var login struct {
		Token string `json:"token"`
	}
	decode(t, rr.Body.Bytes(), &login)

	authHeader = "Bearer " + login.Token
	rr = do(t, r, "GET", "/auth/profile", "", authHeader)
	if rr.Code != http.StatusOK {
		t.Fatalf("profile %s: got %d (body %s)", username, rr.Code, rr.Body)
	}
	var profile struct {
		ID uint `json:"id"`
	}
	decode(t, rr.Body.Bytes(), &profile)
	if profile.ID == 0 {
		t.Fatalf("profile %s: token carried no user id (body %s)", username, rr.Body)
	}
	return authHeader, profile.ID
}

func decode(t *testing.T, body []byte, into any) {
	t.Helper()
	if err := json.Unmarshal(body, into); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
}

func TestConversationFlow(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	bob, bobID := signUp(t, r, "bob")

	// Alice makes a room. She is in it even though she named no members.
	rr := do(t, r, "POST", "/conversations", `{"title":"standup"}`, alice)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create: got %d, want %d (body %s)", rr.Code, http.StatusCreated, rr.Body)
	}
	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)
	if room.ID == 0 || room.Title != "standup" || len(room.Members) != 1 {
		t.Fatalf("create returned %+v, want one member and a real id", room)
	}

	// Bob is not in it yet, so for him it does not exist.
	path := fmt.Sprintf("/conversations/%d/messages", room.ID)
	if rr := do(t, r, "GET", path, "", bob); rr.Code != http.StatusNotFound {
		t.Fatalf("outsider history: got %d, want %d", rr.Code, http.StatusNotFound)
	}

	// Alice adds him.
	addPath := fmt.Sprintf("/conversations/%d/members", room.ID)
	body := fmt.Sprintf(`{"user_id":%d}`, bobID)
	if rr := do(t, r, "POST", addPath, body, alice); rr.Code != http.StatusCreated {
		t.Fatalf("add member: got %d, want %d (body %s)", rr.Code, http.StatusCreated, rr.Body)
	}
	if rr := do(t, r, "POST", addPath, body, alice); rr.Code != http.StatusConflict {
		t.Fatalf("add member twice: got %d, want %d", rr.Code, http.StatusConflict)
	}

	// Now the room shows up in his list, with both members.
	rr = do(t, r, "GET", "/conversations", "", bob)
	if rr.Code != http.StatusOK {
		t.Fatalf("list: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	var list struct {
		Conversations []conversationDTO `json:"conversations"`
	}
	decode(t, rr.Body.Bytes(), &list)
	if len(list.Conversations) != 1 || len(list.Conversations[0].Members) != 2 {
		t.Fatalf("list returned %+v, want one room with two members", list.Conversations)
	}

	// He can write, and Alice reads it back with his name on it.
	if rr := do(t, r, "POST", path, `{"content":"hello"}`, bob); rr.Code != http.StatusCreated {
		t.Fatalf("send: got %d, want %d (body %s)", rr.Code, http.StatusCreated, rr.Body)
	}
	rr = do(t, r, "GET", path, "", alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("history: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != 1 || page.Messages[0].Content != "hello" || page.Messages[0].Sender.Username != "bob" {
		t.Fatalf("history returned %+v, want one message from bob", page.Messages)
	}
	if page.NextBeforeID != nil {
		t.Fatalf("history: next_before_id = %v, want null on a short page", *page.NextBeforeID)
	}
}

// A room the caller is not in must answer exactly like a room that was never
// created. Anything else tells an outsider which ids are real.
func TestConversationHidesRoomsFromNonMembers(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")
	mallory, malloryID := signUp(t, r, "mallory")

	rr := do(t, r, "POST", "/conversations", `{"title":"private"}`, alice)
	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)

	real := fmt.Sprintf("%d", room.ID)
	missing := fmt.Sprintf("%d", room.ID+999)

	for _, id := range []string{real, missing} {
		cases := map[string]struct{ method, path, body string }{
			"read history": {"GET", "/conversations/" + id + "/messages", ""},
			"send":         {"POST", "/conversations/" + id + "/messages", `{"content":"hi"}`},
			"add member":   {"POST", "/conversations/" + id + "/members", fmt.Sprintf(`{"user_id":%d}`, malloryID)},
		}
		for name, tc := range cases {
			t.Run(name+" id="+id, func(t *testing.T) {
				rr := do(t, r, tc.method, tc.path, tc.body, mallory)
				if rr.Code != http.StatusNotFound {
					t.Fatalf("got %d, want %d (body %s)", rr.Code, http.StatusNotFound, rr.Body)
				}
			})
		}
	}

	// The room is also absent from her list.
	rr = do(t, r, "GET", "/conversations", "", mallory)
	var list struct {
		Conversations []conversationDTO `json:"conversations"`
	}
	decode(t, rr.Body.Bytes(), &list)
	if len(list.Conversations) != 0 {
		t.Fatalf("list returned %+v, want nothing", list.Conversations)
	}
}

// Resending the same client_msg_id must not double-post. The second call
// answers 200 (nothing was created) with the first message.
func TestSendMessageIsIdempotent(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")

	rr := do(t, r, "POST", "/conversations", `{"title":"retry"}`, alice)
	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)
	path := fmt.Sprintf("/conversations/%d/messages", room.ID)

	const body = `{"content":"only once","client_msg_id":"abc-123"}`

	rr = do(t, r, "POST", path, body, alice)
	if rr.Code != http.StatusCreated {
		t.Fatalf("first send: got %d, want %d (body %s)", rr.Code, http.StatusCreated, rr.Body)
	}
	var first messageDTO
	decode(t, rr.Body.Bytes(), &first)

	rr = do(t, r, "POST", path, body, alice)
	if rr.Code != http.StatusOK {
		t.Fatalf("retry: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	var second messageDTO
	decode(t, rr.Body.Bytes(), &second)
	if second.ID != first.ID {
		t.Fatalf("retry made message %d, want the first one (%d)", second.ID, first.ID)
	}

	rr = do(t, r, "GET", path, "", alice)
	var page messagePageDTO
	decode(t, rr.Body.Bytes(), &page)
	if len(page.Messages) != 1 {
		t.Fatalf("history holds %d messages, want 1", len(page.Messages))
	}
}

// Walking back with ?before_id= must return every message once, newest first.
func TestMessageHistoryPaginates(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")

	rr := do(t, r, "POST", "/conversations", `{"title":"long"}`, alice)
	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)
	path := fmt.Sprintf("/conversations/%d/messages", room.ID)

	const total = 5
	for i := 1; i <= total; i++ {
		body := fmt.Sprintf(`{"content":"m%d"}`, i)
		if rr := do(t, r, "POST", path, body, alice); rr.Code != http.StatusCreated {
			t.Fatalf("send %d: got %d (body %s)", i, rr.Code, rr.Body)
		}
	}

	var seen []string
	query := path + "?limit=2"
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("pagination did not stop")
		}
		rr := do(t, r, "GET", query, "", alice)
		if rr.Code != http.StatusOK {
			t.Fatalf("page %d: got %d (body %s)", pages, rr.Code, rr.Body)
		}
		var page messagePageDTO
		decode(t, rr.Body.Bytes(), &page)
		for _, m := range page.Messages {
			seen = append(seen, m.Content)
		}
		if page.NextBeforeID == nil {
			break
		}
		query = fmt.Sprintf("%s?limit=2&before_id=%d", path, *page.NextBeforeID)
	}

	want := []string{"m5", "m4", "m3", "m2", "m1"}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("paged through %v, want %v", seen, want)
	}
}

func TestConversationRoutesRejectBadInput(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")

	rr := do(t, r, "POST", "/conversations", `{"title":"checks"}`, alice)
	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)
	base := fmt.Sprintf("/conversations/%d", room.ID)

	cases := map[string]struct {
		method, path, body, auth string
		want                     int
	}{
		"no token": {"GET", "/conversations", "", "", http.StatusUnauthorized},
		"no title": {"POST", "/conversations", `{}`, alice, http.StatusBadRequest},
		"member does not exist": {"POST", "/conversations",
			`{"title":"ghosts","member_ids":[9999]}`, alice, http.StatusBadRequest},
		"empty message":    {"POST", base + "/messages", `{"content":""}`, alice, http.StatusBadRequest},
		"unknown user":     {"POST", base + "/members", `{"user_id":9999}`, alice, http.StatusNotFound},
		"bad before_id":    {"GET", base + "/messages?before_id=abc", "", alice, http.StatusBadRequest},
		"bad limit":        {"GET", base + "/messages?limit=0", "", alice, http.StatusBadRequest},
		"non-numeric room": {"GET", "/conversations/abc/messages", "", alice, http.StatusNotFound},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rr := do(t, r, tc.method, tc.path, tc.body, tc.auth)
			if rr.Code != tc.want {
				t.Fatalf("got %d, want %d (body %s)", rr.Code, tc.want, rr.Body)
			}
		})
	}
}

// The limit is capped, not refused, so one bad query cannot pull the whole
// table.
func TestMessageLimitIsCapped(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")

	rr := do(t, r, "POST", "/conversations", `{"title":"cap"}`, alice)
	var room conversationDTO
	decode(t, rr.Body.Bytes(), &room)

	path := fmt.Sprintf("/conversations/%d/messages?limit=100000", room.ID)
	if rr := do(t, r, "GET", path, "", alice); rr.Code != http.StatusOK {
		t.Fatalf("got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
}

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"go-chat-backend/internal/database"
)

// fakeDB is an in-memory database.Service so the HTTP layer can be tested
// without a Postgres container. The real store is exercised by the
// integration tests in internal/database.
type fakeDB struct {
	mu        sync.Mutex
	users     map[string]*database.User
	usersByID map[uint]*database.User
	nextID    uint
	healthy   bool

	// Rooms and their message log. memberIDs holds each room's members in the
	// order they joined; the store methods live in conversations_test.go.
	conversations map[uint]*database.Conversation
	memberIDs     map[uint][]uint
	messages      []database.Message
	nextConvID    uint
	nextMemberID  uint
	nextMsgID     uint

	// Login sessions, keyed by the hex of the token hash because a []byte
	// cannot be a map key. The methods live in refresh_test.go.
	sessions      map[string]*database.RefreshToken
	nextSessionID uint
}

func newFakeDB() *fakeDB {
	return &fakeDB{
		users:         map[string]*database.User{},
		usersByID:     map[uint]*database.User{},
		conversations: map[uint]*database.Conversation{},
		memberIDs:     map[uint][]uint{},
		sessions:      map[string]*database.RefreshToken{},
		healthy:       true,
	}
}

func (f *fakeDB) Health() map[string]string {
	if !f.healthy {
		return map[string]string{"status": "down", "error": "db down"}
	}
	return map[string]string{"status": "up", "message": "It's healthy"}
}

func (f *fakeDB) Migrate(context.Context) error { return nil }

func (f *fakeDB) CreateUser(_ context.Context, username, passwordHash string) (*database.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.users[username]; ok {
		return nil, database.ErrUserExists
	}
	f.nextID++
	u := &database.User{
		Model:        gorm.Model{ID: f.nextID},
		Username:     username,
		PasswordHash: passwordHash,
	}
	f.users[username] = u
	f.usersByID[u.ID] = u
	return u, nil
}

func (f *fakeDB) GetUserByUsername(_ context.Context, username string) (*database.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.users[username]
	if !ok {
		return nil, database.ErrUserNotFound
	}
	return u, nil
}

func (f *fakeDB) Close() error { return nil }

func newTestServer(t *testing.T) (*Server, *gin.Engine, *fakeDB) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db := newFakeDB()
	s := &Server{port: 8080, db: db, jwtKey: []byte("test-secret")}
	return s, s.RegisterRoutes(), db
}

func do(t *testing.T, r *gin.Engine, method, path, body, authHeader string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}

func TestRegisterLoginProfileFlow(t *testing.T) {
	_, r, _ := newTestServer(t)
	const creds = `{"username":"alice","password":"correct-horse"}`

	if rr := do(t, r, "POST", "/register", creds, ""); rr.Code != http.StatusCreated {
		t.Fatalf("register: got %d, want %d (body %s)", rr.Code, http.StatusCreated, rr.Body)
	}

	rr := do(t, r, "POST", "/login", creds, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("login: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if resp.Token == "" {
		t.Fatal("login returned an empty token")
	}

	rr = do(t, r, "GET", "/auth/profile", "", "Bearer "+resp.Token)
	if rr.Code != http.StatusOK {
		t.Fatalf("profile: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	if !strings.Contains(rr.Body.String(), `"alice"`) {
		t.Fatalf("profile: expected username in body, got %s", rr.Body)
	}
}

func TestRegisterRejectsDuplicateUsername(t *testing.T) {
	_, r, _ := newTestServer(t)
	const creds = `{"username":"bob","password":"correct-horse"}`

	do(t, r, "POST", "/register", creds, "")
	if rr := do(t, r, "POST", "/register", creds, ""); rr.Code != http.StatusConflict {
		t.Fatalf("duplicate register: got %d, want %d", rr.Code, http.StatusConflict)
	}
}

func TestRegisterRejectsShortPassword(t *testing.T) {
	_, r, _ := newTestServer(t)
	rr := do(t, r, "POST", "/register", `{"username":"carol","password":"short"}`, "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestLoginRejectsBadCredentials(t *testing.T) {
	_, r, _ := newTestServer(t)
	do(t, r, "POST", "/register", `{"username":"dave","password":"correct-horse"}`, "")

	cases := map[string]string{
		"wrong password": `{"username":"dave","password":"wrong-horse"}`,
		"unknown user":   `{"username":"nobody","password":"correct-horse"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if rr := do(t, r, "POST", "/login", body, ""); rr.Code != http.StatusUnauthorized {
				t.Fatalf("got %d, want %d", rr.Code, http.StatusUnauthorized)
			}
		})
	}
}

func TestHealthHandlerReportsDownDatabase(t *testing.T) {
	_, r, db := newTestServer(t)
	db.healthy = false

	if rr := do(t, r, "GET", "/health", "", ""); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

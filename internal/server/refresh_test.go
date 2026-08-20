package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/database"
)

// ---------------------------------------------------------------------------
// The session half of fakeDB. Sessions are keyed by the hex of the token hash,
// because a []byte cannot be a map key.
// ---------------------------------------------------------------------------

func (f *fakeDB) CreateRefreshToken(_ context.Context, userID uint, tokenHash []byte, expiresAt time.Time) (*database.RefreshToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextSessionID++
	s := &database.RefreshToken{
		ID:        f.nextSessionID,
		UserID:    userID,
		TokenHash: tokenHash,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now(),
	}
	f.sessions[hex.EncodeToString(tokenHash)] = s
	return s, nil
}

func (f *fakeDB) GetValidRefreshToken(_ context.Context, tokenHash []byte) (*database.RefreshToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sessions[hex.EncodeToString(tokenHash)]
	if !ok || s.RevokedAt != nil || !s.ExpiresAt.After(time.Now()) {
		return nil, database.ErrRefreshTokenNotFound
	}
	out := *s
	if u, ok := f.usersByID[s.UserID]; ok {
		out.User = *u
	}
	return &out, nil
}

func (f *fakeDB) RevokeRefreshToken(_ context.Context, tokenHash []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if s, ok := f.sessions[hex.EncodeToString(tokenHash)]; ok && s.RevokedAt == nil {
		now := time.Now()
		s.RevokedAt = &now
	}
	return nil
}

func (f *fakeDB) PurgeExpiredRefreshTokens(_ context.Context, userID uint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for key, s := range f.sessions {
		if s.UserID == userID && (s.RevokedAt != nil || !s.ExpiresAt.After(time.Now())) {
			delete(f.sessions, key)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// doWithCookie is do() plus a refresh cookie, which is how every session route
// is really called.
func doWithCookie(t *testing.T, r *gin.Engine, method, path, body, cookie string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: refreshCookie, Value: cookie})
	}
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}

// refreshCookieFrom pulls the refresh token out of a Set-Cookie header.
func refreshCookieFrom(t *testing.T, rr *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rr.Result().Cookies() {
		if c.Name == refreshCookie {
			return c
		}
	}
	t.Fatalf("no %s cookie in the response (headers %v)", refreshCookie, rr.Header())
	return nil
}

// loginFor registers a user, logs in, and hands back both halves of the login.
func loginFor(t *testing.T, r *gin.Engine, username string) (accessToken string, refresh *http.Cookie) {
	t.Helper()
	creds := fmt.Sprintf(`{"username":%q,"password":"correct-horse"}`, username)

	if rr := do(t, r, "POST", "/register", creds, ""); rr.Code != http.StatusCreated {
		t.Fatalf("register %s: got %d (body %s)", username, rr.Code, rr.Body)
	}
	rr := do(t, r, "POST", "/login", creds, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("login %s: got %d (body %s)", username, rr.Code, rr.Body)
	}
	var resp struct {
		Token string `json:"token"`
	}
	decode(t, rr.Body.Bytes(), &resp)
	return resp.Token, refreshCookieFrom(t, rr)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// The cookie has to be unreadable by JavaScript and narrow in scope, or it is
// not doing the job it was chosen for.
func TestLoginSetsHardenedRefreshCookie(t *testing.T) {
	_, r, _ := newTestServer(t)
	_, cookie := loginFor(t, r, "alice")

	if cookie.Value == "" {
		t.Fatal("refresh cookie is empty")
	}
	if !cookie.HttpOnly {
		t.Error("refresh cookie is readable by JavaScript; an XSS bug would take the whole login")
	}
	if cookie.Path != refreshCookiePath {
		t.Errorf("cookie path = %q, want %q so it is not sent with every message", cookie.Path, refreshCookiePath)
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Errorf("cookie SameSite = %v, want Lax", cookie.SameSite)
	}
	if cookie.MaxAge != int(refreshTTL.Seconds()) {
		t.Errorf("cookie MaxAge = %d, want %d (7 days)", cookie.MaxAge, int(refreshTTL.Seconds()))
	}

	// The access token must not be in the cookie, and the refresh token must
	// not be in the body.
	if strings.Count(cookie.Value, ".") == 2 {
		t.Error("the refresh cookie looks like a JWT; it should be opaque random bytes")
	}
}

// The whole point: an expired access token does not mean a new password prompt.
func TestRefreshGivesANewAccessToken(t *testing.T) {
	_, r, _ := newTestServer(t)
	first, cookie := loginFor(t, r, "alice")

	// A second apart, so the new token's iat/exp really differ from the first.
	time.Sleep(time.Second)

	rr := doWithCookie(t, r, "POST", "/auth/refresh", "", cookie.Value)
	if rr.Code != http.StatusOK {
		t.Fatalf("refresh: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	var resp struct {
		Token     string `json:"token"`
		ExpiresIn int    `json:"expires_in"`
	}
	decode(t, rr.Body.Bytes(), &resp)

	if resp.Token == "" {
		t.Fatal("refresh returned an empty token")
	}
	if resp.Token == first {
		t.Error("refresh returned the same access token; it should be freshly signed")
	}
	if resp.ExpiresIn != int(tokenTTL.Seconds()) {
		t.Errorf("expires_in = %d, want %d", resp.ExpiresIn, int(tokenTTL.Seconds()))
	}

	// And the new token actually works.
	if rr := do(t, r, "GET", "/auth/profile", "", "Bearer "+resp.Token); rr.Code != http.StatusOK {
		t.Fatalf("profile with the refreshed token: got %d (body %s)", rr.Code, rr.Body)
	}
}

// No rotation was a deliberate choice: two tabs must be able to refresh
// without one of them killing the other.
func TestRefreshDoesNotRotateTheToken(t *testing.T) {
	_, r, _ := newTestServer(t)
	_, cookie := loginFor(t, r, "alice")

	for i := 1; i <= 3; i++ {
		rr := doWithCookie(t, r, "POST", "/auth/refresh", "", cookie.Value)
		if rr.Code != http.StatusOK {
			t.Fatalf("refresh %d: got %d, want %d (body %s)", i, rr.Code, http.StatusOK, rr.Body)
		}
		if len(rr.Result().Cookies()) != 0 {
			t.Errorf("refresh %d set a cookie; the token was chosen not to rotate", i)
		}
	}
}

// Logging out must actually end the session, not only clear the browser's copy.
func TestLogoutEndsTheSession(t *testing.T) {
	_, r, _ := newTestServer(t)
	_, cookie := loginFor(t, r, "alice")

	rr := doWithCookie(t, r, "POST", "/auth/logout", "", cookie.Value)
	if rr.Code != http.StatusOK {
		t.Fatalf("logout: got %d, want %d (body %s)", rr.Code, http.StatusOK, rr.Body)
	}
	// The browser is told to drop it.
	if cleared := refreshCookieFrom(t, rr); cleared.MaxAge >= 0 || cleared.Value != "" {
		t.Errorf("logout did not clear the cookie: value=%q MaxAge=%d", cleared.Value, cleared.MaxAge)
	}

	// And, more importantly, the token is dead on the server even if someone
	// kept a copy of it.
	if rr := doWithCookie(t, r, "POST", "/auth/refresh", "", cookie.Value); rr.Code != http.StatusUnauthorized {
		t.Fatalf("refresh after logout: got %d, want %d — the session is still alive", rr.Code, http.StatusUnauthorized)
	}

	// Logging out twice is not an error.
	if rr := doWithCookie(t, r, "POST", "/auth/logout", "", cookie.Value); rr.Code != http.StatusOK {
		t.Fatalf("second logout: got %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestRefreshRejectsBadSessions(t *testing.T) {
	_, r, db := newTestServer(t)
	_, cookie := loginFor(t, r, "alice")

	t.Run("no cookie", func(t *testing.T) {
		if rr := doWithCookie(t, r, "POST", "/auth/refresh", "", ""); rr.Code != http.StatusUnauthorized {
			t.Fatalf("got %d, want %d", rr.Code, http.StatusUnauthorized)
		}
	})

	t.Run("made up token", func(t *testing.T) {
		if rr := doWithCookie(t, r, "POST", "/auth/refresh", "", "not-a-real-token"); rr.Code != http.StatusUnauthorized {
			t.Fatalf("got %d, want %d", rr.Code, http.StatusUnauthorized)
		}
	})

	t.Run("expired session", func(t *testing.T) {
		// Age the stored session rather than waiting seven days.
		db.mu.Lock()
		for _, s := range db.sessions {
			s.ExpiresAt = time.Now().Add(-time.Minute)
		}
		db.mu.Unlock()

		if rr := doWithCookie(t, r, "POST", "/auth/refresh", "", cookie.Value); rr.Code != http.StatusUnauthorized {
			t.Fatalf("got %d, want %d", rr.Code, http.StatusUnauthorized)
		}
	})
}

// A refresh cookie is not a way into the API. It only buys a new access token.
func TestRefreshCookieCannotReplaceTheAccessToken(t *testing.T) {
	_, r, _ := newTestServer(t)
	_, cookie := loginFor(t, r, "alice")

	for _, path := range []string{"/auth/profile", "/conversations"} {
		if rr := doWithCookie(t, r, "GET", path, "", cookie.Value); rr.Code != http.StatusUnauthorized {
			t.Errorf("GET %s with only a refresh cookie: got %d, want %d", path, rr.Code, http.StatusUnauthorized)
		}
	}
}

// Two logins are two sessions. Logging out of one must not touch the other.
func TestLogoutOnOneDeviceKeepsTheOther(t *testing.T) {
	_, r, _ := newTestServer(t)
	const creds = `{"username":"alice","password":"correct-horse"}`
	do(t, r, "POST", "/register", creds, "")

	phone := refreshCookieFrom(t, do(t, r, "POST", "/login", creds, ""))
	laptop := refreshCookieFrom(t, do(t, r, "POST", "/login", creds, ""))
	if phone.Value == laptop.Value {
		t.Fatal("two logins produced the same refresh token")
	}

	if rr := doWithCookie(t, r, "POST", "/auth/logout", "", phone.Value); rr.Code != http.StatusOK {
		t.Fatalf("logout on the phone: got %d", rr.Code)
	}

	if rr := doWithCookie(t, r, "POST", "/auth/refresh", "", phone.Value); rr.Code != http.StatusUnauthorized {
		t.Errorf("the phone can still refresh: got %d", rr.Code)
	}
	if rr := doWithCookie(t, r, "POST", "/auth/refresh", "", laptop.Value); rr.Code != http.StatusOK {
		t.Errorf("the laptop was logged out too: got %d (body %s)", rr.Code, rr.Body)
	}
}

// The server must never be able to hand the plain token back, however the
// database leaks. Only its hash is kept.
func TestOnlyTheHashIsStored(t *testing.T) {
	_, r, db := newTestServer(t)
	_, cookie := loginFor(t, r, "alice")

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, s := range db.sessions {
		if string(s.TokenHash) == cookie.Value {
			t.Fatal("the plain refresh token was stored")
		}
		if len(s.TokenHash) != 32 {
			t.Errorf("stored hash is %d bytes, want 32 (SHA-256)", len(s.TokenHash))
		}
	}
}

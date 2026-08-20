package server

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/database"
)

const (
	// refreshTTL is how long a login lasts. A refresh token is not rotated, so
	// this is also the longest a stolen one stays useful. Seven days rather
	// than thirty for exactly that reason.
	refreshTTL = 7 * 24 * time.Hour

	// refreshCookie is scoped to /auth, so it is never sent along with a
	// message or a history read. The only routes that need it live there.
	refreshCookie     = "refresh_token"
	refreshCookiePath = "/auth"

	// refreshTokenBytes is the size of the random token, before base64. 256
	// bits is far past anything that can be guessed.
	refreshTokenBytes = 32
)

// newRefreshToken makes a fresh token and its hash. The plain token goes to
// the client once, in a cookie, and is never written down here; the hash is
// what the database keeps.
func newRefreshToken() (token string, hash []byte, err error) {
	raw := make([]byte, refreshTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("read random bytes: %w", err)
	}

	token = base64.RawURLEncoding.EncodeToString(raw)
	return token, hashRefreshToken(token), nil
}

// hashRefreshToken is plain SHA-256, not bcrypt. bcrypt is slow on purpose to
// make guessing a human password expensive; this input is 256 random bits, so
// there is nothing to guess, and the slowness would only tax every refresh.
func hashRefreshToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// setRefreshCookie hands the token to the browser.
//
// httpOnly keeps JavaScript from reading it, so an XSS bug on the page cannot
// walk off with a week-long login. Secure is off only for local development,
// because a browser refuses a Secure cookie over plain http.
func setRefreshCookie(c *gin.Context, token string) {
	// Lax is enough here: the frontend on :5173 and the API on :8080 are the
	// same site, because the port is not part of a site. It still blocks the
	// cookie on a cross-site request, which is what CSRF needs.
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(
		refreshCookie,
		token,
		int(refreshTTL.Seconds()),
		refreshCookiePath,
		"", // host-only: the browser sends it back to this host and no other
		secureCookies(),
		true, // httpOnly
	)
}

// clearRefreshCookie removes it, by setting the same cookie with no value and
// an age of zero. The path has to match the one it was set with, or the
// browser keeps the original.
func clearRefreshCookie(c *gin.Context) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(refreshCookie, "", -1, refreshCookiePath, "", secureCookies(), true)
}

func secureCookies() bool {
	return os.Getenv("APP_ENV") != "local"
}

// issueRefreshSession opens a session and puts the cookie on the response. It
// is called at login.
func (s *Server) issueRefreshSession(c *gin.Context, userID uint) error {
	token, hash, err := newRefreshToken()
	if err != nil {
		return err
	}

	// Clear this user's dead sessions while we are here. It is one indexed
	// delete, and it means the table never needs a cron job.
	//
	// A failure here is logged and ignored on purpose. This is housekeeping,
	// not part of logging in, and refusing someone the door because we could
	// not take out the rubbish would be the wrong trade. The rows are harmless
	// until the next attempt clears them.
	if err := s.db.PurgeExpiredRefreshTokens(c.Request.Context(), userID); err != nil {
		log.Printf("could not purge expired sessions for user %d: %v", userID, err)
	}

	if _, err := s.db.CreateRefreshToken(c.Request.Context(), userID, hash, time.Now().Add(refreshTTL)); err != nil {
		return err
	}

	setRefreshCookie(c, token)
	return nil
}

// RefreshHandler handles POST /auth/refresh.
//
// This route must not sit behind AuthMiddleware. It is called precisely when
// the access token has expired, so demanding a valid one would make it useless.
// The refresh cookie is the credential here.
func (s *Server) RefreshHandler(c *gin.Context) {
	token, err := c.Cookie(refreshCookie)
	if err != nil || token == "" {
		unauthorizedSession(c)
		return
	}

	session, err := s.db.GetValidRefreshToken(c.Request.Context(), hashRefreshToken(token))
	if errors.Is(err, database.ErrRefreshTokenNotFound) {
		// Missing, logged out or expired. The client's cookie is worthless, so
		// clear it rather than let the browser keep sending it for a week.
		clearRefreshCookie(c)
		unauthorizedSession(c)
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not read session"})
		return
	}

	accessToken, err := s.signAccessToken(session.UserID, session.User.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not issue token"})
		return
	}

	// The refresh token is deliberately not replaced. Without rotation two
	// browser tabs can refresh at the same moment without one invalidating the
	// other, at the cost of a stolen token staying good for its whole week.
	c.JSON(http.StatusOK, gin.H{"token": accessToken, "expires_in": int(tokenTTL.Seconds())})
}

// LogoutHandler handles POST /auth/logout.
//
// It ends the session, so no new access token can be minted. The access token
// the caller already holds keeps working until it expires — at most 15 more
// minutes. That gap is the price of a stateless access token, and closing it
// would mean a database lookup on every single request.
func (s *Server) LogoutHandler(c *gin.Context) {
	if token, err := c.Cookie(refreshCookie); err == nil && token != "" {
		if err := s.db.RevokeRefreshToken(c.Request.Context(), hashRefreshToken(token)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not end session"})
			return
		}
	}

	// Always the same answer, cookie or no cookie, real token or invented one.
	// Logging out is not a place to tell anyone what exists.
	clearRefreshCookie(c)
	c.JSON(http.StatusOK, gin.H{"message": "Logged out"})
}

func unauthorizedSession(c *gin.Context) {
	c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired session"})
}

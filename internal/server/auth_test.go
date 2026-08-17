package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestBearerToken(t *testing.T) {
	cases := []struct {
		header    string
		wantToken string
		wantOK    bool
	}{
		{"Bearer abc.def.ghi", "abc.def.ghi", true},
		{"  Bearer abc.def.ghi  ", "abc.def.ghi", true},
		{"abc.def.ghi", "abc.def.ghi", true}, // bare token, no scheme
		{"", "", false},
		{"   ", "", false},
		{"Bearer ", "", false},
		{"Basic dXNlcjpwYXNz", "", false},
	}

	for _, tc := range cases {
		token, ok := bearerToken(tc.header)
		if ok != tc.wantOK || token != tc.wantToken {
			t.Errorf("bearerToken(%q) = (%q, %v), want (%q, %v)",
				tc.header, token, ok, tc.wantToken, tc.wantOK)
		}
	}
}

func TestAuthMiddlewareRejectsBadTokens(t *testing.T) {
	_, r, _ := newTestServer(t)

	// Signed with a different key than the server's.
	wrongKey := signToken(t, []byte("some-other-secret"), time.Now().Add(time.Hour))
	// Signed with the right key but already expired.
	expired := signToken(t, []byte("test-secret"), time.Now().Add(-time.Hour))
	// Valid claims, but "alg": "none" — must not be accepted.
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone, &Claims{
		Username: "mallory",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none-alg token: %v", err)
	}

	cases := map[string]string{
		"missing header": "",
		"garbage":        "Bearer not-a-jwt",
		"wrong key":      "Bearer " + wrongKey,
		"expired":        "Bearer " + expired,
		"alg none":       "Bearer " + unsigned,
	}

	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			rr := do(t, r, "GET", "/auth/profile", "", header)
			if rr.Code != http.StatusUnauthorized {
				t.Fatalf("got %d, want %d (body %s)", rr.Code, http.StatusUnauthorized, rr.Body)
			}
		})
	}
}

func signToken(t *testing.T, key []byte, expires time.Time) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, &Claims{
		Username: "mallory",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expires),
		},
	}).SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return token
}

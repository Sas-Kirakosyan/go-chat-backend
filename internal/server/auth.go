// internal/server/auth.go
package server

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"go-chat-backend/internal/database"
)

//this file contains the authentication logic, including token validation and user identification

const tokenTTL = 15 * time.Minute

// dummyHash is compared against when the username does not exist, so that a
// bad username costs the same time as a bad password and cannot be
// distinguished by an attacker probing for valid accounts.
var dummyHash = func() []byte {
	h, err := bcrypt.GenerateFromPassword([]byte("no-such-user"), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	return h
}()

type Credentials struct {
	Username string `json:"username" binding:"required,min=3,max=64"`
	Password string `json:"password" binding:"required,min=8,max=72"`
}

// Claims carries the user id as well as the name. Every conversation handler
// needs the id, and reading it from the signed token saves a SELECT on each
// request. The token is already trusted at that point: the signature check has
// passed, so the id inside it is one the server put there at login.
type Claims struct {
	UserID   uint   `json:"user_id"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

func (s *Server) RegisterHandler(c *gin.Context) {
	var creds Credentials
	if err := c.ShouldBindJSON(&creds); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(creds.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not hash password"})
		return
	}

	_, err = s.db.CreateUser(c.Request.Context(), creds.Username, string(hash))
	if errors.Is(err, database.ErrUserExists) {
		c.JSON(http.StatusConflict, gin.H{"error": "Username already taken"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not create user"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"message": "User registered"})
}

func (s *Server) LoginHandler(c *gin.Context) {
	var creds Credentials
	if err := c.ShouldBindJSON(&creds); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	user, err := s.db.GetUserByUsername(c.Request.Context(), creds.Username)
	if err != nil && !errors.Is(err, database.ErrUserNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not look up user"})
		return
	}

	storedHash := dummyHash
	if user != nil {
		storedHash = []byte(user.PasswordHash)
	}
	if bcrypt.CompareHashAndPassword(storedHash, []byte(creds.Password)) != nil || user == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}

	tokenString, err := s.signAccessToken(user.ID, user.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not issue token"})
		return
	}

	// The long-lived half of the login. It goes back as an httpOnly cookie, so
	// the page's JavaScript never touches it.
	if err := s.issueRefreshSession(c, user.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not start session"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": tokenString, "expires_in": int(tokenTTL.Seconds())})
}

// signAccessToken mints the short-lived JWT. Login and refresh both need one,
// and they must mint it the same way.
func (s *Server) signAccessToken(userID uint, username string) (string, error) {
	claims := &Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(tokenTTL)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtKey)
}

// errInvalidToken is what parseAccessToken returns for anything wrong with the
// token: a bad signature, the wrong algorithm, an expired token, a malformed
// string. The caller answers 401 to all of them, and saying which one it was
// would only help someone forging tokens.
var errInvalidToken = errors.New("invalid access token")

// parseAccessToken checks the signature and the expiry, and hands back what
// the token says.
//
// AuthMiddleware and the WebSocket handler both call it, so there is exactly
// one place that decides a token is good.
func (s *Server) parseAccessToken(tokenStr string) (*Claims, error) {
	claims := &Claims{}

	// WithValidMethods pins HS256. Without it a forged token could name "none"
	// as its algorithm and be accepted with no signature at all.
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		return s.jwtKey, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))

	if err != nil || !token.Valid {
		return nil, errInvalidToken
	}
	return claims, nil
}

func (s *Server) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenStr, ok := bearerToken(c.GetHeader("Authorization"))
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing token"})
			return
		}

		claims, err := s.parseAccessToken(tokenStr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token"})
			return
		}

		c.Set("userID", claims.UserID)
		c.Set("username", claims.Username)
		c.Next()
	}
}

// currentUser reads the caller's identity, which AuthMiddleware put on the
// context after the token check. Handlers behind that middleware can rely on
// both values being there.
func currentUser(c *gin.Context) (id uint, username string) {
	if v, ok := c.Get("userID"); ok {
		id, _ = v.(uint)
	}
	if v, ok := c.Get("username"); ok {
		username, _ = v.(string)
	}
	return id, username
}

// bearerToken pulls the token out of an "Authorization: Bearer <token>" header.
// A bare token with no scheme is accepted too, which keeps older clients working.
func bearerToken(header string) (string, bool) {
	fields := strings.Fields(header)
	switch len(fields) {
	case 1:
		// A bare token, unless the client sent the scheme with nothing after it.
		if strings.EqualFold(fields[0], "Bearer") {
			return "", false
		}
		return fields[0], true
	case 2:
		if !strings.EqualFold(fields[0], "Bearer") {
			return "", false
		}
		return fields[1], true
	default:
		return "", false
	}
}

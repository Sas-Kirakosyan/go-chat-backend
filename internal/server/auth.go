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

type Claims struct {
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

	claims := &Claims{
		Username: user.Username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   user.Username,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(tokenTTL)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.jwtKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not issue token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": tokenString})
}

func (s *Server) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenStr, ok := bearerToken(c.GetHeader("Authorization"))
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing token"})
			return
		}

		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
			return s.jwtKey, nil
		}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token"})
			return
		}

		c.Set("username", claims.Username)
		c.Next()
	}
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

package server

import (
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/metrics"
)

// trustedProxies reads TRUSTED_PROXIES: a comma separated list of addresses or
// CIDR ranges that sit in front of this node.
//
// In compose it is the Docker bridge network, because that is where nginx is.
// An empty result means no proxy is trusted at all, which is the safe default:
// a forged header is then simply ignored.
func trustedProxies() []string {
	raw := strings.TrimSpace(os.Getenv("TRUSTED_PROXIES"))
	if raw == "" {
		return nil
	}

	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

//which URL goes to which function

func (s *Server) RegisterRoutes() *gin.Engine {
	limits := s.limits.orDefaults()
	authLimiter := newRateLimiter(limits.authRPS, limits.authBurst, limits.now)
	apiLimiter := newRateLimiter(limits.apiRPS, limits.apiBurst, limits.now)

	// This is gin.Default() written out, because both halves of it are
	// replaced: the logger writes structured lines through slog, and recovery
	// reports a panic to the log and to /metrics instead of printing it.
	//
	// The order is the order a request passes through them, and it is chosen:
	//
	//  1. observeRequests  outermost, so even a refused request is counted
	//  2. requestID        everything after this can log with the id
	//  3. requestLogger    its work happens on the way OUT, so it sees the
	//                      status recovery set
	//  4. recovery         nearest the handlers, which is where panics come from
	r := gin.New()

	// Whose X-Forwarded-For do we believe?
	//
	// gin trusts every proxy by default, which means it trusts every client:
	// anyone may send "X-Forwarded-For: 1.2.3.4" and the per-IP rate limit from
	// Stage 2 then counts a different caller on every request. The limit was
	// real, and one header walked around it.
	//
	// The fix needs an address to trust, and Stage 3 is what created one. With
	// TRUSTED_PROXIES unset the list is empty, which means "trust nobody": the
	// client IP is the TCP address, and headers are ignored. That is the right
	// answer when the server is reached directly, as it is in `make run`.
	if err := r.SetTrustedProxies(trustedProxies()); err != nil {
		slog.Error("bad TRUSTED_PROXIES, trusting no proxy", "err", err)
		_ = r.SetTrustedProxies(nil)
	}

	r.Use(observeRequests())
	r.Use(s.requestID())
	r.Use(s.requestLogger())
	r.Use(s.recovery())

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowHeaders:     []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true,
	}))

	// Operations endpoints. None of them is rate limited: a probe that is
	// refused looks exactly like a node that is dead, and the monitoring would
	// take down a healthy server.
	//
	// /metrics is open here because everything runs on one machine. Once there
	// is a cluster it belongs on the internal network only — the numbers say
	// how many users are online and how the service is holding up, which is not
	// something to hand to the internet.
	r.GET("/livez", s.livezHandler)
	r.GET("/readyz", s.readyzHandler)
	r.GET("/health", s.healthHandler)
	r.GET("/metrics", gin.WrapH(metrics.Handler()))

	// Guessing a password and making accounts in bulk are the two things worth
	// slowing down here, and both are limited by IP, because there is no user
	// yet — that is the part being guessed.
	byIP := rateLimit(authLimiter, "auth", ipKey)
	r.POST("/register", byIP, s.RegisterHandler)
	r.POST("/login", byIP, s.LoginHandler)

	// The socket sits outside the auth group on purpose. A browser cannot put
	// an Authorization header on a WebSocket, so the token arrives as ?token=
	// and the handler checks it itself, before the upgrade.
	//
	// It is limited by IP, because at the moment the limiter runs nobody has
	// proved who they are yet — and opening sockets in a loop is cheap for a
	// client and expensive for us: two goroutines and a kernel buffer each.
	//
	// It uses the API limiter, not the login one. The login limit is low
	// because each attempt burns a bcrypt hash on purpose; a handshake only
	// parses a JWT, which costs microseconds. Charging it the login rate was
	// the first version, and it refused 68 of 100 sockets from one machine —
	// the same thing an office behind one NAT address would have seen.
	r.GET("/ws", rateLimit(apiLimiter, "ws", ipKey), s.WSHandler)

	// Two groups share the /auth prefix on purpose, because they are guarded
	// by two different things.
	//
	// These two authenticate with the refresh cookie, not with the access
	// token, so they must NOT sit behind AuthMiddleware: refresh is called
	// exactly when the access token has expired, and requiring a valid one
	// would make it useless.
	session := r.Group("/auth")
	session.Use(byIP)
	{
		session.POST("/refresh", s.RefreshHandler)
		session.POST("/logout", s.LogoutHandler)
	}

	auth := r.Group("/auth")
	auth.Use(s.AuthMiddleware())
	// The limiter comes after the auth check, so it can key on the user id.
	// That is the whole reason for having two limiters: here we know who it is,
	// and one noisy client must not use up the allowance of everybody else
	// behind the same office IP.
	auth.Use(rateLimit(apiLimiter, "api", userKey))
	auth.GET("/profile", func(c *gin.Context) {
		id, username := currentUser(c)
		c.JSON(http.StatusOK, gin.H{"id": id, "user": username})
	})

	// Rooms live under /conversations, not under /auth. Auth is the mechanism
	// that guards them, not the thing they belong to, so it is the same
	// middleware on a separate group.
	//
	// Every write goes through these routes. The WebSocket only pushes out what
	// was already stored here, so validation, persistence and the idempotency
	// key stay in one place and the hub stays a plain fan-out.
	conversations := r.Group("/conversations")
	conversations.Use(s.AuthMiddleware())
	conversations.Use(rateLimit(apiLimiter, "api", userKey))
	{
		conversations.POST("", s.CreateConversationHandler)
		conversations.GET("", s.ListConversationsHandler)
		conversations.POST("/:id/members", s.AddMemberHandler)
		conversations.POST("/:id/messages", s.SendMessageHandler)
		conversations.GET("/:id/messages", s.ListMessagesHandler)
		conversations.GET("/:id/presence", s.PresenceHandler)
	}

	return r
}

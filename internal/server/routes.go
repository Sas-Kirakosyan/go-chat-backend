package server

import (
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

//which URL goes to which function

func (s *Server) healthHandler(c *gin.Context) {
	stats := s.db.Health()
	if stats["status"] != "up" {
		c.JSON(http.StatusServiceUnavailable, stats)
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (s *Server) RegisterRoutes() *gin.Engine {
	// This is gin.Default() written out, with one change: the request logger
	// skips /ws.
	//
	// A socket carries its access token in the query string, because a browser
	// cannot put it in a header. Logging the request line would then write that
	// token into the log on every connect, which is exactly the leak the choice
	// was trying to keep small. The connect is still visible — the ws package
	// logs what matters about a socket — just without the credential.
	r := gin.New()
	r.Use(gin.LoggerWithConfig(gin.LoggerConfig{SkipPaths: []string{"/ws"}}))
	r.Use(gin.Recovery())

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowHeaders:     []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true,
	}))

	r.GET("/health", s.healthHandler)
	// r.GET("/", s.HelloWorldHandler) // optional

	r.POST("/register", s.RegisterHandler)
	r.POST("/login", s.LoginHandler)

	// The socket sits outside the auth group on purpose. A browser cannot put
	// an Authorization header on a WebSocket, so the token arrives as ?token=
	// and the handler checks it itself, before the upgrade.
	r.GET("/ws", s.WSHandler)

	// Two groups share the /auth prefix on purpose, because they are guarded
	// by two different things.
	//
	// These two authenticate with the refresh cookie, not with the access
	// token, so they must NOT sit behind AuthMiddleware: refresh is called
	// exactly when the access token has expired, and requiring a valid one
	// would make it useless.
	session := r.Group("/auth")
	{
		session.POST("/refresh", s.RefreshHandler)
		session.POST("/logout", s.LogoutHandler)
	}

	auth := r.Group("/auth")
	auth.Use(s.AuthMiddleware())
	auth.GET("/profile", func(c *gin.Context) {
		id, username := currentUser(c)
		c.JSON(http.StatusOK, gin.H{"id": id, "user": username})
	})

	// Rooms live under /conversations, not under /auth. Auth is the mechanism
	// that guards them, not the thing they belong to, so it is the same
	// middleware on a separate group.
	//
	// Every write goes through these routes. When the WebSocket arrives it
	// will only push out what was already stored here, so validation,
	// persistence and the idempotency key stay in one place and the hub stays
	// a plain fan-out.
	conversations := r.Group("/conversations")
	conversations.Use(s.AuthMiddleware())
	{
		conversations.POST("", s.CreateConversationHandler)
		conversations.GET("", s.ListConversationsHandler)
		conversations.POST("/:id/members", s.AddMemberHandler)
		conversations.POST("/:id/messages", s.SendMessageHandler)
		conversations.GET("/:id/messages", s.ListMessagesHandler)
	}

	return r
}

// default part
// func (s *Server) RegisterRoutes() http.Handler {
// 	r := gin.Default()

// 	r.Use(cors.New(cors.Config{
// 		AllowOrigins:     []string{"http://localhost:5173"}, // Add your frontend URL
// 		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
// 		AllowHeaders:     []string{"Accept", "Authorization", "Content-Type"},
// 		AllowCredentials: true, // Enable cookies/auth
// 	}))

// 	r.GET("/", s.HelloWorldHandler)

// 	r.GET("/health", s.healthHandler)

// 	return r
// }

// func (s *Server) HelloWorldHandler(c *gin.Context) {
// 	resp := make(map[string]string)
// 	resp["message"] = "Hello World"

// 	c.JSON(http.StatusOK, resp)
// }

// func (s *Server) healthHandler(c *gin.Context) {
// 	c.JSON(http.StatusOK, s.db.Health())
// }

//default part

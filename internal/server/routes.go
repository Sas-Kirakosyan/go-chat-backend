package server

import (
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func (s *Server) healthHandler(c *gin.Context) {
	stats := s.db.Health()
	if stats["status"] != "up" {
		c.JSON(http.StatusServiceUnavailable, stats)
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (s *Server) RegisterRoutes() *gin.Engine {
	r := gin.Default()

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

	auth := r.Group("/auth")
	auth.Use(s.AuthMiddleware())
	auth.GET("/profile", func(c *gin.Context) {
		username, _ := c.Get("username")
		c.JSON(200, gin.H{"user": username})
	})

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

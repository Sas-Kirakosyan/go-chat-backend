package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"

	"go-chat-backend/internal/database"
)

const defaultPort = 8080

type Server struct {
	port int

	db database.Service

	jwtKey []byte
}

func NewServer() *http.Server {
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil || port <= 0 {
		log.Printf("PORT not set or invalid, falling back to %d", defaultPort)
		port = defaultPort
	}

	jwtKey := os.Getenv("JWT_SECRET")
	if jwtKey == "" {
		log.Fatal("JWT_SECRET is not set; add it to your .env before starting the server")
	}

	db := database.New()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Migrate(ctx); err != nil {
		log.Fatalf("could not prepare database: %v", err)
	}

	NewServer := &Server{
		port: port,

		db: db,

		jwtKey: []byte(jwtKey),
	}

	// Declare Server config
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", NewServer.port),
		Handler:           NewServer.RegisterRoutes(),
		IdleTimeout:       time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,

		// WriteTimeout must stay 0 (no deadline). It is an absolute deadline
		// measured from the start of the request, so any non-zero value cuts
		// long-lived streaming responses (SSE) off mid-flight. Per-request
		// limits belong on the handler's context instead.
		WriteTimeout: 0,
	}

	return server
}

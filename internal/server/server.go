package server

import (
	"context"
	"errors"
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

// App owns everything the process has to shut down: the HTTP server, and the
// database behind it.
//
// main only starts it and stops it. Every later stage adds one more thing that
// must be closed in the right order — the hub and its open sockets, Redis, the
// broker — and that list belongs next to the code that opened them, not in
// main.
type App struct {
	HTTP *http.Server

	db database.Service
}

// New builds the app: config, database, migrations, routes, HTTP server.
//
// It exits the process on anything it cannot recover from. A server with no
// JWT secret, or with a database it failed to migrate, is not worth starting.
func New() *App {
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

	s := &Server{
		port: port,

		db: db,

		jwtKey: []byte(jwtKey),
	}

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           s.RegisterRoutes(),
		IdleTimeout:       time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,

		// WriteTimeout must stay 0 (no deadline). It is an absolute deadline
		// measured from the start of the request, so any non-zero value cuts
		// long-lived streaming responses (SSE, and the WebSocket to come) off
		// mid-flight. Per-request limits belong on the handler context instead.
		WriteTimeout: 0,
	}

	return &App{HTTP: httpServer, db: db}
}

// Shutdown stops the app from the outside in: first the traffic, then what the
// traffic needed.
//
// http.Server.Shutdown does not kill open requests. It refuses new ones and
// waits for the ones already running, and those are still reading and writing
// rows. Closing the pool first would break exactly the requests we are trying
// to let finish.
//
// The database is closed even when the HTTP step fails. A request that never
// returns makes Shutdown give back context.DeadlineExceeded, and the process
// is exiting either way, so keeping the pool open helps nobody.
//
// The caller owns the deadline. This method does not invent one.
func (a *App) Shutdown(ctx context.Context) error {
	httpErr := a.HTTP.Shutdown(ctx)
	dbErr := a.db.Close()

	// errors.Join keeps both problems instead of hiding one, and returns nil
	// when both are nil.
	return errors.Join(httpErr, dbErr)
}

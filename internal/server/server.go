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
	"go-chat-backend/internal/ws"
)

const defaultPort = 8080

type Server struct {
	port int

	db database.Service

	jwtKey []byte

	// hub holds every open WebSocket on this node and fans messages out to
	// them. It is delivery only: the write path is still REST.
	hub *ws.Hub
}

// App owns everything the process has to shut down: the HTTP server, the
// WebSocket hub, and the database behind them.
//
// main only starts it and stops it. Every later stage adds one more thing that
// must be closed in the right order — Redis, the broker — and that list
// belongs next to the code that opened them, not in main.
type App struct {
	HTTP *http.Server

	hub *ws.Hub
	db  database.Service
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

	hub := ws.New()
	// The hub is one goroutine that owns the client map. Nothing works until it
	// runs, and Shutdown is what stops it again.
	go hub.Run()

	s := &Server{
		port: port,

		db: db,

		jwtKey: []byte(jwtKey),

		hub: hub,
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

	return &App{HTTP: httpServer, hub: hub, db: db}
}

// Shutdown stops the app from the outside in: first the traffic, then what the
// traffic needed.
//
// http.Server.Shutdown does not kill open requests. It refuses new ones and
// waits for the ones already running, and those are still reading and writing
// rows. Closing the pool first would break exactly the requests we are trying
// to let finish.
//
// The sockets are closed in the middle step, and they need their own step
// because Shutdown does not cover them. A WebSocket connection is hijacked:
// once it is upgraded, the HTTP server no longer owns it, does not count it,
// and does not wait for it. Without hub.Close every client would be cut off
// mid-frame when the process exits, instead of getting a close frame.
//
// The database is closed even when the earlier steps fail. A request that
// never returns makes Shutdown give back context.DeadlineExceeded, and the
// process is exiting either way, so keeping the pool open helps nobody.
//
// The caller owns the deadline. This method does not invent one.
func (a *App) Shutdown(ctx context.Context) error {
	httpErr := a.HTTP.Shutdown(ctx)
	a.hub.Close()
	dbErr := a.db.Close()

	// errors.Join keeps both problems instead of hiding one, and returns nil
	// when both are nil.
	return errors.Join(httpErr, dbErr)
}

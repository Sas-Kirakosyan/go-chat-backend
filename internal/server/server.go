package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	_ "github.com/joho/godotenv/autoload"

	"go-chat-backend/internal/database"
	"go-chat-backend/internal/metrics"
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

	// draining is set at the start of Shutdown, and makes /readyz fail, so a
	// load balancer stops sending work to a node that is about to close.
	//
	// It is atomic because the shutdown goroutine writes it while request
	// goroutines read it. It is written as "draining" rather than "ready" so
	// that the zero value — a Server nobody has shut down — is the healthy one.
	draining atomic.Bool

	// limits configures the two rate limiters. The zero value means "use the
	// defaults", so a Server built by hand still works.
	limits rateLimits
}

// App owns everything the process has to shut down: the HTTP server, the
// WebSocket hub, and the database behind them.
//
// main only starts it and stops it. Every later stage adds one more thing that
// must be closed in the right order — Redis, the broker — and that list
// belongs next to the code that opened them, not in main.
type App struct {
	HTTP *http.Server

	srv *Server
	hub *ws.Hub
	db  database.Service
}

// New builds the app: logging, config, database, migrations, metrics, routes,
// HTTP server.
//
// It exits the process on anything it cannot recover from. A server with no
// JWT secret, or with a database it failed to migrate, is not worth starting.
func New() *App {
	// First, before anything can want to log. Everything below writes
	// structured lines, including the failures that stop the process.
	log := setupLogging()

	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil || port <= 0 {
		log.Info("PORT not set or invalid, using the default", "port", defaultPort)
		port = defaultPort
	}

	jwtKey := os.Getenv("JWT_SECRET")
	if jwtKey == "" {
		log.Error("JWT_SECRET is not set; add it to your .env before starting the server")
		os.Exit(1)
	}

	db := database.New()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Migrate(ctx); err != nil {
		log.Error("could not prepare database", "err", err)
		os.Exit(1)
	}

	hub := ws.New()
	// The hub is one goroutine that owns the client map. Nothing works until it
	// runs, and Shutdown is what stops it again.
	go hub.Run()

	// Publish the counters these two already keep. Nothing is pushed: the
	// values are read when Prometheus scrapes, so there is one source of truth
	// per number instead of a counter and a metric drifting apart.
	metrics.RegisterWebSocket(hub)
	metrics.RegisterDBPool(db)

	limits := rateLimitsFromEnv().orDefaults()
	log.Info("rate limits",
		"auth_rps", limits.authRPS, "auth_burst", limits.authBurst,
		"api_rps", limits.apiRPS, "api_burst", limits.apiBurst,
	)

	s := &Server{
		port: port,

		db: db,

		jwtKey: []byte(jwtKey),

		hub: hub,

		limits: limits,
	}

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           s.RegisterRoutes(),
		IdleTimeout:       time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,

		// WriteTimeout must stay 0 (no deadline). It is an absolute deadline
		// measured from the start of the request, so any non-zero value cuts
		// long-lived streaming responses (SSE, and the WebSocket) off
		// mid-flight. Per-request limits belong on the handler context instead.
		WriteTimeout: 0,

		// The HTTP server's own errors — a bad TLS handshake, a malformed
		// request line — otherwise go to the standard logger, in a different
		// format from every other line this process writes.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	return &App{HTTP: httpServer, srv: s, hub: hub, db: db}
}

// Shutdown stops the app from the outside in: first the traffic, then what the
// traffic needed.
//
// Step zero is /readyz going false. It is not cosmetic: a load balancer needs
// a moment to notice, and until it does it keeps sending new requests into a
// process that is already draining. Failing the probe first is what makes a
// rolling deploy quiet.
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
	a.srv.draining.Store(true)

	httpErr := a.HTTP.Shutdown(ctx)
	a.hub.Close()
	dbErr := a.db.Close()

	// errors.Join keeps both problems instead of hiding one, and returns nil
	// when both are nil.
	return errors.Join(httpErr, dbErr)
}

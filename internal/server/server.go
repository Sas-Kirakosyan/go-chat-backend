package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/joho/godotenv/autoload"

	"go-chat-backend/internal/cluster"
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

	// cluster is Redis, and it is nil when REDIS_ADDR is not set.
	//
	// nil is a supported mode, not a broken one: it means "one node", and then
	// the write path fans out straight to the local hub exactly as it did in
	// Stage 2. That is what `make run` and every test uses, and it is why none
	// of them need a Redis container.
	cluster *cluster.Cluster

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

	srv     *Server
	hub     *ws.Hub
	db      database.Service
	cluster *cluster.Cluster

	// The subscriber and the presence heartbeat run for the life of the
	// process. stopBackground ends them, and background waits until they have
	// really stopped — a goroutine still publishing presence for a node that is
	// closing would keep dead users online.
	stopBackground context.CancelFunc
	background     sync.WaitGroup
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

	// Redis, if there is any. A missing REDIS_ADDR is single-node mode. A Redis
	// that does not answer is a warning and not a stop: see cluster.FromEnv for
	// what happened when this exited instead.
	redis := cluster.FromEnv(ctx)
	if redis == nil {
		log.Info("no REDIS_ADDR, running as a single node: sockets on other nodes will not be reached")
	} else {
		// The node name is on every line already, from setupLogging.
		log.Info("cluster mode", "redis", os.Getenv("REDIS_ADDR"))
	}

	// Publish the counters these already keep. Nothing is pushed: the values are
	// read when Prometheus scrapes, so there is one source of truth per number
	// instead of a counter and a metric drifting apart.
	metrics.RegisterWebSocket(hub)
	metrics.RegisterDBPool(db)
	if redis != nil {
		metrics.RegisterCluster(redis)
	}

	limits := rateLimitsFromEnv().orDefaults()
	log.Info("rate limits",
		"auth_rps", limits.authRPS, "auth_burst", limits.authBurst,
		"api_rps", limits.apiRPS, "api_burst", limits.apiBurst,
	)

	s := &Server{
		port: port,

		db: db,

		jwtKey: []byte(jwtKey),

		hub:     hub,
		cluster: redis,

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

	app := &App{HTTP: httpServer, srv: s, hub: hub, db: db, cluster: redis}
	app.startBackground()
	return app
}

// startBackground starts the two goroutines that make this node part of a
// cluster. With no Redis there is nothing to start.
//
//  1. The subscriber. It is the ONLY thing that delivers a message to a socket
//     now, including a message this very node published. One path in, so a
//     message cannot arrive twice.
//  2. The presence heartbeat.
func (a *App) startBackground() {
	ctx, cancel := context.WithCancel(context.Background())
	a.stopBackground = cancel

	if a.cluster == nil {
		return
	}

	a.background.Add(2)

	go func() {
		defer a.background.Done()
		// hub.Broadcast is exactly the callback shape Subscribe wants, and that
		// is not a coincidence: the hub was written in Stage 1 so that Redis
		// could plug in beside it and not inside it.
		a.cluster.Subscribe(ctx, a.hub.Broadcast)
	}()

	go func() {
		defer a.background.Done()
		a.runPresence(ctx)
	}()
}

// runPresence tells Redis, on a timer, which users have a socket here.
//
// A failed beat is logged and the timer carries on. Redis being down must not
// kill the loop: when it comes back, the next beat puts everyone online again
// with no restart and no repair step.
func (a *App) runPresence(ctx context.Context) {
	ticker := time.NewTicker(cluster.PresenceHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			// A short deadline of its own. This runs every 10 seconds, so a beat
			// that is still waiting when the next one is due is a beat worth
			// abandoning.
			beat, cancel := context.WithTimeout(ctx, cluster.PresenceHeartbeat/2)
			_ = a.cluster.Heartbeat(beat, a.hub.UserIDs())
			cancel()
		}
	}
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

	// The background goroutines go after the requests and before the hub. A
	// request still finishing may publish a fan-out, and the subscriber is what
	// delivers it — stopping the subscriber first would drop the last messages
	// of a draining node.
	//
	// Stopping the heartbeat here is also what takes this node's users offline:
	// nobody refreshes them, so within one PresenceTTL they fall out of the
	// window on their own. There is no goodbye message to send, which is the
	// point — a crash cannot send one either.
	if a.stopBackground != nil {
		a.stopBackground()
	}
	a.background.Wait()

	a.hub.Close()

	redisErr := a.cluster.Close()
	dbErr := a.db.Close()

	// errors.Join keeps every problem instead of hiding some, and returns nil
	// when they are all nil.
	return errors.Join(httpErr, redisErr, dbErr)
}

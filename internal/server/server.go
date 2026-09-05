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

	"go-chat-backend/internal/broker"
	"go-chat-backend/internal/cluster"
	"go-chat-backend/internal/database"
	"go-chat-backend/internal/metrics"
	"go-chat-backend/internal/outbox"
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

	// cluster is Redis, and it is nil when REDIS_ADDR is not set. Since Stage 5
	// it does presence only.
	//
	// nil is a supported mode, not a broken one: it means "one node", and then
	// nobody is tracked as online. Delivery does not depend on it. That is what
	// `make run` and every test uses, and it is why none of them need a Redis
	// container.
	cluster *cluster.Cluster

	// broker is NATS, and it is nil when NATS_URL is not set.
	//
	// It is here only so /health can say whether it is reachable. Nothing in a
	// request path touches it: the send path stops at the outbox row, and the
	// relay owns everything after that.
	broker *broker.Broker

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
// WebSocket hub, the database, Redis and the broker.
//
// main only starts it and stops it. The order things are closed in matters
// more than the list itself, and it belongs next to the code that opened them,
// not in main. See Shutdown.
type App struct {
	HTTP *http.Server

	srv     *Server
	hub     *ws.Hub
	db      database.Service
	cluster *cluster.Cluster
	broker  *broker.Broker
	relay   *outbox.Relay

	// The relay, the two broker consumers and the presence heartbeat run for
	// the life of the process. stopBackground ends them, and background waits
	// until they have really stopped — a goroutine still publishing presence
	// for a node that is closing would keep dead users online, and a relay
	// killed mid-batch would leave its advisory lock to time out instead of
	// handing it over.
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
		log.Info("no REDIS_ADDR, running as a single node: presence is not tracked")
	} else {
		// The node name is on every line already, from setupLogging.
		log.Info("presence enabled", "redis", os.Getenv("REDIS_ADDR"))
	}

	// NATS, if there is any. A missing NATS_URL is single-node mode, and so is
	// a NATS that will not answer — see broker.FromEnv. Neither stops the
	// process, because writes do not depend on the broker: they land in the
	// outbox, in Postgres, and go out when it comes back.
	nats := broker.FromEnv(ctx)
	if nats == nil {
		log.Info("no NATS_URL, delivering locally: sockets on other nodes will not be reached")
	}

	// Publish the counters these already keep. Nothing is pushed: the values are
	// read when Prometheus scrapes, so there is one source of truth per number
	// instead of a counter and a metric drifting apart.
	metrics.RegisterWebSocket(hub)
	metrics.RegisterDBPool(db)
	if redis != nil {
		metrics.RegisterCluster(redis)
	}
	if nats != nil {
		metrics.RegisterBroker(nats)
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
		broker:  nats,

		limits: limits,
	}

	// The relay, and where a drained row goes.
	//
	// With a broker, rows go onto the stream and come back to every node,
	// including this one. Without a broker there is nobody to tell, so the
	// same relay hands the event straight to this node's hub and does the
	// unread work inline. One relay, one outbox, two last steps — see
	// internal/outbox/local.go for why the single-node path was not just left
	// in the handler.
	var publisher outbox.Publisher = nats
	if nats == nil {
		publisher = outbox.NewLocalPublisher(s.deliverMessage, s.applyUnread)
	}
	relay := outbox.New(db, publisher, log)
	metrics.RegisterOutbox(relay)

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

	app := &App{HTTP: httpServer, srv: s, hub: hub, db: db, cluster: redis, broker: nats, relay: relay}
	app.startBackground()
	return app
}

// startBackground starts the goroutines that run for the life of the process.
//
//  1. The relay. It runs on EVERY node, always, even with no broker and no
//     Redis, because it is what turns a committed message into a delivered
//     one. Only one node drains at a time; the rest wait on the lock. This is
//     the one goroutine the service cannot work without.
//  2. The fan-out consumer, with a broker. It is the only thing that delivers
//     a message to a socket on a clustered node — including a message this
//     very node's relay published. One path in, so a message cannot arrive
//     twice.
//  3. The unread consumer, with a broker. Shared with the other nodes.
//  4. The presence heartbeat, with Redis.
func (a *App) startBackground() {
	ctx, cancel := context.WithCancel(context.Background())
	a.stopBackground = cancel

	a.background.Add(1)
	go func() {
		defer a.background.Done()
		a.relay.Run(ctx)
	}()

	if a.broker != nil {
		a.background.Add(2)

		go func() {
			defer a.background.Done()
			// deliverMessage is exactly the callback shape SubscribeFanout
			// wants, and that is not a coincidence: the hub was written in
			// Stage 1 so that a transport could plug in beside it and not
			// inside it. Redis did in Stage 3; NATS does now; neither package
			// knows what a socket is.
			a.broker.SubscribeFanout(ctx, a.srv.deliverMessage)
		}()

		go func() {
			defer a.background.Done()
			a.broker.ConsumeUnread(ctx, a.srv.applyUnread)
		}()
	}

	if a.cluster == nil {
		return
	}

	a.background.Add(1)
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
	// request still finishing writes an outbox row, and the relay is what
	// sends it — stopping the relay first would leave the last messages of a
	// draining node in the table until another node's relay noticed.
	//
	// They are also what makes shutdown clean rather than merely quick: the
	// relay releases its advisory lock on the way out, so the next leader
	// starts in seconds instead of waiting for Postgres to notice a dead
	// connection.
	//
	// Stopping the heartbeat here is also what takes this node's users offline:
	// nobody refreshes them, so within one PresenceTTL they fall out of the
	// window on their own. There is no goodbye message to send, which is the
	// point — a crash cannot send one either.
	//
	// Nothing is lost if this node is killed before any of it happens. That is
	// the whole reason the outbox exists: the instruction to deliver is a row
	// in Postgres, and another node picks it up.
	if a.stopBackground != nil {
		a.stopBackground()
	}
	a.background.Wait()

	a.hub.Close()

	brokerErr := a.broker.Close()
	redisErr := a.cluster.Close()
	dbErr := a.db.Close()

	// errors.Join keeps every problem instead of hiding some, and returns nil
	// when they are all nil.
	return errors.Join(httpErr, brokerErr, redisErr, dbErr)
}

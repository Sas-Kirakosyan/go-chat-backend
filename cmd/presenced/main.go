// Command presenced is the presence service.
//
// It is the second service, and it is the whole of Stage 6. Until now this
// project was one binary that happened to run twice; presence lived inside it
// and read Redis directly. Now "who is online" is a contract
// (proto/presence/v1/presence.proto), a process, and a port — and everything
// that used to be a function call is a thing that can time out, be slow, be a
// different version, or not be there at all.
//
// # Why presence, and not something bigger
//
// Splitting the wrong thing is the expensive mistake, and it is worth being
// able to say why this one is safe. Presence is the only part of the system
// whose data nothing else joins to: no route reads presence and a message in
// one query, no transaction spans them, and no other table has a foreign key to
// it. So the split costs no join and no distributed transaction.
//
// It is also the only feature the product can lose without being broken. A chat
// service with no green dots is a chat service; a chat service that cannot
// store a message is not. Splitting messages out would have meant a network
// call on the write path, and a write path that can fail in a new way is a much
// worse first split.
//
// # What this process does not have
//
// No database, no JWT secret, no NATS. It holds one Redis connection and
// answers two RPCs. That is worth noticing: the API nodes no longer have a
// Redis client at all, so the presence key cannot be written by anything but
// this, and this cannot write a message even by mistake. The boundary is real,
// not a naming convention.
//
//	REDIS_ADDR=localhost:6379 PRESENCE_PORT=9090 go run ./cmd/presenced
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"go-chat-backend/internal/logging"
	"go-chat-backend/internal/metrics"
	"go-chat-backend/internal/presence"
	"go-chat-backend/internal/presencepb"
)

const (
	// defaultPort is the gRPC port: the service's actual job.
	defaultPort = 9090

	// defaultMetricsPort is a small HTTP listener beside it, for /metrics and
	// /livez.
	//
	// Two ports, because a gRPC server cannot serve a Prometheus text page and
	// Prometheus cannot speak gRPC. This is the normal shape for a gRPC
	// service: the data plane speaks gRPC and the operational surface speaks
	// HTTP, so everything that already watches a service can watch this one.
	//
	// gRPC health is NOT duplicated here. That question is answered on the gRPC
	// port, by the standard health service, because a health check that goes
	// through a different port and a different server can be perfectly happy
	// while the port that matters is wedged.
	defaultMetricsPort = 9091
)

// shutdownTimeout is how long an in-flight RPC gets to finish after the signal
// arrives. Presence calls are one Redis command, so this is generous by an
// order of magnitude — which is the point: if it is ever reached, something is
// wrong and the log will say so.
const shutdownTimeout = 5 * time.Second

// healthInterval is how often the store is checked so the gRPC health service
// can answer honestly.
//
// Polling rather than checking inside Check() on demand: a health probe that
// makes a Redis call is a way to turn a liveness check into load, and worse, a
// slow Redis would make the probe slow and get the container killed for a
// problem that is not the container's.
const healthInterval = 5 * time.Second

func main() {
	log := logging.Setup()

	port := portFromEnv(log, "PRESENCE_PORT", defaultPort)
	metricsPort := portFromEnv(log, "PRESENCE_METRICS_PORT", defaultMetricsPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The store. Unlike the API node, this process cannot do its job without
	// Redis, so a missing REDIS_ADDR stops it — that is a configuration
	// mistake, not a degraded mode. A Redis that is merely down does NOT stop
	// it: see presence.OpenStore.
	store := presence.OpenStore(ctx)
	if store == nil {
		log.Error("REDIS_ADDR is not set; presenced has nowhere to keep presence")
		os.Exit(1)
	}
	defer store.Close()

	metrics.RegisterPresenceStore(store)
	metricsSrv := serveMetrics(metricsPort, log)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Error("could not listen", "port", port, "err", err)
		os.Exit(1)
	}

	server := grpc.NewServer(
		// Keepalive, and it is not optional for this service.
		//
		// The API nodes hold one connection open and beat on it every ten
		// seconds, which means the connection is idle most of the time. An idle
		// TCP connection through a load balancer or a NAT is silently dropped
		// after a few minutes, and neither side finds out until something is
		// sent. Pinging keeps it real, and — more importantly — makes a dead
		// peer visible instead of a write that hangs.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		// A client that pings faster than this is refused. Without it, a buggy
		// or hostile client can ping in a loop, and the server answers every
		// one.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	presencepb.RegisterPresenceServiceServer(server, presence.NewService(store))

	// The standard health service, on the standard name. It is what the API
	// nodes' /health calls and what a Kubernetes gRPC probe would use, and
	// using the standard one means neither has to be taught anything specific
	// to this service.
	healthSrv := health.NewServer()

	// It starts NOT_SERVING, and the watcher below is what turns it on. The
	// default is SERVING, which would mean a process whose Redis has never
	// answered reports itself healthy until the first check — a small window,
	// and exactly the window a container orchestrator uses to decide the new
	// instance is good and take the old one out.
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	grpc_health_v1.RegisterHealthServer(server, healthSrv)
	go watchStore(ctx, store, healthSrv, log)

	// Reflection lets grpcurl call this service without a copy of the .proto:
	//
	//	grpcurl -plaintext localhost:9090 list
	//	grpcurl -plaintext -d '{"user_ids":[1,2]}' localhost:9090 presence.v1.PresenceService/Online
	//
	// It is on because this is a private service on a private network and being
	// able to ask it a question by hand is worth a great deal while debugging.
	// On a public port it would be off: it publishes the whole API surface to
	// anyone who can reach it.
	reflection.Register(server)

	go func() {
		log.Info("presence service starting", "addr", listener.Addr().String())
		if err := server.Serve(listener); err != nil {
			log.Error("presence service failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	stop()
	log.Info("shutting down")

	// SetServingStatus(NOT_SERVING) before the drain, for the same reason
	// /readyz goes false before the API node drains: a caller needs a moment to
	// notice, and until it does it keeps sending work to a process that is
	// closing.
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// GracefulStop refuses new RPCs and waits for the ones already running. It
	// can wait forever, so it gets a deadline and a hard Stop behind it — the
	// same shape as the HTTP shutdown in cmd/api.
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("presence service exited cleanly")
	case <-time.After(shutdownTimeout):
		log.Warn("graceful stop timed out, closing connections", "after", shutdownTimeout)
		server.Stop()
	}

	// The metrics listener goes last, so a scrape taken during the drain still
	// finds a page. It gets no grace period of its own: nothing is left to
	// serve by now.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
}

// portFromEnv reads a port, or stops the process.
//
// A misspelled port is a configuration mistake that will never fix itself, and
// starting on the default instead would give a service nobody can reach at the
// address they configured — which is a far more confusing failure than not
// starting at all.
func portFromEnv(log *slog.Logger, key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	port, err := strconv.Atoi(raw)
	if err != nil || port <= 0 || port > 65535 {
		log.Error("not a valid port", "var", key, "value", raw)
		os.Exit(1)
	}
	return port
}

// serveMetrics starts the small HTTP listener beside the gRPC server.
//
// A failure to listen here is logged and not fatal, and that is deliberate:
// losing the metrics page is bad, and refusing to run presence because the
// metrics page could not bind a port would be worse. Watching a service is not
// the service.
func serveMetrics(port int, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	go func() {
		log.Info("presence metrics listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listener stopped", "err", err)
		}
	}()

	return srv
}

// watchStore keeps the health status in step with Redis.
//
// This is the difference between "the process is running" and "the process can
// do its job". A presenced whose Redis is gone answers TCP, accepts RPCs and
// fails every one of them; reporting SERVING through that would be a lie that
// the API nodes have no way to see through.
func watchStore(ctx context.Context, store *presence.Store, healthSrv *health.Server, log *slog.Logger) {
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()

	serving := false
	for {
		check, cancel := context.WithTimeout(ctx, healthInterval/2)
		err := store.Ping(check)
		cancel()

		if ok := err == nil; ok != serving {
			serving = ok
			status := grpc_health_v1.HealthCheckResponse_NOT_SERVING
			if serving {
				status = grpc_health_v1.HealthCheckResponse_SERVING
			}
			// The empty service name is the status of the whole server, which
			// is what a client asking about nothing in particular gets.
			healthSrv.SetServingStatus("", status)
			log.Info("presence health changed", "serving", serving, "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

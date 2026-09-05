// Package metrics is the Prometheus view of the service.
//
// Everything in here is a counter, a gauge or a histogram, and nothing in here
// decides anything. It knows nothing about Gin, about the hub, or about the
// database: callers hand it numbers, or hand it a small interface it can read
// numbers from.
//
// # Why labels are the dangerous part
//
// Prometheus stores one time series per unique label combination. A label
// whose value comes from the caller — a raw URL path, a user id, a message id
// — makes a new series every time a new value appears, and a target with a
// million series is how a Prometheus server falls over.
//
// So the HTTP metrics are labelled with the ROUTE TEMPLATE
// ("/conversations/:id/messages"), never the path that was really asked for.
// Ten thousand rooms are still one series. Anything unrouted (a 404) is
// labelled "other", because an attacker choosing the path must not be able to
// choose our series names.
package metrics

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// namespace prefixes every metric, so ours are easy to tell apart from the Go
// runtime and process metrics the client library adds for free.
const namespace = "chat"

// durationBuckets are the latency buckets for HTTP requests.
//
// The default buckets start at 5 ms, which is too coarse here: most handlers
// are one indexed query and finish well under that, so everything would land
// in the first bucket and the histogram would say nothing. These start at 1 ms
// and reach 10 s, which is past any request we would call healthy.
var durationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

var (
	// HTTPRequests counts finished requests.
	//
	// Status is a label, so the error rate is a query and not a second metric:
	// rate(chat_http_requests_total{status=~"5.."}[5m]).
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "http_requests_total",
		Help:      "Finished HTTP requests, by method, route template and status code.",
	}, []string{"method", "route", "status"})

	// HTTPDuration is how long requests took.
	//
	// Status is deliberately NOT a label here. A histogram already costs one
	// series per bucket, and multiplying that by every status code buys little:
	// when the p99 moves, the counter above says whether it was errors.
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "http_request_duration_seconds",
		Help:      "How long a request took, by method and route template.",
		Buckets:   durationBuckets,
	}, []string{"method", "route"})

	// HTTPInFlight is how many requests are being served right now. A rising
	// in-flight count with flat throughput means requests are queueing
	// somewhere — in Stage 1 that somewhere was the database pool.
	HTTPInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "http_requests_in_flight",
		Help:      "Requests being served right now.",
	})

	// MessagesStored counts rows written by POST /conversations/:id/messages.
	//
	// A retry of a client_msg_id we already hold stores nothing and is not
	// counted, so rate() over this is the real write rate, not the request
	// rate.
	MessagesStored = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "messages_stored_total",
		Help:      "Messages written to the database. A repeated client_msg_id stores nothing and is not counted.",
	})

	// UnreadApplied counts unread-counter rows the consumer really changed.
	//
	// It counts ROWS and not messages: one message in a room of fifty moves
	// forty-nine counters. So this rises much faster than messages_stored_total
	// on a busy service, and the ratio between them is the average room size,
	// which is a useful thing to know for free.
	UnreadApplied = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "unread_applied_total",
		Help:      "Unread counter rows changed by the broker consumer.",
	})

	// UnreadDuplicates counts messages the consumer was handed again and
	// correctly did nothing with.
	//
	// This is the idempotency guard doing its job, and it is the only place
	// where at-least-once delivery becomes visible. Zero forever is suspicious
	// rather than perfect: it usually means redelivery has never been tested.
	// A steady trickle is normal. A spike means something is timing out and
	// the same work is being handed out twice.
	UnreadDuplicates = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "unread_duplicates_total",
		Help:      "Messages the unread consumer had already applied and skipped. This is at-least-once delivery being caught.",
	})

	// GapMessages counts messages handed back through ?after_seq=, the gap
	// read a client uses after reconnecting.
	//
	// This is the honest measure of how much live delivery is being missed.
	// Nothing else shows it: a message that never reaches a socket is still
	// stored, still answered with 201, and still counted by MessagesStored. A
	// rising rate here means sockets are dropping, or a node has lost its
	// broker subscription while its HTTP metrics stayed perfectly healthy.
	GapMessages = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "gap_messages_total",
		Help:      "Messages served through ?after_seq= to a client catching up after a reconnect.",
	})

	// GapSize is how many messages one gap read returned.
	//
	// The counter above says how much was missed in total; this says whether
	// that is many clients missing one message each, or one client that was
	// away for an hour. Those two have completely different causes.
	//
	// The buckets are hand-picked: the default histogram buckets are latency
	// seconds and mean nothing for a message count.
	GapSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "gap_size",
		Help:      "Messages returned by one gap read. Zero means the client had missed nothing.",
		Buckets:   []float64{0, 1, 5, 10, 50, 100, 500},
	})

	// RateLimited counts requests refused with 429. The scope label says which
	// limiter refused it: "auth" (per IP) or "api" (per user).
	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "rate_limited_total",
		Help:      "Requests refused by a rate limiter, by limiter scope.",
	}, []string{"scope"})

	// Panics counts recovered panics. It should be flat at zero. An alert on
	// any increase at all is a reasonable one.
	Panics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "panics_recovered_total",
		Help:      "Panics caught by the recovery middleware.",
	})
)

// WSStats is what the metrics package needs from the WebSocket hub.
//
// It is an interface, not the hub itself, so internal/ws keeps knowing nothing
// about Prometheus. The hub already holds these numbers as atomic counters; we
// only read them at scrape time.
type WSStats interface {
	Open() int
	Sent() int
	Dropped() int
	Shed() int
	Expired() int
}

// RegisterWebSocket publishes the hub's counters.
//
// These are *Func collectors: nothing is pushed, the value is read from the
// hub when Prometheus scrapes. That keeps one source of truth for each number
// instead of a hub counter and a metric that can drift apart.
//
// Calling it twice — two hubs in one process — keeps the first hub and ignores
// the rest. There is one hub per process in real life; this only stops a test
// from panicking.
func RegisterWebSocket(s WSStats) {
	register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "ws_connections_open",
		Help:      "WebSocket connections open on this node right now.",
	}, func() float64 { return float64(s.Open()) }))

	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ws_frames_sent_total",
		Help:      "Frames queued to a client socket. One message to a room of 50 counts 50.",
	}, func() float64 { return float64(s.Sent()) }))

	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ws_clients_dropped_total",
		Help:      "Sockets dropped for not reading fast enough.",
	}, func() float64 { return float64(s.Dropped()) }))

	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ws_broadcasts_shed_total",
		Help:      "Fan-outs thrown away because the hub's own queue was full.",
	}, func() float64 { return float64(s.Shed()) }))

	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ws_sockets_expired_total",
		Help:      "Sockets closed because the access token they were opened with ran out.",
	}, func() float64 { return float64(s.Expired()) }))
}

// ClusterStats is what the metrics package needs from the Redis layer.
//
// It used to carry the fan-out counters too. Those moved to BrokerStats in
// Stage 5, along with the fan-out itself; Redis keeps only presence.
type ClusterStats interface {
	PresenceFailed() int
}

// RegisterCluster publishes what is left of the Redis counters.
func RegisterCluster(s ClusterStats) {
	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "cluster",
		Name:      "presence_failures_total",
		Help:      "Presence heartbeats that failed. Enough in a row and this node's users look offline.",
	}, func() float64 { return float64(s.PresenceFailed()) }))
}

// OutboxStats is what the metrics package needs from the relay.
type OutboxStats interface {
	IsLeader() float64
	Published() int64
	Failed() int64
	Pending() float64
	LagSeconds() float64
}

// RegisterOutbox publishes the relay's numbers.
//
// # The two that matter
//
// lag_seconds is the health of the whole stage in one number. A pending count
// on its own says nothing — 900 rows could be one busy second — but 900 rows
// whose oldest has waited four minutes means the relay has stopped, and every
// message sent in those four minutes is sitting in Postgres. Alert on this
// one.
//
// leader must sum to exactly 1 across the cluster. Zero means nobody is
// draining and nothing is being delivered anywhere. Two means the advisory
// lock is broken and two relays are publishing the same rooms out of order.
// Both are invisible everywhere else: sends still answer 201 either way.
func RegisterOutbox(s OutboxStats) {
	register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "outbox",
		Name:      "lag_seconds",
		Help:      "How long the oldest unpublished outbox row has waited. 0 when nothing is waiting.",
	}, s.LagSeconds))

	register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "outbox",
		Name:      "pending",
		Help:      "Outbox rows not yet published. At rest this is 0.",
	}, s.Pending))

	register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "outbox",
		Name:      "relay_leader",
		Help:      "1 when this node is the relay. Summed across the cluster this must be exactly 1.",
	}, s.IsLeader))

	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "outbox",
		Name:      "published_total",
		Help:      "Outbox rows this node has drained.",
	}, func() float64 { return float64(s.Published()) }))

	register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "outbox",
		Name:      "publish_failures_total",
		Help:      "Failed publish attempts. The rows stay in the table, so delivery is late and not lost.",
	}, func() float64 { return float64(s.Failed()) }))
}

// BrokerStats is what the metrics package needs from the NATS layer.
type BrokerStats interface {
	Published() int64
	PublishFailed() int64
	FanoutReceived() int64
	UnreadHandled() int64
	Redelivered() int64
	DeadLettered() int64
	Consuming() float64
}

// RegisterBroker publishes the NATS counters.
//
// The pair worth watching is published against fanout_received. Every node
// receives every message, so summed across N nodes, received should be about N
// times published. If this node's received stops rising while published keeps
// going, its subscription is dead and its sockets have gone quiet — which is
// invisible in the HTTP metrics, because sends are still returning 201.
//
// dead_lettered_total should be flat at zero. Any other value is a message
// that failed five times and now needs a person.
func RegisterBroker(s BrokerStats) {
	counter := func(name, help string, read func() float64) {
		register(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "broker",
			Name:      name,
			Help:      help,
		}, read))
	}

	counter("published_total", "Events this node's relay put on the stream.",
		func() float64 { return float64(s.Published()) })
	counter("publish_failures_total", "Publishes the broker refused. The outbox row stays, so the message is late and not lost.",
		func() float64 { return float64(s.PublishFailed()) })
	counter("fanout_received_total", "Events this node received for its own sockets.",
		func() float64 { return float64(s.FanoutReceived()) })
	counter("unread_handled_total", "Events this node acknowledged on the shared unread consumer.",
		func() float64 { return float64(s.UnreadHandled()) })
	counter("redelivered_total", "Events that arrived on the shared consumer at least a second time.",
		func() float64 { return float64(s.Redelivered()) })
	counter("dead_lettered_total", "Events that failed too many times and went to CHAT_DLQ. Expected value: zero.",
		func() float64 { return float64(s.DeadLettered()) })

	// The one to alert on. A node at 0 is storing messages and pushing none of
	// them, and nothing in the HTTP metrics shows it: sends still answer 201.
	register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "broker",
		Name:      "consuming",
		Help:      "1 when this node holds its fan-out subscription, 0 when it does not and its sockets are silent.",
	}, s.Consuming))
}

// PoolStats is what the metrics package needs from the database layer.
type PoolStats interface {
	PoolStats() sql.DBStats
}

// RegisterDBPool publishes the database/sql pool counters.
//
// Stage 1 measured a p99 of 787 ms and found the whole delay was a message
// waiting for a database handle, not for a socket. That was only visible
// because /health happened to print the pool stats. These metrics make it a
// graph instead of a lucky guess: wait_count and wait_duration rising while
// open_connections sits at the maximum is exactly that picture.
func RegisterDBPool(p PoolStats) {
	gauge := func(name, help string, read func(sql.DBStats) float64) {
		register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "db",
			Name:      name,
			Help:      help,
		}, func() float64 { return read(p.PoolStats()) }))
	}
	counter := func(name, help string, read func(sql.DBStats) float64) {
		register(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "db",
			Name:      name,
			Help:      help,
		}, func() float64 { return read(p.PoolStats()) }))
	}

	gauge("pool_open_connections", "Connections open to the database, in use and idle together.",
		func(s sql.DBStats) float64 { return float64(s.OpenConnections) })
	gauge("pool_in_use", "Connections currently serving a query.",
		func(s sql.DBStats) float64 { return float64(s.InUse) })
	gauge("pool_idle", "Connections open and doing nothing.",
		func(s sql.DBStats) float64 { return float64(s.Idle) })
	gauge("pool_max_open_connections", "The configured ceiling on open connections.",
		func(s sql.DBStats) float64 { return float64(s.MaxOpenConnections) })

	counter("pool_waits_total", "Times a caller had to wait for a free connection.",
		func(s sql.DBStats) float64 { return float64(s.WaitCount) })
	counter("pool_wait_seconds_total", "Total time spent waiting for a free connection.",
		func(s sql.DBStats) float64 { return s.WaitDuration.Seconds() })
}

// Handler serves the metrics in the Prometheus text format.
//
// It uses the default registry, so the Go runtime collector (goroutines, heap,
// GC pauses) and the process collector (open file descriptors, CPU) come with
// it. Goroutine count is worth watching here in particular: this service runs
// two goroutines per socket, so a leak shows up there first.
func Handler() http.Handler {
	return promhttp.Handler()
}

// register adds a collector and tolerates it already being there.
//
// MustRegister would panic on a second registration, which in a test binary
// means the whole package fails because two servers were built. A duplicate is
// not a problem worth killing a process over.
func register(c prometheus.Collector) {
	var already prometheus.AlreadyRegisteredError
	if err := prometheus.Register(c); err != nil && !errors.As(err, &already) {
		panic(err)
	}
}

// Package cluster is this node's Redis connection, and it now does one job:
// presence.
//
// # What it used to do
//
// Stage 3 built the fan-out here, on Redis Pub/Sub. One node published, every
// node received, and each pushed to its own sockets. That worked, and it is
// gone. Pub/Sub keeps no copy of anything: a node that was restarting when a
// message was published never learned about it, and there was nobody left to
// ask. Stage 5 moved the fan-out to NATS JetStream, which does keep a copy —
// see internal/broker.
//
// # Why Redis stayed
//
// Presence is the opposite kind of data. "Who is online" is a fact about right
// now that is worthless a minute later, so it wants exactly what Redis is good
// at: a shared value with a TTL that repairs itself. A node that dies stops
// its heartbeat and its users fall out of the set on their own. Putting that
// in a durable log would mean storing, and then having to delete, a fact that
// is only true for ten seconds.
//
// So the split is by the shape of the data, not by taste: events that must not
// be lost go to the broker, state that expires stays in Redis.
//
// It knows nothing about Gin, the hub, or the database.
package cluster

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cluster is this node's connection to the others.
type Cluster struct {
	rdb    *redis.Client
	nodeID string

	// Counters, read by Prometheus at scrape time. Same pattern as the hub:
	// the value lives in one place and the metric reads it.
	presenceFail atomic.Int64
}

// FromEnv builds the cluster connection from REDIS_ADDR.
//
// It returns nil when REDIS_ADDR is not set. That is not an error: it is
// single-node mode, which is what `make run` and every test uses. Delivery
// does not depend on this any more — that moved to the broker in Stage 5 — so
// a nil Cluster now means only that presence is not tracked.
//
// # Why a Redis that does not answer is not a startup failure
//
// The first version pinged here and called os.Exit on failure. Then Redis was
// stopped on purpose, both nodes were rebuilt, and they went into a restart
// loop: every few seconds, exit 1, restart, fail the ping, exit 1. A Redis
// outage had turned into a total outage — no login, no history, no sending —
// for a service that is supposed to lose only its live push.
//
// So the ping stays, but only to write one honest line in the log. The client
// is returned either way, and go-redis reconnects on its own when Redis comes
// back. The same rule as /readyz: a shared dependency may degrade this node,
// never remove it.
func FromEnv(ctx context.Context) *Cluster {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return nil
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),

		// A presence beat that cannot be written quickly is not worth waiting
		// for: the next one is ten seconds away, and the TTL covers the gap.
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("redis is not answering, starting in degraded mode: "+
			"messages will be stored but not pushed live until it comes back",
			"addr", addr, "err", err)
	}

	return &Cluster{rdb: rdb, nodeID: nodeID()}
}

// nodeID is how this process names itself to the others. NODE_ID in compose,
// the hostname otherwise — inside a container that is the container id, which
// is exactly what you want to grep for.
func nodeID() string {
	if id := os.Getenv("NODE_ID"); id != "" {
		return id
	}
	if host, err := os.Hostname(); err == nil {
		return host
	}
	return "unknown"
}

// NodeID is the name this node publishes under.
func (c *Cluster) NodeID() string { return c.nodeID }

// Ping is the readiness check. It is used by /readyz.
func (c *Cluster) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close shuts the connection. Safe on a nil Cluster, so shutdown paths do not
// have to check for single-node mode.
func (c *Cluster) Close() error {
	if c == nil {
		return nil
	}
	return c.rdb.Close()
}

// PresenceFailed is how many heartbeats or lookups Redis refused. It is on
// /metrics because presence failing quietly looks exactly like nobody being
// online, and those two need to be told apart.
func (c *Cluster) PresenceFailed() int { return int(c.presenceFail.Load()) }

// isRedisDown says whether an error means Redis is unreachable rather than the
// command being wrong. It is only used for logging, so that a Redis outage
// writes one clear line instead of a wall of stack-shaped noise.
func isRedisDown(err error) bool {
	return err != nil && !errors.Is(err, redis.Nil)
}

// formatScore renders a unix time for a ZSET score. Redis takes scores as
// strings on the wire, and the default float formatting would use scientific
// notation for a timestamp this large.
func formatScore(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}

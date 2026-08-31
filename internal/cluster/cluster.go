// Package cluster is what turns several API processes into one chat system.
//
// Stage 1 gave each node a hub that owns its own sockets. That is still true
// and still right: a hub must never try to write to a socket on another
// machine. What was missing is a way for a node to say "this happened" and for
// every other node to hear it.
//
// Redis Pub/Sub is that way. One node publishes; every node — including the
// one that published — receives, and each fans the message out to its own
// local sockets only.
//
// # What this package deliberately does not do
//
// It does not store anything. Pub/Sub is fire and forget: a node that is down
// when a message is published never learns about it, and Redis keeps no copy.
// That is fine here, because the message is already committed to Postgres and
// history will show it. Closing the live gap is Stage 4's job, not this one's.
//
// It also knows nothing about Gin, the hub, or the database. It moves bytes
// between nodes.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// fanoutChannel is the one Pub/Sub channel every node listens on.
//
// One channel, not one per room. A channel per room would mean subscribing and
// unsubscribing as people join and leave, and a node with sockets in 5000 rooms
// would hold 5000 subscriptions. Every node reading everything is more traffic
// but far less bookkeeping, and the filter is cheap: a node drops any fan-out
// whose users it does not hold.
const fanoutChannel = "chat:fanout"

// publishTimeout caps one Redis call from the HTTP write path.
//
// It exists so that a sick Redis — reachable, but slow — cannot hold a POST
// open. The message is already in Postgres by the time we publish, so giving up
// costs a live push and nothing more.
//
// It started at two seconds, and with Redis stopped every send took exactly
// that: 8 ms became 2.009 s. The timeout was not protecting the write path, it
// WAS the write path's problem. Half a second is still far above a healthy
// publish (well under a millisecond on the same machine) and it keeps a full
// outage survivable.
//
// Publishing in a goroutine would make the POST fast and is the wrong answer:
// two messages sent in order could then reach Redis out of order, and a chat
// that reorders lines is broken in a way a slow one is not. The real fix for a
// long outage is a circuit breaker — stop calling a service that is known to be
// down — and that arrives with the other cross-service patterns in Stage 6.
const publishTimeout = 500 * time.Millisecond

// Cluster is this node's connection to the others.
type Cluster struct {
	rdb    *redis.Client
	nodeID string

	// Counters, read by Prometheus at scrape time. Same pattern as the hub:
	// the value lives in one place and the metric reads it.
	published    atomic.Int64
	receivedFrom atomic.Int64
	publishFails atomic.Int64
	presenceFail atomic.Int64

	// subscribed is the current state of the subscription, used only to log a
	// change instead of a repetition. It is also on /metrics, where a node
	// stuck at 0 is the clearest sign that its sockets have gone quiet.
	subscribed atomic.Bool
}

// fanout is one message as it travels between nodes.
//
// Payload is RawMessage, not []byte: a []byte would be base64-encoded on the
// way through JSON, which costs a third more bytes and makes the traffic
// unreadable with redis-cli MONITOR. It is already JSON — it travels as JSON.
type fanout struct {
	UserIDs []uint          `json:"user_ids"`
	Payload json.RawMessage `json:"payload"`

	// From names the node that published. Nothing uses it to decide anything;
	// it is there so that `redis-cli SUBSCRIBE chat:fanout` tells you who is
	// talking when delivery goes wrong.
	From string `json:"from"`
}

// FromEnv builds the cluster connection from REDIS_ADDR.
//
// It returns nil when REDIS_ADDR is not set. That is not an error: it is
// single-node mode, which is what `make run` and every test uses. The server
// checks for nil and falls back to fanning out locally, so one process with no
// Redis at all still delivers messages exactly as it did in Stage 2.
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

		// A fan-out that cannot be published quickly is not worth waiting for;
		// see publishTimeout.
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

// Publish sends one fan-out to every node, this one included.
//
// The sender does NOT deliver locally as well. Every node, including this one,
// delivers only what comes back out of the subscription, so there is exactly
// one delivery path and a message cannot arrive twice. The cost is honest and
// visible: with Redis down, live delivery stops everywhere rather than working
// for whoever happens to share a node with the sender. Half-working delivery is
// far harder to reason about than none.
func (c *Cluster) Publish(ctx context.Context, userIDs []uint, payload []byte) error {
	if len(userIDs) == 0 || len(payload) == 0 {
		return nil
	}

	raw, err := json.Marshal(fanout{UserIDs: userIDs, Payload: payload, From: c.nodeID})
	if err != nil {
		return fmt.Errorf("encode fan-out: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	if err := c.rdb.Publish(ctx, fanoutChannel, raw).Err(); err != nil {
		c.publishFails.Add(1)
		return fmt.Errorf("publish fan-out: %w", err)
	}
	c.published.Add(1)
	return nil
}

// Subscribe listens for fan-outs from every node and calls deliver for each
// one. It blocks until ctx is cancelled, so it belongs in its own goroutine.
//
// go-redis reconnects on its own when Redis goes away and comes back, and
// resubscribes to the channel. What it cannot do is replay: anything published
// while this node was disconnected is gone. Again — the message is in Postgres,
// so this is a missed live push, not a lost message.
func (c *Cluster) Subscribe(ctx context.Context, deliver func(userIDs []uint, payload []byte)) {
	// The retry loop is what makes a Redis outage survivable rather than
	// permanent. go-redis reconnects an ESTABLISHED subscription on its own,
	// but it cannot do anything about a subscribe that never succeeded — and a
	// node started while Redis was down is exactly that case. Without this
	// loop, such a node would run forever with no subscription: sending fine,
	// storing fine, and never pushing anything to its own sockets again.
	for ctx.Err() == nil {
		c.subscribeOnce(ctx, deliver)

		select {
		case <-ctx.Done():
			return
		case <-time.After(subscribeRetry):
		}
	}
}

// subscribeRetry is the wait between attempts to subscribe. Short, because the
// cost of being unsubscribed is that this node's sockets are silent.
const subscribeRetry = 2 * time.Second

// subscribeOnce holds one subscription until it fails or ctx ends.
func (c *Cluster) subscribeOnce(ctx context.Context, deliver func(userIDs []uint, payload []byte)) {
	// A child context, cancelled when this attempt ends, so the watcher
	// goroutine below does not pile up one copy per retry.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sub := c.rdb.Subscribe(ctx, fanoutChannel)
	defer sub.Close()

	// Cancelling ctx is not enough on its own. The context given to Subscribe
	// covers the subscribe command, not the stream that follows it: the channel
	// below stays open until the subscription is closed. Without this goroutine,
	// Shutdown would hang waiting for a loop that has no reason to end.
	go func() {
		<-ctx.Done()
		sub.Close()
	}()

	// Wait for the subscription to be confirmed before returning to the caller's
	// world. Without this, a message published in the first milliseconds after
	// startup can be missed by a node that is technically "subscribing".
	if _, err := sub.Receive(ctx); err != nil {
		if ctx.Err() == nil {
			// Once per outage, not once per retry. A Redis that is down for an
			// hour would otherwise write eighteen hundred identical lines, and
			// bury whatever else went wrong in the same hour.
			if c.subscribed.CompareAndSwap(true, false) {
				slog.Error("cluster lost its subscription, retrying",
					"channel", fanoutChannel, "err", err)
			}
		}
		return
	}

	// No "node" attribute on any line in this package. setupLogging already
	// puts one on every line the process writes, and a second one makes the
	// JSON hold the same key twice — which is legal JSON and a mess to query.
	if c.subscribed.CompareAndSwap(false, true) {
		slog.Info("cluster subscribed", "channel", fanoutChannel)
	}

	for msg := range sub.Channel() {
		var f fanout
		if err := json.Unmarshal([]byte(msg.Payload), &f); err != nil {
			slog.Warn("cluster got an unreadable fan-out", "err", err)
			continue
		}
		c.receivedFrom.Add(1)
		deliver(f.UserIDs, f.Payload)
	}
}

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

// Subscribed says whether this node currently holds its subscription. A node
// that is not subscribed stores messages and pushes nothing.
func (c *Cluster) Subscribed() bool { return c.subscribed.Load() }

// Counters, for internal/metrics.
func (c *Cluster) Published() int      { return int(c.published.Load()) }
func (c *Cluster) Received() int       { return int(c.receivedFrom.Load()) }
func (c *Cluster) PublishFailed() int  { return int(c.publishFails.Load()) }
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

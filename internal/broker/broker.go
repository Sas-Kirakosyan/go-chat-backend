// Package broker carries events between nodes over NATS JetStream.
//
// It replaces the Redis Pub/Sub fan-out from Stage 3. Redis is still here, but
// only for presence.
//
// # Why the change
//
// Pub/Sub has no memory. A node that was restarting when a message was
// published never learns about it, and there is nobody to ask. There is no
// ack, no retry, and no way to find out what failed. That was acceptable while
// the live push was best effort and history was the real record. It stops
// being acceptable the moment something other than a socket has to react to a
// message — a counter, a notification — because that work must happen exactly
// once eventually, not "probably, if the node was up".
//
// JetStream keeps the message until a consumer acknowledges it. That single
// difference is what makes a consumer with retries, redelivery and a
// dead-letter queue possible at all.
//
// # Two consumers, two different shapes
//
// The same stream feeds two things that want opposite delivery rules, and that
// contrast is the most useful thing in this package.
//
//   - Fan-out: EVERY node needs EVERY message, because each node owns
//     different sockets. So each node makes its own consumer, and a node that
//     was away does not replay — its sockets are gone, and a reconnecting
//     client repairs itself with ?after_seq= from Stage 4.
//
//   - Unread counters: exactly ONE node should do the work, because the work
//     is a write. So every node joins one shared durable consumer, and
//     JetStream hands each message to one of them. This is where acks,
//     redelivery and the dead-letter queue live.
//
// # What this package does not know
//
// It knows nothing about Gin, the hub, or the database. It moves events. The
// caller says what to do with one.
package broker

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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go-chat-backend/internal/event"
)

const (
	// streamName holds every chat event. One stream, subjects underneath it,
	// so a second kind of event later is a new subject and not a new stream to
	// create, size and watch.
	streamName = "CHAT"

	// dlqStreamName holds the messages that failed too many times. It is a
	// separate stream because it wants the opposite settings: tiny, and kept
	// for a long time, because somebody has to come and read it.
	dlqStreamName = "CHAT_DLQ"

	// messageSubject is where a stored message is published. The room id is
	// the last token, so `nats sub "chat.message.7"` shows one room while
	// debugging, even though nothing subscribes that narrowly in production.
	messageSubjectPrefix = "chat.message."

	// deadSubject is where a message goes when it has failed MaxDeliver times.
	deadSubject = "chat.dead.unread"

	// unreadDurable is the shared consumer name. Every node uses the same one,
	// which is exactly what makes it shared: JetStream tracks one set of
	// pending messages and gives each to one node.
	unreadDurable = "unread"
)

const (
	// maxDeliver is how many times a message is handed out before it is
	// treated as poison. Four retries is enough to ride out a database restart
	// and short enough that a message that will never work stops burning
	// capacity within a couple of minutes.
	maxDeliver = 5

	// ackWait is how long JetStream waits for an ack before deciding the
	// consumer died and handing the message to somebody else. It has to be
	// comfortably longer than the work: the unread update is one statement, so
	// 30 seconds is a very wide margin, and a wide margin is right — a
	// redelivery caused by a slow query is work done twice for no reason.
	ackWait = 30 * time.Second

	// duplicateWindow is how long JetStream remembers a message id and drops a
	// repeat of it.
	//
	// This is what protects against the relay crashing after publishing and
	// before it could mark the outbox row done: on restart it republishes, and
	// JetStream throws the copy away. Five minutes covers a restart many times
	// over.
	//
	// It is a window and not a promise. After five minutes a duplicate gets
	// through, which is why the consumer is idempotent as well. Two layers,
	// because either one alone has a hole.
	duplicateWindow = 5 * time.Minute

	// maxAge is how long the stream keeps a message. It is a delivery buffer,
	// not an archive — Postgres is the archive — so a day is generous.
	maxAge = 24 * time.Hour

	// publishTimeout caps one publish from the relay.
	//
	// Unlike the Redis publish it replaces, this one is NOT on the HTTP write
	// path: the handler returns as soon as the outbox row is committed. So a
	// slow broker delays delivery and never delays a user's request. The
	// timeout is here so the relay notices and retries, not to protect a
	// caller.
	publishTimeout = 5 * time.Second

	// connectTimeout caps the first connection at startup, so a broker that is
	// unreachable does not hold the process from serving HTTP.
	connectTimeout = 3 * time.Second
)

// Broker is this node's connection to the others.
type Broker struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
	nodeID string

	// Counters, read by Prometheus at scrape time. The value lives here and
	// the metric reads it, which is the same pattern the hub and the cluster
	// use.
	published    atomic.Int64
	publishFails atomic.Int64
	fanoutRecv   atomic.Int64
	unreadDone   atomic.Int64
	redelivered  atomic.Int64
	deadLettered atomic.Int64

	// consuming says whether both subscriptions are live. A node stuck at 0
	// here is the clearest sign that its sockets have gone quiet.
	consuming atomic.Bool
}

// envelope is one event as it travels between nodes.
//
// It carries the member list beside the event because the relay already looked
// it up once. Letting every node repeat that lookup would turn one query per
// message into one query per message per node, for an answer that would be the
// same on all of them.
type envelope struct {
	Event   event.MessageCreated `json:"event"`
	UserIDs []uint               `json:"user_ids"`

	// From names the node whose relay published. Nothing decides anything with
	// it; it is there so `nats sub "chat.>"` tells you who is talking when
	// delivery goes wrong.
	From string `json:"from"`
}

// FromEnv builds the broker connection from NATS_URL.
//
// It returns nil when NATS_URL is not set. That is not an error: it is
// single-node mode, which is what `make run` and every test uses. The outbox
// relay still runs and still drains — it just publishes straight into the
// local hub instead. See internal/outbox.
//
// A broker that does not answer is a warning and not a startup failure, for
// the same reason Redis is not: a shared dependency may degrade this node,
// never remove it. Writes keep working because the outbox lives in Postgres,
// which is the whole point of the outbox. nats.go reconnects on its own, and
// the relay drains the backlog when it does.
func FromEnv(ctx context.Context) *Broker {
	url := os.Getenv("NATS_URL")
	if url == "" {
		return nil
	}

	id := nodeID()

	nc, err := nats.Connect(url,
		nats.Name("go-chat-"+id),
		// Reconnect forever. The default gives up after 60 tries, and a broker
		// that comes back after an hour should find its nodes waiting, not a
		// cluster that quietly stopped trying.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Timeout(connectTimeout),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats disconnected, messages will queue in the outbox", "err", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			slog.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
	)
	if err != nil {
		slog.Warn("could not reach NATS; delivery will wait in the outbox until it returns",
			"url", url, "err", err)
		return nil
	}

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Warn("could not start JetStream", "err", err)
		nc.Close()
		return nil
	}

	b := &Broker{nc: nc, js: js, nodeID: id}
	if err := b.setupStreams(ctx); err != nil {
		slog.Warn("could not prepare NATS streams", "err", err)
		nc.Close()
		return nil
	}

	slog.Info("connected to NATS", "url", nc.ConnectedUrl(), "node_id", id)
	return b
}

// setupStreams creates the two streams, or updates them if they already exist.
//
// Every node runs this on every start, which is safe: the call describes the
// wanted shape rather than asking for a new object, so the second node through
// changes nothing. It is the same idea as running migrations on every start.
func (b *Broker) setupStreams(ctx context.Context) error {
	stream, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{messageSubjectPrefix + ">"},
		// On disk, not in memory. A broker restart must not lose the messages
		// the relay has already marked as published — those rows are gone from
		// the outbox's point of view.
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     maxAge,
		Duplicates: duplicateWindow,
	})
	if err != nil {
		return fmt.Errorf("create stream %s: %w", streamName, err)
	}
	b.stream = stream

	_, err = b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     dlqStreamName,
		Subjects: []string{"chat.dead.>"},
		Storage:  jetstream.FileStorage,
		// A month. Nobody looks at a dead-letter queue on the day it fills up,
		// and a message that is thrown away before anyone reads it might as
		// well never have been kept.
		MaxAge: 30 * 24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("create stream %s: %w", dlqStreamName, err)
	}
	return nil
}

// Publish sends one event to every node.
//
// outboxID becomes the JetStream message id, and that is what makes a repeat
// harmless. The relay publishes, then marks the outbox row done; if it dies
// between those two steps it will publish the same row again on restart, and
// JetStream drops the second copy because it has seen that id inside the
// duplicate window.
func (b *Broker) Publish(ctx context.Context, outboxID uint64, ev event.MessageCreated, userIDs []uint) error {
	if b == nil {
		return nil
	}

	data, err := json.Marshal(envelope{Event: ev, UserIDs: userIDs, From: b.nodeID})
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	// Publish and wait for the ack, not PublishAsync. The relay must know the
	// broker really has the message before it stamps the outbox row, because
	// the stamp is what makes the row unreadable forever.
	_, err = b.js.Publish(ctx, messageSubject(ev.ConversationID), data, jetstream.WithMsgID(msgID(outboxID)))
	if err != nil {
		b.publishFails.Add(1)
		return fmt.Errorf("publish to nats: %w", err)
	}

	b.published.Add(1)
	return nil
}

// msgID is the dedupe key JetStream stores. The outbox row id is already
// unique for all time, so nothing has to be invented.
func msgID(outboxID uint64) string {
	return "outbox-" + strconv.FormatUint(outboxID, 10)
}

// messageSubject is the subject one room's messages are published on.
func messageSubject(conversationID uint) string {
	return messageSubjectPrefix + strconv.FormatUint(uint64(conversationID), 10)
}

// Close shuts the connection down. It is nil-safe, because single-node mode
// has no broker and Shutdown should not have to know that.
func (b *Broker) Close() error {
	if b == nil || b.nc == nil {
		return nil
	}
	// Drain, not Close: it finishes delivering what has already been pulled
	// before the connection goes. A hard Close here would drop the last
	// messages of a node that is shutting down cleanly.
	if err := b.nc.Drain(); err != nil {
		return fmt.Errorf("drain nats: %w", err)
	}
	return nil
}

// Ping reports whether the broker is reachable right now. /health uses it;
// /readyz deliberately does not — see internal/server/health.go.
func (b *Broker) Ping(ctx context.Context) error {
	if b == nil {
		return errors.New("no broker configured")
	}
	if !b.nc.IsConnected() {
		return errors.New("not connected to nats")
	}
	// A real round trip. IsConnected alone can be true while the server has
	// stopped answering.
	if _, err := b.js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("nats account info: %w", err)
	}
	return nil
}

// nodeID names this process in logs and in published envelopes. It falls back
// to the hostname, which is what the container already sets.
func nodeID() string {
	if id := os.Getenv("NODE_ID"); id != "" {
		return id
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "unknown"
}

package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go-chat-backend/internal/event"
)

// DeliverFunc pushes one event to the sockets of the listed users on THIS
// node. It is the fan-out side of delivery, and the caller supplies it because
// this package must not know what a socket is.
type DeliverFunc func(ev event.MessageCreated, userIDs []uint)

// ApplyFunc does the durable work for one event — today, the unread counters.
//
// An error means "try again": the message is redelivered, and after enough
// tries it goes to the dead-letter queue. So return an error only for
// something that might work next time, like a database that is restarting. A
// message that can never work must not return an error forever; it should be
// reported and accepted, or the queue stops behind it.
type ApplyFunc func(ctx context.Context, ev event.MessageCreated) error

// SubscribeFanout delivers every message to this node's sockets, and blocks
// until ctx ends.
//
// # Why this consumer is per-node and throws history away
//
// Every node needs every message, because each node holds different sockets.
// A shared consumer would give each message to one node and the members
// connected everywhere else would see nothing. So each node creates its own
// consumer.
//
// It starts at DeliverNew, so a node that was down does not replay what it
// missed. That looks like giving up, and it is the right answer: the sockets
// that would have received those messages are closed. Their clients reconnect
// and ask for the gap with ?after_seq=, which Stage 4 built for exactly this.
// Replaying here would push messages at sockets that no longer exist and
// deliver every one of them twice to the sockets that do.
//
// Acks are off for the same reason. There is nothing to retry: if the socket
// is gone, sending again does not help, and the client repairs itself.
//
// The consumer is ephemeral with a short inactive threshold, so a node that
// never comes back leaves no state behind on the broker.
func (b *Broker) SubscribeFanout(ctx context.Context, deliver DeliverFunc) {
	if b == nil {
		return
	}

	cons, err := b.stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		// Named after the node so the same node restarting reuses its own
		// consumer instead of leaving a new one behind each time.
		Name:              "fanout-" + b.nodeID,
		FilterSubject:     messageSubjectPrefix + ">",
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		slog.Error("could not create fan-out consumer; this node will not deliver live messages", "err", err)
		return
	}

	sub, err := cons.Consume(func(msg jetstream.Msg) {
		var env envelope
		if err := json.Unmarshal(msg.Data(), &env); err != nil {
			// Nothing to retry: the bytes will not improve. Acks are off on
			// this consumer anyway, so it is dropped either way.
			slog.Error("fan-out could not decode envelope", "err", err)
			return
		}
		b.fanoutRecv.Add(1)
		deliver(env.Event, env.UserIDs)
	})
	if err != nil {
		slog.Error("could not start fan-out consumer", "err", err)
		return
	}
	defer sub.Stop()

	slog.Info("subscribed to message fan-out", "consumer", "fanout-"+b.nodeID)
	b.consuming.Store(true)
	defer b.consuming.Store(false)

	<-ctx.Done()
}

// ConsumeUnread runs the durable work for every message, and blocks until ctx
// ends.
//
// # Why this consumer is shared and acknowledges
//
// The work here is a write. It must happen once for the whole cluster, not
// once per node, so every node joins ONE durable consumer by name and
// JetStream hands each message to exactly one of them. Add a third node and
// the work spreads; lose a node mid-message and the message comes back to
// somebody else after AckWait.
//
// That is also the failure mode this package exists to survive, and it is why
// each step below is where it is:
//
//   - apply first, ack second. Ack first would lose the work of any process
//     that died in between.
//   - a failed apply is Nak'd with a delay, so a database that is restarting
//     gets a few seconds instead of five instant retries.
//   - after maxDeliver tries the message is poison. It goes to the dead-letter
//     queue and is Term'd, which means "never send this again". Without that
//     step one bad message is retried forever and the rest of the queue waits
//     behind it.
func (b *Broker) ConsumeUnread(ctx context.Context, apply ApplyFunc) {
	if b == nil {
		return
	}

	cons, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		// The same name on every node. That is what makes it shared.
		Durable:       unreadDurable,
		FilterSubject: messageSubjectPrefix + ">",
		// Explicit acks are the whole point: JetStream keeps the message until
		// somebody says the work is done.
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    ackWait,
		MaxDeliver: maxDeliver,
		// Start from the beginning of the stream the first time this consumer
		// is created. Unlike fan-out, this work is not tied to a live socket,
		// so a message that arrived while every node was down still has to be
		// counted.
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		slog.Error("could not create unread consumer", "err", err)
		return
	}

	sub, err := cons.Consume(func(msg jetstream.Msg) {
		b.handleUnread(ctx, msg, apply)
	})
	if err != nil {
		slog.Error("could not start unread consumer", "err", err)
		return
	}
	defer sub.Stop()

	slog.Info("consuming unread work", "consumer", unreadDurable)
	<-ctx.Done()
}

// handleUnread processes one message. It is separate from ConsumeUnread only
// so the retry and dead-letter rules can be read in one screen.
func (b *Broker) handleUnread(ctx context.Context, msg jetstream.Msg, apply ApplyFunc) {
	meta, err := msg.Metadata()
	if err != nil {
		// Not a JetStream message, which should be impossible on this
		// subscription. Terminate rather than loop on it.
		slog.Error("unread consumer got a message with no metadata", "err", err)
		_ = msg.Term()
		return
	}
	if meta.NumDelivered > 1 {
		b.redelivered.Add(1)
	}

	var env envelope
	if err := json.Unmarshal(msg.Data(), &env); err != nil {
		// Poison on arrival. Bytes that do not parse today will not parse on
		// the fifth try either, so it goes straight to the dead letter queue
		// instead of burning four redeliveries first.
		b.deadLetter(ctx, msg, meta, fmt.Sprintf("decode envelope: %v", err))
		return
	}

	// A deadline of its own. Without one, a database that accepts the
	// connection and then never answers would hold this message past AckWait,
	// and JetStream would hand a second copy to another node while the first
	// is still running.
	work, cancel := context.WithTimeout(ctx, ackWait/2)
	defer cancel()

	if err := apply(work, env.Event); err != nil {
		if meta.NumDelivered >= maxDeliver {
			b.deadLetter(ctx, msg, meta, err.Error())
			return
		}
		slog.Warn("unread work failed, will retry",
			"message_id", env.Event.MessageID,
			"delivered", meta.NumDelivered,
			"err", err)
		// A delay, not an instant Nak. The usual cause is a dependency that is
		// briefly down, and hammering it does not help it come back.
		_ = msg.NakWithDelay(time.Duration(meta.NumDelivered) * time.Second)
		return
	}

	if err := msg.Ack(); err != nil {
		// The work is done and the ack was lost. The message will come back,
		// and the inbox row written by the work makes the second run a no-op.
		// This is exactly the case idempotency was built for.
		slog.Warn("unread work done but ack failed; expect one redelivery",
			"message_id", env.Event.MessageID, "err", err)
		return
	}
	b.unreadDone.Add(1)
}

// deadLetter moves a message that cannot be processed out of the way.
//
// The order matters: copy it to the dead-letter stream FIRST, and only then
// Term it. Term is final — JetStream will never hand the message out again —
// so terminating before the copy is safe would throw the evidence away.
//
// If the copy itself fails, the message is Nak'd instead. It comes back, and
// on the next try the copy may work. A message that is stuck is better than a
// message that is silently gone.
func (b *Broker) deadLetter(ctx context.Context, msg jetstream.Msg, meta *jetstream.MsgMetadata, reason string) {
	dead := struct {
		Reason    string          `json:"reason"`
		Delivered uint64          `json:"delivered"`
		Node      string          `json:"node"`
		At        time.Time       `json:"at"`
		Original  json.RawMessage `json:"original"`
	}{
		Reason:    reason,
		Delivered: meta.NumDelivered,
		Node:      b.nodeID,
		At:        time.Now().UTC(),
		Original:  json.RawMessage(msg.Data()),
	}

	body, err := json.Marshal(dead)
	if err != nil {
		// The original did not parse, so it cannot be embedded as JSON. Keep
		// the reason and the raw bytes as a string, which is still enough for
		// a person to work out what happened.
		body = []byte(fmt.Sprintf(`{"reason":%q,"raw":%q}`, reason, string(msg.Data())))
	}

	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	if _, err := b.js.Publish(pubCtx, deadSubject, body); err != nil {
		slog.Error("could not write to the dead-letter queue; message will be retried instead",
			"reason", reason, "err", err)
		_ = msg.NakWithDelay(ackWait)
		return
	}

	_ = msg.Term()
	b.deadLettered.Add(1)
	slog.Error("message sent to the dead-letter queue",
		"stream", dlqStreamName,
		"subject", deadSubject,
		"delivered", meta.NumDelivered,
		"reason", reason)
}

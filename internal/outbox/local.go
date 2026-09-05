package outbox

import (
	"context"

	"go-chat-backend/internal/event"
)

// DeliverFunc pushes one event to the sockets of the listed users on this
// node.
type DeliverFunc func(ev event.MessageCreated, userIDs []uint)

// ApplyFunc does the durable work for one event — the unread counters.
type ApplyFunc func(ctx context.Context, ev event.MessageCreated) error

// LocalPublisher is the Publisher used when there is no broker: one process,
// no NATS_URL, which is what `make run` and every handler test look like.
//
// It does in one step what the broker does in three: hand the event to this
// node's hub, and do the durable work here and now. There is no other node to
// tell, so there is nothing to send anywhere.
//
// # Why the relay still runs in this mode
//
// It would be less code to skip the outbox entirely and push from the handler,
// the way Stage 3 did. It would also mean the single-node path and the
// clustered path were two different programs, and the one that gets tested
// most would be the one that does not run in production.
//
// So the outbox row is always written and always drained. Only the last step
// changes. That also makes the outbox easy to believe in: it clearly does not
// depend on the broker, because here there is no broker.
type LocalPublisher struct {
	deliver DeliverFunc
	apply   ApplyFunc
}

// NewLocalPublisher wires the relay straight to this node's hub and database.
func NewLocalPublisher(deliver DeliverFunc, apply ApplyFunc) *LocalPublisher {
	return &LocalPublisher{deliver: deliver, apply: apply}
}

// Publish delivers the event locally.
//
// The durable work goes first. If it fails, the relay is told, the outbox row
// stays unpublished, and the whole thing is tried again on the next poll —
// which is the same guarantee the broker path gives, reached without a broker.
// The socket push happens after, and its result is not checked, because a
// socket that missed a frame is repaired by the client with ?after_seq= and
// never by a retry here.
//
// outboxID is ignored: it exists to dedupe across processes, and there is only
// one process.
func (p *LocalPublisher) Publish(ctx context.Context, _ uint64, ev event.MessageCreated, userIDs []uint) error {
	if p.apply != nil {
		if err := p.apply(ctx, ev); err != nil {
			return err
		}
	}
	if p.deliver != nil {
		p.deliver(ev, userIDs)
	}
	return nil
}

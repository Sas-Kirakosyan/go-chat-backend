package broker

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// headerCarrier lets the trace propagator read and write NATS headers.
//
// # Why the headers and not the body
//
// The body is the envelope, and both consumers have to decode it before they
// know anything. Put the trace context in there and a message whose JSON is
// broken loses the trace of its own failure — exactly the message you most
// want to follow, because it is the one going to the dead-letter queue.
//
// Headers are read without touching the payload, so a poison message still
// arrives attached to the request that sent it.
//
// nats.Header is a map[string][]string, the same shape as http.Header, so this
// is a rename and nothing more. The propagator only ever writes single values.
type headerCarrier nats.Header

var _ propagation.TextMapCarrier = headerCarrier{}

func (h headerCarrier) Get(key string) string {
	v := nats.Header(h).Values(key)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (h headerCarrier) Set(key, value string) {
	nats.Header(h).Set(key, value)
}

func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// injectTrace writes the current trace context into a message's headers.
//
// With tracing off the propagator writes nothing and the message goes out with
// an empty header set, which NATS is perfectly happy with.
func injectTrace(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(msg.Header))
}

// extractTrace reads a trace context back off a received message.
//
// A message with no traceparent — published by a node with tracing off, or
// sitting in the stream since before this stage — gives back ctx unchanged, and
// the consumer starts a root span. Consuming must never depend on being traced.
func extractTrace(ctx context.Context, header nats.Header) context.Context {
	if len(header) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier(header))
}

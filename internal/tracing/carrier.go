package tracing

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// This file is how a trace survives being written to a database and read back
// minutes later, possibly by a different process.
//
// Everywhere else the context crosses a boundary that already has a place for
// it: an HTTP header, gRPC metadata, a NATS header. The outbox has none. The
// row sits in Postgres until the relay — which may be on another node — comes
// to fetch it, and by then the request that wrote it has answered its 201 and
// gone.
//
// So the carrier is stored in a column of its own. Two functions, and both of
// them treat "there is no trace here" as the normal case, because that is what
// every row written before this stage, and every row written with tracing off,
// looks like.

// InjectCarrier turns the trace context in ctx into JSON to store.
//
// It returns nil when there is nothing to store: tracing is off, or this code
// path had no span. nil becomes a NULL column, which is exactly right — an
// empty JSON object would claim there was a trace and be a lie.
func InjectCarrier(ctx context.Context) []byte {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}

	// The carrier is a small map of short strings written by the propagator
	// itself, so this cannot fail. Returning nil on the impossible branch keeps
	// the caller free of an error it would have nothing useful to do with:
	// a message must not fail to send because its trace could not be recorded.
	b, err := json.Marshal(map[string]string(carrier))
	if err != nil {
		return nil
	}
	return b
}

// ExtractCarrier puts a stored carrier back on a context, so the next span
// becomes a child of the request that caused it.
//
// # The span it re-parents is a child of a span that has already ended
//
// That looks wrong the first time and it is the point of the whole stage. The
// HTTP span finished at the 201. The relay publishes up to 100 ms later. A span
// context stays valid after its span ends — it is an id, not a live object — so
// the publish attaches to the finished request span and Jaeger draws the wait
// between them at scale. That gap IS the relay poll interval, measured instead
// of estimated.
//
// Bad or missing bytes give back the parent context unchanged, so the caller
// starts a root span instead. A trace that begins at the relay is much better
// than a relay that stops publishing because a column would not parse.
func ExtractCarrier(ctx context.Context, stored []byte) context.Context {
	if len(stored) == 0 {
		return ctx
	}

	var carrier propagation.MapCarrier
	if err := json.Unmarshal(stored, &carrier); err != nil {
		return ctx
	}

	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// TraceID is the trace id as a string, or "" when there is no trace.
//
// It is what puts trace_id on a log line. That one field is the join between
// the two views: find a slow trace in Jaeger, then grep four processes' logs
// for its id and read everything they said about it, in order.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanID is the current span's id, or "".
func SpanID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasSpanID() {
		return ""
	}
	return sc.SpanID().String()
}

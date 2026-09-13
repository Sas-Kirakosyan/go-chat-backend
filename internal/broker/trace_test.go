package broker

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"go-chat-backend/internal/tracing"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
	})

	return recorder
}

// TestTraceCrossesNATSHeaders checks the third of the four boundaries: the
// relay publishes on one node and a consumer picks the message up on another.
//
// It exercises the carrier directly rather than through a broker, because the
// only thing that can break here is the header adapter — and a running NATS
// would test JetStream, not this.
func TestTraceCrossesNATSHeaders(t *testing.T) {
	recordSpans(t)

	pubCtx, pubSpan := tracing.Tracer().Start(context.Background(), "outbox.publish")
	defer pubSpan.End()

	msg := &nats.Msg{Subject: "chat.message.7"}
	injectTrace(pubCtx, msg)

	if msg.Header.Get("traceparent") == "" {
		t.Fatal("no traceparent header was written, so the trace stops at the broker")
	}

	// The other node: a fresh context, as a consumer callback really has.
	gotCtx := extractTrace(context.Background(), msg.Header)
	_, consumerSpan := tracing.Tracer().Start(gotCtx, "fanout.deliver")
	defer consumerSpan.End()

	want := pubSpan.SpanContext().TraceID()
	if got := consumerSpan.SpanContext().TraceID(); got != want {
		t.Fatalf("consumer span is in trace %s, want %s", got, want)
	}
}

// TestExtractTraceWithNoHeaders covers a message published by a node with
// tracing off, or one sitting in the stream since before this stage. The
// consumer must start its own trace, not refuse the message.
func TestExtractTraceWithNoHeaders(t *testing.T) {
	recordSpans(t)

	ctx := extractTrace(context.Background(), nil)
	_, span := tracing.Tracer().Start(ctx, "fanout.deliver")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Fatal("a message with no trace headers produced no span at all")
	}
}

// TestHeaderCarrierKeys checks the small adapter does what the propagator
// expects: NATS headers are a map of slices, and a carrier deals in single
// values.
func TestHeaderCarrierKeys(t *testing.T) {
	carrier := headerCarrier(nats.Header{})
	carrier.Set("traceparent", "abc")

	if got := carrier.Get("traceparent"); got != "abc" {
		t.Errorf("Get() = %q, want %q", got, "abc")
	}
	if got := carrier.Get("missing"); got != "" {
		t.Errorf("Get() on a missing key = %q, want empty", got)
	}
	if keys := carrier.Keys(); len(keys) != 1 {
		t.Errorf("Keys() = %v, want exactly one", keys)
	}
}

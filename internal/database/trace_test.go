package database

import (
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"go-chat-backend/internal/tracing"
)

// Stage 7 stores a trace carrier in the outbox row, because that is the one
// boundary in this system with nowhere else to put one: the row is read later,
// by a relay that may be on a different node, long after the request that wrote
// it has answered.
//
// These run against a real Postgres, like the rest of this package, because
// what is being checked is that a jsonb column round-trips a carrier — and a
// fake database would only prove the fake agrees with itself.

func withRecordedSpans(t *testing.T) {
	t.Helper()

	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(tracetest.NewSpanRecorder()))
	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
	})
}

func TestCreateMessageStoresTheTraceContext(t *testing.T) {
	withRecordedSpans(t)

	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "trace-sender")
	conv, err := srv.CreateConversation(ctx, "trace room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)

	// The span the HTTP middleware would have started.
	spanCtx, span := tracing.Tracer().Start(ctx, "http.request")
	_, created, err := srv.CreateMessage(spanCtx, conv.ID, sender.ID, sender.Username, "traced", nil)
	span.End()
	if err != nil || !created {
		t.Fatalf("CreateMessage() = created %v, err %v", created, err)
	}

	rows := pendingSince(t, srv, before)
	if len(rows) != 1 {
		t.Fatalf("one message wrote %d outbox rows, want 1", len(rows))
	}

	stored := rows[0].TraceContext
	if stored == nil {
		t.Fatal("trace_context is NULL, so the relay could never continue this trace")
	}

	var carrier map[string]string
	if err := json.Unmarshal([]byte(*stored), &carrier); err != nil {
		t.Fatalf("trace_context is not a readable carrier: %v", err)
	}
	if carrier["traceparent"] == "" {
		t.Fatalf("stored carrier has no traceparent: %v", carrier)
	}

	// The stored carrier must name the span that wrote the row. This is the
	// assertion that would catch an injection taken from the wrong context.
	want := span.SpanContext().TraceID().String()
	extracted := tracing.ExtractCarrier(context.Background(), []byte(*stored))
	if got := tracing.TraceID(extracted); got != want {
		t.Fatalf("stored trace id is %s, want %s", got, want)
	}
}

// TestCreateMessageWithNoTraceWritesNull is the ordinary case, not the edge
// one: no collector configured, which is `make run` and every other test in
// this package. The column has to be NULL and the send has to work.
func TestCreateMessageWithNoTraceWritesNull(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}

	sender := mustUser(t, srv, "untraced-sender")
	conv, err := srv.CreateConversation(ctx, "untraced room", sender.ID, nil)
	if err != nil {
		t.Fatalf("CreateConversation() returned %v", err)
	}

	before := mustPending(t, srv)

	if _, created, err := srv.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, "plain", nil); err != nil || !created {
		t.Fatalf("CreateMessage() = created %v, err %v", created, err)
	}

	rows := pendingSince(t, srv, before)
	if len(rows) != 1 {
		t.Fatalf("one message wrote %d outbox rows, want 1", len(rows))
	}
	if rows[0].TraceContext != nil {
		t.Fatalf("trace_context is %q with tracing off, want NULL", *rows[0].TraceContext)
	}
}

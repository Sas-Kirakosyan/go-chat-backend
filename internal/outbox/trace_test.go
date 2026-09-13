package outbox

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"go-chat-backend/internal/event"
	"go-chat-backend/internal/tracing"
)

// recordSpans installs a real tracer that keeps its spans in memory, for the
// length of one test.
//
// A real SDK and not a mock, because the thing under test is whether a span
// really becomes the child of a trace id that came out of a database column.
// A mock that recorded the calls would pass while the parenting was wrong,
// which is the only interesting way this code can break.
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

// TestDrainContinuesTheTraceThatWroteTheRow is the whole point of Stage 7 in
// one test.
//
// A request writes an outbox row and finishes. Later — here, immediately; in
// production, up to a poll interval later and usually on another node — the
// relay picks the row up. The publish must land in the SAME trace as the
// request, or the two halves of one message are two unrelated pictures.
func TestDrainContinuesTheTraceThatWroteTheRow(t *testing.T) {
	recorder := recordSpans(t)

	// Pretend to be the HTTP request: start a span, take the carrier the way
	// CreateMessage does, and end it. The span is over before the relay runs,
	// which is exactly the situation in production.
	reqCtx, reqSpan := tracing.Tracer().Start(context.Background(), "http.request")
	carrier := tracing.InjectCarrier(reqCtx)
	wantTrace := reqSpan.SpanContext().TraceID()
	reqSpan.End()

	if len(carrier) == 0 {
		t.Fatal("InjectCarrier() gave nothing to store, so no trace could ever cross the outbox")
	}

	store := newFakeStore(event.MessageCreated{MessageID: 1, ConversationID: 7, Seq: 1})
	stored := string(carrier)
	store.rows[0].TraceContext = &stored

	pub := &capturePublisher{}
	relay := New(store, pub, slog.Default())

	if _, err := relay.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() returned %v", err)
	}

	publish := findSpan(t, recorder, "outbox.publish")
	if got := publish.SpanContext().TraceID(); got != wantTrace {
		t.Fatalf("publish span is in trace %s, want %s — the trace broke at the outbox", got, wantTrace)
	}
	if publish.Parent().SpanID() != reqSpan.SpanContext().SpanID() {
		t.Errorf("publish span's parent is %s, want the request span %s",
			publish.Parent().SpanID(), reqSpan.SpanContext().SpanID())
	}
	if publish.SpanKind() != trace.SpanKindProducer {
		t.Errorf("publish span kind is %v, want Producer", publish.SpanKind())
	}
}

// TestDrainPublishesRowsWithNoTrace is the case that must never regress.
//
// Rows written before Stage 7, rows written with tracing off, and rows that
// were not sampled all have a NULL trace_context. Every one of them still has
// to go out. A relay that needed a trace to publish would have turned an
// observability feature into a delivery bug.
func TestDrainPublishesRowsWithNoTrace(t *testing.T) {
	recorder := recordSpans(t)

	store := newFakeStore(event.MessageCreated{MessageID: 1, ConversationID: 7, Seq: 1})
	if store.rows[0].TraceContext != nil {
		t.Fatal("the fake store should start with no trace context")
	}

	pub := &capturePublisher{}
	relay := New(store, pub, slog.Default())

	batch, err := relay.Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Published != 1 {
		t.Fatalf("published %d rows, want 1 — a row with no trace must still go out", batch.Published)
	}

	// It still gets a span; it is simply the root of a shorter trace.
	publish := findSpan(t, recorder, "outbox.publish")
	if publish.Parent().IsValid() {
		t.Errorf("publish span has parent %v, want a root span", publish.Parent())
	}
}

// TestDrainSurvivesAnUnreadableTraceContext covers a column that holds
// something that is not a carrier — a hand-edited row, or a format we stop
// writing one day. A trace that cannot be read is not a reason to stop
// delivering messages.
func TestDrainSurvivesAnUnreadableTraceContext(t *testing.T) {
	recordSpans(t)

	store := newFakeStore(event.MessageCreated{MessageID: 1, ConversationID: 7, Seq: 1})
	junk := `{"traceparent": 12345}`
	store.rows[0].TraceContext = &junk

	pub := &capturePublisher{}
	relay := New(store, pub, slog.Default())

	batch, err := relay.Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain() returned %v", err)
	}
	if batch.Published != 1 {
		t.Fatalf("published %d rows, want 1", batch.Published)
	}
}

func findSpan(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, s := range recorder.Ended() {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("no span named %q was recorded", name)
	return nil
}

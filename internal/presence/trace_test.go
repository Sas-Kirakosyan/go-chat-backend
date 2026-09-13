package presence

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stage 6's worst bug was confusing two questions: "did this call fail" and
// "did the service reply". They look the same in a log, and every unit test
// passed while the answer was wrong, because the tests all ran against a server
// that answered.
//
// On a trace they are not confusable, and these tests check that the two shapes
// really are distinguishable — a failed call that reached the network, and a
// refused call that never left the process.

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

func TestAnOpenBreakerIsVisibleOnTheTrace(t *testing.T) {
	recorder := recordSpans(t)

	svc := &fakeService{answer: func(int) ([]uint64, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}}
	client := start(t, svc)

	// Enough failures to open it, and then one more that is refused.
	for i := 0; i < breakerThreshold+1; i++ {
		_, _ = client.Online(context.Background(), []uint{1})
	}

	var withEvent, failedCalls int
	for _, span := range recorder.Ended() {
		if span.Name() != "presence.Online" {
			continue
		}
		for _, ev := range span.Events() {
			if ev.Name == "short_circuited" {
				withEvent++
			}
		}
		if span.Status().Code != 0 {
			failedCalls++
		}
	}

	if withEvent != 1 {
		t.Errorf("%d spans carry a short_circuited event, want exactly 1 — the call after the breaker opened", withEvent)
	}
	if failedCalls != breakerThreshold+1 {
		t.Errorf("%d presence.Online spans are marked failed, want %d", failedCalls, breakerThreshold+1)
	}
}

// A call the service really answered — with a refusal — must be marked
// answered, because that is exactly what the breaker keys on. If this
// attribute is ever wrong, the Stage 6 bug is back.
func TestTraceRecordsWhetherTheServiceAnswered(t *testing.T) {
	recorder := recordSpans(t)

	svc := &fakeService{answer: func(int) ([]uint64, error) {
		return nil, status.Error(codes.InvalidArgument, "bad request")
	}}
	client := start(t, svc)

	if _, err := client.Online(context.Background(), []uint{1}); err == nil {
		t.Fatal("Online() returned no error, want InvalidArgument")
	}

	for _, span := range recorder.Ended() {
		if span.Name() != "presence.Online" {
			continue
		}
		for _, attr := range span.Attributes() {
			if attr.Key == "presence.answered" {
				if !attr.Value.AsBool() {
					t.Fatal("an InvalidArgument reply was recorded as unanswered; the breaker would open on a healthy service")
				}
				return
			}
		}
		t.Fatal("the failed presence.Online span has no presence.answered attribute")
	}
	t.Fatal("no presence.Online span was recorded")
}

package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestSetupWithNoEndpointIsANoOp(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown := Setup(context.Background(), "test")
	if shutdown == nil {
		t.Fatal("Setup() returned a nil Shutdown; callers defer it without checking")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() returned %v with tracing off", err)
	}

	// The propagator is installed even with no exporter. A node that is not
	// recording must still pass a trace through it, or it becomes a hole in
	// somebody else's picture.
	if _, ok := otel.GetTextMapPropagator().(propagation.TextMapPropagator); !ok {
		t.Fatal("no propagator was installed")
	}
	carrier := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("an incoming traceparent was not parsed, so a trace passing through this node would break")
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	cases := map[string]string{
		// What a person types in a compose file, and what would otherwise be
		// rejected as a bad URL and silently turn tracing off.
		"jaeger:4317":          "http://jaeger:4317",
		"localhost:4317":       "http://localhost:4317",
		"http://jaeger:4317":   "http://jaeger:4317",
		"https://otel.example": "https://otel.example",
	}
	for in, want := range cases {
		if got := normalizeEndpoint(in); got != want {
			t.Errorf("normalizeEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSamplerDropsHeartbeatRoots is why the sampler exists. Two API nodes beat
// every ten seconds forever, and every beat with no parent would be its own
// trace in Jaeger's list.
//
// The names checked here are the ones the presence CLIENT gives its own
// wrapper span, and that detail is the whole bug this test now guards.
//
// The first version listed only the gRPC method names. It passed, and did
// nothing: the client's wrapper span starts the trace, so it is the only name a
// root sampler ever sees, and the gRPC span is a child that ParentBased keeps
// whatever this map says. A real run found it — Jaeger filled with heartbeats.
func TestSamplerDropsHeartbeatRoots(t *testing.T) {
	s := sampler()

	for _, name := range []string{"presence.Heartbeat", "presence.v1.PresenceService/Heartbeat"} {
		got := s.ShouldSample(sdktrace.SamplingParameters{
			ParentContext: context.Background(),
			Name:          name,
		})
		if got.Decision != sdktrace.Drop {
			t.Errorf("root span %q was sampled (%v); it would bury every real trace", name, got.Decision)
		}
	}

	// Online is the call a user waits for, and its shape under an open circuit
	// breaker is the most interesting thing Stage 6 left behind. It must stay.
	online := s.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: context.Background(),
		Name:          "presence.Online",
	})
	if online.Decision == sdktrace.Drop {
		t.Error("Online was dropped; that is the presence call worth tracing")
	}
}

// TestSamplerFollowsTheParent checks the half that keeps a trace whole. Once
// somebody has decided to record a trace, every process after them obeys —
// otherwise a fan-out span arrives whose parent was never recorded, which is
// worse than no span at all.
func TestSamplerFollowsTheParent(t *testing.T) {
	s := sampler()

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	// Even a name that would be dropped as a root is kept when it is part of a
	// trace somebody else already decided to record.
	got := s.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: ctx,
		Name:          "presence.Heartbeat",
	})
	if got.Decision != sdktrace.RecordAndSample {
		t.Errorf("a sampled parent gave %v, want RecordAndSample", got.Decision)
	}
}

func TestInjectCarrierWithNoSpan(t *testing.T) {
	if got := InjectCarrier(context.Background()); got != nil {
		t.Fatalf("InjectCarrier() with no span = %q, want nil so the column is NULL", got)
	}
}

func TestExtractCarrierIsSafeOnJunk(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	for _, stored := range [][]byte{nil, {}, []byte("not json"), []byte(`{"traceparent": 7}`)} {
		ctx := ExtractCarrier(context.Background(), stored)
		if trace.SpanContextFromContext(ctx).IsValid() {
			t.Errorf("ExtractCarrier(%q) invented a span context", stored)
		}
	}
}

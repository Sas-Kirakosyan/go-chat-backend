package tracing

import (
	"math"
	"os"
	"strconv"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// dropRootSpans are span names that must never START a trace.
//
// # Why anything is dropped at all
//
// Everything else here samples at 100 %, which is right for one machine being
// watched by hand and wrong for production (that is a Stage 8 problem). But
// there is one kind of span that would drown the ones worth reading.
//
// The presence heartbeat runs on a timer, every 10 seconds, on every API node,
// whether a user is looking at anything or not. It has no parent — nobody asked
// for it — so each beat starts a NEW trace. Two nodes make 12 root traces a
// minute, forever, and Jaeger's trace list becomes a wall of heartbeats with
// the one message you are trying to find somewhere underneath.
//
// The Online call is NOT in this list, and that is the distinction that
// matters: Online is a call a user is waiting for, and its shape — one second
// paid in full, or five milliseconds refused by an open circuit breaker — is
// the most interesting thing Stage 6 left behind.
// # The name here has to be the OUTERMOST span, not the interesting one
//
// The first version listed the gRPC method name that otelgrpc uses,
// "presence.v1.PresenceService/Heartbeat". It did nothing at all, and a real
// run found it: Jaeger filled up with heartbeat traces anyway.
//
// The reason is that a root sampler only ever sees the span that STARTS a
// trace. By the time the gRPC span exists, the client's own wrapper span has
// already begun the trace and already been sampled, and the gRPC span is a
// child that ParentBased keeps. Dropping a child cannot undo a root.
//
// So the name below is the client wrapper's, from internal/presence/client.go,
// and the two must stay in step.
var dropRootSpans = map[string]bool{
	"presence.Heartbeat": true,

	// The gRPC method name as well, for the case where a heartbeat somehow
	// starts a trace on the server side — a beat from a node that has tracing
	// off, arriving at a presenced that has it on.
	"presence.v1.PresenceService/Heartbeat": true,
}

// sampler decides which spans are recorded.
//
// ParentBased is the important half. It means the decision is made ONCE, by
// whoever starts the trace, and every process after that obeys what the
// traceparent header already says. Without it each service would decide for
// itself and a trace would arrive with holes in it — a fan-out span whose
// parent was never recorded is worse than no span at all.
//
// So the root sampler below only ever runs for a span with no parent: an
// incoming HTTP request, or a background timer.
func sampler() sdktrace.Sampler {
	return sdktrace.ParentBased(rootSampler{inner: baseRootSampler()})
}

// baseRootSampler is AlwaysSample unless OTEL_TRACES_SAMPLER_ARG says
// otherwise, which is the standard env var for exactly this.
//
// It exists so that turning the rate down later is a compose change and not a
// code change. Anything unreadable falls back to "record everything", because
// a typo in a ratio must not silently switch tracing off.
func baseRootSampler() sdktrace.Sampler {
	arg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
	if arg == "" {
		return sdktrace.AlwaysSample()
	}
	ratio, err := parseRatio(arg)
	if err != nil {
		return sdktrace.AlwaysSample()
	}
	return sdktrace.TraceIDRatioBased(ratio)
}

// rootSampler drops the names in dropRootSpans and defers to inner for the
// rest.
type rootSampler struct {
	inner sdktrace.Sampler
}

func (s rootSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if dropRootSpans[p.Name] {
		// Drop, not RecordOnly. RecordOnly would build the span, fill in its
		// attributes and then throw it away, which costs the allocation for no
		// reason. Nothing downstream wants this span.
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return s.inner.ShouldSample(p)
}

func (s rootSampler) Description() string {
	return "RootDrop{heartbeat}+" + s.inner.Description()
}

// parseRatio reads a number and clamps it into 0..1.
func parseRatio(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return math.Min(1, math.Max(0, f)), nil
}

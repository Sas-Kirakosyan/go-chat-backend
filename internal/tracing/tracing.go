// Package tracing installs the process-wide tracer.
//
// # What a trace is, and why this stage exists
//
// A log line says "something happened here". A trace says "this one thing
// happened, and here is every step it took, in every process, with how long
// each step waited". One message in this system crosses four processes: the
// HTTP handler that stores it, the relay that publishes it, NATS, and the
// consumer on another node that pushes it to a socket. Until now, following
// one message meant opening four logs and matching timestamps by eye.
//
// A trace is a tree of spans. A span is one step with a start, an end, and a
// parent. The parent link is what survives leaving the process: it is written
// into an HTTP header, into gRPC metadata, into a NATS header, or — the
// interesting one here — into a database column, so that a step which happens
// 100 ms later on a different machine still knows what caused it.
//
// # Unset means off
//
// With no OTEL_EXPORTER_OTLP_ENDPOINT this package installs nothing, and every
// otel.Tracer in the tree becomes a no-op that allocates nothing. That is the
// same rule as NATS_URL, PRESENCE_ADDR and REDIS_ADDR: unset is local
// development, not a broken configuration. `make run` and every test need no
// collector.
//
// A collector that is SET and unreachable is also not a failure. Telemetry is
// the one thing in a system that must never take the system down: spans are
// batched in memory and dropped when the queue fills. See Setup.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope every tracer in this repo uses. One
// name, so a span can be told apart from one a library created.
const ScopeName = "go-chat-backend"

// exportTimeout caps one attempt to hand a batch to the collector.
//
// It is short on purpose. A collector that has stopped answering must not hold
// a batch worker for a minute; the batch is dropped and the next one is tried.
const exportTimeout = 5 * time.Second

// Shutdown flushes whatever has not been exported yet. It is never nil, so a
// caller does not have to check before deferring it.
type Shutdown func(context.Context) error

// Tracer returns the tracer everything in this repo starts spans with.
//
// It reads the global provider at call time rather than holding one, so a
// package can keep a package-level tracer and still get the real provider once
// Setup has installed it.
func Tracer() trace.Tracer {
	return otel.Tracer(ScopeName)
}

// Setup installs the tracer provider and the propagator, and returns the
// function that flushes it.
//
// service is the name this process shows up as in Jaeger — "chat-api" or
// "presence". It is what turns one waterfall into a picture of the system
// rather than a list of function calls.
//
// It never returns an error and never stops the process. A tracer that cannot
// be built is a warning: the service's job is chat, not telemetry.
func Setup(ctx context.Context, service string) Shutdown {
	// The propagator is installed even with no exporter. It costs nothing, and
	// it means a traceparent arriving from outside is still parsed and still
	// passed on — this process simply records no spans of its own. A node with
	// tracing off must not break a trace that is passing through it.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		slog.Info("no OTEL_EXPORTER_OTLP_ENDPOINT, tracing is off")
		return func(context.Context) error { return nil }
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpointURL(normalizeEndpoint(endpoint)),
		otlptracegrpc.WithTimeout(exportTimeout),
	)
	if err != nil {
		// Not fatal, and not even retried here. otlptracegrpc.New does no I/O
		// unless WithDialOption(grpc.WithBlock()) is given, so this only fails
		// on a malformed endpoint — a typo in an env var, which should not cost
		// anyone their chat.
		slog.Warn("could not build the trace exporter; running without tracing",
			"endpoint", endpoint, "err", err)
		return func(context.Context) error { return nil }
	}

	provider := sdktrace.NewTracerProvider(
		// Batched, not synchronous. A synchronous exporter would put a network
		// call to Jaeger inside the send path, which is exactly the thing this
		// stage is supposed to be measuring rather than causing.
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithExportTimeout(exportTimeout),
		),
		sdktrace.WithSampler(sampler()),
		sdktrace.WithResource(newResource(service)),
	)

	otel.SetTracerProvider(provider)

	// Where the SDK's own complaints go. Without this they are written with the
	// standard logger in a format nothing else in this process uses, and a
	// collector that is refusing every batch says so once and is never seen.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Warn("tracing error", "err", err)
	}))

	slog.Info("tracing on", "service", service, "endpoint", endpoint)

	return func(ctx context.Context) error {
		if err := provider.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown tracer provider: %w", err)
		}
		return nil
	}
}

// newResource describes this process. Every span it exports carries these, and
// they are what let Jaeger say WHICH node a step ran on.
//
// service.instance.id is the same NODE_ID that is already on every log line.
// With two API nodes running the same image, "the fan-out span" is not a useful
// thing to see unless you can tell api1's from api2's — that difference is the
// whole point of the cross-node trace.
func newResource(service string) *resource.Resource {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(service),
	}
	if node := os.Getenv("NODE_ID"); node != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(node))
	} else if host, err := os.Hostname(); err == nil && host != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(host))
	}

	// Merge with the default resource rather than replacing it, so the SDK and
	// language attributes survive. Merge only fails on a schema URL conflict,
	// and then the hand-built one is still correct and still better than none.
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, attrs...))
	if err != nil {
		return resource.NewWithAttributes(semconv.SchemaURL, attrs...)
	}
	return res
}

// normalizeEndpoint lets the env var be written either way.
//
// WithEndpointURL wants a scheme. "jaeger:4317" is what a person types and what
// most compose files contain, and without this it is rejected as a bad URL —
// which would then be swallowed as "tracing is off" and cost an hour.
//
// http:// (not https://) means the connection is plaintext, which is right for
// a collector on the same private network and wrong across the internet. That
// choice stays in the env var: pass an https:// endpoint and it is honoured.
func normalizeEndpoint(endpoint string) string {
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	return "http://" + endpoint
}

package server

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"go-chat-backend/internal/tracing"
)

// notTraced are the routes that must not get a span.
//
// # /ws is the one that would actually break something
//
// A span ends when the work ends. A WebSocket handshake does not end — it
// becomes a connection that lives for hours. Tracing /ws would start a span
// that is never closed, never exported, and never freed, on every socket. With
// 5000 of them from the Stage 1 load test, that is 5000 spans held in memory
// for as long as the process runs. The delivery this stage wants to see is
// traced on the OTHER side anyway, in the fan-out consumer.
//
// The three probes are here for the ordinary reason: Prometheus scrapes every
// 5 seconds and Kubernetes probes every few seconds, and a trace list that is
// nine tenths health checks is a trace list nobody opens. It is the same
// judgement as quietWhenHealthy in logging.go, which is why the map is built
// from it rather than typed out twice.
var notTraced = func() map[string]bool {
	m := map[string]bool{"/ws": true}
	for path := range quietWhenHealthy {
		m[path] = true
	}
	return m
}()

// traceRequests starts a span for each request and continues one that arrived
// in a header.
//
// It is the entry point of every trace in this system. The traceparent it
// creates here is what ends up in a Postgres column, then in a NATS header,
// then on another node's fan-out span — one id for one message, across four
// processes.
//
// With tracing off (no OTEL_EXPORTER_OTLP_ENDPOINT) the global provider is a
// no-op, so this middleware still runs and costs almost nothing. That is worth
// more than a flag: there is one code path, not a traced one and an untraced
// one that drift apart.
func traceRequests() gin.HandlerFunc {
	return otelgin.Middleware(serviceName(),
		// Span names come from the route template — "/conversations/:id/messages"
		// — and not from the URL. Without this every room id would be its own
		// operation name and Jaeger's list would have one entry per room.
		otelgin.WithFilter(func(r *http.Request) bool {
			return !notTraced[r.URL.Path]
		}),
	)
}

// serviceName is how this process appears in Jaeger.
//
// OTEL_SERVICE_NAME is the standard env var and wins when it is set. The
// default is the API's name, because this package is the API; cmd/presenced
// passes its own.
func serviceName() string {
	if name := os.Getenv("OTEL_SERVICE_NAME"); name != "" {
		return name
	}
	return "chat-api"
}

// traceAttrs are the log fields that join a log line to a trace.
//
// This is the small, unglamorous half of the stage and it may be the half that
// gets used most. A trace shows the shape of a request; the logs say what the
// code was thinking. With trace_id on both, finding one from the other is a
// grep across four processes instead of matching timestamps by eye.
//
// It returns nothing when there is no span, so a line from an untraced route —
// or from a process with tracing off — is exactly what it was before.
func traceAttrs(c *gin.Context) []any {
	ctx := c.Request.Context()
	id := tracing.TraceID(ctx)
	if id == "" {
		return nil
	}
	return []any{"trace_id", id, "span_id", tracing.SpanID(ctx)}
}

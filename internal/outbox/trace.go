package outbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"go-chat-backend/internal/database"
	"go-chat-backend/internal/event"
	"go-chat-backend/internal/tracing"
)

// startPublishSpan joins one row back to the request that wrote it.
//
// # The span whose parent has already ended
//
// This is the step the whole tracing stage was built for, and it is the one
// that looks wrong at first.
//
// The HTTP request committed the message and the outbox row together and
// answered 201 — about 10 ms, and then it was gone. This code runs afterwards,
// on the relay's own timer, on whichever node holds the advisory lock, up to
// one poll interval later. Its parent span finished long ago.
//
// Attaching to a finished span is fine: a span context is an id, not a live
// object. And doing it is what makes Jaeger draw ONE trace with a visible gap
// between the commit and the publish, instead of two unrelated traces that a
// person has to guess belong together.
//
// That gap is the number Stage 5 estimated and never measured. It is the price
// of polling instead of listening, drawn to scale.
//
// A row with no stored carrier — every row from before Stage 7, every row
// written with tracing off — starts its own root span here instead. The relay
// behaves identically either way; only the picture is shorter.
func startPublishSpan(ctx context.Context, row database.Outbox, ev event.MessageCreated) (context.Context, trace.Span) {
	var stored []byte
	if row.TraceContext != nil {
		stored = []byte(*row.TraceContext)
	}
	ctx = tracing.ExtractCarrier(ctx, stored)

	ctx, span := tracing.Tracer().Start(ctx, "outbox.publish",
		// Producer: this is the step that hands the message to the broker. The
		// kind is what makes Jaeger and Grafana draw it as a messaging hop
		// rather than as an ordinary function call.
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.Int64("outbox.id", int64(row.ID)),
			attribute.Int("outbox.attempts", row.Attempts),
			// How long the row sat in Postgres before anybody looked at it.
			// Duplicated on the span as a number, not left implicit in the
			// waterfall, so it can be searched: "show me publishes that waited
			// more than a second" is a question about a stuck relay.
			attribute.Int64("outbox.queued_ms", time.Since(row.CreatedAt).Milliseconds()),
			attribute.Int64("message.id", int64(ev.MessageID)),
			attribute.Int64("conversation.id", int64(ev.ConversationID)),
			attribute.Int64("message.seq", int64(ev.Seq)),
		),
	)
	return ctx, span
}

// endSpanOK closes a publish that worked.
//
// The recipient count is on the span because it is the first number that
// explains a "nobody got my message" report: a publish to a room the sender is
// the only member of is a delivery that worked perfectly and reached one
// socket.
func endSpanOK(span trace.Span, recipients int) {
	span.SetAttributes(attribute.Int("message.recipients", recipients))
	span.End()
}

// endSpanErr closes a publish that did not work.
//
// Both calls matter and they do different things. RecordError attaches the
// message and stack as an event you can read; SetStatus is what colours the
// span red and makes it findable with an error filter. A span with only the
// first looks healthy in every list.
func endSpanErr(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	span.End()
}

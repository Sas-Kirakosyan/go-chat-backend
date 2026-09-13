-- +goose Up

-- Where a trace crosses the outbox.
--
-- Every other boundary in this system already has somewhere to put a trace
-- context. HTTP has headers, gRPC has metadata, NATS has headers. The outbox
-- has none: the row is written inside the send transaction, and then it simply
-- waits in Postgres until a relay — possibly on a different node — comes to
-- fetch it, up to one poll interval later. By then the request that wrote it
-- has already answered its 201 and gone.
--
-- So the link between "a user pressed send" and "the message reached the
-- broker" has to be stored, and this is the column that stores it. Without it
-- the trace breaks exactly at the step whose cost nobody has ever measured.
--
-- It holds the W3C carrier as the propagator writes it: {"traceparent": "..."}
-- and, when there is one, "tracestate". jsonb rather than text for the same
-- reason as payload beside it — Postgres checks that it really is JSON, and you
-- can read it at a psql prompt while debugging instead of guessing.
--
-- NULLABLE, and that is the normal case, not an edge case:
--
--   * every row written before this migration has no trace;
--   * every row written while OTEL_EXPORTER_OTLP_ENDPOINT is unset has none
--     either, which is `make run` and every test;
--   * a row whose trace was not sampled has none.
--
-- The relay must publish all three exactly as it always did. A missing trace
-- means one trace starts later than it could have. It must never mean a
-- message stops moving.
ALTER TABLE outbox ADD COLUMN trace_context jsonb;

-- No index. Nothing ever searches by it: the column is read only as part of a
-- row the relay has already found through idx_outbox_pending, and written only
-- once. An index here would cost every insert on the send path to speed up a
-- query nobody makes.

-- +goose Down

ALTER TABLE outbox DROP COLUMN trace_context;

package presence

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	// Aliased because this file already imports grpc/codes, and the two mean
	// different things: one is what the service answered, the other is whether
	// the span is marked as failed.
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"go-chat-backend/internal/logging"
	"go-chat-backend/internal/presencepb"
	"go-chat-backend/internal/tracing"
)

// The client half of the split, and the half where all the interesting
// decisions are.
//
// A method call inside one process cannot be slow for a reason that has nothing
// to do with you, cannot half-succeed, and cannot be answered by a version of
// the code you were not compiled against. An RPC can do all three. Everything
// in this file is one of those three problems.

const (
	// onlineTimeout caps ONE Online call, retries included.
	//
	// It is the same one second the in-process version used, and it is a
	// budget, not a per-attempt limit: a caller waiting for green dots on a
	// page must never wait longer than this, however many attempts fit inside
	// it. A deadline that resets on every retry is not a deadline.
	onlineTimeout = time.Second

	// heartbeatTimeout caps one Heartbeat call. It runs on a 10-second timer,
	// so a beat still trying when the next one is due is a beat worth
	// abandoning.
	heartbeatTimeout = Heartbeat / 2

	// healthTimeout caps the /health probe. It is a page a human is watching,
	// and a hung dependency must not hang the page.
	healthTimeout = time.Second

	// maxAttempts is the first call plus two retries.
	//
	// Retries fix exactly one thing: a call that failed for a reason that has
	// already stopped — a connection dropped mid-deploy, one instance restarted.
	// They fix nothing about a service that is down, which is what the breaker
	// is for. So the number stays small; three attempts against a healthy
	// service that hiccuped is plenty, and three attempts against a dead one is
	// three times the load it cannot handle.
	maxAttempts = 3

	// initialBackoff is the wait before the second attempt; it doubles after
	// that. Retrying instantly is barely a retry — whatever went wrong has had
	// no time to stop going wrong — and it turns one failure into three at the
	// same instant.
	initialBackoff = 50 * time.Millisecond

	// breakerThreshold and breakerOpenFor configure the circuit. Five
	// consecutive failed calls is enough to be sure, and five seconds is short
	// enough that a service which comes back is used again long before anybody
	// falls out of the presence window.
	breakerThreshold = 5
	breakerOpenFor   = 5 * time.Second
)

// ErrCircuitOpen is returned instead of making a call the breaker has cut off.
// It is a distinct error so a caller can tell "presence is known to be down"
// from "presence did not answer in time", which look the same in a log
// otherwise.
var ErrCircuitOpen = errors.New("presence: circuit open")

// Client is an API node's connection to the presence service.
//
// nil is a supported value and every method tolerates it, because a nil Client
// means single-node mode: no PRESENCE_ADDR, no presence service, and the node
// answers presence questions from its own hub. That is what `make run` and
// every test uses, and it is why neither needs a Redis or a second process.
type Client struct {
	conn   *grpc.ClientConn
	rpc    presencepb.PresenceServiceClient
	health grpc_health_v1.HealthClient
	nodeID string

	breaker *breaker

	// Counters, read by Prometheus at scrape time.
	calls          atomic.Int64
	failures       atomic.Int64
	retries        atomic.Int64
	shortCircuited atomic.Int64
}

// FromEnv builds the client from PRESENCE_ADDR.
//
// It returns nil when PRESENCE_ADDR is not set. That is single-node mode, not
// an error.
//
// # Why it does not wait for a connection
//
// grpc.NewClient does no I/O: it resolves and connects lazily, in the
// background, and reconnects on its own with backoff. So a presence service
// that is down — or simply started a few seconds later than the API nodes,
// which is every `docker compose up` — costs nothing here, and the first call
// after it comes up succeeds with no restart and no repair step.
//
// The blocking alternative is the mistake this project already made once with
// Redis: pinging at startup and exiting on failure put both API nodes in a
// crash loop during a Redis outage, turning one degraded feature into a total
// outage. A dependency may degrade a node. It may not remove it.
func FromEnv() *Client {
	addr := os.Getenv("PRESENCE_ADDR")
	if addr == "" {
		return nil
	}

	// Insecure because this is a private network inside compose, and the honest
	// note is that it would not be acceptable across a real one: presence says
	// who is online, and anyone on the path can read it or write to it. mTLS is
	// the fix, and it belongs with the deployment, not here.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		// Cap what one answer may be. The default is 4MB, which for a message
		// containing a list of user ids is not a limit at all.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20)),

		// Stage 7: the trace crosses to the other process here, and this is the
		// cheapest boundary of the four. gRPC already sends metadata with every
		// call, so the traceparent rides along with no code of ours — no column
		// like the outbox needed, no header adapter like NATS needed.
		//
		// A stats handler, not an interceptor. Interceptors see the call; the
		// stats handler also sees the stream underneath it, which is what makes
		// the span end at the right moment on a call that fails at the
		// transport rather than in the handler — a dead service, which is the
		// case Stage 6 kept getting wrong.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		// Only a bad target string can get here — everything else is deferred
		// to the first call. A malformed address is a configuration mistake
		// that will never fix itself, so the node runs without presence rather
		// than retrying a name that cannot work.
		slog.Error("bad PRESENCE_ADDR, running without presence", "addr", addr, "err", err)
		return nil
	}

	return &Client{
		conn:    conn,
		rpc:     presencepb.NewPresenceServiceClient(conn),
		health:  grpc_health_v1.NewHealthClient(conn),
		nodeID:  logging.NodeID(),
		breaker: newBreaker(breakerThreshold, breakerOpenFor, nil),
	}
}

// Online reports which of the given users are online anywhere in the cluster.
//
// The map is keyed by user id and holds an entry for every id asked about, so
// the caller does not have to tell "offline" from "not in the answer".
func (c *Client) Online(ctx context.Context, userIDs []uint) (map[uint]bool, error) {
	online := make(map[uint]bool, len(userIDs))
	if c == nil || len(userIDs) == 0 {
		return online, nil
	}

	ids := make([]uint64, len(userIDs))
	for i, id := range userIDs {
		ids[i] = uint64(id)
	}

	var got []uint64
	err := c.call(ctx, "presence.Online", onlineTimeout, func(ctx context.Context) error {
		resp, err := c.rpc.Online(ctx, &presencepb.OnlineRequest{UserIds: ids})
		if err != nil {
			return err
		}
		got = resp.GetOnlineUserIds()
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, id := range userIDs {
		online[id] = false
	}
	for _, id := range got {
		online[uint(id)] = true
	}
	return online, nil
}

// Heartbeat tells the service which users have a socket on this node.
func (c *Client) Heartbeat(ctx context.Context, userIDs []uint) error {
	if c == nil {
		return nil
	}

	ids := make([]uint64, len(userIDs))
	for i, id := range userIDs {
		ids[i] = uint64(id)
	}

	return c.call(ctx, "presence.Heartbeat", heartbeatTimeout, func(ctx context.Context) error {
		_, err := c.rpc.Heartbeat(ctx, &presencepb.HeartbeatRequest{
			NodeId:  c.nodeID,
			UserIds: ids,
		})
		return err
	})
}

// Ping asks the standard gRPC health service whether presence is serving.
//
// It is used by /health on the API node, and it is a separate call on purpose:
// asking Online about nobody would also prove the connection works, but it
// would not say whether the service considers itself healthy — a presence
// service whose Redis is gone answers a gRPC connection perfectly well and can
// do none of its job.
//
// It skips the breaker. /health is the page a person opens to find out whether
// presence is down; answering it out of the breaker's memory instead of asking
// would make it report an outage that may already be over.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	resp, err := c.health.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return errors.New("presence service is not serving: " + resp.GetStatus().String())
	}
	return nil
}

// Close shuts the connection. Safe on a nil Client, so shutdown paths do not
// have to check for single-node mode.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	return c.conn.Close()
}

// call runs one logical presence call: breaker, deadline, retries, backoff.
//
// The order is the point.
//
//  1. The breaker first. When it is open there is no connection attempt, no
//     goroutine parked on a deadline and no packet sent — the failure costs a
//     mutex.
//  2. Then ONE deadline for the whole thing, shared by every attempt. The
//     caller said how long it is prepared to wait; retries spend that budget,
//     they do not extend it.
//  3. Then the attempts, with backoff between them.
func (c *Client) call(parent context.Context, op string, timeout time.Duration, fn func(context.Context) error) error {
	// The span covers the WHOLE logical call — breaker, deadline and every
	// attempt — not one gRPC round trip. otelgrpc already traces each attempt
	// underneath; this is the span that says what this client did around them.
	//
	// It is also the answer to the worst bug of Stage 6. The code there treated
	// "did this fail" and "did the service reply" as the same question, so a
	// dead service (which returns DeadlineExceeded) was booked as a success,
	// the breaker never opened, and every request kept paying the full second.
	// Every unit test passed, because they all ran against a server that
	// answered. On a trace the two are not confusable: a call that pays 1s and
	// then fails and a call refused in 5 microseconds with a short_circuited
	// event are different shapes, visible in one screenshot.
	//
	// The span is named after the OPERATION, and that is not cosmetics — it is
	// what makes the sampler work. The first version called every span
	// "presence.call", and the heartbeat's span is a root, because a timer has
	// nobody above it. So two nodes started a new trace every ten seconds
	// forever, and the sampler could not drop them: it was matching the gRPC
	// method name, which belongs to a span created further down, INSIDE the
	// trace this one had already begun. Dropping a child does not undo a root.
	// The name of the outermost span is the only one a root sampler ever sees.
	parent, span := tracing.Tracer().Start(parent, op)
	defer span.End()

	if !c.breaker.allow() {
		c.shortCircuited.Add(1)
		span.AddEvent("short_circuited")
		span.SetStatus(otelcodes.Error, ErrCircuitOpen.Error())
		return ErrCircuitOpen
	}

	c.calls.Add(1)

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	backoff := initialBackoff
	var err error

	for attempt := 1; ; attempt++ {
		err = fn(ctx)
		if err == nil {
			c.breaker.success()
			return nil
		}

		if answered(err) {
			// The service answered and said no. That is an answer, so the
			// dependency is up and the breaker must not count it: a bug in this
			// node's own request would otherwise cut off a service that is
			// working perfectly.
			c.breaker.success()
			return markFailed(span, err)
		}

		// Nobody answered. Retry if there is any point, and give up otherwise —
		// a spent deadline cannot be retried inside itself.
		if !retryable(err) {
			break
		}

		if attempt >= maxAttempts {
			break
		}

		// Full jitter. Without it, every node in the cluster retries at the same
		// two moments after a shared failure, and a service coming back is met
		// by the whole fleet at once. The randomness is the fix, and it is worth
		// more than the exact backoff curve.
		wait := time.Duration(rand.Int64N(int64(backoff)))
		backoff *= 2

		// Sleeping past the deadline would burn the whole budget waiting rather
		// than trying. If there is not enough time left for the wait plus a
		// real attempt, stop now and return the error we already have.
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= wait {
			break
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.record(parent, err)
			return markFailed(span, err)
		case <-timer.C:
		}

		c.retries.Add(1)
		span.AddEvent("retry", trace.WithAttributes(
			attribute.Int("attempt", attempt+1),
			attribute.Int64("waited_ms", wait.Milliseconds()),
		))
	}

	c.record(parent, err)
	return markFailed(span, err)
}

// markFailed puts a failed call on its span and hands the error back unchanged.
//
// Every failing exit from call goes through it, which is the point: the first
// version set the attributes only at the bottom of the function, and the branch
// that matters most — "the service answered and said no" — returns early and
// never reached them. A test caught it, which is a small version of the same
// lesson Stage 6 taught: the interesting case is usually the one that returns
// early.
func markFailed(span trace.Span, err error) error {
	if err == nil {
		return nil
	}
	span.RecordError(err)
	span.SetStatus(otelcodes.Error, err.Error())
	// Whether the breaker learned anything from this call. It is the exact
	// distinction the Stage 6 bug got wrong — "did it fail" against "did the
	// service reply" — so it is worth being able to read it off a single failed
	// call rather than inferring it from a counter five minutes later.
	span.SetAttributes(attribute.Bool("presence.answered", answered(err)))
	return err
}

// record books a call that nobody answered.
//
// The one exception is a caller who gave up first. A cancelled request says
// nothing at all about the presence service — the user closed the tab — and
// counting it would let a burst of impatient clients open the breaker on a
// dependency that is perfectly healthy.
func (c *Client) record(parent context.Context, err error) {
	if parent.Err() != nil {
		return
	}
	c.breaker.failure()
	c.failures.Add(1)
}

// answered says whether the service itself produced this error.
//
// This is the distinction the first version of this file got wrong, and it took
// an outage to see it. The code said "if it is not retryable, the service
// answered", which reads sensibly and is false for exactly one code:
// DeadlineExceeded. Nobody answers a deadline. So a dead presence service
// produced a DeadlineExceeded on every call, each one was booked as a SUCCESS,
// the breaker never opened, and every request went on paying the full
// one-second timeout for as long as the outage lasted. The breaker was there,
// the tests passed, and it protected nothing.
//
// A gRPC status code from the server means the server was reached and had an
// opinion. Unavailable, DeadlineExceeded and Canceled are the three that mean
// the opposite — no answer — and they are the only ones the breaker counts.
//
// Unavailable is ambiguous on purpose: gRPC uses it for a connection that could
// not be made, and this service also returns it when its own store is down.
// Both mean "presence is not working", which is what the breaker cares about,
// so the ambiguity costs nothing here.
func answered(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return false
	default:
		return true
	}
}

// retryable says whether trying the same call again could plausibly work.
//
// Only Unavailable: a connection dropped mid-deploy, one instance restarting, a
// store that is coming back. All of those can stop being true a moment later.
//
// DeadlineExceeded is NOT retried, even though it is a failure. The deadline is
// shared by every attempt, so it being spent means there is no time left to try
// again — retrying would only return the same error more slowly.
//
// Codes the service answered with are not retried either. Unimplemented is the
// interesting one: it is what an old service returns for a new method, which is
// a version skew during a rolling deploy, and hammering it will not upgrade it.
func retryable(err error) bool {
	return status.Code(err) == codes.Unavailable
}

// State, Calls, Failures, Retries and ShortCircuited are read by
// internal/metrics at scrape time.
func (c *Client) BreakerState() string {
	if c == nil {
		return "closed"
	}
	return c.breaker.State()
}

func (c *Client) Calls() int64          { return c.calls.Load() }
func (c *Client) Failures() int64       { return c.failures.Load() }
func (c *Client) Retries() int64        { return c.retries.Load() }
func (c *Client) ShortCircuited() int64 { return c.shortCircuited.Load() }

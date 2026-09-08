package presence

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"go-chat-backend/internal/logging"
	"go-chat-backend/internal/presencepb"
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
	err := c.call(ctx, onlineTimeout, func(ctx context.Context) error {
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

	return c.call(ctx, heartbeatTimeout, func(ctx context.Context) error {
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
func (c *Client) call(parent context.Context, timeout time.Duration, fn func(context.Context) error) error {
	if !c.breaker.allow() {
		c.shortCircuited.Add(1)
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
			return err
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
			return err
		case <-timer.C:
		}

		c.retries.Add(1)
	}

	c.record(parent, err)
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

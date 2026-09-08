package presence

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"go-chat-backend/internal/presencepb"
)

// These run a real gRPC server on a real port, in the test process.
//
// A mock of the generated client interface would be faster and would prove much
// less: half of what this file checks — a deadline that travels, a connection
// that is refused, an error code turning into a retry — only exists once there
// is a wire. The server is in-process, so the whole file still runs in about a
// second.

// fakeService is a presence service whose answer the test chooses, per attempt.
type fakeService struct {
	presencepb.UnimplementedPresenceServiceServer

	mu       sync.Mutex
	attempts int
	nodeIDs  []string

	// answer is asked what to do with attempt number n, starting at 1.
	answer func(attempt int) ([]uint64, error)
}

func (f *fakeService) Online(ctx context.Context, req *presencepb.OnlineRequest) (*presencepb.OnlineResponse, error) {
	f.mu.Lock()
	f.attempts++
	n := f.attempts
	f.mu.Unlock()

	online, err := f.answer(n)
	if err != nil {
		return nil, err
	}
	return &presencepb.OnlineResponse{OnlineUserIds: online}, nil
}

func (f *fakeService) Heartbeat(ctx context.Context, req *presencepb.HeartbeatRequest) (*presencepb.HeartbeatResponse, error) {
	f.mu.Lock()
	f.attempts++
	f.nodeIDs = append(f.nodeIDs, req.GetNodeId())
	f.mu.Unlock()

	return &presencepb.HeartbeatResponse{}, nil
}

func (f *fakeService) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// start puts the fake behind a real gRPC server and returns a Client pointing
// at it. It goes through FromEnv on purpose, so the wiring under test is the
// wiring that runs in production.
func start(t *testing.T, svc *fakeService) *Client {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	presencepb.RegisterPresenceServiceServer(server, svc)

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthSrv)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	t.Setenv("PRESENCE_ADDR", listener.Addr().String())
	t.Setenv("NODE_ID", "test-node")

	client := FromEnv()
	if client == nil {
		t.Fatal("FromEnv returned nil with PRESENCE_ADDR set")
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func alwaysOnline(ids ...uint64) func(int) ([]uint64, error) {
	return func(int) ([]uint64, error) { return ids, nil }
}

// The map has to hold an entry for every user asked about. A caller that got
// back only the online ones would have to tell "offline" from "not mentioned"
// itself, and one of those two is a bug on this side of the wire.
func TestOnlineAnswersForEveryUserAsked(t *testing.T) {
	client := start(t, &fakeService{answer: alwaysOnline(2)})

	got, err := client.Online(context.Background(), []uint{1, 2, 3})
	if err != nil {
		t.Fatalf("Online: %v", err)
	}

	want := map[uint]bool{1: false, 2: true, 3: false}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for id, online := range want {
		if got[id] != online {
			t.Fatalf("user %d: got %v, want %v (whole answer %v)", id, got[id], online, got)
		}
	}
}

// Asking about nobody must not make an RPC. An empty room is a normal thing and
// it should not cost a network round trip.
func TestOnlineWithNoUsersMakesNoCall(t *testing.T) {
	svc := &fakeService{answer: alwaysOnline()}
	client := start(t, svc)

	got, err := client.Online(context.Background(), nil)
	if err != nil {
		t.Fatalf("Online: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want an empty map", got)
	}
	if n := svc.count(); n != 0 {
		t.Fatalf("the service was called %d times for an empty question", n)
	}
}

// Unavailable is the code that means "this may work in a moment": a connection
// that was dropped mid-deploy, one instance restarting.
func TestUnavailableIsRetried(t *testing.T) {
	svc := &fakeService{answer: func(attempt int) ([]uint64, error) {
		if attempt == 1 {
			return nil, status.Error(codes.Unavailable, "restarting")
		}
		return []uint64{7}, nil
	}}
	client := start(t, svc)

	got, err := client.Online(context.Background(), []uint{7})
	if err != nil {
		t.Fatalf("Online: %v", err)
	}
	if !got[7] {
		t.Fatalf("got %v, want user 7 online", got)
	}
	if n := svc.count(); n != 2 {
		t.Fatalf("attempts: got %d, want 2", n)
	}
	if n := client.Retries(); n != 1 {
		t.Fatalf("retries counter: got %d, want 1", n)
	}
	// One logical call, whatever it took. The counter is what a rate() is
	// computed over, so it must count requests and not attempts.
	if n := client.Calls(); n != 1 {
		t.Fatalf("calls counter: got %d, want 1", n)
	}
}

// InvalidArgument is this node's own mistake. Repeating it changes nothing, and
// — the part that matters — it must not count against the circuit breaker: the
// service answered, so it is up, and a bug in one request must never cut a
// healthy dependency off.
func TestInvalidArgumentIsNotRetriedAndDoesNotTripTheBreaker(t *testing.T) {
	svc := &fakeService{answer: func(int) ([]uint64, error) {
		return nil, status.Error(codes.InvalidArgument, "no")
	}}
	client := start(t, svc)

	for i := 0; i < breakerThreshold+2; i++ {
		if _, err := client.Online(context.Background(), []uint{1}); err == nil {
			t.Fatal("Online: got no error, want InvalidArgument")
		} else if errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("the breaker opened on request %d, on an error the service answered", i+1)
		}
	}

	if n := svc.count(); n != breakerThreshold+2 {
		t.Fatalf("attempts: got %d, want %d — one per call, no retries", n, breakerThreshold+2)
	}
	if n := client.Failures(); n != 0 {
		t.Fatalf("failures counter: got %d, want 0 — the service answered every time", n)
	}
}

// The whole point of the breaker: after enough failures the calls stop leaving
// the process. The proof is the service's own counter, which stops rising.
func TestBreakerStopsCallingAFailingService(t *testing.T) {
	svc := &fakeService{answer: func(int) ([]uint64, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}}
	client := start(t, svc)

	for i := 0; i < breakerThreshold; i++ {
		if _, err := client.Online(context.Background(), []uint{1}); err == nil {
			t.Fatalf("call %d: got no error, want Unavailable", i+1)
		}
	}

	reached := svc.count()
	if got := client.BreakerState(); got != "open" {
		t.Fatalf("breaker: got %q after %d failed calls, want open", got, breakerThreshold)
	}

	began := time.Now()
	_, err := client.Online(context.Background(), []uint{1})
	elapsed := time.Since(began)

	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("got %v, want ErrCircuitOpen", err)
	}
	if svc.count() != reached {
		t.Fatalf("the service was reached %d more times through an open breaker", svc.count()-reached)
	}
	// A refused call is a mutex, not a network. 50ms is a very loose bound; the
	// real number is microseconds, and the point is that it is nowhere near the
	// one-second timeout it would otherwise have cost.
	if elapsed > 50*time.Millisecond {
		t.Fatalf("a short-circuited call took %s, it should be immediate", elapsed)
	}
	if n := client.ShortCircuited(); n != 1 {
		t.Fatalf("short-circuited counter: got %d, want 1", n)
	}
}

// The test that was missing, and the bug it would have caught.
//
// The first version of the client booked "not retryable" as "the service
// answered", which is true of every code except one: DeadlineExceeded. A dead
// presence service produced exactly that on every call, each was recorded as a
// success, and the breaker never opened. The whole outage was paid for at one
// second per request, and every unit test passed — because they all used a
// server that answered.
//
// So the check is not "does the breaker open", it is "does it open on the kind
// of failure a dead service actually produces": one that never answers at all.
func TestABreakerOpensOnTimeoutsAsWellAsRefusals(t *testing.T) {
	svc := &fakeService{answer: func(int) ([]uint64, error) {
		time.Sleep(2 * onlineTimeout)
		return nil, nil
	}}
	client := start(t, svc)

	for i := 0; i < breakerThreshold; i++ {
		if _, err := client.Online(context.Background(), []uint{1}); err == nil {
			t.Fatalf("call %d: got no error, want a deadline", i+1)
		}
	}

	if got := client.BreakerState(); got != "open" {
		t.Fatalf("breaker: got %q after %d timed-out calls, want open", got, breakerThreshold)
	}

	began := time.Now()
	if _, err := client.Online(context.Background(), []uint{1}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("got %v, want ErrCircuitOpen", err)
	}
	if elapsed := time.Since(began); elapsed > 50*time.Millisecond {
		t.Fatalf("a call after %d timeouts still took %s", breakerThreshold, elapsed)
	}
}

// A caller who gives up must not be counted against the dependency. Otherwise a
// page full of impatient clients, or one client on a bad phone connection, can
// open the breaker on a presence service that is answering everyone else fine.
func TestCallerCancellationDoesNotTripTheBreaker(t *testing.T) {
	svc := &fakeService{answer: func(int) ([]uint64, error) {
		time.Sleep(2 * onlineTimeout)
		return nil, nil
	}}
	client := start(t, svc)

	// Warm the connection up first, and this is not politeness. The very first
	// RPC on a fresh client also has to dial, and a dial that has not finished
	// gives back Unavailable rather than a cancellation — so without this the
	// test books a real transport failure now and then, and fails perhaps one
	// run in twenty. Ping goes to the health service, so it costs nothing here
	// and leaves the channel ready.
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	for i := 0; i < breakerThreshold+2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, _ = client.Online(ctx, []uint{1})
		cancel()
	}

	if got := client.BreakerState(); got != "closed" {
		t.Fatalf("breaker: got %q, want closed — every call was the caller giving up", got)
	}
	if n := client.Failures(); n != 0 {
		t.Fatalf("failures counter: got %d, want 0", n)
	}
}

// The deadline is a budget for the whole call, not for each attempt. A deadline
// that resets on every retry is not a deadline, and it is how a page that
// promises to answer in one second answers in three.
func TestRetriesShareOneDeadline(t *testing.T) {
	svc := &fakeService{answer: func(int) ([]uint64, error) {
		// Longer than onlineTimeout, so the first attempt alone spends the
		// whole budget.
		time.Sleep(2 * onlineTimeout)
		return nil, nil
	}}
	client := start(t, svc)

	started := time.Now()
	_, err := client.Online(context.Background(), []uint{1})
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("Online: got no error, want a deadline")
	}
	if code := status.Code(err); code != codes.DeadlineExceeded {
		t.Fatalf("code: got %s, want DeadlineExceeded", code)
	}
	// Generous slack for a loaded CI machine, and still far below the
	// 3 x onlineTimeout a per-attempt deadline would have cost.
	if elapsed > onlineTimeout+500*time.Millisecond {
		t.Fatalf("the call took %s, the budget is %s", elapsed, onlineTimeout)
	}
}

// A caller who gives up must not be waited for. The context passed in wins over
// the client's own timeout when it is shorter.
func TestCallerCancellationWins(t *testing.T) {
	svc := &fakeService{answer: func(int) ([]uint64, error) {
		time.Sleep(2 * onlineTimeout)
		return nil, nil
	}}
	client := start(t, svc)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	started := time.Now()
	if _, err := client.Online(ctx, []uint{1}); err == nil {
		t.Fatal("Online: got no error, want a cancellation")
	}
	if elapsed := time.Since(started); elapsed > onlineTimeout {
		t.Fatalf("the call took %s, the caller gave up after 100ms", elapsed)
	}
}

func TestHeartbeatCarriesTheNodeID(t *testing.T) {
	svc := &fakeService{answer: alwaysOnline()}
	client := start(t, svc)

	if err := client.Heartbeat(context.Background(), []uint{1, 2}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.nodeIDs) != 1 || svc.nodeIDs[0] != "test-node" {
		t.Fatalf("node ids: got %v, want [test-node]", svc.nodeIDs)
	}
}

// Ping asks the standard health service, not Online. A presence service whose
// Redis is gone answers a gRPC connection perfectly well and can do none of its
// job, so "the connection works" is the wrong question.
func TestPingUsesTheHealthService(t *testing.T) {
	svc := &fakeService{answer: alwaysOnline()}
	client := start(t, svc)

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if n := svc.count(); n != 0 {
		t.Fatalf("Ping made %d presence calls, it should only ask health", n)
	}
}

// A dead address must fail fast and must not hang the caller. This is the state
// every node is in for the first second of a `docker compose up`, and during
// every deploy of the presence service.
func TestUnreachableServiceFailsWithoutHanging(t *testing.T) {
	// Port 1 on the loopback: nothing listens there, and the connection is
	// refused rather than dropped, so this does not depend on a firewall.
	t.Setenv("PRESENCE_ADDR", "127.0.0.1:1")
	client := FromEnv()
	if client == nil {
		t.Fatal("FromEnv returned nil with PRESENCE_ADDR set")
	}
	defer client.Close()

	started := time.Now()
	_, err := client.Online(context.Background(), []uint{1})

	if err == nil {
		t.Fatal("Online: got no error against a dead address")
	}
	if elapsed := time.Since(started); elapsed > onlineTimeout+500*time.Millisecond {
		t.Fatalf("the call took %s against a refused connection", elapsed)
	}
}

// nil is single-node mode, and every method has to survive it — otherwise the
// tests and `make run`, which have no presence service, would need a different
// code path from production, and the path that runs least would be the one
// production uses.
func TestNilClientIsSingleNodeMode(t *testing.T) {
	var client *Client

	got, err := client.Online(context.Background(), []uint{1, 2})
	if err != nil || len(got) != 0 {
		t.Fatalf("Online on a nil client: got %v, %v", got, err)
	}
	if err := client.Heartbeat(context.Background(), []uint{1}); err != nil {
		t.Fatalf("Heartbeat on a nil client: %v", err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping on a nil client: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close on a nil client: %v", err)
	}
	if got := client.BreakerState(); got != "closed" {
		t.Fatalf("BreakerState on a nil client: got %q", got)
	}
}

func TestFromEnvWithoutAnAddressIsNil(t *testing.T) {
	t.Setenv("PRESENCE_ADDR", "")
	if client := FromEnv(); client != nil {
		t.Fatal("FromEnv returned a client with no PRESENCE_ADDR")
	}
}

package presence

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go-chat-backend/internal/presencepb"
)

// Validation only, so these need no Redis: every case here is refused before
// the store is touched.
//
// They exist because the input stopped being trustworthy the moment presence
// became a port. A function inside the API node could only ever be handed a
// list that node had built; an RPC can be handed anything by anyone who can
// reach it.

func TestHeartbeatRequiresANodeID(t *testing.T) {
	svc := NewService(nil)

	_, err := svc.Heartbeat(context.Background(), &presencepb.HeartbeatRequest{
		UserIds: []uint64{1, 2},
	})

	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code: got %s, want InvalidArgument", got)
	}
	// InvalidArgument and not Unavailable matters: the client retries one and
	// not the other, so the wrong code here is a request repeated three times
	// for nothing, on every single beat.
	if !strings.Contains(err.Error(), "node_id") {
		t.Fatalf("the message does not say what was wrong: %v", err)
	}
}

func TestCallsAreCapped(t *testing.T) {
	svc := NewService(nil)
	tooMany := make([]uint64, maxUsersPerCall+1)

	if _, err := svc.Online(context.Background(), &presencepb.OnlineRequest{UserIds: tooMany}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Online: got %s, want InvalidArgument", status.Code(err))
	}

	_, err := svc.Heartbeat(context.Background(), &presencepb.HeartbeatRequest{
		NodeId:  "api1",
		UserIds: tooMany,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Heartbeat: got %s, want InvalidArgument", status.Code(err))
	}
}

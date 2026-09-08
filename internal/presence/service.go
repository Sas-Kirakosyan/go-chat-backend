package presence

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go-chat-backend/internal/presencepb"
)

// maxUsersPerCall caps one request.
//
// A limit like this did not exist while presence was a function call, and it
// has to exist now. A method inside the process could only ever be handed a
// list the same process had built; an RPC can be handed anything by anyone who
// can reach the port. Without a cap, one request asking about ten million ids
// allocates ten million entries in this process and makes Redis read the whole
// set for each one.
//
// 10000 is far above a real room and far below a problem.
const maxUsersPerCall = 10000

// Service is the server side of presence.v1.PresenceService.
//
// It is thin on purpose: validate, call the store, turn an error into a status
// code. The rules about what "online" means live in the store, so that moving
// the service somewhere else — or putting a second transport in front of it —
// does not fork them.
type Service struct {
	// Embedding this is what makes adding an RPC to the .proto a compile-time
	// warning instead of a runtime one: the new method exists here at once,
	// returning Unimplemented, and the build keeps working while it is written.
	presencepb.UnimplementedPresenceServiceServer

	store *Store
}

func NewService(store *Store) *Service { return &Service{store: store} }

// Heartbeat records a node's users.
//
// The context is the caller's, deadline and all. gRPC puts the client's
// remaining time on the wire, so this handler already knows how long the caller
// is prepared to wait, and it hands that same deadline to Redis. That is the
// part that does not exist in a REST call by default, and it is why a slow
// Redis cannot pile up work here for callers who gave up long ago.
func (s *Service) Heartbeat(ctx context.Context, req *presencepb.HeartbeatRequest) (*presencepb.HeartbeatResponse, error) {
	if req.GetNodeId() == "" {
		// InvalidArgument, not Internal: the caller sent something wrong, and
		// retrying it unchanged will fail the same way. The client's retry
		// logic reads exactly this distinction.
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if len(req.GetUserIds()) > maxUsersPerCall {
		return nil, status.Errorf(codes.InvalidArgument,
			"too many user_ids: %d, the limit is %d", len(req.GetUserIds()), maxUsersPerCall)
	}

	if err := s.store.Heartbeat(ctx, req.GetNodeId(), req.GetUserIds()); err != nil {
		// Unavailable and not Internal, and the difference is the whole
		// contract: Unavailable means "try again, this is not about your
		// request". The client retries it, and enough of them in a row open its
		// circuit breaker.
		return nil, status.Errorf(codes.Unavailable, "presence store: %v", err)
	}
	return &presencepb.HeartbeatResponse{}, nil
}

// Online reports which of the given users are online.
func (s *Service) Online(ctx context.Context, req *presencepb.OnlineRequest) (*presencepb.OnlineResponse, error) {
	if len(req.GetUserIds()) > maxUsersPerCall {
		return nil, status.Errorf(codes.InvalidArgument,
			"too many user_ids: %d, the limit is %d", len(req.GetUserIds()), maxUsersPerCall)
	}

	online, err := s.store.Online(ctx, req.GetUserIds())
	if err != nil {
		slog.Warn("presence lookup failed", "users", len(req.GetUserIds()), "err", err)
		return nil, status.Errorf(codes.Unavailable, "presence store: %v", err)
	}
	return &presencepb.OnlineResponse{OnlineUserIds: online}, nil
}

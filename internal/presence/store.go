package presence

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Who is online?
//
// The honest answer is "whoever had a socket open a moment ago", and the hard
// part is the word "had". A node that crashes cannot tell anyone its users went
// away, so anything a node writes down must expire by itself. Otherwise a
// killed node leaves ghosts online forever, and the first thing a user notices
// about a distributed system is a friend who has been "online" for three days.
//
// # Why a sorted set and not a key per user
//
// The obvious design is one Redis key per user with a TTL. It does expire on
// its own, but asking "which of these 50 room members are online" then means 50
// round trips, or a SCAN over the whole keyspace.
//
// A sorted set holds every online user under ONE key, scored by the unix time
// they were last seen. Online is then "score newer than the cutoff", which is
// one command for the whole room. Expiry is not a TTL but arithmetic: an entry
// nobody refreshes simply falls out of the window, whether its node said
// goodbye or was killed with -9.
//
// The member is "userID:nodeID", not just the user id. One user with a phone
// and a laptop on two different nodes is two entries, and either one keeps them
// online. If they were one entry, the node that lost them would erase the node
// that still has them.
//
// # What changed in Stage 6
//
// Nothing above. What changed is who runs it: this used to be inside every API
// node, and now it is inside one service that the API nodes call. The Redis
// connection moved with it, which is the real boundary — an API node has no
// Redis client at all any more, and cannot reach the presence key even by
// accident.
const presenceKey = "chat:presence"

const (
	// TTL is how long an entry counts as online without a refresh.
	TTL = 30 * time.Second

	// Heartbeat is how often a node refreshes its own users.
	//
	// Three refreshes fit inside one TTL. That margin is on purpose: a single
	// slow or lost heartbeat must not make a room full of people blink offline,
	// and with a 10s beat it takes three failures in a row before anyone does.
	//
	// It matters more now than it did in Stage 3. A missed beat used to be one
	// failed Redis command; it is now a failed RPC to another process, which is
	// a thing that happens during every deploy of that process.
	Heartbeat = 10 * time.Second
)

// Store is the presence data itself: a Redis connection and two operations on
// it. Only cmd/presenced builds one.
type Store struct {
	rdb *redis.Client

	// failures is read by Prometheus at scrape time. The value lives here and
	// the metric reads it, which is the pattern the hub and the broker use.
	failures atomic.Int64
}

// OpenStore builds the store from REDIS_ADDR.
//
// It returns nil when REDIS_ADDR is not set, and that is a real mode: an
// in-memory store would be a different program with the same name, and a
// presence service with nowhere to keep presence should say so at startup
// rather than answer "nobody is online" forever.
//
// # Why a Redis that does not answer is not a startup failure
//
// The first version of this, back when it lived in the API node, pinged here
// and called os.Exit on failure. Then Redis was stopped on purpose, the nodes
// were rebuilt, and they went into a restart loop: every few seconds, exit 1,
// restart, fail the ping, exit 1. A Redis outage had turned into a total
// outage.
//
// The reasoning survives the split, and it now has a second half. This process
// exists only to serve presence, so a Redis outage really does break its whole
// job — and it still must not crash-loop, because the API nodes treat an
// answer of "unavailable" as a degraded feature while they treat a connection
// that never establishes the same way. Staying up and failing calls honestly is
// what lets the caller's circuit breaker do its work.
func OpenStore(ctx context.Context) *Store {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return nil
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),

		// A presence beat that cannot be written quickly is not worth waiting
		// for: the next one is ten seconds away, and the TTL covers the gap.
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("redis is not answering, starting anyway: presence calls will fail "+
			"until it comes back, and the API nodes will trip their breakers",
			"addr", addr, "err", err)
	}

	return &Store{rdb: rdb}
}

// Heartbeat writes down that these users are online on this node right now, and
// clears out entries nobody has refreshed.
//
// The node id comes from the caller, not from this process. That is the one
// line that had to change when presence moved out of the API node: this process
// is not a node, it is the place the nodes write to.
func (s *Store) Heartbeat(ctx context.Context, nodeID string, userIDs []uint64) error {
	now := time.Now()

	// One round trip for the whole node, however many users it holds. Fifty
	// separate ZADDs would be fifty network waits.
	pipe := s.rdb.Pipeline()

	for _, id := range userIDs {
		pipe.ZAdd(ctx, presenceKey, redis.Z{
			Score:  float64(now.Unix()),
			Member: member(id, nodeID),
		})
	}

	// Trimming is done on whatever beat happens to arrive, and it covers every
	// node's stale entries, not only the caller's. That is what makes a crashed
	// node's ghosts disappear: the survivors clean up after it.
	//
	// Reading already ignores anything older than the cutoff, so this is about
	// memory, not correctness. Without it the set grows forever.
	pipe.ZRemRangeByScore(ctx, presenceKey, "-inf", "("+formatScore(now.Add(-TTL)))

	if _, err := pipe.Exec(ctx); err != nil {
		s.failures.Add(1)
		if isRedisDown(err) {
			slog.Warn("presence heartbeat failed", "node", nodeID, "users", len(userIDs), "err", err)
		}
		return err
	}
	return nil
}

// Online reports which of the given users are online anywhere.
//
// The whole online set is read and then filtered here, rather than asking Redis
// about each user. At this size that is one small command against fifty. If the
// set ever grows to a size where reading it all is silly, the fix is a second
// index — a set per room — and not fifty round trips.
//
// It returns only the online ones, which is the shape the RPC sends back: the
// caller already knows the list it asked about.
func (s *Store) Online(ctx context.Context, userIDs []uint64) ([]uint64, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	cutoff := time.Now().Add(-TTL)
	members, err := s.rdb.ZRangeByScore(ctx, presenceKey, &redis.ZRangeBy{
		Min: "(" + formatScore(cutoff),
		Max: "+inf",
	}).Result()
	if err != nil {
		s.failures.Add(1)
		return nil, err
	}

	// A set of everyone online, then the answer for the users that were asked
	// about. A user on two nodes appears twice and is counted once.
	all := make(map[uint64]struct{}, len(members))
	for _, m := range members {
		if id, ok := parseMember(m); ok {
			all[id] = struct{}{}
		}
	}

	online := make([]uint64, 0, len(userIDs))
	for _, id := range userIDs {
		if _, ok := all[id]; ok {
			online = append(online, id)
		}
	}
	return online, nil
}

// Ping is the readiness check, used by the service's own health server.
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// Close shuts the connection.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	return s.rdb.Close()
}

// Failures is how many Redis commands failed. It is on /metrics because
// presence failing quietly looks exactly like nobody being online, and those
// two need to be told apart.
func (s *Store) Failures() int64 { return s.failures.Load() }

func member(userID uint64, node string) string {
	return strconv.FormatUint(userID, 10) + ":" + node
}

// parseMember reads the user id back out of "userID:nodeID".
//
// It cuts at the FIRST colon, because a node id may contain one — an IPv6
// hostname does — and a user id never does.
func parseMember(m string) (uint64, bool) {
	raw, _, found := strings.Cut(m, ":")
	if !found {
		return 0, false
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// isRedisDown says whether an error means Redis is unreachable rather than the
// command being wrong. It is only used for logging, so that a Redis outage
// writes one clear line instead of a wall of stack-shaped noise.
func isRedisDown(err error) bool {
	return err != nil && !errors.Is(err, redis.Nil)
}

// formatScore renders a unix time for a ZSET score. Redis takes scores as
// strings on the wire, and the default float formatting would use scientific
// notation for a timestamp this large.
func formatScore(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}

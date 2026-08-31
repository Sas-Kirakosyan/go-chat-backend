package cluster

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
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
const presenceKey = "chat:presence"

const (
	// PresenceTTL is how long an entry counts as online without a refresh.
	PresenceTTL = 30 * time.Second

	// presenceTimeout caps one presence read from an HTTP handler. A page that
	// paints green dots is not worth a second of anybody's time, and with Redis
	// down it must fail quickly rather than hold the request open.
	presenceTimeout = time.Second

	// PresenceHeartbeat is how often a node refreshes its own users.
	//
	// Three refreshes fit inside one TTL. That margin is on purpose: a single
	// slow or lost heartbeat must not make a room full of people blink offline,
	// and with a 10s beat it takes three failures in a row before anyone does.
	PresenceHeartbeat = 10 * time.Second
)

// Heartbeat writes down that these users are online on this node right now, and
// clears out entries nobody has refreshed.
//
// It is called on a timer with the hub's current users, not when a socket opens
// or closes. Events would be cheaper and would be wrong: the one moment a node
// cannot send an event is the moment it dies, which is exactly the case this
// has to survive.
func (c *Cluster) Heartbeat(ctx context.Context, userIDs []uint) error {
	now := time.Now()

	// One round trip for the whole node, however many users it holds. Fifty
	// separate ZADDs would be fifty network waits.
	pipe := c.rdb.Pipeline()

	for _, id := range userIDs {
		pipe.ZAdd(ctx, presenceKey, redis.Z{
			Score:  float64(now.Unix()),
			Member: presenceMember(id, c.nodeID),
		})
	}

	// Trimming is done by whoever happens to be beating, and it covers every
	// node's stale entries, not only this node's. That is what makes a crashed
	// node's ghosts disappear: the survivors clean up after it.
	//
	// Reading already ignores anything older than the cutoff, so this is about
	// memory, not correctness. Without it the set grows forever.
	pipe.ZRemRangeByScore(ctx, presenceKey, "-inf", "("+formatScore(now.Add(-PresenceTTL)))

	if _, err := pipe.Exec(ctx); err != nil {
		c.presenceFail.Add(1)
		if isRedisDown(err) {
			slog.Warn("presence heartbeat failed", "users", len(userIDs), "err", err)
		}
		return err
	}
	return nil
}

// Online reports which of the given users are online anywhere in the cluster.
//
// The whole online set is read and then filtered here, rather than asking Redis
// about each user. At this size that is one small command against fifty. If the
// set ever grows to a size where reading it all is silly, the fix is a second
// index — a set per room — and not fifty round trips.
func (c *Cluster) Online(ctx context.Context, userIDs []uint) (map[uint]bool, error) {
	online := make(map[uint]bool, len(userIDs))
	if len(userIDs) == 0 {
		return online, nil
	}

	// A deadline of its own, and it was not here in the first version.
	//
	// With Redis stopped, this call took over fifteen seconds and the client
	// gave up before the server did. The per-call timeouts on the connection
	// are not the whole story: go-redis retries a failed command, and a DNS
	// lookup for a container that no longer exists is slow on its own, so the
	// waits add up. A handler that depends on a shared service must bound its
	// own wait, or one dead dependency turns into a pile of stuck requests.
	ctx, cancel := context.WithTimeout(ctx, presenceTimeout)
	defer cancel()

	cutoff := time.Now().Add(-PresenceTTL)
	members, err := c.rdb.ZRangeByScore(ctx, presenceKey, &redis.ZRangeBy{
		Min: "(" + formatScore(cutoff),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}

	// A set of everyone online, then the answer for the users that were asked
	// about. A user on two nodes appears twice and lands in the map once.
	all := make(map[uint]struct{}, len(members))
	for _, m := range members {
		if id, ok := parsePresenceMember(m); ok {
			all[id] = struct{}{}
		}
	}
	for _, id := range userIDs {
		_, ok := all[id]
		online[id] = ok
	}
	return online, nil
}

func presenceMember(userID uint, node string) string {
	return strconv.FormatUint(uint64(userID), 10) + ":" + node
}

// parsePresenceMember reads the user id back out of "userID:nodeID".
//
// It cuts at the FIRST colon, because a node id may contain one and a user id
// never does.
func parsePresenceMember(member string) (uint, bool) {
	raw, _, found := strings.Cut(member, ":")
	if !found {
		return 0, false
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return uint(id), true
}

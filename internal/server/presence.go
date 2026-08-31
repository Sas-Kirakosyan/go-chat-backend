package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// presenceDTO answers "who in this room is online right now?".
//
// Two lists rather than a flag per member, because a client almost always
// wants one of them: paint the online ones green, or count them. Both lists
// are member ids, and the caller already knows the members.
type presenceDTO struct {
	Online  []uint `json:"online"`
	Offline []uint `json:"offline"`
}

// PresenceHandler handles GET /conversations/:id/presence.
//
// "Online" here means "had a socket open somewhere in the cluster in the last
// PresenceTTL". It is never exact, and it cannot be: the only honest way to
// know a client is gone is that it stopped saying it is here. A node that is
// killed with -9 answers no goodbye, so a system that waits for one leaves
// ghosts online forever.
//
// It is member-only, like every other route on a room. Who is online is a fact
// about people, and an outsider has no business reading it.
func (s *Server) PresenceHandler(c *gin.Context) {
	conversationID, ok := s.memberOnly(c)
	if !ok {
		return
	}

	memberIDs, err := s.db.ListConversationMemberIDs(c.Request.Context(), conversationID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not list members"})
		return
	}

	online, err := s.onlineMembers(c, memberIDs)
	if err != nil {
		// Redis being down is not a reason to fail the request with a 500 —
		// but it IS a reason not to answer, because the honest answer would be
		// "everyone is offline", and that is worse than no answer at all.
		logFrom(c).Warn("presence lookup failed", "conversation_id", conversationID, "err", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Presence is not available right now"})
		return
	}

	out := presenceDTO{Online: []uint{}, Offline: []uint{}}
	for _, id := range memberIDs {
		if online[id] {
			out.Online = append(out.Online, id)
			continue
		}
		out.Offline = append(out.Offline, id)
	}
	c.JSON(http.StatusOK, out)
}

// onlineMembers answers the same question in both modes.
//
// With Redis it is the whole cluster. Without it, this node's own sockets are
// the whole world, and that answer is correct — for a single node it IS the
// whole world.
func (s *Server) onlineMembers(c *gin.Context, memberIDs []uint) (map[uint]bool, error) {
	if s.cluster != nil {
		return s.cluster.Online(c.Request.Context(), memberIDs)
	}

	local := make(map[uint]bool, len(memberIDs))
	for _, id := range s.hub.UserIDs() {
		local[id] = true
	}
	return local, nil
}

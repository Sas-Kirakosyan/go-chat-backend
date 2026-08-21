package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// allowedWSOrigin is the browser page that may open a socket. It is the same
// origin the CORS config allows for REST.
const allowedWSOrigin = "http://localhost:5173"

// wsMessageEvent is the type field of a pushed new message.
const wsMessageEvent = "message.new"

// wsEnvelope wraps everything the server pushes down a socket.
//
// A bare messageDTO would be simpler today and painful later: Stage 3 adds
// presence, Stage 4 adds sequence numbers and gap answers. A client that
// switches on "type" from the first day never has to guess what a frame is.
type wsEnvelope struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	// CheckOrigin is our own CSRF defence, and it has to be, because the CORS
	// middleware does not cover this route. A browser sends no preflight for a
	// WebSocket and ignores Access-Control-Allow-Origin on the handshake: any
	// page on the internet may open a socket to us, and the browser will
	// happily attach cookies. Only this check stops that.
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")

		// No Origin header means the caller is not a browser: curl, wscat,
		// cmd/wsload, a mobile app. There is no other site to protect them
		// from, so there is nothing to block.
		if origin == "" {
			return true
		}
		return origin == allowedWSOrigin
	},
}

// WSHandler handles GET /ws. It authenticates the caller, upgrades the
// connection, and hands it to the hub.
//
// The socket is delivery only. Nothing a client writes into it is used —
// messages are created by POST /conversations/:id/messages and nowhere else.
func (s *Server) WSHandler(c *gin.Context) {
	tokenStr, ok := wsAccessToken(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing token"})
		return
	}

	// Check the token before upgrading, not after. A caller with a bad token
	// then gets a plain 401 with a JSON body it can read. Upgrading first and
	// closing after would tell a browser only "the socket closed", which is
	// far harder to debug.
	claims, err := s.parseAccessToken(tokenStr)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		// Upgrade has already written its own HTTP error, so there is nothing
		// left to answer with here.
		log.Printf("ws: upgrade failed for user %d: %v", claims.UserID, err)
		return
	}

	// One frame so the client knows the socket is live and authenticated. A
	// TCP connection being open does not prove either.
	hello, err := json.Marshal(wsEnvelope{
		Type: "connected",
		Data: gin.H{"user_id": claims.UserID},
	})
	if err == nil {
		_ = conn.WriteMessage(websocket.TextMessage, hello)
	}

	// From here the hub owns the connection: it starts the read and write
	// goroutines, and this handler must never touch conn again.
	if !s.hub.Add(conn, claims.UserID) {
		log.Printf("ws: hub is closing, refused socket for user %d", claims.UserID)
	}
}

// wsAccessToken reads the access token for a socket.
//
// The browser WebSocket API cannot set request headers, so the token has to
// travel in the query string: new WebSocket("ws://host/ws?token=...").
// Clients that *can* send headers (tests, Go clients, wscat) use the normal
// Authorization header instead.
//
// A token in a URL is a real cost: URLs land in access logs, in proxy logs,
// and in a Referer header. It is accepted here and nowhere else, and the
// access token lives 15 minutes, which is what keeps that cost small.
func wsAccessToken(c *gin.Context) (string, bool) {
	if token, ok := bearerToken(c.GetHeader("Authorization")); ok {
		return token, true
	}
	if token := c.Query("token"); token != "" {
		return token, true
	}
	return "", false
}

// broadcastMessage pushes a stored message to every member of its room.
//
// It is best effort, on purpose. The message is already committed to Postgres
// and the sender already has its 201; a delivery that fails here is a missed
// live push, not a lost message, and the client can still read it from
// history. Stage 4 is where that gap gets closed properly.
func (s *Server) broadcastMessage(c *gin.Context, conversationID uint, msg messageDTO) {
	memberIDs, err := s.db.ListConversationMemberIDs(c.Request.Context(), conversationID)
	if err != nil {
		log.Printf("ws: could not list members of conversation %d: %v", conversationID, err)
		return
	}

	// Marshalled once and shared by every receiver: fifty members in a room
	// cost one encode, not fifty.
	payload, err := json.Marshal(wsEnvelope{Type: wsMessageEvent, Data: msg})
	if err != nil {
		log.Printf("ws: could not encode message %d: %v", msg.ID, err)
		return
	}

	s.hub.Broadcast(memberIDs, payload)
}

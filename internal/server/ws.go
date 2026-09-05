package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"go-chat-backend/internal/event"
	"go-chat-backend/internal/metrics"
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
		logFrom(c).Warn("ws upgrade failed", "user_id", claims.UserID, "err", err)
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
	//
	// The token's own expiry travels with the socket, and the hub closes it at
	// that moment. Without it the check at the top of this function would be
	// the ONLY check this connection ever gets, and a socket opened a second
	// before the token died would go on delivering for as long as the process
	// lived.
	if !s.hub.Add(conn, claims.UserID, claims.ExpiresAt.Time) {
		logFrom(c).Warn("ws hub is closing, socket refused", "user_id", claims.UserID)
		return
	}

	logFrom(c).Info("ws connected",
		"user_id", claims.UserID,
		"expires_in_s", int(time.Until(claims.ExpiresAt.Time).Seconds()),
	)
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
// access token lives 15 minutes by default, which is what keeps that cost
// small. Raising ACCESS_TOKEN_TTL raises this cost with it.
func wsAccessToken(c *gin.Context) (string, bool) {
	if token, ok := bearerToken(c.GetHeader("Authorization")); ok {
		return token, true
	}
	if token := c.Query("token"); token != "" {
		return token, true
	}
	return "", false
}

// deliverMessage pushes one message to the sockets this node holds.
//
// It is the last step of the delivery path and the only place that turns an
// event into WebSocket bytes. Whatever brought the event here — the broker on
// a clustered node, or the relay's local publisher on a single one — ends up
// calling this.
//
// It is a method with no *gin.Context, and that is the Stage 5 change in one
// line. Delivery used to happen inside the request that caused it; now the
// request is long finished and this runs on a background goroutine.
//
// It stays best effort. A frame that does not reach a socket is a missed live
// push, not a lost message: the message is committed, and a client that
// notices a hole in its seq numbers asks for it with ?after_seq=. Retrying at
// this level would push at sockets that are already gone.
func (s *Server) deliverMessage(ev event.MessageCreated, userIDs []uint) {
	// Marshalled once and shared by every receiver: fifty members in a room
	// cost one encode, not fifty.
	payload, err := json.Marshal(wsEnvelope{Type: wsMessageEvent, Data: eventToMessageDTO(ev)})
	if err != nil {
		slog.Error("ws could not encode message", "message_id", ev.MessageID, "err", err)
		return
	}
	s.hub.Broadcast(userIDs, payload)
}

// applyUnread is the durable work the broker consumer does for one message.
//
// It lives here, and not in the broker package, because it is a database
// write and the broker knows nothing about the database. The broker decides
// when it runs and what happens if it fails; this decides what it does.
//
// An error returned here means "hand this message out again", so it must only
// ever describe a problem that could go away — a database that is restarting.
// The statement behind it is idempotent, so a message counted twice moves the
// number once.
func (s *Server) applyUnread(ctx context.Context, ev event.MessageCreated) error {
	changed, err := s.db.ApplyUnread(ctx, ev.MessageID, ev.ConversationID, ev.SenderID, ev.Seq)
	if err != nil {
		return err
	}
	if changed == 0 {
		// Nothing moved, which means the inbox row was already there and this
		// message has been counted before. That is a redelivery doing no harm,
		// which is exactly what it is supposed to do.
		metrics.UnreadDuplicates.Inc()
		return nil
	}
	metrics.UnreadApplied.Add(float64(changed))
	return nil
}

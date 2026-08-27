package ws

import (
	"time"

	"github.com/gorilla/websocket"
)

const (
	// defaultWriteWait is how long one write may take before the socket is
	// treated as dead. Without a deadline a write to a client that stopped
	// reading blocks forever, and its writePump goroutine leaks.
	defaultWriteWait = 10 * time.Second

	// defaultPongWait is how long we wait for an answer to our ping before we
	// decide the connection is gone.
	//
	// This is what catches a pulled cable. TCP itself does not notice: nothing
	// is sent, so nothing fails, and the socket sits open for hours. The
	// ping/pong pair turns that silence into an error.
	defaultPongWait = 60 * time.Second

	// defaultPingPeriod must be shorter than pongWait, or we would time out
	// before we even asked. Nine tenths leaves room for one slow round trip.
	defaultPingPeriod = (defaultPongWait * 9) / 10

	// maxMessageSize caps what a client may send us. Clients are not supposed
	// to send anything at all here (writes go over REST), so this only has to
	// be big enough for a close frame's reason text. It stops a client from
	// pushing megabytes into the server's read buffer.
	maxMessageSize = 512
)

// CloseTokenExpired is the close code for a socket whose access token ran out.
//
// 4000-4999 is the range reserved for the application, and 4401 is chosen to
// echo HTTP 401: it tells a client "get a new token and reconnect", which is a
// different instruction from CloseGoingAway ("this server is stopping, try
// again later"). Without the distinction a client cannot tell a deploy from an
// expired login, and would reconnect with the same dead token forever.
const CloseTokenExpired = 4401

// Client is one open socket.
//
// Two goroutines run per client, and they have separate jobs:
//
//   - readPump owns reading. It is also the one that notices the socket died.
//   - writePump owns writing. Only it may call conn.Write*, because gorilla
//     allows exactly one concurrent writer.
//
// The send channel is filled by the hub and drained by writePump. The hub is
// its only sender and its only closer.
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	userID uint

	// expiresAt is when the access token used to open this socket runs out.
	// The zero value means the socket has no expiry.
	expiresAt time.Time

	send chan []byte
}

// UserID is who this socket belongs to.
func (c *Client) UserID() uint { return c.userID }

// readPump reads from the socket until it fails, then unregisters the client.
//
// It throws every message away. That is deliberate: this project accepts
// writes over REST only, so there is one validation path and one place that
// stores a message. The loop still has to exist, because a WebSocket
// connection only processes pongs and close frames while somebody is reading.
func (c *Client) readPump() {
	defer func() {
		// Tell the hub first, then close the socket. The hub may already have
		// dropped this client for being slow; unregistering twice is safe.
		select {
		case c.hub.unregister <- c:
		case <-c.hub.done:
		}
		c.conn.Close()
	}()

	pongWait := c.hub.pongWait
	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))

	// Every pong pushes the deadline forward. No pong, and the next read fails
	// on its own — no timer to manage, no goroutine to leak.
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

// writePump writes queued messages, keeps the heartbeat going, and closes the
// socket when its token runs out.
func (c *Client) writePump() {
	writeWait := c.hub.writeWait
	ticker := time.NewTicker(c.hub.pingPeriod)
	expiry, stopExpiry := c.expiryTimer()
	defer func() {
		ticker.Stop()
		stopExpiry()
		c.conn.Close()
	}()

	for {
		select {
		case payload, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel: either we were dropped for being
				// slow, or the server is shutting down. Say goodbye properly,
				// so the client sees a close frame and not a broken pipe.
				_ = c.conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseGoingAway, "server closing"))
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-expiry:
			// The token this socket was opened with has run out. Say why, then
			// go; the deferred Close ends the connection, and readPump notices
			// and unregisters us.
			c.hub.expired.Add(1)
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(CloseTokenExpired, "token expired"))
			return
		}
	}
}

// expiryTimer returns a channel that fires once, when this socket's token runs
// out, and the function that gives the timer back.
//
// # Why the socket has to close at all
//
// The token is checked once, at the handshake, and never again. Nothing after
// the upgrade looks at it. So without this, a socket opened one second before
// a token expired would keep receiving every message in every room its owner
// is in — forever, or until the process restarted. Logging out did not help
// either: logout stops new access tokens being minted, it does not reach
// inside a connection that is already open.
//
// # Why a deadline and not a re-check
//
// The other option was to re-check the token on a timer, or to look the
// session up in the database every so often. Both put a clock and a query into
// the socket's own goroutine, times five thousand sockets, to learn something
// we already know: exp is inside the token, so the moment it dies is known at
// connect time. One timer per socket, no database, no polling.
//
// # What this does NOT fix
//
// Logout is still not instant for a socket. It is now bounded by the token's
// life instead of being unbounded, which makes a socket no worse than a REST
// call with the same token — the same 15-minute window the session design
// already accepts. Closing it completely means a revocation check on every
// use, which is the exact database lookup a stateless access token exists to
// avoid.
func (c *Client) expiryTimer() (<-chan time.Time, func()) {
	if c.expiresAt.IsZero() {
		// A receive on a nil channel blocks forever, which inside a select
		// means "this case never happens". No special case needed in the loop.
		return nil, func() {}
	}

	// A Timer we can stop, not time.After: time.After's timer is not collected
	// until it fires, so every socket that closed early would hold one for the
	// rest of its token's life. With 5000 sockets reconnecting, that adds up.
	t := time.NewTimer(time.Until(c.expiresAt))
	return t.C, func() { t.Stop() }
}

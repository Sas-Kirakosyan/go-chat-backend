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

// writePump writes queued messages and keeps the heartbeat going.
func (c *Client) writePump() {
	writeWait := c.hub.writeWait
	ticker := time.NewTicker(c.hub.pingPeriod)
	defer func() {
		ticker.Stop()
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
		}
	}
}

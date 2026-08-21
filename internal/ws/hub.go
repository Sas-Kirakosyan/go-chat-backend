// Package ws is the WebSocket delivery layer: one hub, many client sockets.
//
// It knows nothing about Gin, JWT, or the database. It moves bytes to user
// ids. Everything that decides *what* to send lives in internal/server, and
// everything that stores a message lives in internal/database.
//
// That line matters for later stages. In Stage 3 there will be two nodes and a
// Redis subscriber, and the subscriber will call the same Broadcast this hub
// already has. Redis plugs in beside the hub, not inside it.
package ws

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// sendBuffer is how many messages may wait for one client before it is
// considered too slow and dropped.
//
// Small on purpose. A big buffer does not fix a slow reader, it only delays
// the moment we notice, while holding more memory per socket — and with 5000
// sockets that adds up.
const sendBuffer = 16

// broadcastBuffer is how many fan-outs may wait for the hub's own goroutine.
const broadcastBuffer = 256

// Hub owns every open socket on this node and fans messages out to them.
//
// The client map is guarded by a goroutine, not by a mutex: only run() ever
// touches it. Everything else talks to run() over channels. That is the Go way
// of sharing state, and it makes the ownership rules below possible.
type Hub struct {
	register   chan *Client
	unregister chan *Client
	broadcast  chan envelope

	// done is closed by Close. It tells run() to stop and tells Broadcast
	// there is nobody left to deliver to.
	done chan struct{}
	// stopped is closed by run() just before it returns, so Close can wait
	// until every socket really is shut.
	stopped chan struct{}

	// clients maps a user id to that user's open sockets. One user can have
	// several: a phone, a laptop, two browser tabs.
	//
	// ONLY run() may read or write this map.
	clients map[uint]map[*Client]struct{}

	// Counters. They are read from other goroutines (the load test, and the
	// /metrics endpoint in Stage 2), so they are atomic.
	open    atomic.Int64
	dropped atomic.Int64
	shed    atomic.Int64

	// Heartbeat timings. They are fields rather than plain constants so a test
	// can shrink them: proving that a silent connection is noticed should take
	// a fraction of a second, not the full minute a real client gets. They are
	// set once in New and never written again after Run starts.
	writeWait  time.Duration
	pongWait   time.Duration
	pingPeriod time.Duration
}

// envelope is one fan-out: a payload, and the users who should get it.
type envelope struct {
	userIDs []uint

	// payload is JSON that was marshalled once by the caller and is shared,
	// read-only, by every receiver. Fifty members cost one encode, not fifty.
	payload []byte
}

func New() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan envelope, broadcastBuffer),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		clients:    make(map[uint]map[*Client]struct{}),

		writeWait:  defaultWriteWait,
		pongWait:   defaultPongWait,
		pingPeriod: defaultPingPeriod,
	}
}

// Run is the hub's single goroutine. Start it once, in the background, and
// stop it with Close.
func (h *Hub) Run() {
	defer close(h.stopped)

	for {
		select {
		case c := <-h.register:
			h.add(c)

		case c := <-h.unregister:
			h.remove(c)

		case e := <-h.broadcast:
			h.deliver(e)

		case <-h.done:
			h.closeAll()
			return
		}
	}
}

// Add takes over a freshly upgraded connection: it registers the socket and
// starts its two pumps. After this call the connection belongs to the hub, and
// the caller must not read from it or write to it again.
//
// It returns false when the hub is already closing, in which case the
// connection is closed here and no goroutine is started.
func (h *Hub) Add(conn *websocket.Conn, userID uint) bool {
	c := &Client{
		hub:    h,
		conn:   conn,
		userID: userID,
		send:   make(chan []byte, sendBuffer),
	}

	select {
	case h.register <- c:
	case <-h.done:
		conn.Close()
		return false
	}

	go c.writePump()
	go c.readPump()
	return true
}

// Broadcast sends one payload to every open socket of every user in userIDs.
//
// It never blocks the caller. It is called from the HTTP write path, and a
// slow socket somewhere must not slow down a POST: the message is already
// safely in Postgres, so the worst case here is a missed live push, not a lost
// message.
func (h *Hub) Broadcast(userIDs []uint, payload []byte) {
	if len(userIDs) == 0 || len(payload) == 0 {
		return
	}

	select {
	case h.broadcast <- envelope{userIDs: userIDs, payload: payload}:

	case <-h.done:
		// The hub is shutting down. There is nothing to deliver to, and that
		// is not an error.

	default:
		// The hub's own queue is full, which means run() is behind. Shedding
		// this fan-out keeps the write path fast. Stage 2 puts this counter on
		// /metrics; until then it is at least logged.
		h.shed.Add(1)
		log.Printf("ws: hub queue full, dropped a fan-out to %d users", len(userIDs))
	}
}

// Close shuts every socket down and stops the hub. It returns once run() has
// finished, so the caller knows the sockets are really closed.
//
// Calling it twice would panic on the second close(h.done), so it guards with
// a select. Shutdown paths get called from odd places, and a shutdown that
// panics is worse than no shutdown at all.
func (h *Hub) Close() {
	select {
	case <-h.done:
		// Already closing. Wait for the same finish line and return.
	default:
		close(h.done)
	}
	<-h.stopped
}

// Open is how many sockets are connected right now.
func (h *Hub) Open() int { return int(h.open.Load()) }

// Dropped is how many sockets were dropped for being too slow to read.
func (h *Hub) Dropped() int { return int(h.dropped.Load()) }

// ---------------------------------------------------------------------------
// Everything below runs on the run() goroutine, and only there.
//
// That is what makes closing c.send safe. A channel must be closed by its only
// sender, and run() is the only sender: writePump reads from it, Broadcast
// hands work to run() instead of sending directly.
// ---------------------------------------------------------------------------

func (h *Hub) add(c *Client) {
	if h.clients[c.userID] == nil {
		h.clients[c.userID] = make(map[*Client]struct{})
	}
	h.clients[c.userID][c] = struct{}{}
	h.open.Add(1)
}

// remove takes a socket out of the map and closes its send channel.
//
// It is a no-op when the client is already gone. That case is normal, not a
// bug: a client dropped for being slow is removed here, and then its readPump
// notices the closed connection and unregisters it a second time.
func (h *Hub) remove(c *Client) {
	sockets, ok := h.clients[c.userID]
	if !ok {
		return
	}
	if _, ok := sockets[c]; !ok {
		return
	}

	delete(sockets, c)
	if len(sockets) == 0 {
		delete(h.clients, c.userID)
	}
	close(c.send)
	h.open.Add(-1)
}

// deliver fans one payload out to the local sockets of the listed users.
func (h *Hub) deliver(e envelope) {
	for _, userID := range e.userIDs {
		for c := range h.clients[userID] {
			select {
			case c.send <- e.payload:

			default:
				// The client's buffer is full: it is not reading fast enough,
				// or not reading at all. Drop it instead of waiting.
				//
				// This is the rule that keeps one bad client from freezing the
				// whole node. Waiting here would block run(), and run() is the
				// single goroutine that serves every other socket.
				h.dropped.Add(1)
				log.Printf("ws: dropping slow client (user %d)", c.userID)
				h.remove(c)
			}
		}
	}
}

// closeAll shuts every socket at shutdown. Closing send makes each writePump
// send a proper close frame and exit, so clients see a clean goodbye instead
// of a broken pipe.
func (h *Hub) closeAll() {
	for userID, sockets := range h.clients {
		for c := range sockets {
			close(c.send)
			h.open.Add(-1)
		}
		delete(h.clients, userID)
	}
}

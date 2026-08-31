// Command splitcheck is the "break it" half of Stage 3.
//
// It puts two members of the same room on two different API nodes, sends a
// message, and reports who actually received it. Before Redis Pub/Sub the
// answer is "only the sender's node", because each node's hub knows its own
// sockets and nothing else. That is the split-brain bug, and this tool is how
// you watch it happen instead of reading about it.
//
// Start the cluster and seed two users first:
//
//	docker compose up --build -d
//	make seed ARGS="-n 2"
//	make splitcheck
//
// It talks to each node directly (8081, 8082) rather than through nginx. Going
// through the proxy would work too, but round robin decides where each socket
// lands, so the run would prove something different on every attempt.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type config struct {
	nodeA    string
	nodeB    string
	userA    string
	userB    string
	password string
	wait     time.Duration

	// presenceWait is how long to wait before asking who is online. It has to
	// be at least one heartbeat, because presence is written on a timer.
	presenceWait time.Duration

	// hold keeps the sockets open afterwards, printing presence as it goes.
	// It is the ghost test: kill a node while this runs and watch its users
	// fall out of the online list on their own.
	hold time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.nodeA, "node-a", "http://localhost:8081", "base URL of the first API node")
	flag.StringVar(&cfg.nodeB, "node-b", "http://localhost:8082", "base URL of the second API node")
	flag.StringVar(&cfg.userA, "user-a", "testuser001", "seeded user who connects to node A")
	flag.StringVar(&cfg.userB, "user-b", "testuser002", "seeded user who connects to node B")
	flag.StringVar(&cfg.password, "password", "password123", "password shared by the seeded users")
	flag.DurationVar(&cfg.wait, "wait", 3*time.Second, "how long to wait for a message to arrive")
	flag.DurationVar(&cfg.presenceWait, "presence-wait", 11*time.Second,
		"how long to wait before asking who is online (one presence heartbeat is 10s)")
	flag.DurationVar(&cfg.hold, "hold", 0,
		"keep the sockets open this long, printing presence — kill a node during it to hunt ghosts")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	client := &http.Client{Timeout: 15 * time.Second}

	// Each user logs in on the node they will live on. Any node would do — the
	// token is signed with a secret both nodes share and neither keeps session
	// state — and doing it this way makes the run read like two real clients.
	a, err := signIn(client, cfg.nodeA, cfg.userA, cfg.password)
	if err != nil {
		return fmt.Errorf("user A: %w", err)
	}
	b, err := signIn(client, cfg.nodeB, cfg.userB, cfg.password)
	if err != nil {
		return fmt.Errorf("user B: %w", err)
	}
	fmt.Printf("user A %s (id %d) -> %s\n", cfg.userA, a.id, cfg.nodeA)
	fmt.Printf("user B %s (id %d) -> %s\n", cfg.userB, b.id, cfg.nodeB)

	roomID, err := createRoom(client, cfg.nodeA, a.token, b.id)
	if err != nil {
		return fmt.Errorf("create room: %w", err)
	}
	fmt.Printf("room   %d, with both users as members\n\n", roomID)

	// The sockets. Each one lands on a named node, which is the whole point:
	// the hub that accepts this socket is the only hub that can push to it.
	socketA, err := connect(cfg.nodeA, a.token)
	if err != nil {
		return fmt.Errorf("socket A: %w", err)
	}
	defer socketA.close()

	socketB, err := connect(cfg.nodeB, b.token)
	if err != nil {
		return fmt.Errorf("socket B: %w", err)
	}
	defer socketB.close()

	// Both directions, because a one-way test leaves room for "maybe A is just
	// a bad sender". Each round names the node that took the POST.
	ok1 := round(client, cfg, "A sends on node A", cfg.nodeA, a.token, roomID, socketA, socketB)
	ok2 := round(client, cfg, "B sends on node B", cfg.nodeB, b.token, roomID, socketA, socketB)

	// Presence is the second half of Stage 3, and it is a different question
	// from delivery. Delivery asks "can node A push to a socket on node B".
	// Presence asks "does node A even know node B has that user". Both sockets
	// are still open here, so the honest answer is "both online" — from either
	// node.
	checkPresence(client, cfg, roomID, a, b)

	if cfg.hold > 0 {
		holdOpen(client, cfg, roomID, a, b)
	}

	verdict(ok1 && ok2)
	return nil
}

// holdOpen keeps both sockets open and reports presence on a timer.
//
// This is the ghost hunt. Kill node A while it runs:
//
//	docker compose kill api1
//
// User A's socket dies with the node, and nothing tells Redis about it — a
// killed process sends no goodbye. What has to happen is that user A stops
// being refreshed and falls out of the online window within one PresenceTTL.
// If they stay online forever, presence is a lie and every user list in the
// product is wrong.
func holdOpen(client *http.Client, cfg config, roomID uint, a, b user) {
	fmt.Printf("holding the sockets open for %s — now is the time to `docker compose kill api1`\n", cfg.hold)

	deadline := time.Now().Add(cfg.hold)
	start := time.Now()

	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)

		// Asked from node B, because node A is the one being killed. A dead node
		// cannot answer questions about itself, and the whole point is that a
		// SURVIVING node stops believing in the dead one's users.
		online, err := presence(client, cfg.nodeB, b.token, roomID)
		if err != nil {
			fmt.Printf("  +%3.0fs  node B: %v\n", time.Since(start).Seconds(), err)
			continue
		}
		fmt.Printf("  +%3.0fs  node B says: user A %s, user B %s\n",
			time.Since(start).Seconds(), onlineWord(online[a.id]), onlineWord(online[b.id]))
	}
	fmt.Println()
}

// checkPresence asks each node who is online in the room.
//
// The interesting cell is node A's answer about user B. User B's socket is on
// the other node, so a node that only knows its own hub has no way to say
// anything but "offline".
func checkPresence(client *http.Client, cfg config, roomID uint, a, b user) {
	fmt.Println("presence (both sockets are still open)")

	// A moment for the first heartbeat. Presence is a timer, not an event: a
	// node writes down its users every few seconds rather than when a socket
	// opens, because the one thing a dying node cannot do is send an event.
	time.Sleep(cfg.presenceWait)

	for _, ask := range []struct {
		node  string
		token string
	}{
		{cfg.nodeA, a.token},
		{cfg.nodeB, b.token},
	} {
		online, err := presence(client, ask.node, ask.token, roomID)
		if err != nil {
			fmt.Printf("  %s says: %v\n", ask.node, err)
			continue
		}

		fmt.Printf("  %s says: user A %s, user B %s\n",
			ask.node, onlineWord(online[a.id]), onlineWord(online[b.id]))
	}
	fmt.Println()
}

func onlineWord(online bool) string {
	if online {
		return "online"
	}
	return "OFFLINE"
}

// round sends one message and reports which sockets saw it.
//
// It returns true only when both sockets received it, which is what "one
// cluster" means. Anything else is two servers wearing one address.
func round(client *http.Client, cfg config, name, node, token string, roomID uint, sockets ...*socket) bool {
	clientMsgID := fmt.Sprintf("splitcheck-%d", time.Now().UnixNano())

	fmt.Printf("%s\n", name)
	if err := sendMessage(client, node, token, roomID, clientMsgID); err != nil {
		fmt.Printf("  send failed: %v\n\n", err)
		return false
	}

	all := true
	deadline := time.Now().Add(cfg.wait)
	for _, s := range sockets {
		if s.waitFor(clientMsgID, deadline) {
			fmt.Printf("  %s  received it\n", s.name)
			continue
		}
		fmt.Printf("  %s  NOTHING after %s\n", s.name, cfg.wait)
		all = false
	}
	fmt.Println()
	return all
}

func verdict(delivered bool) {
	fmt.Println("---------------------------------------------")
	if delivered {
		fmt.Println("BOTH NODES DELIVERED.")
		fmt.Println("A message sent on one node reached a socket on the other,")
		fmt.Println("so the nodes are sharing fan-out and not only a database.")
	} else {
		fmt.Println("SPLIT BRAIN.")
		fmt.Println("The message is in Postgres and history will show it, but the")
		fmt.Println("socket on the other node was never told. Each node's hub only")
		fmt.Println("knows the sockets it accepted itself.")
	}
	fmt.Println("---------------------------------------------")

	// Always exit 0. Before the fix, failure is the expected result and the
	// point of the run; a non-zero exit would make `make splitcheck` look
	// broken when it is doing exactly its job.
}

// ---------------------------------------------------------------------------
// The socket
// ---------------------------------------------------------------------------

// socket is one open WebSocket, with a background reader.
//
// Reading has to happen in its own goroutine and not on demand: the server
// also sends pings, and a client that only reads when it expects a message
// answers none of them.
type socket struct {
	name string
	conn *websocket.Conn

	// seen carries the client_msg_id of every message.new that arrived.
	seen chan string
}

func connect(node, token string) (*socket, error) {
	base, err := wsURL(node)
	if err != nil {
		return nil, err
	}

	conn, resp, err := websocket.DefaultDialer.Dial(base+"/ws?token="+url.QueryEscape(token), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial %s: %s: %w", node, resp.Status, err)
		}
		return nil, fmt.Errorf("dial %s: %w", node, err)
	}

	// The server's first frame is {"type":"connected"}. It is read here, before
	// the reader goroutine starts, for two reasons. One connection may have only
	// one reader, so the two must not overlap. And waiting for that frame means
	// the run never sends a message to a hub that has not registered this socket
	// yet — a race that would look exactly like the bug being hunted.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("no hello frame from %s: %w", node, err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	s := &socket{
		name: fmt.Sprintf("socket on %s", strings.TrimPrefix(node, "http://")),
		conn: conn,
		seen: make(chan string, 32),
	}
	go s.read()

	return s, nil
}

func (s *socket) read() {
	for {
		_, raw, err := s.conn.ReadMessage()
		if err != nil {
			close(s.seen)
			return
		}

		var frame struct {
			Type string `json:"type"`
			Data struct {
				ClientMsgID string `json:"client_msg_id"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Type != "message.new" {
			continue
		}
		select {
		case s.seen <- frame.Data.ClientMsgID:
		default:
		}
	}
}

// waitFor reports whether this socket received a given message before the
// deadline. Ids that belong to an earlier round are skipped, not counted.
func (s *socket) waitFor(clientMsgID string, deadline time.Time) bool {
	for {
		select {
		case got, open := <-s.seen:
			if !open {
				return false
			}
			if got == clientMsgID {
				return true
			}
		case <-time.After(time.Until(deadline)):
			return false
		}
	}
}

func (s *socket) close() { s.conn.Close() }

// ---------------------------------------------------------------------------
// REST calls
// ---------------------------------------------------------------------------

type user struct {
	token string
	id    uint
}

func signIn(client *http.Client, node, username, password string) (user, error) {
	var login struct {
		Token string `json:"token"`
	}
	body := map[string]string{"username": username, "password": password}
	if err := postJSON(client, node+"/login", "", body, &login); err != nil {
		return user{}, fmt.Errorf("login %s: %w (did you run `make seed ARGS=\"-n 2\"`?)", username, err)
	}

	req, err := http.NewRequest(http.MethodGet, node+"/auth/profile", nil)
	if err != nil {
		return user{}, err
	}
	req.Header.Set("Authorization", "Bearer "+login.Token)

	resp, err := client.Do(req)
	if err != nil {
		return user{}, err
	}
	defer resp.Body.Close()

	var profile struct {
		ID uint `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&profile); err != nil {
		return user{}, err
	}
	return user{token: login.Token, id: profile.ID}, nil
}

func createRoom(client *http.Client, node, token string, memberID uint) (uint, error) {
	var out struct {
		ID uint `json:"id"`
	}
	body := map[string]any{
		"title":      fmt.Sprintf("splitcheck %s", time.Now().Format(time.TimeOnly)),
		"member_ids": []uint{memberID},
	}
	if err := postJSON(client, node+"/conversations", token, body, &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// presence asks one node who is online in a room, and returns a map keyed by
// user id.
func presence(client *http.Client, node, token string, roomID uint) (map[uint]bool, error) {
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/conversations/%d/presence", node, roomID), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		answer, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(answer))
	}

	var out struct {
		Online  []uint `json:"online"`
		Offline []uint `json:"offline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	who := make(map[uint]bool, len(out.Online)+len(out.Offline))
	for _, id := range out.Offline {
		who[id] = false
	}
	for _, id := range out.Online {
		who[id] = true
	}
	return who, nil
}

func sendMessage(client *http.Client, node, token string, roomID uint, clientMsgID string) error {
	body := map[string]string{
		"content":       "does the other node hear me?",
		"client_msg_id": clientMsgID,
	}
	return postJSON(client, fmt.Sprintf("%s/conversations/%d/messages", node, roomID), token, body, nil)
}

func postJSON(client *http.Client, url, token string, body, into any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		answer, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(answer))
	}
	if into == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

// wsURL turns http://host into ws://host, and https into wss.
func wsURL(api string) (string, error) {
	u, err := url.Parse(strings.TrimSuffix(api, "/"))
	if err != nil {
		return "", fmt.Errorf("bad node URL %q: %w", api, err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("bad node scheme %q, want http or https", u.Scheme)
	}
	return u.String(), nil
}

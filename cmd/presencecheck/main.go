// Command presencecheck is the "break it" half of Stage 6.
//
// It answers three questions, in this order:
//
//  1. Does the split still work? Two users on two different API nodes, and each
//     node has to say both are online — which it can only do by asking the
//     presence service, because neither node knows the other's sockets.
//
//  2. What does the product lose when the presence service dies? It should lose
//     exactly one endpoint. Sending, receiving, history and the socket must all
//     keep working, because none of them touch presence. This is "degrade, do
//     not crash", and it is the reason presence was the right thing to split
//     first.
//
//  3. What does the failure COST? This is the number the stage is really about.
//     With the service down, every presence request should first be slow — it
//     waits for a deadline — and then suddenly fast, once the circuit breaker
//     has seen enough failures and starts refusing without a network call. Watch
//     the milliseconds column change.
//
// Start the cluster and seed two users first:
//
//	docker compose up --build -d
//	make seed ARGS="-n 2"
//	make presencecheck
//
// Then the real run. Kill the service during the pause and watch the numbers:
//
//	make presencecheck ARGS="-pause 40s"
//	docker compose kill presenced     # during the pause
//	docker compose start presenced    # 15 seconds later
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

	wait time.Duration

	// presenceWait is how long to wait before asking who is online. It has to
	// be at least one heartbeat, because presence is written on a timer and not
	// when a socket opens.
	presenceWait time.Duration

	// pause is the outage window. During it the tool asks for presence once a
	// second and sends a message every few seconds, printing what each costs.
	// Kill and restart presenced while it runs.
	pause time.Duration

	// probe is how often to ask during the pause.
	probe time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.nodeA, "node-a", "http://localhost:8081", "base URL of the first API node")
	flag.StringVar(&cfg.nodeB, "node-b", "http://localhost:8082", "base URL of the second API node")
	flag.StringVar(&cfg.userA, "user-a", "testuser001", "seeded user who connects to node A")
	flag.StringVar(&cfg.userB, "user-b", "testuser002", "seeded user who connects to node B")
	flag.StringVar(&cfg.password, "password", "password123", "password shared by the seeded users")
	flag.DurationVar(&cfg.wait, "wait", 5*time.Second, "how long to wait for a message to arrive")
	flag.DurationVar(&cfg.presenceWait, "presence-wait", 11*time.Second,
		"how long to wait before asking who is online (one heartbeat is 10s)")
	flag.DurationVar(&cfg.pause, "pause", 0,
		"probe presence and delivery for this long — kill presenced during it")
	flag.DurationVar(&cfg.probe, "probe", time.Second, "how often to probe during the pause")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	client := &http.Client{Timeout: 15 * time.Second}

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

	// The sockets. Each lands on a named node, and the hub that accepted it is
	// the only hub that knows about it — which is exactly why neither node can
	// answer the presence question alone.
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

	// Question 1: is the second service actually joining the two nodes?
	shared := checkPresence(client, cfg, roomID, a, b)

	// Question 2 and 3 need an outage, so they only run when asked for one.
	if cfg.pause > 0 {
		probeDuringOutage(client, cfg, roomID, a, b, socketA, socketB)
	}

	verdict(shared)
	return nil
}

// checkPresence asks each node who is online in the room.
//
// The interesting cell is node A's answer about user B. User B's socket is on
// the other node, so a node that cannot ask anybody has no way to say anything
// but "offline". Before Stage 3 that was the answer; from Stage 3 to Stage 5 it
// came from this node's own Redis client; now it comes from another process
// over gRPC, and the answer has to be the same.
func checkPresence(client *http.Client, cfg config, roomID uint, a, b user) bool {
	fmt.Println("presence, with both sockets open")

	// A moment for the first heartbeat. Presence is a timer, not an event: a
	// node writes down its users every ten seconds rather than when a socket
	// opens, because the one thing a dying node cannot do is send an event.
	time.Sleep(cfg.presenceWait)

	shared := true
	for _, ask := range []struct {
		node  string
		token string
	}{
		{cfg.nodeA, a.token},
		{cfg.nodeB, b.token},
	} {
		online, took, err := presence(client, ask.node, ask.token, roomID)
		if err != nil {
			fmt.Printf("  %s says: %v\n", ask.node, err)
			shared = false
			continue
		}

		fmt.Printf("  %-22s user A %s, user B %s   (%s)\n",
			strings.TrimPrefix(ask.node, "http://")+" says:",
			onlineWord(online[a.id]), onlineWord(online[b.id]), round(took))

		if !online[a.id] || !online[b.id] {
			shared = false
		}
	}
	fmt.Println()
	return shared
}

// probeDuringOutage is the interesting part.
//
// It asks for presence on a timer and sends a message every few probes, and it
// prints how long each took. Kill presenced while it runs. Three things should
// be visible:
//
//   - The presence column starts failing, and its cost starts around the
//     one-second client deadline.
//   - After five failed calls the reason changes from a status code to
//     "breaker", and the cost collapses to microseconds. That is the circuit
//     breaker: the request never leaves the API node.
//   - The send column never changes at all. Not one millisecond, not one error.
//     Nothing on the write path knows the presence service exists.
func probeDuringOutage(client *http.Client, cfg config, roomID uint, a, b user, sockets ...*socket) {
	fmt.Printf("probing for %s — now is the time to `docker compose kill presenced`\n", cfg.pause)
	fmt.Println("  (and `docker compose start presenced` a few seconds later)")
	fmt.Println()
	fmt.Printf("  %6s  %-42s  %s\n", "time", "presence on node A", "chat")
	fmt.Printf("  %6s  %-42s  %s\n", "----", strings.Repeat("-", 42), "----")

	deadline := time.Now().Add(cfg.pause)
	started := time.Now()

	var (
		sent      int
		delivered int
		probes    int
		failed    int
	)

	for i := 0; time.Now().Before(deadline); i++ {
		elapsed := time.Since(started)

		online, took, err := presence(client, cfg.nodeA, a.token, roomID)
		probes++

		var presenceCol string
		switch {
		case err != nil:
			failed++
			presenceCol = fmt.Sprintf("FAILED %-28s %8s", short(err), round(took))
		default:
			presenceCol = fmt.Sprintf("A %-7s B %-7s %20s",
				onlineWord(online[a.id]), onlineWord(online[b.id]), round(took))
		}

		// A message every fifth probe, from node B to node A. This is the
		// control: it must not change while presence is broken.
		chatCol := ""
		if i%5 == 0 {
			sent++
			id := fmt.Sprintf("presencecheck-%d", time.Now().UnixNano())
			sendStart := time.Now()
			sendErr := sendMessage(client, cfg.nodeB, b.token, roomID, id)
			sendTook := time.Since(sendStart)

			switch {
			case sendErr != nil:
				chatCol = fmt.Sprintf("SEND FAILED: %s", short(sendErr))
			case sockets[0].waitFor(id, time.Now().Add(cfg.wait)):
				delivered++
				chatCol = fmt.Sprintf("sent+delivered in %s", round(sendTook))
			default:
				chatCol = fmt.Sprintf("sent in %s, NOT DELIVERED", round(sendTook))
			}
		}

		fmt.Printf("  %5.0fs  %-42s  %s\n", elapsed.Seconds(), presenceCol, chatCol)
		time.Sleep(cfg.probe)
	}

	fmt.Println()
	fmt.Printf("  presence: %d of %d probes failed\n", failed, probes)
	fmt.Printf("  chat:     %d sent, %d delivered\n\n", sent, delivered)

	if sent > 0 && delivered == sent {
		fmt.Println("  Chat was untouched by the presence outage. That is the whole point of")
		fmt.Println("  the boundary: one service can be down and the product still works,")
		fmt.Println("  minus one endpoint.")
	} else if sent > 0 {
		fmt.Println("  A message went missing during the presence outage. That should not be")
		fmt.Println("  possible — nothing on the write or delivery path calls presence — so")
		fmt.Println("  look for a shared dependency that was killed by accident.")
	}
	fmt.Println()
}

func onlineWord(online bool) string {
	if online {
		return "online"
	}
	return "OFFLINE"
}

// short trims an error to something that fits in a column. The interesting word
// is usually at the front — the status code, or "breaker".
func short(err error) string {
	s := err.Error()
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 26 {
		return s[:26]
	}
	return s
}

func round(d time.Duration) string {
	if d < time.Millisecond {
		return d.Round(10 * time.Microsecond).String()
	}
	return d.Round(time.Millisecond).String()
}

func verdict(shared bool) {
	fmt.Println("---------------------------------------------")
	if shared {
		fmt.Println("ONE PRESENCE VIEW.")
		fmt.Println("Both nodes reported both users online, and neither node has a")
		fmt.Println("Redis client any more. The only way either of them could know")
		fmt.Println("about a socket on the other is the gRPC call to presenced.")
	} else {
		fmt.Println("SPLIT PRESENCE.")
		fmt.Println("At least one node could not see the other's user. Either the")
		fmt.Println("presence service is unreachable — check `docker compose logs")
		fmt.Println("presenced` and PRESENCE_ADDR on the nodes — or the heartbeat")
		fmt.Println("has not run yet. Try a longer -presence-wait.")
	}
	fmt.Println("---------------------------------------------")

	// Always exit 0, like the other check tools: a failure here is often the
	// thing being demonstrated, and a non-zero exit would make `make
	// presencecheck` look broken when it is doing its job.
}

// ---------------------------------------------------------------------------
// The socket
// ---------------------------------------------------------------------------

// socket is one open WebSocket, with a background reader.
//
// Reading has to happen in its own goroutine and not on demand: the server also
// sends pings, and a client that only reads when it expects a message answers
// none of them.
type socket struct {
	name string
	conn *websocket.Conn
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
	// the reader goroutine starts: one connection may have only one reader, and
	// waiting for that frame means nothing is sent to a hub that has not
	// registered this socket yet.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("no hello frame from %s: %w", node, err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	s := &socket{
		name: fmt.Sprintf("socket on %s", strings.TrimPrefix(node, "http://")),
		conn: conn,
		seen: make(chan string, 64),
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
// deadline. Ids from an earlier round are skipped, not counted.
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
		"title":      fmt.Sprintf("presencecheck %s", time.Now().Format(time.TimeOnly)),
		"member_ids": []uint{memberID},
	}
	if err := postJSON(client, node+"/conversations", token, body, &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// presence asks one node who is online in a room, and reports how long the
// answer took. The duration is the point of this tool: it is where a circuit
// breaker becomes visible.
func presence(client *http.Client, node, token string, roomID uint) (map[uint]bool, time.Duration, error) {
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/conversations/%d/presence", node, roomID), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	started := time.Now()
	resp, err := client.Do(req)
	took := time.Since(started)
	if err != nil {
		return nil, took, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		answer, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, took, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(answer))
	}

	var out struct {
		Online  []uint `json:"online"`
		Offline []uint `json:"offline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, took, err
	}

	who := make(map[uint]bool, len(out.Online)+len(out.Offline))
	for _, id := range out.Offline {
		who[id] = false
	}
	for _, id := range out.Online {
		who[id] = true
	}
	return who, took, nil
}

func sendMessage(client *http.Client, node, token string, roomID uint, clientMsgID string) error {
	body := map[string]string{
		"content":       "presence may be down; this must not be",
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

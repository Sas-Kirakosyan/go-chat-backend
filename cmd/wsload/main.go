// Command wsload is the "break it" half of Stage 1. It opens thousands of
// WebSockets against a running API, sends messages through the REST write
// path, and measures how long each message takes to come back out of a socket.
//
// It talks to the server the same way a browser would: /login for a token,
// /conversations to build rooms, /ws?token=... for the socket. Nothing here
// imports internal/, so it can also be pointed at a server on another machine.
//
// Before running it, seed the users it logs in as:
//
//	make seed ARGS="-n 5000"
//	go run ./cmd/wsload -n 5000 -slow 10
//
// The numbers it prints belong in the README. That is the whole point of the
// exercise: "I ran 5000 sockets and p99 fan-out was 40ms" is a different
// sentence from "I know WebSockets".
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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type config struct {
	api        string
	users      int
	prefix     string
	password   string
	slow       int
	roomSize   int
	messages   int
	size       int
	rate       time.Duration
	hold       time.Duration
	dialAtOnce int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.api, "api", "http://localhost:8080", "base URL of the API")
	flag.IntVar(&cfg.users, "n", 100, "how many users to log in and connect")
	flag.StringVar(&cfg.prefix, "prefix", "testuser", "username prefix, matching cmd/seed")
	flag.StringVar(&cfg.password, "password", "password123", "password shared by the seeded users")
	flag.IntVar(&cfg.slow, "slow", 0, "how many clients connect and then never read")
	flag.IntVar(&cfg.roomSize, "room-size", 50, "users per room (the API caps member_ids at 50)")
	flag.IntVar(&cfg.messages, "messages", 20, "how many messages to send per room")

	// Why this flag exists: a client that never reads is not noticed after 16
	// small messages. The kernel keeps its own send and receive buffers, and on
	// loopback they are large, so hundreds of tiny frames disappear into them
	// before our send channel fills at all. Big messages fill those buffers
	// fast, which is what makes the drop visible in seconds instead of hours.
	flag.IntVar(&cfg.size, "size", 64, "message size in bytes (the API caps content at 4000)")
	flag.DurationVar(&cfg.rate, "rate", 100*time.Millisecond, "pause between messages in one room")
	flag.DurationVar(&cfg.hold, "hold", 0, "keep the sockets open this long after the last message")
	flag.IntVar(&cfg.dialAtOnce, "dial-at-once", 64, "how many sockets to open in parallel")
	flag.Parse()

	if cfg.users < 1 || cfg.roomSize < 2 {
		log.Fatal("-n must be at least 1 and -room-size at least 2")
	}
	if cfg.slow > cfg.users {
		log.Fatal("-slow cannot be larger than -n")
	}
	if cfg.size < 1 || cfg.size > 4000 {
		log.Fatal("-size must be between 1 and 4000, to match the API's binding rules")
	}

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	// One client with a big connection pool: 5000 logins over the default
	// transport would spend most of their time queueing for a connection.
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 512,
		},
	}

	log.Printf("logging in %d users...", cfg.users)
	tokens, ids, err := login(httpClient, cfg)
	if err != nil {
		return err
	}

	rooms := buildRooms(httpClient, cfg, tokens, ids)
	log.Printf("built %d rooms of up to %d members", len(rooms), cfg.roomSize)

	log.Printf("connecting %d sockets (%d of them will never read)...", cfg.users, cfg.slow)
	fleet, err := connectAll(cfg, tokens)
	if err != nil {
		return err
	}
	defer fleet.close()

	log.Printf("sending %d messages per room...", cfg.messages)
	fleet.sendAll(httpClient, cfg, rooms, tokens)

	if cfg.slow > 0 {
		log.Printf("checking whether the %d never-reading clients were dropped...", cfg.slow)
		fleet.checkSlow()
	}

	if cfg.hold > 0 {
		log.Printf("holding the sockets open for %s — now is the time to kill the server", cfg.hold)
		time.Sleep(cfg.hold)
	}

	fleet.report(cfg)
	return nil
}

// ---------------------------------------------------------------------------
// Logging in and building rooms
// ---------------------------------------------------------------------------

// login gets an access token and a user id for every seeded user.
//
// It runs in parallel but not all at once: bcrypt is deliberately slow, so
// 5000 logins at the same moment would bury the server's CPU before the real
// test even starts.
func login(client *http.Client, cfg config) (tokens []string, ids []uint, err error) {
	tokens = make([]string, cfg.users)
	ids = make([]uint, cfg.users)
	sem := make(chan struct{}, 32)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed int

	start := time.Now()
	for i := range tokens {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			username := fmt.Sprintf("%s%03d", cfg.prefix, i+1)
			token, err := loginOne(client, cfg, username)
			if err == nil {
				var id uint
				id, err = profileID(client, cfg, token)
				if err == nil {
					tokens[i], ids[i] = token, id
					return
				}
			}

			mu.Lock()
			failed++
			if failed == 1 {
				log.Printf("login %s failed: %v (did you run `make seed ARGS=\"-n %d\"`?)", username, err, cfg.users)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if failed > 0 {
		return nil, nil, fmt.Errorf("%d of %d logins failed", failed, cfg.users)
	}
	log.Printf("logged in %d users in %s", cfg.users, time.Since(start).Round(time.Millisecond))
	return tokens, ids, nil
}

func loginOne(client *http.Client, cfg config, username string) (string, error) {
	body := map[string]string{"username": username, "password": cfg.password}

	var out struct {
		Token string `json:"token"`
	}
	if err := postJSON(client, cfg.api+"/login", "", body, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("login returned no token")
	}
	return out.Token, nil
}

// buildRooms puts the users into rooms of roomSize. The first user of each
// room creates it and names the rest as members, so it is one request per
// room instead of one per member.
func buildRooms(client *http.Client, cfg config, tokens []string, ids []uint) []room {
	var rooms []room

	for start := 0; start < len(tokens); start += cfg.roomSize {
		end := min(start+cfg.roomSize, len(tokens))
		if end-start < 2 {
			// A room of one has nobody to deliver to, so it would measure
			// nothing. The leftover user still connects a socket; they just
			// never receive anything.
			break
		}

		var out struct {
			ID uint `json:"id"`
		}
		body := map[string]any{
			"title":      fmt.Sprintf("load room %d", len(rooms)+1),
			"member_ids": ids[start+1 : end],
		}
		if err := postJSON(client, cfg.api+"/conversations", tokens[start], body, &out); err != nil {
			log.Fatalf("create room: %v", err)
		}

		rooms = append(rooms, room{id: out.ID, ownerIndex: start, members: end - start})
	}
	return rooms
}

type room struct {
	id         uint
	ownerIndex int // index into tokens: whoever sends the messages
	members    int
}

func profileID(client *http.Client, cfg config, token string) (uint, error) {
	req, err := http.NewRequest(http.MethodGet, cfg.api+"/auth/profile", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var out struct {
		ID uint `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// ---------------------------------------------------------------------------
// The socket fleet
// ---------------------------------------------------------------------------

type fleet struct {
	conns []*websocket.Conn

	// slowConns is the subset that never reads. They are kept apart so the
	// run can end by asking each of them the only question that matters: did
	// the server drop you?
	slowConns []*websocket.Conn

	mu        sync.Mutex
	latencies []time.Duration
	// sentAt maps a client_msg_id to the moment its POST started. Every
	// receiver of that message measures against the same instant, and it is
	// one process and one clock, so the number is honest.
	sentAt map[string]time.Time

	connectTime time.Duration
	connected   int
	closed      int
	cleanClosed int
	received    int
	slowClosed  int
}

func connectAll(cfg config, tokens []string) (*fleet, error) {
	f := &fleet{
		conns:  make([]*websocket.Conn, len(tokens)),
		sentAt: make(map[string]time.Time),
	}

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 30 * time.Second

	wsBase, err := wsURL(cfg.api)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	// The dial concurrency is deliberately modest. A listening socket has a
	// fixed accept queue, and dialing thousands at once overflows it: the
	// kernel then refuses connections outright ("actively refused"), which
	// looks like a server crash and is really just an impatient client.
	sem := make(chan struct{}, cfg.dialAtOnce)
	var mu sync.Mutex
	var failed, retried int

	start := time.Now()
	for i, token := range tokens {
		wg.Add(1)
		go func(i int, token string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			url := wsBase + "/ws?token=" + url.QueryEscape(token)

			// A refused connection is worth one or two retries, exactly as a
			// real client would. Backing off is what turns a full accept queue
			// from an error into a wait.
			var conn *websocket.Conn
			var err error
			for attempt := range 4 {
				if attempt > 0 {
					mu.Lock()
					retried++
					mu.Unlock()
					time.Sleep(time.Duration(attempt*attempt) * 200 * time.Millisecond)
				}
				if conn, _, err = dialer.Dial(url, nil); err == nil {
					f.conns[i] = conn
					return
				}
			}

			mu.Lock()
			failed++
			if failed == 1 {
				log.Printf("dial failed for user %d: %v", i+1, err)
			}
			mu.Unlock()
		}(i, token)
	}
	wg.Wait()
	if retried > 0 {
		log.Printf("%d dials had to be retried", retried)
	}
	f.connectTime = time.Since(start)

	for i, conn := range f.conns {
		if conn == nil {
			continue
		}
		f.connected++

		// The last -slow clients connect and then do nothing at all. They never
		// read, so the server's send buffer for them fills up, and the hub must
		// drop them instead of waiting.
		if i >= len(f.conns)-cfg.slow {
			f.slowConns = append(f.slowConns, conn)
			continue
		}
		go f.readLoop(conn)
	}

	log.Printf("connected %d/%d sockets in %s (%d failed)",
		f.connected, len(tokens), f.connectTime.Round(time.Millisecond), failed)
	if f.connected == 0 {
		return nil, fmt.Errorf("no socket could be opened")
	}
	return f, nil
}

// readLoop drains one socket and times every message it sees.
func (f *fleet) readLoop(conn *websocket.Conn) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			f.mu.Lock()
			f.closed++
			// A close frame means the server said goodbye on purpose. A reset
			// or an EOF means the process simply vanished. Telling the two
			// apart is the whole point of a graceful shutdown.
			if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				f.cleanClosed++
			}
			f.mu.Unlock()
			return
		}
		arrived := time.Now()

		var frame struct {
			Type string `json:"type"`
			Data struct {
				ClientMsgID string `json:"client_msg_id"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Type != "message.new" {
			continue
		}

		f.mu.Lock()
		if sent, ok := f.sentAt[frame.Data.ClientMsgID]; ok {
			f.latencies = append(f.latencies, arrived.Sub(sent))
		}
		f.received++
		f.mu.Unlock()
	}
}

// sendAll writes messages into every room at the same time, over REST, exactly
// as a real client would.
func (f *fleet) sendAll(client *http.Client, cfg config, rooms []room, tokens []string) {
	var wg sync.WaitGroup

	for _, r := range rooms {
		wg.Add(1)
		go func(r room) {
			defer wg.Done()

			for n := range cfg.messages {
				clientMsgID := fmt.Sprintf("load-%d-%d", r.id, n)
				body := map[string]string{
					"content":       pad(fmt.Sprintf("load message %d ", n), cfg.size),
					"client_msg_id": clientMsgID,
				}

				// Written down before the POST, so the measurement includes
				// the whole path: handler, insert, fan-out, socket.
				f.mu.Lock()
				f.sentAt[clientMsgID] = time.Now()
				f.mu.Unlock()

				if err := postJSON(client, fmt.Sprintf("%s/conversations/%d/messages", cfg.api, r.id), tokens[r.ownerIndex], body, nil); err != nil {
					log.Printf("send to room %d: %v", r.id, err)
				}
				time.Sleep(cfg.rate)
			}
		}(r)
	}
	wg.Wait()

	// The last messages are still in flight. Give them a moment to land before
	// counting, or the p99 is just a measure of how fast this loop exited.
	time.Sleep(2 * time.Second)
}

// checkSlow asks every never-reading client whether the server hung up on it.
//
// It has to drain first: the data the client ignored is still sitting in its
// own receive buffer, and the close frame is at the end of that queue. What
// comes out at the end is the answer — a close, or a timeout because the
// server is still politely waiting for a client that will never read.
func (f *fleet) checkSlow() {
	for _, conn := range f.slowConns {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				if !isTimeout(err) {
					f.mu.Lock()
					f.slowClosed++
					f.mu.Unlock()
				}
				break
			}
		}
	}
}

func isTimeout(err error) bool {
	type timeouter interface{ Timeout() bool }
	te, ok := err.(timeouter)
	return ok && te.Timeout()
}

func (f *fleet) report(cfg config) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sort.Slice(f.latencies, func(i, j int) bool { return f.latencies[i] < f.latencies[j] })

	fmt.Println()
	fmt.Println("---------------------------------------------")
	fmt.Printf("sockets asked for   %d\n", cfg.users)
	fmt.Printf("sockets connected   %d in %s\n", f.connected, f.connectTime.Round(time.Millisecond))
	fmt.Printf("never-reading       %d, of which the server dropped %d\n", cfg.slow, f.slowClosed)
	fmt.Printf("reading sockets     %d closed during the run, %d of them with a close frame\n", f.closed, f.cleanClosed)
	fmt.Printf("frames received     %d\n", f.received)

	if len(f.latencies) == 0 {
		fmt.Println("fan-out latency     no samples")
		fmt.Println("---------------------------------------------")
		return
	}
	fmt.Printf("fan-out samples     %d\n", len(f.latencies))
	fmt.Printf("fan-out p50         %s\n", percentile(f.latencies, 50).Round(time.Millisecond))
	fmt.Printf("fan-out p95         %s\n", percentile(f.latencies, 95).Round(time.Millisecond))
	fmt.Printf("fan-out p99         %s\n", percentile(f.latencies, 99).Round(time.Millisecond))
	fmt.Printf("fan-out max         %s\n", f.latencies[len(f.latencies)-1].Round(time.Millisecond))
	fmt.Println("---------------------------------------------")
}

func (f *fleet) close() {
	for _, conn := range f.conns {
		if conn != nil {
			conn.Close()
		}
	}
}

// pad grows a message to size bytes, so -size can decide how much traffic each
// message is worth. It never shrinks the prefix, because that prefix is what
// makes the message readable in a log.
func pad(prefix string, size int) string {
	if len(prefix) >= size {
		return prefix
	}
	return prefix + strings.Repeat("x", size-len(prefix))
}

// percentile expects a sorted slice.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := (len(sorted)*p + 99) / 100 // round up, so p99 of 100 samples is the 99th
	return sorted[min(i, len(sorted))-1]
}

// ---------------------------------------------------------------------------
// Small HTTP helpers
// ---------------------------------------------------------------------------

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
		return "", fmt.Errorf("bad -api URL: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("bad -api scheme %q, want http or https", u.Scheme)
	}
	return u.String(), nil
}

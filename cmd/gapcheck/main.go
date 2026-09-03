// Command gapcheck is the "break it" half of Stage 4.
//
// It proves that a client which loses its socket does not lose messages.
//
// The run has four phases:
//
//  1. User B is connected. User A sends. B sees the messages live.
//  2. B's socket is killed. A keeps sending. Nobody pushes anything to B.
//  3. B reconnects and repairs the hole with ?after_seq=.
//  4. A sends more. B sees those live again.
//
// At the end it counts. The number that matters is `missing`, and it has to be
// zero. Before Stage 4 phase 2 was simply lost: the messages were in Postgres,
// but B had no way to know they existed, because ids are global and a jump in
// them proves nothing about one room.
//
// Start the cluster and seed two users first:
//
//	docker compose up --build -d
//	make seed ARGS="-n 2"
//	make gapcheck
//
// The harder variant, which is the one worth running twice:
//
//	make gapcheck ARGS="-pause 20s"
//
// then `docker compose kill redis` during the pause. Live push dies completely
// — nothing is delivered to any socket — and the gap read still has to bring
// back every message. That is the difference between "delivery works" and
// "delivery is guaranteed".
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type config struct {
	node     string
	userA    string
	userB    string
	password string

	before int
	during int
	after  int

	wait  time.Duration
	pause time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.node, "node", "http://localhost:8081", "base URL of the API node to use")
	flag.StringVar(&cfg.userA, "user-a", "testuser001", "seeded user who sends")
	flag.StringVar(&cfg.userB, "user-b", "testuser002", "seeded user who receives, and whose socket is broken")
	flag.StringVar(&cfg.password, "password", "password123", "password shared by the seeded users")

	flag.IntVar(&cfg.before, "before", 3, "messages sent while B is connected")
	flag.IntVar(&cfg.during, "during", 5, "messages sent while B is disconnected — the gap")
	flag.IntVar(&cfg.after, "after", 3, "messages sent while B is reconnecting")

	flag.DurationVar(&cfg.wait, "wait", 3*time.Second, "how long to wait for a live frame")
	flag.DurationVar(&cfg.pause, "pause", 0,
		"hold before the gap phase — long enough to `docker compose kill redis`")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	client := &http.Client{Timeout: 15 * time.Second}

	a, err := signIn(client, cfg.node, cfg.userA, cfg.password)
	if err != nil {
		return fmt.Errorf("user A: %w", err)
	}
	b, err := signIn(client, cfg.node, cfg.userB, cfg.password)
	if err != nil {
		return fmt.Errorf("user B: %w", err)
	}

	roomID, err := createRoom(client, cfg.node, a.token, b.id)
	if err != nil {
		return fmt.Errorf("create room: %w", err)
	}
	fmt.Printf("node   %s\n", cfg.node)
	fmt.Printf("users  A %s (id %d) sends, B %s (id %d) receives\n", cfg.userA, a.id, cfg.userB, b.id)
	fmt.Printf("room   %d\n\n", roomID)

	// Every seq the server ever handed out in this room, and how B came to know
	// about each one. This is the whole result of the run.
	var sent []uint
	seen := map[uint]string{} // seq -> "live" or "gap"
	duplicates := 0

	// note records one arrival. The second copy of a seq is a duplicate, not a
	// second message: delivery is at-least-once, so this is expected and
	// harmless — as long as the client drops it instead of showing it twice.
	note := func(seq uint, how string) {
		if _, already := seen[seq]; already {
			duplicates++
			return
		}
		seen[seq] = how
	}

	// -----------------------------------------------------------------------
	// Phase 1 — connected. This is the happy path, and it has to work first,
	// or nothing later in the run means anything.
	// -----------------------------------------------------------------------
	fmt.Printf("1. B is connected, A sends %d\n", cfg.before)

	socket, err := connect(cfg.node, b.token)
	if err != nil {
		return fmt.Errorf("first socket: %w", err)
	}

	for i := range cfg.before {
		seq, err := send(client, cfg.node, a.token, roomID, fmt.Sprintf("before %d", i+1))
		if err != nil {
			socket.close()
			return fmt.Errorf("send: %w", err)
		}
		sent = append(sent, seq)
	}
	for _, seq := range socket.collect(cfg.before, cfg.wait) {
		note(seq, "live")
	}
	fmt.Printf("   seq %v, B saw %d live\n\n", sent, len(seen))

	if cfg.pause > 0 {
		fmt.Printf("   pausing %s — now is the time to `docker compose kill redis`\n\n", cfg.pause)
		time.Sleep(cfg.pause)
	}

	// -----------------------------------------------------------------------
	// Phase 2 — the break. B's socket dies and A keeps sending. Nothing is
	// pushed to B, and nothing tries to be: the fan-out looks up the room's
	// members and finds no socket for B on any node.
	//
	// The messages are stored, and A's sends all answer 201. Nothing anywhere
	// reports a problem. That is what makes this failure worth a whole stage —
	// it is completely silent.
	// -----------------------------------------------------------------------
	socket.close()
	fmt.Printf("2. B's socket is dead, A sends %d into the dark\n", cfg.during)

	gapStart := highest(sent) // the last seq B knows about
	for i := range cfg.during {
		seq, err := send(client, cfg.node, a.token, roomID, fmt.Sprintf("during %d", i+1))
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}
		sent = append(sent, seq)
	}
	fmt.Printf("   seq %v, B saw none of them\n", sent[cfg.before:])
	fmt.Printf("   B's last known seq is %d\n\n", gapStart)

	// -----------------------------------------------------------------------
	// Phase 3 — the repair, in the order that actually works:
	//
	//   1. open the socket FIRST and let frames pile up
	//   2. only then ask for the gap
	//   3. merge, dropping any seq already held
	//
	// The order is not a detail. Fetching the gap before connecting leaves a
	// second, smaller hole between the two calls — and that one is even harder
	// to see, because it only opens when the room is busy at the wrong moment.
	//
	// So this phase sends more messages WHILE the gap read is in flight, on
	// purpose. If the order were wrong, those are the ones that would vanish.
	// -----------------------------------------------------------------------
	fmt.Printf("3. B reconnects — socket first, then ?after_seq=%d\n", gapStart)

	socket, err = connect(cfg.node, b.token)
	if err != nil {
		return fmt.Errorf("second socket: %w", err)
	}
	defer socket.close()

	var (
		wg       sync.WaitGroup
		sendErr  error
		sentLate []uint
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range cfg.after {
			seq, err := send(client, cfg.node, a.token, roomID, fmt.Sprintf("after %d", i+1))
			if err != nil {
				sendErr = err
				return
			}
			sentLate = append(sentLate, seq)
		}
	}()

	gap, err := fetchGap(client, cfg.node, b.token, roomID, gapStart)
	if err != nil {
		return fmt.Errorf("gap read: %w", err)
	}
	for _, seq := range gap {
		note(seq, "gap")
	}
	fmt.Printf("   the gap read returned seq %v\n", gap)

	wg.Wait()
	if sendErr != nil {
		return fmt.Errorf("send during reconnect: %w", sendErr)
	}
	sent = append(sent, sentLate...)

	// -----------------------------------------------------------------------
	// Phase 4 — live again. Whatever the socket collected, including anything
	// that arrived while the gap read was still running.
	// -----------------------------------------------------------------------
	fmt.Printf("4. A sent %d more while B was catching up: seq %v\n", cfg.after, sentLate)

	for _, seq := range socket.collect(cfg.after, cfg.wait) {
		note(seq, "live")
	}
	// One last gap read closes anything the socket still missed. A real client
	// does this too — it is cheap, and it is the difference between "probably
	// fine" and "known to be complete".
	if late := fillFromGap(client, cfg, b.token, roomID, sent, seen, note); late > 0 {
		fmt.Printf("   a second gap read picked up %d more\n", late)
	}
	fmt.Println()

	report(sent, seen, duplicates)
	return nil
}

// fillFromGap asks once more for anything still missing, starting from the
// oldest hole. A frame can arrive after collect() gave up waiting — that is not
// a lost message, only a slow one, and the point of the tool is to tell those
// two apart.
func fillFromGap(client *http.Client, cfg config, token string, roomID uint, sent []uint,
	seen map[uint]string, note func(uint, string)) int {

	var lowestMissing uint
	for _, seq := range sent {
		if _, ok := seen[seq]; !ok && (lowestMissing == 0 || seq < lowestMissing) {
			lowestMissing = seq
		}
	}
	if lowestMissing == 0 {
		return 0
	}

	gap, err := fetchGap(client, cfg.node, token, roomID, lowestMissing-1)
	if err != nil {
		return 0
	}

	before := len(seen)
	for _, seq := range gap {
		note(seq, "gap")
	}
	return len(seen) - before
}

func report(sent []uint, seen map[uint]string, duplicates int) {
	live, gap := 0, 0
	var missing []uint
	for _, seq := range sent {
		switch seen[seq] {
		case "live":
			live++
		case "gap":
			gap++
		default:
			missing = append(missing, seq)
		}
	}

	fmt.Println("---------------------------------------------")
	fmt.Printf("  sent          %3d\n", len(sent))
	fmt.Printf("  seen live     %3d\n", live)
	fmt.Printf("  recovered     %3d   (through ?after_seq=)\n", gap)
	fmt.Printf("  duplicates    %3d   (arrived twice, dropped by seq)\n", duplicates)
	fmt.Printf("  MISSING       %3d\n", len(missing))
	fmt.Println("---------------------------------------------")

	if len(missing) == 0 {
		fmt.Println("NOTHING WAS LOST.")
		fmt.Println("B's socket died in the middle and B still ended up holding")
		fmt.Println("every message, in order. Live push stayed best effort; the")
		fmt.Println("sequence number is what made the hole visible and fixable.")
		return
	}

	fmt.Printf("MESSAGES LOST: seq %v\n", missing)
	fmt.Println("These rows are in Postgres. B was never pushed them and could")
	fmt.Println("not recover them either, so no client will ever show them.")

	// Always exit 0. On a build without Stage 4 this failure is the expected
	// result and the point of the run, and a non-zero exit would make
	// `make gapcheck` look broken when it is doing exactly its job.
}

func highest(seqs []uint) uint {
	var top uint
	for _, s := range seqs {
		if s > top {
			top = s
		}
	}
	return top
}

// ---------------------------------------------------------------------------
// The socket
// ---------------------------------------------------------------------------

// socket is one open WebSocket with a background reader.
//
// Reading happens on its own goroutine and not on demand, because the server
// also sends pings and a client that only reads when it expects a message
// answers none of them — it would be dropped as a dead reader mid-run.
type socket struct {
	conn *websocket.Conn
	seqs chan uint

	closeOnce sync.Once
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

	// The server's first frame is {"type":"connected"}. Reading it here, before
	// the reader goroutine starts, means the run never sends a message to a hub
	// that has not registered this socket yet — a race that would look exactly
	// like the bug being hunted.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("no hello frame from %s: %w", node, err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	// The buffer is what makes step 1 of the reconnect order real: frames that
	// arrive while the gap read is still in flight wait here instead of being
	// thrown away.
	s := &socket{conn: conn, seqs: make(chan uint, 256)}
	go s.read()

	return s, nil
}

func (s *socket) read() {
	for {
		_, raw, err := s.conn.ReadMessage()
		if err != nil {
			close(s.seqs)
			return
		}

		var frame struct {
			Type string `json:"type"`
			Data struct {
				Seq uint `json:"seq"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Type != "message.new" {
			continue
		}
		select {
		case s.seqs <- frame.Data.Seq:
		default: // a full buffer is a dropped frame, which the gap read repairs
		}
	}
}

// collect takes up to want seq numbers, giving up after the deadline. Giving up
// is a normal outcome here, not a failure: half this tool's job is watching
// frames NOT arrive.
func (s *socket) collect(want int, wait time.Duration) []uint {
	var got []uint
	deadline := time.After(wait)

	for len(got) < want {
		select {
		case seq, open := <-s.seqs:
			if !open {
				return got
			}
			got = append(got, seq)
		case <-deadline:
			return got
		}
	}
	// One short look for a straggler, so a duplicate is counted rather than
	// left sitting in the buffer looking like a message that never came.
	select {
	case seq, open := <-s.seqs:
		if open {
			got = append(got, seq)
		}
	case <-time.After(200 * time.Millisecond):
	}
	return got
}

func (s *socket) close() { s.closeOnce.Do(func() { s.conn.Close() }) }

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

	var profile struct {
		ID uint `json:"id"`
	}
	if err := getJSON(client, node+"/auth/profile", login.Token, &profile); err != nil {
		return user{}, err
	}
	return user{token: login.Token, id: profile.ID}, nil
}

func createRoom(client *http.Client, node, token string, memberID uint) (uint, error) {
	var out struct {
		ID uint `json:"id"`
	}
	body := map[string]any{
		"title":      fmt.Sprintf("gapcheck %s", time.Now().Format(time.TimeOnly)),
		"member_ids": []uint{memberID},
	}
	if err := postJSON(client, node+"/conversations", token, body, &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// send posts one message and returns the seq the server gave it.
//
// It honours a 429 rather than failing, because a run with a big -during would
// otherwise die on the rate limiter instead of testing anything.
func send(client *http.Client, node, token string, roomID uint, text string) (uint, error) {
	body := map[string]string{
		"content":       text,
		"client_msg_id": fmt.Sprintf("gapcheck-%d", time.Now().UnixNano()),
	}
	target := fmt.Sprintf("%s/conversations/%d/messages", node, roomID)

	var out struct {
		Seq uint `json:"seq"`
	}
	for attempt := range 5 {
		err := postJSON(client, target, token, body, &out)

		var limited rateLimited
		if !asRateLimited(err, &limited) {
			if err != nil {
				return 0, err
			}
			if out.Seq == 0 {
				return 0, fmt.Errorf("the server returned no seq — is this build older than Stage 4?")
			}
			return out.Seq, nil
		}
		if attempt == 4 {
			return 0, err
		}
		time.Sleep(limited.retryAfter)
	}
	return 0, fmt.Errorf("unreachable")
}

// fetchGap reads ?after_seq= and follows next_after_seq until the room runs
// out. One page is not enough: a client away for an hour misses more than the
// server will hand over at once.
func fetchGap(client *http.Client, node, token string, roomID, afterSeq uint) ([]uint, error) {
	var (
		out    []uint
		cursor = afterSeq
	)

	for range 100 { // a stop, so a server bug cannot spin this forever
		target := fmt.Sprintf("%s/conversations/%d/messages?after_seq=%d&limit=100", node, roomID, cursor)

		var page struct {
			Messages []struct {
				Seq uint `json:"seq"`
			} `json:"messages"`
			NextAfterSeq *uint `json:"next_after_seq"`
		}
		if err := getJSON(client, target, token, &page); err != nil {
			return nil, err
		}
		for _, m := range page.Messages {
			out = append(out, m.Seq)
		}
		if page.NextAfterSeq == nil {
			return out, nil
		}
		cursor = *page.NextAfterSeq
	}
	return out, fmt.Errorf("the gap never ended after 100 pages")
}

// rateLimited is a 429 with the wait the server asked for.
type rateLimited struct {
	retryAfter time.Duration
}

func (rateLimited) Error() string { return "rate limited" }

func asRateLimited(err error, into *rateLimited) bool {
	got, ok := err.(rateLimited)
	if ok {
		*into = got
	}
	return ok
}

func getJSON(client *http.Client, target, token string, into any) error {
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return do(client, req, into)
}

func postJSON(client *http.Client, target, token string, body, into any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return do(client, req, into)
}

func do(client *http.Client, req *http.Request, into any) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		wait := time.Second
		if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
			wait = time.Duration(secs) * time.Second
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return rateLimited{retryAfter: wait}
	}
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

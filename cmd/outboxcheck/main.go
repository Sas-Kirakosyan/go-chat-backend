// Command outboxcheck is the "break it" half of Stage 5.
//
// It proves that killing the broker delays messages and does not lose them.
//
// Before the outbox, the send path did two writes: commit the message to
// Postgres, then publish. A process that died in between left a message that
// existed and that nobody was ever told about. Now there is one write, and a
// separate relay does the publishing, so the broker can be gone for a minute
// and every message still arrives.
//
// The run has four phases:
//
//  1. B is connected. A sends. B sees the messages live.
//  2. NATS is killed. A keeps sending. Every send still answers 201 — that is
//     the point — and nothing is delivered. The outbox fills up.
//  3. NATS comes back. The relay drains, and everything from phase 2 arrives.
//  4. A sends more, and delivery is live again.
//
// At the end it counts three things:
//
//   - missing must be 0. Nothing may be lost.
//   - refused must be 0. No send may fail while the broker is down.
//   - C's unread count must be exactly the number of messages sent. C never
//     connects, so their badge comes only from the broker consumer, and a
//     number that is too high means the consumer is not idempotent.
//
// Start the cluster and seed three users first:
//
//	docker compose up --build -d
//	make seed ARGS="-n 3"
//	make outboxcheck
//
// The variant worth running, and the reason this tool exists:
//
//	make outboxcheck ARGS="-pause 25s"
//
// then, during the pause:
//
//	docker compose kill nats
//	docker compose start nats     # after a few seconds
//
// Watch outbox_pending on /metrics climb while it is down and fall when it
// comes back. Nothing should be missing at the end.
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
	userC    string
	password string

	before int
	during int
	after  int

	wait    time.Duration
	pause   time.Duration
	recover time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.node, "node", "http://localhost:8081", "base URL of the API node to use")
	flag.StringVar(&cfg.userA, "user-a", "testuser001", "seeded user who sends")
	flag.StringVar(&cfg.userB, "user-b", "testuser002", "seeded user with a socket open")
	flag.StringVar(&cfg.userC, "user-c", "testuser003", "seeded user who never connects, so only the consumer counts for them")
	flag.StringVar(&cfg.password, "password", "password123", "password shared by the seeded users")

	flag.IntVar(&cfg.before, "before", 3, "messages sent while the broker is up")
	flag.IntVar(&cfg.during, "during", 5, "messages sent while the broker is expected to be down")
	flag.IntVar(&cfg.after, "after", 3, "messages sent once the broker is back")

	flag.DurationVar(&cfg.wait, "wait", 5*time.Second, "how long to wait for a live frame")
	flag.DurationVar(&cfg.pause, "pause", 0,
		"hold before the outage phase — long enough to `docker compose kill nats`")
	flag.DurationVar(&cfg.recover, "recover", 30*time.Second,
		"how long to wait for the relay to drain the backlog after the broker returns")
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
	c, err := signIn(client, cfg.node, cfg.userC, cfg.password)
	if err != nil {
		return fmt.Errorf("user C: %w", err)
	}

	roomID, err := createRoom(client, cfg.node, a.token, b.id, c.id)
	if err != nil {
		return fmt.Errorf("create room: %w", err)
	}

	fmt.Printf("node   %s\n", cfg.node)
	fmt.Printf("users  A %s sends, B %s watches a socket, C %s never connects\n", cfg.userA, cfg.userB, cfg.userC)
	fmt.Printf("room   %d\n", roomID)
	// Said once, because the outbox lines below are easy to misread. The relay
	// refreshes those gauges every five seconds so that a Prometheus scrape
	// does not turn into a database query, which means a phase that takes less
	// than that prints the same numbers as the one before it.
	fmt.Printf("note   the outbox numbers below refresh every 5s, so a fast phase can repeat them\n\n")

	// Every seq the server handed out, and how B came to know about it.
	var sent []uint
	seen := map[uint]string{} // seq -> the phase it arrived in
	duplicates := 0
	refused := 0

	note := func(seq uint, how string) {
		if _, already := seen[seq]; already {
			// At-least-once delivery, so this is expected and harmless as long
			// as the client drops it instead of showing the line twice.
			duplicates++
			return
		}
		seen[seq] = how
	}

	sock, err := connect(cfg.node, b.token)
	if err != nil {
		return fmt.Errorf("connect B: %w", err)
	}
	defer sock.close()

	// -----------------------------------------------------------------------
	// Phase 1 — everything up. This is also the latency measurement: the relay
	// polls, so live delivery now costs one poll interval more than it did.
	// -----------------------------------------------------------------------

	fmt.Printf("phase 1  %d messages with the broker up\n", cfg.before)

	var slowest time.Duration
	for i := range cfg.before {
		start := time.Now()
		seq, err := send(client, cfg.node, a.token, roomID, fmt.Sprintf("before-%d", i+1))
		if err != nil {
			return fmt.Errorf("send before-%d: %w", i+1, err)
		}
		sent = append(sent, seq)

		if got := sock.collect(1, cfg.wait); len(got) == 1 {
			if took := time.Since(start); took > slowest {
				slowest = took
			}
			note(got[0], "live")
		}
	}
	fmt.Printf("         %d of %d arrived live, slowest %v\n", countIn(seen, "live"), cfg.before, slowest.Round(time.Millisecond))
	fmt.Printf("         %s\n\n", outboxLine(client, cfg.node))

	// -----------------------------------------------------------------------
	// Phase 2 — the outage. Sends must keep working.
	// -----------------------------------------------------------------------

	if cfg.pause > 0 {
		fmt.Printf("paused %v — run `docker compose kill nats` now\n", cfg.pause)
		time.Sleep(cfg.pause)
		fmt.Println()
	}

	fmt.Printf("phase 2  %d messages, broker expected down\n", cfg.during)

	var gapSeqs []uint
	for i := range cfg.during {
		seq, err := send(client, cfg.node, a.token, roomID, fmt.Sprintf("during-%d", i+1))
		if err != nil {
			// This is the failure the whole stage exists to prevent. A send
			// must not depend on the broker: the message goes to Postgres and
			// the outbox row goes with it.
			fmt.Printf("         REFUSED during-%d: %v\n", i+1, err)
			refused++
			continue
		}
		sent = append(sent, seq)
		gapSeqs = append(gapSeqs, seq)
	}
	fmt.Printf("         %d accepted, %d refused\n", len(gapSeqs), refused)
	fmt.Printf("         %s\n\n", outboxLine(client, cfg.node))

	// Anything that arrives here arrived because the broker was actually up,
	// which is fine — the tool is honest about that in its summary.
	for _, seq := range sock.collect(len(gapSeqs), 2*time.Second) {
		note(seq, "live")
	}

	// -----------------------------------------------------------------------
	// Phase 3 — recovery. The relay drains what piled up.
	// -----------------------------------------------------------------------

	fmt.Printf("phase 3  waiting up to %v for the backlog to drain\n", cfg.recover)
	if cfg.pause > 0 {
		fmt.Println("         start the broker now: `docker compose start nats`")
	}

	deadline := time.Now().Add(cfg.recover)
	for time.Now().Before(deadline) && len(seen) < len(sent) {
		for _, seq := range sock.collect(len(sent)-len(seen), time.Second) {
			note(seq, "recovered")
		}
	}
	fmt.Printf("         %d recovered after the broker returned\n", countIn(seen, "recovered"))
	fmt.Printf("         %s\n\n", outboxLine(client, cfg.node))

	// -----------------------------------------------------------------------
	// Phase 4 — live again, with no restart and no repair step.
	// -----------------------------------------------------------------------

	fmt.Printf("phase 4  %d messages after the broker is back\n", cfg.after)
	for i := range cfg.after {
		seq, err := send(client, cfg.node, a.token, roomID, fmt.Sprintf("after-%d", i+1))
		if err != nil {
			return fmt.Errorf("send after-%d: %w", i+1, err)
		}
		sent = append(sent, seq)

		if got := sock.collect(1, cfg.wait); len(got) == 1 {
			note(got[0], "live-again")
		}
	}
	fmt.Printf("         %d arrived live again\n\n", countIn(seen, "live-again"))

	// -----------------------------------------------------------------------
	// The count
	// -----------------------------------------------------------------------

	// Anything still missing gets one last chance through the Stage 4 gap
	// read, which tells a real failure ("the message is not in Postgres") apart
	// from a delivery one ("it is there, the push never came").
	var missing []uint
	for _, seq := range sent {
		if _, ok := seen[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	inHistory, err := historySeqs(client, cfg.node, b.token, roomID)
	if err != nil {
		return fmt.Errorf("read history: %w", err)
	}

	unread, err := unreadCount(client, cfg.node, c.token, roomID)
	if err != nil {
		return fmt.Errorf("read C's unread count: %w", err)
	}

	fmt.Println("result")
	fmt.Printf("  sent           %d\n", len(sent))
	fmt.Printf("  refused        %d   (must be 0: a send must not need the broker)\n", refused)
	fmt.Printf("  delivered      %d   live %d, recovered %d, live again %d\n",
		len(seen), countIn(seen, "live"), countIn(seen, "recovered"), countIn(seen, "live-again"))
	fmt.Printf("  duplicates     %d   (harmless: delivery is at-least-once)\n", duplicates)
	fmt.Printf("  missing        %d   %v\n", len(missing), sortedSeqs(missing))
	fmt.Printf("  in history     %d of %d\n", countKnown(inHistory, sent), len(sent))
	fmt.Printf("  C unread       %d   (want %d: C never connected, so this is the consumer's work)\n",
		unread, len(sent))
	fmt.Println()

	var problems []string
	if refused > 0 {
		problems = append(problems, fmt.Sprintf("%d sends were refused while the broker was down; the write path still depends on it", refused))
	}
	if len(missing) > 0 {
		problems = append(problems, fmt.Sprintf("%d messages never reached the socket: %v", len(missing), missing))
	}
	if got := countKnown(inHistory, sent); got != len(sent) {
		problems = append(problems, fmt.Sprintf("only %d of %d messages are in Postgres, so some were lost outright", got, len(sent)))
	}
	if unread != len(sent) {
		problems = append(problems, fmt.Sprintf("C's unread count is %d, want %d: the consumer either missed work or did it twice", unread, len(sent)))
	}

	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Printf("FAIL  %s\n", p)
		}
		return fmt.Errorf("%d checks failed", len(problems))
	}

	fmt.Println("OK  nothing was lost, no send was refused, and every message was counted exactly once")
	return nil
}

func countIn(seen map[uint]string, how string) int {
	var n int
	for _, got := range seen {
		if got == how {
			n++
		}
	}
	return n
}

func countKnown(have map[uint]bool, want []uint) int {
	var n int
	for _, seq := range want {
		if have[seq] {
			n++
		}
	}
	return n
}

// outboxLine reads outbox_pending and outbox_lag_seconds off /metrics, so each
// phase can print what the queue looked like at the time. This is the number
// that makes the outage visible: it climbs while the broker is down and falls
// when the relay catches up.
func outboxLine(client *http.Client, node string) string {
	resp, err := client.Get(node + "/metrics")
	if err != nil {
		return "outbox: /metrics unreachable"
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "outbox: could not read /metrics"
	}

	pending, okP := metricValue(string(body), "chat_outbox_pending")
	lag, okL := metricValue(string(body), "chat_outbox_lag_seconds")
	leader, okLd := metricValue(string(body), "chat_outbox_relay_leader")
	if !okP || !okL {
		return "outbox: metrics not found (is this build Stage 5?)"
	}

	line := fmt.Sprintf("outbox: %.0f pending, oldest %.1fs old", pending, lag)
	if okLd {
		line += fmt.Sprintf(", this node is leader: %.0f", leader)
	}
	return line
}

// metricValue pulls one unlabelled gauge out of a Prometheus text page. It is
// deliberately simple: a real parser would be a dependency for three numbers
// printed to a terminal.
func metricValue(page, name string) (float64, bool) {
	for line := range strings.SplitSeq(page, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, name+" ") {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name)), 64)
		if err != nil {
			return 0, false
		}
		return value, true
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// The socket
// ---------------------------------------------------------------------------

type socket struct {
	conn      *websocket.Conn
	seqs      chan uint
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

	// A big buffer, because phase 3 can deliver a whole outage's worth of
	// messages at once and a dropped frame here would read as a lost message.
	s := &socket{conn: conn, seqs: make(chan uint, 1024)}
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
		default:
		}
	}
}

// collect takes up to want seq numbers, giving up after the deadline. Giving
// up is a normal outcome here, not a failure: half this tool's job is watching
// frames NOT arrive.
func (s *socket) collect(want int, wait time.Duration) []uint {
	var got []uint
	if want <= 0 {
		return got
	}
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
	// left in the buffer looking like a message that never came.
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
		return user{}, fmt.Errorf("login %s: %w (did you run `make seed ARGS=\"-n 3\"`?)", username, err)
	}

	var profile struct {
		ID uint `json:"id"`
	}
	if err := getJSON(client, node+"/auth/profile", login.Token, &profile); err != nil {
		return user{}, err
	}
	return user{token: login.Token, id: profile.ID}, nil
}

func createRoom(client *http.Client, node, token string, memberIDs ...uint) (uint, error) {
	var out struct {
		ID uint `json:"id"`
	}
	body := map[string]any{
		"title":      fmt.Sprintf("outboxcheck %s", time.Now().Format(time.TimeOnly)),
		"member_ids": memberIDs,
	}
	if err := postJSON(client, node+"/conversations", token, body, &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// send posts one message and returns the seq the server gave it.
//
// It honours a 429 rather than failing, so a run with a big -during dies on
// something real instead of on the rate limiter.
func send(client *http.Client, node, token string, roomID uint, text string) (uint, error) {
	body := map[string]string{
		"content":       text,
		"client_msg_id": fmt.Sprintf("outboxcheck-%d", time.Now().UnixNano()),
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

// historySeqs reads the whole room out of Postgres, so a missing live push can
// be told apart from a message that was never stored.
func historySeqs(client *http.Client, node, token string, roomID uint) (map[uint]bool, error) {
	out := map[uint]bool{}
	cursor := uint(0)

	for range 100 {
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
			out[m.Seq] = true
		}
		if page.NextAfterSeq == nil {
			return out, nil
		}
		cursor = *page.NextAfterSeq
	}
	return out, fmt.Errorf("history never ended after 100 pages")
}

// unreadCount reads one room's badge off GET /conversations.
//
// This is the consumer's work, and C is the honest witness for it: C never
// opens a socket, so nothing but the broker consumer can have moved this
// number. Too low means work was lost; too high means the same message was
// counted twice and the idempotency guard is not working.
func unreadCount(client *http.Client, node, token string, roomID uint) (int, error) {
	var body struct {
		Conversations []struct {
			ID          uint `json:"id"`
			UnreadCount int  `json:"unread_count"`
		} `json:"conversations"`
	}
	if err := getJSON(client, node+"/conversations", token, &body); err != nil {
		return 0, err
	}

	for _, conv := range body.Conversations {
		if conv.ID == roomID {
			return conv.UnreadCount, nil
		}
	}
	return 0, fmt.Errorf("room %d is not in the conversation list", roomID)
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

// sortedSeqs is used only when printing, so a failure lists the missing
// numbers in an order a person can scan.
func sortedSeqs(in []uint) []uint {
	out := append([]uint(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

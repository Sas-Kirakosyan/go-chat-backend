// Command tracecheck is the "break it" half of Stage 7.
//
// Every earlier stage's claim could be printed as a table: a message arrived or
// it did not, a send answered 201 or it did not. This stage's claim cannot.
// "One trace id follows one message across four processes" is either true in
// Jaeger or it is a sentence in a README, so this tool goes and asks Jaeger.
//
// What it does:
//
//  1. Puts two members of one room on two DIFFERENT nodes, like splitcheck.
//  2. Invents a trace id of its own and sends it on the POST as a traceparent
//     header. That makes this tool the root of the trace, so it knows the id
//     without having to parse anything out of a response.
//  3. Waits for the message to arrive on the other node's socket.
//  4. Asks Jaeger for that trace and checks what is in it: the HTTP request,
//     the relay's publish, the fan-out on the other node — and that they really
//     did run in more than one process.
//  5. Prints the spans as a waterfall, so the gap between the commit and the
//     publish is a NUMBER. That gap is the relay's poll interval, which Stage 5
//     estimated at ~100 ms and never measured.
//
// Start everything first — Jaeger is in the observability profile:
//
//	docker compose --profile observability up --build -d
//	make seed ARGS="-n 3"
//	make tracecheck
//
// The interesting run: pause, and kill Jaeger during the pause.
//
//	make tracecheck ARGS="-pause 20s"
//	docker compose kill jaeger      # during the pause
//
// Sends must keep answering in the same ~10 ms with the collector gone.
// Telemetry going down is allowed to cost telemetry and nothing else.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type config struct {
	nodeA    string
	nodeB    string
	jaeger   string
	userA    string
	userB    string
	password string

	wait    time.Duration
	collect time.Duration
	pause   time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.nodeA, "node-a", "http://localhost:8081", "base URL of the node that sends")
	flag.StringVar(&cfg.nodeB, "node-b", "http://localhost:8082", "base URL of the node that receives")
	flag.StringVar(&cfg.jaeger, "jaeger", "http://localhost:16686", "base URL of the Jaeger query API")
	flag.StringVar(&cfg.userA, "user-a", "testuser001", "seeded user who sends")
	flag.StringVar(&cfg.userB, "user-b", "testuser002", "seeded user who receives")
	flag.StringVar(&cfg.password, "password", "password123", "password shared by the seeded users")
	flag.DurationVar(&cfg.wait, "wait", 10*time.Second, "how long to wait for the message to arrive")
	flag.DurationVar(&cfg.collect, "collect", 30*time.Second,
		"how long to wait for the trace to reach Jaeger (spans are batched, so it is never instant)")
	flag.DurationVar(&cfg.pause, "pause", 0,
		"pause before collecting — kill jaeger during it and watch sends carry on regardless")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	client := &http.Client{Timeout: 15 * time.Second}

	sender, err := signIn(client, cfg.nodeA, cfg.userA, cfg.password)
	if err != nil {
		return err
	}
	receiver, err := signIn(client, cfg.nodeB, cfg.userB, cfg.password)
	if err != nil {
		return err
	}

	roomID, err := createRoom(client, cfg.nodeA, sender.token, receiver.id)
	if err != nil {
		return fmt.Errorf("create room: %w", err)
	}

	// The socket lives on the OTHER node. That is what makes the trace worth
	// looking at: the fan-out span has to appear in a different process from
	// the one that answered the POST, or the picture proves nothing that a
	// single-node log could not.
	sock, err := connect(cfg.nodeB, receiver.token)
	if err != nil {
		return fmt.Errorf("connect the receiver: %w", err)
	}
	defer sock.close()

	traceID, traceparent := newTraceparent()
	clientMsgID := "tracecheck-" + traceID[:8]

	fmt.Printf("room %d, sending on %s, listening on %s\n", roomID, cfg.nodeA, cfg.nodeB)
	fmt.Printf("trace  %s\n\n", traceID)

	began := time.Now()
	if err := sendMessage(client, cfg.nodeA, sender.token, roomID, clientMsgID, traceparent); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	sendTook := time.Since(began)

	arrived := sock.waitFor(clientMsgID, time.Now().Add(cfg.wait))
	deliveredIn := time.Since(began)

	fmt.Printf("send answered in %-8s (this is the user's wait; everything after it is not)\n", round(sendTook))
	if arrived {
		fmt.Printf("delivered in     %-8s to a socket on the other node\n\n", round(deliveredIn))
	} else {
		fmt.Printf("delivered        NEVER — nothing arrived within %s\n\n", cfg.wait)
	}

	if cfg.pause > 0 {
		fmt.Printf("pausing %s — now is the moment to `docker compose kill jaeger`\n", cfg.pause)
		time.Sleep(cfg.pause)
		fmt.Println()
	}

	trace, err := fetchTrace(client, cfg.jaeger, traceID, cfg.collect)
	if err != nil {
		return err
	}

	report(trace)
	return verdict(trace, arrived)
}

// ---------------------------------------------------------------------------
// The trace id
// ---------------------------------------------------------------------------

// newTraceparent invents a W3C traceparent and returns the trace id inside it.
//
// Making it here rather than reading it back off the response is what keeps the
// tool honest and simple. The API would have to be asked to hand its trace id
// back in a header, which is a feature that exists only for this tool; instead
// the caller starts the trace, exactly as a browser with a tracing SDK would.
//
// The flags byte is 01: sampled. That is the bit that tells every process
// downstream to record, and it is why ParentBased sampling matters — one
// decision, obeyed by four processes.
func newTraceparent() (traceID, header string) {
	var t [16]byte
	var s [8]byte
	_, _ = rand.Read(t[:])
	_, _ = rand.Read(s[:])

	traceID = hex.EncodeToString(t[:])
	return traceID, "00-" + traceID + "-" + hex.EncodeToString(s[:]) + "-01"
}

// ---------------------------------------------------------------------------
// Asking Jaeger
// ---------------------------------------------------------------------------

// jaegerTrace is the part of Jaeger's answer this tool reads.
//
// Times are microseconds, which is Jaeger's own unit and not a choice made
// here.
type jaegerTrace struct {
	TraceID string `json:"traceID"`
	Spans   []struct {
		SpanID        string `json:"spanID"`
		OperationName string `json:"operationName"`
		StartTime     int64  `json:"startTime"`
		Duration      int64  `json:"duration"`
		ProcessID     string `json:"processID"`
		References    []struct {
			RefType string `json:"refType"`
			SpanID  string `json:"spanID"`
		} `json:"references"`
	} `json:"spans"`
	Processes map[string]struct {
		ServiceName string `json:"serviceName"`
		Tags        []struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		} `json:"tags"`
	} `json:"processes"`
}

// fetchTrace polls until the trace shows up, or gives up.
//
// Polling and not one request, because a trace is never there immediately and
// that is not a bug: spans are batched in each process before export, so the
// last one arrives a couple of seconds after the message did. A tool that asked
// once would report "tracing is broken" on a system that was working.
func fetchTrace(client *http.Client, jaeger, traceID string, within time.Duration) (*jaegerTrace, error) {
	deadline := time.Now().Add(within)
	endpoint := strings.TrimSuffix(jaeger, "/") + "/api/traces/" + traceID

	var lastErr error
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		trace, err := getTrace(client, endpoint)
		if err == nil && trace != nil {
			fmt.Printf("trace found in Jaeger after %d %s\n\n", attempt, plural(attempt, "ask", "asks"))
			return trace, nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}

	if lastErr != nil {
		return nil, fmt.Errorf("could not read the trace from Jaeger within %s: %w\n"+
			"is it running? `docker compose --profile observability up -d jaeger`", within, lastErr)
	}
	return nil, fmt.Errorf("Jaeger never saw trace %s within %s.\n"+
		"Check OTEL_EXPORTER_OTLP_ENDPOINT is set on the API nodes", traceID, within)
}

func getTrace(client *http.Client, endpoint string) (*jaegerTrace, error) {
	resp, err := client.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 404 while the spans are still in a batch queue somewhere. Not an error,
	// just "not yet".
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(body))
	}

	var out struct {
		Data []jaegerTrace `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Data) == 0 {
		return nil, nil
	}
	return &out.Data[0], nil
}

// ---------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------

// report draws the trace as a waterfall.
//
// The offset column is the one to read. It is when each step STARTED, relative
// to the request, and the jump between the HTTP span and outbox.publish is the
// relay poll — the cost of never losing a message, in milliseconds, measured.
func report(trace *jaegerTrace) {
	spans := trace.Spans
	if len(spans) == 0 {
		fmt.Println("the trace is empty")
		return
	}

	sort.Slice(spans, func(i, j int) bool { return spans[i].StartTime < spans[j].StartTime })
	origin := spans[0].StartTime

	fmt.Printf("%-38s %-12s %10s %10s\n", "SPAN", "SERVICE", "OFFSET", "TOOK")
	fmt.Println(strings.Repeat("-", 74))
	for _, s := range spans {
		service := trace.Processes[s.ProcessID].ServiceName
		if node := processTag(trace, s.ProcessID, "service.instance.id"); node != "" {
			service = node
		}
		fmt.Printf("%-38s %-12s %10s %10s\n",
			truncate(s.OperationName, 38),
			truncate(service, 12),
			round(time.Duration(s.StartTime-origin)*time.Microsecond),
			round(time.Duration(s.Duration)*time.Microsecond),
		)
	}
	fmt.Println()
}

// verdict says whether the trace proves what the stage claims.
func verdict(trace *jaegerTrace, arrived bool) error {
	names := map[string]bool{}
	services := map[string]bool{}
	for _, s := range trace.Spans {
		names[s.OperationName] = true
		if node := processTag(trace, s.ProcessID, "service.instance.id"); node != "" {
			services[node] = true
		}
	}

	var problems []string

	// The two spans that make this a cross-process trace rather than a log with
	// extra steps. The HTTP span's name comes from otelgin and carries the
	// route template, so it is matched loosely.
	if !hasPrefix(names, "POST /conversations") {
		problems = append(problems, "no HTTP span: the request never started a trace")
	}
	if !names["outbox.publish"] {
		problems = append(problems,
			"no outbox.publish span: the trace did not survive the database column")
	}
	if arrived && !names["fanout.deliver"] {
		problems = append(problems,
			"no fanout.deliver span: the message was delivered but the trace did not cross NATS")
	}

	// The claim of the whole stage. One trace, more than one process.
	if len(services) < 2 {
		problems = append(problems,
			fmt.Sprintf("every span ran in one process (%v); a cross-node trace should show at least two",
				keys(services)))
	}

	fmt.Printf("processes in this one trace: %s\n", strings.Join(keys(services), ", "))
	fmt.Printf("spans: %d\n\n", len(trace.Spans))

	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Println("FAIL:", p)
		}
		return fmt.Errorf("%d %s", len(problems), plural(len(problems), "problem", "problems"))
	}

	fmt.Println("PASS: one trace id followed one message across every process it touched.")
	return nil
}

func processTag(trace *jaegerTrace, processID, key string) string {
	for _, tag := range trace.Processes[processID].Tags {
		if tag.Key == key {
			if s, ok := tag.Value.(string); ok {
				return s
			}
		}
	}
	return ""
}

func hasPrefix(set map[string]bool, prefix string) bool {
	for name := range set {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// round trims a duration to something a person reads rather than parses.
func round(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(100 * time.Microsecond).String()
	default:
		return d.Round(time.Microsecond).String()
	}
}

// ---------------------------------------------------------------------------
// The socket
// ---------------------------------------------------------------------------

type socket struct {
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

	// The hello frame, read before the reader goroutine starts. One connection
	// may have only one reader, and waiting for it means the message is never
	// sent to a hub that has not registered this socket yet — a race that would
	// look exactly like a broken trace.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("no hello frame from %s: %w", node, err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	s := &socket{conn: conn, seen: make(chan string, 32)}
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

		// The event is under "data", not "message". The frame shape is set by
		// internal/server/ws.go; this must follow it.
		var frame struct {
			Type string `json:"type"`
			Data struct {
				ClientMsgID string `json:"client_msg_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			continue
		}
		if frame.Type != "message.new" {
			continue
		}
		select {
		case s.seen <- frame.Data.ClientMsgID:
		default:
		}
	}
}

func (s *socket) waitFor(clientMsgID string, deadline time.Time) bool {
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case got, ok := <-s.seen:
			if !ok {
				return false
			}
			if got == clientMsgID {
				return true
			}
		case <-time.After(remaining):
			return false
		}
	}
}

func (s *socket) close() { s.conn.Close() }

// ---------------------------------------------------------------------------
// The API
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
	if err := postJSON(client, node+"/login", "", body, nil, &login); err != nil {
		return user{}, fmt.Errorf("login %s: %w (did you run `make seed ARGS=\"-n 3\"`?)", username, err)
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
		"title":      fmt.Sprintf("tracecheck %s", time.Now().Format(time.TimeOnly)),
		"member_ids": []uint{memberID},
	}
	if err := postJSON(client, node+"/conversations", token, body, nil, &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// sendMessage is the one call that carries a traceparent, because it is the one
// whose trace this tool is following.
func sendMessage(client *http.Client, node, token string, roomID uint, clientMsgID, traceparent string) error {
	body := map[string]string{
		"content":       "follow me across four processes",
		"client_msg_id": clientMsgID,
	}
	headers := map[string]string{"traceparent": traceparent}
	url := fmt.Sprintf("%s/conversations/%d/messages", node, roomID)
	return postJSON(client, url, token, body, headers, nil)
}

func postJSON(client *http.Client, url, token string, body any, headers map[string]string, into any) error {
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
	for k, v := range headers {
		req.Header.Set(k, v)
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

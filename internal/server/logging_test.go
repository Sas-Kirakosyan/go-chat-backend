package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// captureLogs points the default logger at a buffer for the length of one
// test, and returns the decoded lines.
//
// It writes JSON, which is what production writes, so a test can assert on a
// field instead of matching a sentence with a regular expression.
func captureLogs(t *testing.T) func() []map[string]any {
	t.Helper()

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return func() []map[string]any {
		t.Helper()
		var lines []map[string]any
		for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if raw == "" {
				continue
			}
			var line map[string]any
			if err := json.Unmarshal([]byte(raw), &line); err != nil {
				t.Fatalf("log line is not JSON: %s", raw)
			}
			lines = append(lines, line)
		}
		return lines
	}
}

func TestEveryResponseCarriesARequestID(t *testing.T) {
	_, r, _ := newTestServer(t)

	rr := do(t, r, "GET", "/livez", "", "")
	id := rr.Header().Get(requestIDHeader)
	if id == "" {
		t.Fatal("no request id on the response")
	}

	// Two requests must not share one, or the id joins nothing together.
	second := do(t, r, "GET", "/livez", "", "").Header().Get(requestIDHeader)
	if second == id {
		t.Fatalf("both requests got the same id %q", id)
	}
}

// A proxy that already started a trace keeps it: the id it sent comes back,
// so its logs and ours can be lined up.
func TestAnIncomingRequestIDIsReused(t *testing.T) {
	_, r, _ := newTestServer(t)

	req := httpRequest("GET", "/livez", "")
	req.Header.Set(requestIDHeader, "from-the-proxy")
	rr := serve(r, req)

	if got := rr.Header().Get(requestIDHeader); got != "from-the-proxy" {
		t.Fatalf("request id: got %q, want the one that was sent", got)
	}
}

// The header comes from the network and goes straight into a log line. A
// caller that could put a newline in it would be writing our logs for us:
// one crafted header, and a fake "request status=200" line appears in the
// file, which is how an audit trail stops being evidence.
func TestAnUnsafeRequestIDIsReplaced(t *testing.T) {
	_, r, _ := newTestServer(t)

	cases := map[string]string{
		"a newline":      "abc\ninjected line",
		"a space":        "abc def",
		"json":           `{"status":"ok"}`,
		"far too long":   strings.Repeat("a", maxRequestIDLen+1),
		"an empty one":   "",
		"a tab":          "abc\tdef",
		"a quote":        `abc"def`,
		"a null byte":    "abc\x00def",
		"non-ascii":      "abcé",
		"path traversal": "../../etc/passwd",
	}

	for name, sent := range cases {
		t.Run(name, func(t *testing.T) {
			req := httpRequest("GET", "/livez", "")
			req.Header.Set(requestIDHeader, sent)
			rr := serve(r, req)

			got := rr.Header().Get(requestIDHeader)
			if got == sent {
				t.Fatalf("an unsafe request id was used as it arrived: %q", sent)
			}
			if !validRequestID(got) {
				t.Fatalf("the replacement is not a safe id either: %q", got)
			}
		})
	}
}

// One request writes several lines. Without a shared id they are unrelated
// rows in a log with a thousand other requests in it.
func TestTheLogLineCarriesTheRequestID(t *testing.T) {
	logs := captureLogs(t)
	_, r, _ := newTestServer(t)

	rr := do(t, r, "GET", "/conversations", "", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("setup: got %d, want %d", rr.Code, http.StatusUnauthorized)
	}
	id := rr.Header().Get(requestIDHeader)

	lines := logs()
	if len(lines) == 0 {
		t.Fatal("nothing was logged")
	}
	last := lines[len(lines)-1]

	if last["request_id"] != id {
		t.Fatalf("request_id in the log: got %v, want %q", last["request_id"], id)
	}
	// A refused request is a warning, not an ordinary line.
	if last["level"] != "WARN" {
		t.Fatalf("level: got %v, want WARN for a 401", last["level"])
	}
	if last["path"] != "/conversations" {
		t.Fatalf("path: got %v", last["path"])
	}
}

// The reason /ws can be logged at all.
//
// The token cannot travel in a header on a WebSocket handshake, so it travels
// in the query string. Gin's own logger prints path and query together, which
// would write a live credential to disk on every connect. Ours writes the path
// only.
func TestTheQueryStringIsNeverLogged(t *testing.T) {
	logs := captureLogs(t)
	_, r, _ := newTestServer(t)

	do(t, r, "GET", "/ws?token=super-secret-token", "", "")

	for _, line := range logs() {
		for key, value := range line {
			if text, ok := value.(string); ok && strings.Contains(text, "super-secret-token") {
				t.Fatalf("the token was logged in %q: %s", key, text)
			}
		}
	}
}

// A probe every few seconds and a scrape every fifteen would bury the requests
// a person cares about.
func TestHealthyProbesAreNotLogged(t *testing.T) {
	logs := captureLogs(t)
	_, r, _ := newTestServer(t)

	for _, path := range []string{"/livez", "/readyz", "/metrics"} {
		if rr := do(t, r, "GET", path, "", ""); rr.Code != http.StatusOK {
			t.Fatalf("%s: got %d, want 200", path, rr.Code)
		}
	}

	if lines := logs(); len(lines) != 0 {
		t.Fatalf("healthy probes wrote %d log lines: %v", len(lines), lines)
	}
}

// Quiet while healthy is only half the rule. A probe that starts failing is
// one of the most interesting lines in the log.
func TestAFailingProbeIsLogged(t *testing.T) {
	logs := captureLogs(t)
	_, r, db := newTestServer(t)
	db.healthy = false

	if rr := do(t, r, "GET", "/readyz", "", ""); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz: got %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}

	lines := logs()
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %v", len(lines), lines)
	}
	if lines[0]["path"] != "/readyz" {
		t.Fatalf("the failing probe was not the line that was logged: %v", lines[0])
	}
}

// A panic in one handler must not take the process down, and with it every
// other request in flight and every open socket.
func TestAPanicIsRecovered(t *testing.T) {
	logs := captureLogs(t)
	_, r, _ := newTestServer(t)

	// Routes added after RegisterRoutes still run the engine's middleware.
	r.GET("/boom", func(c *gin.Context) {
		panic("something went very wrong")
	})

	rr := do(t, r, "GET", "/boom", "", "")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusInternalServerError)
	}

	// A stack trace in the body would tell a stranger the file layout and the
	// library versions of the server.
	if strings.Contains(rr.Body.String(), "something went very wrong") ||
		strings.Contains(rr.Body.String(), ".go:") {
		t.Fatalf("the panic leaked into the response body: %s", rr.Body)
	}

	// The server is still serving, which is the whole point.
	if next := do(t, r, "GET", "/livez", "", ""); next.Code != http.StatusOK {
		t.Fatalf("the server stopped working after a panic: got %d", next.Code)
	}

	var logged bool
	for _, line := range logs() {
		if line["msg"] == "panic recovered" {
			logged = true
			if _, ok := line["stack"]; !ok {
				t.Error("the panic was logged without a stack trace")
			}
			if line["request_id"] == nil {
				t.Error("the panic line has no request id, so it cannot be tied to the request")
			}
		}
	}
	if !logged {
		t.Error("the panic was swallowed without a log line")
	}
}

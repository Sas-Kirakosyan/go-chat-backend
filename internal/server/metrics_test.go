package server

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// scrape reads /metrics the way Prometheus would, and returns the text.
//
// The registry is process-wide, so these numbers include whatever every other
// test in this package did. The assertions below are therefore about which
// LABELS exist, never about a count — a test that expected "exactly 3" would
// pass or fail depending on which tests ran first.
func scrape(t *testing.T, r *gin.Engine) string {
	t.Helper()
	rr := serve(r, httpRequest("GET", "/metrics", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("/metrics: got %d, want 200", rr.Code)
	}
	return rr.Body.String()
}

func TestMetricsEndpointServesTheRuntimeCollectors(t *testing.T) {
	_, r, _ := newTestServer(t)
	body := scrape(t, r)

	// These come free with the default registry, and goroutines is the one to
	// watch in this service: two per socket, so a leak shows there first.
	for _, want := range []string{"go_goroutines", "go_memstats_alloc_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not carry %s", want)
		}
	}
}

// The label rule the whole package turns on.
//
// A label whose value comes from the caller makes a new time series per value,
// and a target with a million series takes Prometheus down. Ten thousand rooms
// must be ONE series, so the label is the route template.
func TestTheRouteLabelIsTheTemplateNotThePath(t *testing.T) {
	_, r, _ := newTestServer(t)
	alice, _ := signUp(t, r, "alice")

	first := createRoom(t, r, alice)
	second := createRoom(t, r, alice)

	for _, room := range []uint{first, second} {
		path := fmt.Sprintf("/conversations/%d/messages", room)
		if rr := do(t, r, "POST", path, `{"content":"hello"}`, alice); rr.Code != http.StatusCreated {
			t.Fatalf("send to room %d: got %d (body %s)", room, rr.Code, rr.Body)
		}
	}

	body := scrape(t, r)
	if !strings.Contains(body, `route="/conversations/:id/messages"`) {
		t.Fatal("the route template is not a label value; look for route= in the scrape")
	}
	if strings.Contains(body, fmt.Sprintf(`route="/conversations/%d/messages"`, first)) {
		t.Fatal("a real room id became a label value: one series per room is a Prometheus outage waiting to happen")
	}
}

// The same rule, but for the path an attacker picks. /wp-login.php, /.env, and
// whatever a scanner tries next must all count as one route.
func TestAnUnroutedPathDoesNotBecomeALabel(t *testing.T) {
	_, r, _ := newTestServer(t)

	if rr := do(t, r, "GET", "/nothing-here-at-all", "", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("setup: got %d, want 404", rr.Code)
	}

	body := scrape(t, r)
	if strings.Contains(body, "/nothing-here-at-all") {
		t.Fatal("a 404 path became a metric label, which lets a stranger create time series")
	}
	if !strings.Contains(body, fmt.Sprintf(`route="%s"`, unroutedLabel)) {
		t.Fatalf("the 404 was not counted under %q", unroutedLabel)
	}
}

// A refused request is still a request. A metric that counts only the ones
// that went well hides the outage.
func TestRefusedRequestsAreCounted(t *testing.T) {
	_, r, _ := newTestServerWith(t, frozen(rateLimits{authRPS: 1, authBurst: 1}))

	const creds = `{"username":"alice","password":"wrong-password-here"}`
	do(t, r, "POST", "/login", creds, "")
	if rr := do(t, r, "POST", "/login", creds, ""); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("setup: got %d, want %d", rr.Code, http.StatusTooManyRequests)
	}

	body := scrape(t, r)
	if !strings.Contains(body, `status="429"`) {
		t.Fatal("a rate-limited request was not counted")
	}
	if !strings.Contains(body, "chat_rate_limited_total") {
		t.Fatal("chat_rate_limited_total is missing from the scrape")
	}
}

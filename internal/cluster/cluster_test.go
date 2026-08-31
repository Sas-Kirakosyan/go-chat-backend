package cluster

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// These tests need no Redis. What they check is the part that would be wrong
// on every machine equally: the shape of what travels between nodes, and the
// string handling around presence. Anything that needs a real server is
// checked by hand in the Stage 3 notes, with cmd/splitcheck.

// The payload must stay JSON on the wire. If fanout.Payload were []byte instead
// of json.RawMessage, encoding/json would base64 it: the traffic would grow by
// a third and `redis-cli SUBSCRIBE chat:fanout` would show gibberish.
func TestFanoutKeepsPayloadReadable(t *testing.T) {
	payload := []byte(`{"type":"message.new","data":{"id":7}}`)

	raw, err := json.Marshal(fanout{UserIDs: []uint{1, 2}, Payload: payload, From: "api1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if !strings.Contains(string(raw), `"type":"message.new"`) {
		t.Fatalf("payload was re-encoded, not embedded: %s", raw)
	}

	var back fanout
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(back.Payload) != string(payload) {
		t.Fatalf("payload changed:\n got %s\nwant %s", back.Payload, payload)
	}
	if len(back.UserIDs) != 2 || back.UserIDs[0] != 1 || back.UserIDs[1] != 2 {
		t.Fatalf("user ids changed: %v", back.UserIDs)
	}
	if back.From != "api1" {
		t.Fatalf("from: got %q, want %q", back.From, "api1")
	}
}

func TestPresenceMemberRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		member string
		want   uint
		ok     bool
	}{
		{"plain", presenceMember(42, "api1"), 42, true},
		// A node id with a colon in it is the realistic hostname case, e.g. an
		// IPv6 address. Cutting at the first colon is what keeps this working.
		{"node id with colons", presenceMember(42, "fe80::1"), 42, true},
		{"no colon", "42", 0, false},
		{"not a number", "abc:api1", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePresenceMember(tc.member)
			if ok != tc.ok {
				t.Fatalf("ok: got %v, want %v (member %q)", ok, tc.ok, tc.member)
			}
			if got != tc.want {
				t.Fatalf("id: got %d, want %d (member %q)", got, tc.want, tc.member)
			}
		})
	}
}

// A score is a unix time, and unix times are big. Formatted as a float they
// come out as 1.7568e+09, which Redis reads as a different, much smaller
// number — so every entry would look ancient and everybody would be offline.
func TestScoreIsNotScientificNotation(t *testing.T) {
	got := formatScore(time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC))

	if strings.ContainsAny(got, "e+.") {
		t.Fatalf("score %q is not a plain integer", got)
	}
	if want := "1788004800"; got != want {
		t.Fatalf("score: got %s, want %s", got, want)
	}
}

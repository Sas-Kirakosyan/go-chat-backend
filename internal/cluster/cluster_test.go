package cluster

import (
	"strings"
	"testing"
	"time"
)

// These tests need no Redis. What they check is the part that would be wrong
// on every machine equally: the string handling around presence. Anything that
// needs a real server is checked by hand in the Stage 3 notes, with
// cmd/splitcheck.
//
// The fan-out test that used to live here moved to internal/broker with the
// fan-out itself, in Stage 5.

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

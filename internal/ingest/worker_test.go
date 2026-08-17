package ingest

import "testing"

func TestParseCursorRoundTrip(t *testing.T) {
	cases := []struct {
		in   string
		want cursorState
	}{
		{"catalog:1700000000000000000:4500:88", cursorState{phase: "catalog", epoch: 1700000000000000000, catCursor: 4500, seq: 88}},
		{"offers:1700000000000000000:12345", cursorState{phase: "offers", epoch: 1700000000000000000, seq: 12345}},
		// as_of_seq 0 is a legitimate position on a fresh feed, not a
		// "not pinned yet" sentinel.
		{"catalog:9:1000:0", cursorState{phase: "catalog", epoch: 9, catCursor: 1000, seq: 0}},
		// Pre-epoch cursor formats and garbage restart the sync.
		{"catalog:4500:88", cursorState{phase: "catalog"}},
		{"offers:12345", cursorState{phase: "catalog"}},
		{"", cursorState{phase: "catalog"}},
		{"bogus", cursorState{phase: "catalog"}},
	}
	for _, c := range cases {
		if got := parseCursor(c.in); got != c.want {
			t.Errorf("parseCursor(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

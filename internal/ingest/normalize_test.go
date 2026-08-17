package ingest

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

var now = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

func str(s string) *string { return &s }
func i64(i int64) *int64   { return &i }

func base() RawItem {
	return RawItem{
		SKU:        str("DS-00001234"),
		Title:      str("Arvello Pro Drill 512"),
		Brand:      str("Arvello"),
		Category:   str("tools"),
		PriceCents: i64(4599),
		Currency:   str("USD"),
	}
}

func wantReason(t *testing.T, err error, reason string) {
	t.Helper()
	var re *RejectError
	if !errors.As(err, &re) {
		t.Fatalf("want RejectError %q, got %v", reason, err)
	}
	if re.Reason != reason {
		t.Fatalf("want reason %q, got %q", reason, re.Reason)
	}
}

func TestPriceFormats(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mut   func(*RawItem)
		cents int64
	}{
		{"cents int", func(r *RawItem) {}, 4599},
		{"float dollars", func(r *RawItem) { r.PriceCents = nil; r.Price = json.RawMessage(`45.99`) }, 4599},
		{"string dollars", func(r *RawItem) { r.PriceCents = nil; r.Price = json.RawMessage(`"45.99"`) }, 4599},
		{"float rounding", func(r *RawItem) { r.PriceCents = nil; r.Price = json.RawMessage(`0.07`) }, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := base()
			tc.mut(&raw)
			n, err := Normalize(raw, now)
			if err != nil {
				t.Fatal(err)
			}
			if n.PriceCents != tc.cents {
				t.Fatalf("want %d cents, got %d", tc.cents, n.PriceCents)
			}
		})
	}
}

func TestRejections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mut    func(*RawItem)
		reason string
	}{
		{"no sku", func(r *RawItem) { r.SKU = nil }, "missing_sku"},
		{"blank sku", func(r *RawItem) { r.SKU = str("   ") }, "missing_sku"},
		{"sku too short", func(r *RawItem) { r.SKU = str("a-1") }, "bad_sku"},
		{"no price", func(r *RawItem) { r.PriceCents = nil }, "missing_price"},
		{"garbage price", func(r *RawItem) { r.PriceCents = nil; r.Price = json.RawMessage(`"n/a"`) }, "bad_price_format"},
		{"negative", func(r *RawItem) { r.PriceCents = i64(-500) }, "price_out_of_range"},
		{"unit bug 100x", func(r *RawItem) { r.PriceCents = i64(4_599_00) }, "price_out_of_range"},
		{"euro", func(r *RawItem) { r.Currency = str("EUR") }, "unsupported_currency"},
		{"bad ts", func(r *RawItem) { r.UpdatedAt = str("yesterday") }, "bad_timestamp"},
		{"future ts", func(r *RawItem) { r.UpdatedAt = str("2027-01-01T00:00:00Z") }, "future_timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := base()
			tc.mut(&raw)
			if _, err := Normalize(raw, now); err == nil {
				t.Fatal("want rejection, got accept")
			} else {
				wantReason(t, err, tc.reason)
			}
		})
	}
}

func TestMissingCurrencyToleratedAsUSD(t *testing.T) {
	raw := base()
	raw.Currency = nil
	if _, err := Normalize(raw, now); err != nil {
		t.Fatalf("missing currency should pass as USD: %v", err)
	}
}

func TestSKUVariantsCollapse(t *testing.T) {
	variants := []string{"DS-00001234", "ds-00001234", "DS--00001234", "DS_00001234", "DS-00001234 "}
	want := NormalizeSKU(variants[0])
	for _, v := range variants {
		if got := NormalizeSKU(v); got != want {
			t.Fatalf("%q normalized to %q, want %q", v, got, want)
		}
	}
}

func TestStaleTimestampAccepted(t *testing.T) {
	raw := base()
	raw.UpdatedAt = str(now.Add(-30 * time.Hour).Format(time.RFC3339Nano))
	n, err := Normalize(raw, now)
	if err != nil {
		t.Fatal(err)
	}
	if !n.UpdatedAt.Before(now.Add(-29 * time.Hour)) {
		t.Fatal("stale timestamp not preserved")
	}
}

func TestSpikeSuspect(t *testing.T) {
	if !SpikeSuspect(1299, 129900) {
		t.Fatal("×100 jump not flagged")
	}
	if !SpikeSuspect(129900, 1299) {
		t.Fatal("÷100 drop not flagged")
	}
	if SpikeSuspect(1299, 1499) {
		t.Fatal("normal move flagged")
	}
	if SpikeSuspect(0, 1499) {
		t.Fatal("no held price should never flag")
	}
}

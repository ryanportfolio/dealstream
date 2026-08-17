// Package ingest turns messy retailer feed items into canonical catalog
// rows, quarantining what it cannot trust. Validation is strict on values
// (price, currency) and lenient on presentation (SKU formatting, price
// wire format), because presentation is where feeds disagree hardest.
package ingest

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// RawItem matches the union of shapes the retailers emit. Pointers so a
// missing field is distinguishable from a zero value.
type RawItem struct {
	SKU        *string           `json:"sku"`
	Title      *string           `json:"title"`
	Brand      *string           `json:"brand"`
	Category   *string           `json:"category"`
	Attrs      map[string]string `json:"attrs"`
	PriceCents *int64            `json:"price_cents"`
	Price      json.RawMessage   `json:"price"`
	Currency   *string           `json:"currency"`
	InStock    *bool             `json:"in_stock"`
	UpdatedAt  *string           `json:"updated_at"`
	URL        *string           `json:"url"`
}

type Normalized struct {
	SKUNorm    string
	Title      string
	Brand      string
	Category   string
	Attrs      map[string]string
	PriceCents int64
	InStock    bool
	UpdatedAt  time.Time
	URL        string
	// Raw is the original feed payload, set by the caller. Items rejected
	// after normalization (spikes, unknown product without a title) are
	// quarantined with it so every quarantine row stays replayable.
	Raw json.RawMessage
}

// RejectError carries the quarantine reason for a failed item.
type RejectError struct {
	Reason string
	Detail string
}

func (e *RejectError) Error() string { return e.Reason + ": " + e.Detail }

func reject(reason, format string, args ...any) error {
	return &RejectError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// Price bounds. Catalog categories top out at $1299.99; anything past
// priceMaxCents is a feed bug (usually dollars-as-cents, ×100), not a
// deal. The cap alone catches the ×100 bug for items from $20 up; cheaper
// items rely on the relative spike check.
const (
	priceMinCents = 1
	priceMaxCents = 200_000
)

// spikeFactor: an update this many times above or below the price we
// already hold for the offer is rejected even inside absolute bounds.
// Catches ×100 unit bugs on cheap items.
const spikeFactor = 20

func Normalize(raw RawItem, now time.Time) (Normalized, error) {
	if raw.SKU == nil || strings.TrimSpace(*raw.SKU) == "" {
		return Normalized{}, reject("missing_sku", "no sku field")
	}
	skuNorm := NormalizeSKU(*raw.SKU)
	if len(skuNorm) < 4 || len(skuNorm) > 64 {
		return Normalized{}, reject("bad_sku", "%q", *raw.SKU)
	}

	cents, err := parsePrice(raw)
	if err != nil {
		return Normalized{}, err
	}
	if cents < priceMinCents || cents > priceMaxCents {
		return Normalized{}, reject("price_out_of_range", "%d cents", cents)
	}

	// Missing currency is tolerated as USD: it is the only currency the
	// pipeline accepts, so the assumption is checkable downstream. An
	// explicit non-USD currency is a different feed contract and rejected.
	if raw.Currency != nil && *raw.Currency != "USD" {
		return Normalized{}, reject("unsupported_currency", "%s", *raw.Currency)
	}

	updatedAt := now
	if raw.UpdatedAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *raw.UpdatedAt)
		if err != nil {
			return Normalized{}, reject("bad_timestamp", "%q", *raw.UpdatedAt)
		}
		if t.After(now.Add(5 * time.Minute)) {
			return Normalized{}, reject("future_timestamp", "%s", t)
		}
		updatedAt = t.UTC()
	}

	n := Normalized{
		SKUNorm:    skuNorm,
		Attrs:      raw.Attrs,
		PriceCents: cents,
		InStock:    raw.InStock == nil || *raw.InStock,
		UpdatedAt:  updatedAt,
	}
	if raw.Title != nil {
		n.Title = strings.TrimSpace(*raw.Title)
	}
	if raw.Brand != nil {
		n.Brand = strings.TrimSpace(*raw.Brand)
	}
	if raw.Category != nil {
		n.Category = strings.TrimSpace(*raw.Category)
	}
	if raw.URL != nil {
		n.URL = *raw.URL
	}
	return n, nil
}

// SpikeSuspect reports whether newCents is implausibly far from the price
// already held for this offer.
func SpikeSuspect(heldCents, newCents int64) bool {
	if heldCents <= 0 {
		return false
	}
	ratio := float64(newCents) / float64(heldCents)
	return ratio >= spikeFactor || ratio <= 1.0/spikeFactor
}

// NormalizeSKU collapses formatting variants (case, dashes, underscores,
// whitespace) into one canonical key. This is the cross-retailer identity.
func NormalizeSKU(sku string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(strings.TrimSpace(sku)) {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		}
	}
	return b.String()
}

func parsePrice(raw RawItem) (int64, error) {
	if raw.PriceCents != nil {
		return *raw.PriceCents, nil
	}
	if len(raw.Price) == 0 {
		return 0, reject("missing_price", "no price_cents or price field")
	}
	s := string(raw.Price)
	if strings.HasPrefix(s, `"`) {
		if err := json.Unmarshal(raw.Price, &s); err != nil {
			return 0, reject("bad_price_format", "%s", raw.Price)
		}
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, reject("bad_price_format", "%q", s)
	}
	return int64(math.Round(f * 100)), nil
}

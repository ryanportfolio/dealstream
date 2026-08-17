package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/ryanportfolio/dealstream/internal/metrics"
)

// Worker drives one retailer's feed: full catalog sync first (or after a
// cursor expiry), then incremental polling. The cursor survives restarts
// via ingest_cursors, so a redeploy resumes instead of resyncing.
type Worker struct {
	Slug       string
	RetailerID int16
	Client     *Client
	Store      *Store
	CH         *CHWriter
	PageSize   int
	Poll       time.Duration
}

// Cursor encoding: "catalog:<index>:<asOfSeq>" during sync,
// "offers:<seq>" once incremental.
func (w *Worker) Run(ctx context.Context) {
	cur, err := w.Store.LoadCursor(ctx, w.RetailerID)
	if err != nil {
		log.Printf("%s: load cursor: %v", w.Slug, err)
	}
	phase, catCursor, seq := parseCursor(cur)
	log.Printf("%s: starting phase=%s catalog=%d seq=%d", w.Slug, phase, catCursor, seq)

	backoff := time.Second
	for ctx.Err() == nil {
		var err error
		switch phase {
		case "catalog":
			phase, catCursor, seq, err = w.catalogStep(ctx, catCursor, seq)
		default:
			var idle bool
			seq, idle, err = w.offersStep(ctx, seq)
			if err == nil && idle {
				sleep(ctx, w.Poll)
			}
		}
		switch {
		case errors.Is(err, ErrGone):
			log.Printf("%s: cursor expired at seq %d, full resync", w.Slug, seq)
			metrics.IngestResyncs.WithLabelValues(w.Slug).Inc()
			phase, catCursor, seq = "catalog", 0, 0
			w.saveCursor(ctx, phase, catCursor, seq)
		case err != nil:
			log.Printf("%s: %v (backoff %s)", w.Slug, err, backoff)
			metrics.IngestFeedErrors.WithLabelValues(w.Slug).Inc()
			sleep(ctx, backoff)
			backoff = min(backoff*2, time.Minute)
		default:
			backoff = time.Second
		}
	}
}

func (w *Worker) catalogStep(ctx context.Context, cursor int, asOf uint64) (string, int, uint64, error) {
	page, err := w.Client.Catalog(ctx, w.Slug, cursor, w.PageSize)
	if err != nil {
		return "catalog", cursor, asOf, err
	}
	metrics.IngestPages.WithLabelValues(w.Slug, "catalog").Inc()
	// The change-feed position is pinned before the first page so updates
	// that land mid-sync get replayed, not lost. Replays are safe: the
	// offer upsert is guarded by updated_at.
	if asOf == 0 {
		asOf = page.AsOfSeq
	}
	if err := w.processItems(ctx, page.Items); err != nil {
		return "catalog", cursor, asOf, err
	}
	if page.NextCursor == nil {
		log.Printf("%s: catalog sync complete, switching to incremental at seq %d", w.Slug, asOf)
		w.saveCursor(ctx, "offers", 0, asOf)
		return "offers", 0, asOf, nil
	}
	w.saveCursor(ctx, "catalog", *page.NextCursor, asOf)
	return "catalog", *page.NextCursor, asOf, nil
}

func (w *Worker) offersStep(ctx context.Context, since uint64) (uint64, bool, error) {
	page, err := w.Client.Offers(ctx, w.Slug, since, w.PageSize)
	if err != nil {
		return since, false, err
	}
	metrics.IngestPages.WithLabelValues(w.Slug, "offers").Inc()
	if len(page.Items) == 0 {
		return since, true, nil
	}
	if err := w.processItems(ctx, page.Items); err != nil {
		return since, false, err
	}
	w.saveCursor(ctx, "offers", 0, page.Next)
	// A short page means the feed is drained for now.
	return page.Next, len(page.Items) < w.PageSize, nil
}

func (w *Worker) processItems(ctx context.Context, raws []json.RawMessage) error {
	now := time.Now().UTC()
	var (
		accepted   []Normalized
		quarantine []QuarantineRow
	)
	for _, rawJSON := range raws {
		var raw RawItem
		if err := json.Unmarshal(rawJSON, &raw); err != nil {
			quarantine = append(quarantine, QuarantineRow{Reason: "unparseable", Raw: rawJSON})
			continue
		}
		n, err := Normalize(raw, now)
		if err != nil {
			var re *RejectError
			reason := "invalid"
			if errors.As(err, &re) {
				reason = re.Reason
			}
			quarantine = append(quarantine, QuarantineRow{Reason: reason, Raw: rawJSON})
			continue
		}
		accepted = append(accepted, n)
	}

	batchStart := time.Now()
	res, err := w.Store.UpsertBatch(ctx, w.RetailerID, accepted)
	if err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	metrics.IngestBatchDuration.WithLabelValues(w.Slug).Observe(time.Since(batchStart).Seconds())
	for _, sp := range res.Spikes {
		raw, _ := json.Marshal(map[string]any{
			"sku_norm": sp.Item.SKUNorm, "price_cents": sp.Item.PriceCents, "held_cents": sp.HeldCents,
		})
		quarantine = append(quarantine, QuarantineRow{Reason: "price_spike", Raw: raw})
	}
	if err := w.Store.Quarantine(ctx, w.RetailerID, quarantine); err != nil {
		return fmt.Errorf("quarantine: %w", err)
	}

	metrics.IngestItems.WithLabelValues(w.Slug, "accepted").Add(float64(len(res.Accepted)))
	metrics.IngestItems.WithLabelValues(w.Slug, "stale_skipped").Add(float64(res.StaleSkipped))
	if res.NoTitle > 0 {
		metrics.IngestItems.WithLabelValues(w.Slug, "no_title").Add(float64(res.NoTitle))
	}
	for _, q := range quarantine {
		metrics.IngestItems.WithLabelValues(w.Slug, q.Reason).Inc()
	}

	events := make([]PriceEvent, len(res.Accepted))
	newest := time.Time{}
	for i, a := range res.Accepted {
		events[i] = PriceEvent{
			ProductID: a.ProductID, RetailerID: w.RetailerID,
			PriceCents: a.PriceCents, InStock: a.InStock, ObservedAt: a.UpdatedAt,
		}
		if a.UpdatedAt.After(newest) {
			newest = a.UpdatedAt
		}
	}
	if !newest.IsZero() {
		metrics.IngestFreshness.WithLabelValues(w.Slug).Set(time.Since(newest).Seconds())
	}
	w.CH.Add(events)
	return nil
}

func (w *Worker) saveCursor(ctx context.Context, phase string, catCursor int, seq uint64) {
	var cur string
	if phase == "catalog" {
		cur = fmt.Sprintf("catalog:%d:%d", catCursor, seq)
	} else {
		cur = fmt.Sprintf("offers:%d", seq)
	}
	if err := w.Store.SaveCursor(ctx, w.RetailerID, cur); err != nil {
		log.Printf("%s: save cursor: %v", w.Slug, err)
	}
}

func parseCursor(cur string) (phase string, catCursor int, seq uint64) {
	parts := strings.Split(cur, ":")
	switch {
	case len(parts) == 3 && parts[0] == "catalog":
		catCursor, _ = strconv.Atoi(parts[1])
		seq, _ = strconv.ParseUint(parts[2], 10, 64)
		return "catalog", catCursor, seq
	case len(parts) == 2 && parts[0] == "offers":
		seq, _ = strconv.ParseUint(parts[1], 10, 64)
		return "offers", 0, seq
	default:
		return "catalog", 0, 0
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

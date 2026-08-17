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

// cursorState is the worker's persisted position. epoch is the feed
// instance the position belongs to: a feed restart resets sequences, and
// an old seq can alias into the new sequence space while looking valid.
// During catalog sync, seq holds the pinned change-feed position;
// incremental, it is the last applied update seq.
type cursorState struct {
	phase     string // "catalog" | "offers"
	epoch     uint64
	catCursor int
	seq       uint64
}

// Cursor encoding: "catalog:<epoch>:<index>:<asOfSeq>" during sync,
// "offers:<epoch>:<seq>" once incremental. Anything else (including
// pre-epoch cursors) starts a fresh catalog sync.
func (w *Worker) Run(ctx context.Context) {
	cur, err := w.Store.LoadCursor(ctx, w.RetailerID)
	if err != nil {
		log.Printf("%s: load cursor: %v", w.Slug, err)
	}
	cs := parseCursor(cur)
	log.Printf("%s: starting phase=%s epoch=%d catalog=%d seq=%d", w.Slug, cs.phase, cs.epoch, cs.catCursor, cs.seq)

	backoff := time.Second
	for ctx.Err() == nil {
		var err error
		switch cs.phase {
		case "catalog":
			err = w.catalogStep(ctx, &cs)
		default:
			var idle bool
			idle, err = w.offersStep(ctx, &cs)
			if err == nil && idle {
				sleep(ctx, w.Poll)
			}
		}
		switch {
		case errors.Is(err, ErrGone):
			log.Printf("%s: cursor unusable at seq %d (expired or feed restarted), full resync", w.Slug, cs.seq)
			metrics.IngestResyncs.WithLabelValues(w.Slug).Inc()
			cs = cursorState{phase: "catalog"}
			w.saveCursor(ctx, cs)
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

func (w *Worker) catalogStep(ctx context.Context, cs *cursorState) error {
	page, err := w.Client.Catalog(ctx, w.Slug, cs.catCursor, w.PageSize)
	if err != nil {
		return err
	}
	metrics.IngestPages.WithLabelValues(w.Slug, "catalog").Inc()
	// The epoch and change-feed position are pinned on the first page so
	// updates that land mid-sync get replayed, not lost. Pinning keys off
	// the page position, not a zero sentinel: as_of_seq is legitimately 0
	// on a feed that has emitted no updates yet.
	if cs.catCursor == 0 {
		cs.epoch = page.Epoch
		cs.seq = page.AsOfSeq
	} else if page.Epoch != cs.epoch {
		return ErrGone // feed restarted mid-sync; positions no longer comparable
	}
	if err := w.processItems(ctx, page.Items); err != nil {
		return err
	}
	if page.NextCursor == nil {
		log.Printf("%s: catalog sync complete, switching to incremental at seq %d", w.Slug, cs.seq)
		cs.phase, cs.catCursor = "offers", 0
	} else {
		cs.catCursor = *page.NextCursor
	}
	w.saveCursor(ctx, *cs)
	return nil
}

func (w *Worker) offersStep(ctx context.Context, cs *cursorState) (idle bool, err error) {
	page, err := w.Client.Offers(ctx, w.Slug, cs.seq, w.PageSize)
	if err != nil {
		return false, err
	}
	metrics.IngestPages.WithLabelValues(w.Slug, "offers").Inc()
	if page.Epoch != cs.epoch {
		return false, ErrGone // feed restarted; our seq may alias into its new sequence space
	}
	if len(page.Items) == 0 {
		return true, nil
	}
	if err := w.processItems(ctx, page.Items); err != nil {
		return false, err
	}
	cs.seq = page.Next
	w.saveCursor(ctx, *cs)
	// The feed says whether more updates are waiting; guessing from the
	// page size breaks when the server clamps the requested limit.
	return !page.HasMore, nil
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
		n.Raw = rawJSON
		accepted = append(accepted, n)
	}

	batchStart := time.Now()
	res, err := w.Store.UpsertBatch(ctx, w.RetailerID, accepted)
	if err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	metrics.IngestBatchDuration.WithLabelValues(w.Slug).Observe(time.Since(batchStart).Seconds())
	for _, sp := range res.Spikes {
		quarantine = append(quarantine, QuarantineRow{Reason: "price_spike", Raw: sp.Item.Raw})
	}
	for _, it := range res.NoTitle {
		quarantine = append(quarantine, QuarantineRow{Reason: "no_title", Raw: it.Raw})
	}
	if err := w.Store.Quarantine(ctx, w.RetailerID, quarantine); err != nil {
		return fmt.Errorf("quarantine: %w", err)
	}

	metrics.IngestItems.WithLabelValues(w.Slug, "accepted").Add(float64(len(res.Accepted)))
	metrics.IngestItems.WithLabelValues(w.Slug, "stale_skipped").Add(float64(res.StaleSkipped))
	if res.SpikeOverrides > 0 {
		metrics.IngestItems.WithLabelValues(w.Slug, "spike_overridden").Add(float64(res.SpikeOverrides))
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
		metrics.IngestLastAccept.WithLabelValues(w.Slug).Set(float64(newest.UnixMilli()) / 1000)
	}
	if len(events) == 0 {
		return nil
	}
	// The cursor must not advance past observations ClickHouse has not
	// durably taken: wait for this batch's watermark before the caller
	// saves. A crash then replays the page, and replays are safe on both
	// sides (offers are guarded by updated_at, the rollup's aggregates
	// are duplicate-insensitive).
	wm := w.CH.Add(events)
	return w.CH.WaitFlushed(ctx, wm)
}

func (w *Worker) saveCursor(ctx context.Context, cs cursorState) {
	var cur string
	if cs.phase == "catalog" {
		cur = fmt.Sprintf("catalog:%d:%d:%d", cs.epoch, cs.catCursor, cs.seq)
	} else {
		cur = fmt.Sprintf("offers:%d:%d", cs.epoch, cs.seq)
	}
	if err := w.Store.SaveCursor(ctx, w.RetailerID, cur); err != nil {
		log.Printf("%s: save cursor: %v", w.Slug, err)
	}
}

func parseCursor(cur string) cursorState {
	parts := strings.Split(cur, ":")
	switch {
	case len(parts) == 4 && parts[0] == "catalog":
		epoch, _ := strconv.ParseUint(parts[1], 10, 64)
		catCursor, _ := strconv.Atoi(parts[2])
		seq, _ := strconv.ParseUint(parts[3], 10, 64)
		return cursorState{phase: "catalog", epoch: epoch, catCursor: catCursor, seq: seq}
	case len(parts) == 3 && parts[0] == "offers":
		epoch, _ := strconv.ParseUint(parts[1], 10, 64)
		seq, _ := strconv.ParseUint(parts[2], 10, 64)
		return cursorState{phase: "offers", epoch: epoch, seq: seq}
	default:
		return cursorState{phase: "catalog"}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

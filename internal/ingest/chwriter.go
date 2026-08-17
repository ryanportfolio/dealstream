package ingest

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/ryanportfolio/dealstream/internal/metrics"
)

// CHWriter batches price observations for ClickHouse. ClickHouse wants few
// large inserts, not many small ones; rows buffer until flushSize or
// flushEvery, whichever first. A failed flush requeues the rows and
// retries on the next tick rather than losing observations.
type CHWriter struct {
	conn       driver.Conn
	flushSize  int
	flushEvery time.Duration

	mu   sync.Mutex
	rows []PriceEvent
}

type PriceEvent struct {
	ProductID  int64
	RetailerID int16
	PriceCents int64
	InStock    bool
	ObservedAt time.Time
}

func NewCHWriter(conn driver.Conn) *CHWriter {
	return &CHWriter{conn: conn, flushSize: 10_000, flushEvery: 3 * time.Second}
}

func (w *CHWriter) Add(events []PriceEvent) {
	w.mu.Lock()
	w.rows = append(w.rows, events...)
	n := len(w.rows)
	w.mu.Unlock()
	metrics.CHPending.Set(float64(n))
	if n >= w.flushSize {
		w.Flush(context.Background())
	}
}

// Run flushes on a timer until ctx ends, then drains.
func (w *CHWriter) Run(ctx context.Context) {
	t := time.NewTicker(w.flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.Flush(context.Background())
			return
		case <-t.C:
			w.Flush(ctx)
		}
	}
}

func (w *CHWriter) Flush(ctx context.Context) {
	w.mu.Lock()
	rows := w.rows
	w.rows = nil
	w.mu.Unlock()
	if len(rows) == 0 {
		return
	}

	if err := w.insert(ctx, rows); err != nil {
		log.Printf("chwriter: flush of %d rows failed, requeued: %v", len(rows), err)
		metrics.CHFlushes.WithLabelValues("error").Inc()
		w.mu.Lock()
		w.rows = append(rows, w.rows...)
		metrics.CHPending.Set(float64(len(w.rows)))
		w.mu.Unlock()
		return
	}
	metrics.CHFlushes.WithLabelValues("ok").Inc()
	metrics.CHRowsWritten.Add(float64(len(rows)))
	metrics.CHPending.Set(float64(w.Pending()))
}

func (w *CHWriter) insert(ctx context.Context, rows []PriceEvent) error {
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO price_events (product_id, retailer_id, price_cents, currency, in_stock, observed_at)")
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(uint64(r.ProductID), uint16(r.RetailerID), r.PriceCents, "USD", r.InStock, r.ObservedAt); err != nil {
			return err
		}
	}
	return batch.Send()
}

// Pending reports buffered row count (for shutdown logging and metrics).
func (w *CHWriter) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.rows)
}

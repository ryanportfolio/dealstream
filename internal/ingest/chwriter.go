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
//
// Durability contract: Add returns a watermark, and WaitFlushed blocks
// until every row at or below it has been written (or dropped at the
// buffer cap). Workers wait on it before saving their feed cursor, so a
// crash replays observations instead of losing them; the daily rollup
// aggregates (min/max/argMax) are insensitive to replayed duplicates.
type CHWriter struct {
	conn       driver.Conn
	flushSize  int
	flushEvery time.Duration
	// maxPending bounds buffered rows during a ClickHouse outage. Beyond
	// it the oldest rows are dropped and counted, and their waiters
	// released: bounded memory and an honest metric beat silent growth.
	maxPending int
	insertFn   func(ctx context.Context, rows []PriceEvent) error

	mu         sync.Mutex
	rows       []PriceEvent
	enqSeq     uint64        // rows ever enqueued
	flushedSeq uint64        // rows flushed or dropped, in enqueue order
	notify     chan struct{} // closed and replaced when flushedSeq advances
	kick       chan struct{} // wakes Run for an early flush
}

type PriceEvent struct {
	ProductID  int64
	RetailerID int16
	PriceCents int64
	InStock    bool
	ObservedAt time.Time
}

func NewCHWriter(conn driver.Conn) *CHWriter {
	w := &CHWriter{
		conn: conn, flushSize: 10_000, flushEvery: 3 * time.Second,
		maxPending: 500_000,
		notify:     make(chan struct{}),
		kick:       make(chan struct{}, 1),
	}
	w.insertFn = w.insert
	return w
}

// Add buffers events and returns a watermark for WaitFlushed. The buffer
// is strictly FIFO (failed flushes requeue at the front), so a single
// counter pair tracks what has landed.
func (w *CHWriter) Add(events []PriceEvent) uint64 {
	w.mu.Lock()
	w.rows = append(w.rows, events...)
	w.enqSeq += uint64(len(events))
	w.dropOverflowLocked()
	wm := w.enqSeq
	n := len(w.rows)
	w.mu.Unlock()
	metrics.CHPending.Set(float64(n))
	if n >= w.flushSize {
		w.kickFlush()
	}
	return wm
}

// WaitFlushed blocks until all rows enqueued at or below wm have been
// written to ClickHouse (or dropped at the cap), or ctx ends.
func (w *CHWriter) WaitFlushed(ctx context.Context, wm uint64) error {
	for {
		w.mu.Lock()
		if w.flushedSeq >= wm {
			w.mu.Unlock()
			return nil
		}
		ch := w.notify
		w.mu.Unlock()
		w.kickFlush()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

func (w *CHWriter) kickFlush() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// Run flushes on a timer (or a kick from Add/WaitFlushed) until ctx ends,
// then makes one final drain attempt on its own deadline: the canceled
// ctx cannot carry the last flush.
func (w *CHWriter) Run(ctx context.Context) {
	t := time.NewTicker(w.flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			w.flush(dctx)
			cancel()
			return
		case <-t.C:
			w.flush(ctx)
		case <-w.kick:
			w.flush(ctx)
		}
	}
}

func (w *CHWriter) flush(ctx context.Context) {
	w.mu.Lock()
	rows := w.rows
	w.rows = nil
	w.mu.Unlock()
	if len(rows) == 0 {
		return
	}

	fctx, cancel := context.WithTimeout(ctx, time.Minute)
	err := w.insertFn(fctx, rows)
	cancel()
	if err != nil {
		log.Printf("chwriter: flush of %d rows failed, requeued: %v", len(rows), err)
		metrics.CHFlushes.WithLabelValues("error").Inc()
		w.mu.Lock()
		w.rows = append(rows, w.rows...)
		w.dropOverflowLocked()
		metrics.CHPending.Set(float64(len(w.rows)))
		w.mu.Unlock()
		return
	}
	metrics.CHFlushes.WithLabelValues("ok").Inc()
	metrics.CHRowsWritten.Add(float64(len(rows)))
	w.mu.Lock()
	w.flushedSeq += uint64(len(rows))
	w.advanceLocked()
	n := len(w.rows)
	w.mu.Unlock()
	metrics.CHPending.Set(float64(n))
}

// dropOverflowLocked trims the oldest buffered rows past maxPending. The
// dropped rows count as "handled" for the watermark so their waiters do
// not block forever on data that will never land.
func (w *CHWriter) dropOverflowLocked() {
	over := len(w.rows) - w.maxPending
	if over <= 0 {
		return
	}
	w.rows = w.rows[over:]
	w.flushedSeq += uint64(over)
	w.advanceLocked()
	metrics.CHDropped.Add(float64(over))
	log.Printf("chwriter: buffer cap %d exceeded, dropped %d oldest rows", w.maxPending, over)
}

func (w *CHWriter) advanceLocked() {
	close(w.notify)
	w.notify = make(chan struct{})
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

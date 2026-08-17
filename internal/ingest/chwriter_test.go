package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testWriter(insert func(ctx context.Context, rows []PriceEvent) error) *CHWriter {
	w := &CHWriter{
		flushSize: 10, flushEvery: time.Hour, maxPending: 100,
		notify: make(chan struct{}), kick: make(chan struct{}, 1),
	}
	w.insertFn = insert
	return w
}

func events(n int) []PriceEvent {
	out := make([]PriceEvent, n)
	for i := range out {
		out[i] = PriceEvent{ProductID: int64(i), RetailerID: 1, PriceCents: 999}
	}
	return out
}

func TestWaitFlushedReleasesAfterFlush(t *testing.T) {
	var inserted int
	w := testWriter(func(_ context.Context, rows []PriceEvent) error {
		inserted += len(rows)
		return nil
	})

	wm := w.Add(events(5))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.WaitFlushed(ctx, wm); err == nil {
		t.Fatal("WaitFlushed returned before any flush")
	}

	w.flush(context.Background())
	if err := w.WaitFlushed(context.Background(), wm); err != nil {
		t.Fatalf("WaitFlushed after flush: %v", err)
	}
	if inserted != 5 {
		t.Fatalf("inserted %d rows, want 5", inserted)
	}
}

func TestFailedFlushRequeuesAndHoldsWatermark(t *testing.T) {
	fail := true
	w := testWriter(func(_ context.Context, rows []PriceEvent) error {
		if fail {
			return errors.New("clickhouse down")
		}
		return nil
	})

	wm := w.Add(events(5))
	w.flush(context.Background())
	if w.Pending() != 5 {
		t.Fatalf("failed flush should requeue: pending %d", w.Pending())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.WaitFlushed(ctx, wm); err == nil {
		t.Fatal("watermark advanced past a failed flush")
	}

	fail = false
	w.flush(context.Background())
	if err := w.WaitFlushed(context.Background(), wm); err != nil {
		t.Fatalf("WaitFlushed after recovery: %v", err)
	}
	if w.Pending() != 0 {
		t.Fatalf("pending %d after successful flush", w.Pending())
	}
}

func TestBufferCapDropsOldestAndReleasesWaiters(t *testing.T) {
	w := testWriter(func(_ context.Context, rows []PriceEvent) error {
		return errors.New("clickhouse down")
	})

	early := w.Add(events(60))
	w.Add(events(60)) // 120 > maxPending 100: 20 oldest dropped
	if w.Pending() != 100 {
		t.Fatalf("pending %d, want cap 100", w.Pending())
	}
	// The 20 dropped rows cover watermark 20; waiters at or below it must
	// not block forever on data that will never land.
	if err := w.WaitFlushed(context.Background(), 20); err != nil {
		t.Fatalf("WaitFlushed under drop watermark: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.WaitFlushed(ctx, early); err == nil {
		t.Fatal("watermark advanced past rows still buffered")
	}
}

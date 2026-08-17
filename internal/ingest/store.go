package ingest

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store owns all Postgres writes. Product ids are cached in memory once
// resolved; the sku_norm space is bounded by the catalog (~450k), so the
// cache is too.
type Store struct {
	pool *pgxpool.Pool

	mu     sync.Mutex
	ids    map[string]int64
	spikes map[spikeKey]int
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, ids: make(map[string]int64), spikes: make(map[spikeKey]int)}
}

// spikeAcceptAfter: a corrupt first observation would otherwise wedge the
// offer forever, because every correction is 20× away from the held junk
// and gets rejected. After this many consecutive spike rejections the new
// price is accepted: a real feed bug is a blip, a persistent "spike" is
// the truth. The streak lives in memory; a restart just asks the feed for
// a few more confirmations.
const spikeAcceptAfter = 3

type spikeKey struct {
	retailer int16
	product  int64
}

// noteSpike records one spike rejection and reports whether the streak
// has run long enough to accept the new price instead.
func (s *Store) noteSpike(rid int16, pid int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := spikeKey{rid, pid}
	s.spikes[k]++
	if s.spikes[k] >= spikeAcceptAfter {
		delete(s.spikes, k)
		return true
	}
	return false
}

func (s *Store) clearSpike(rid int16, pid int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.spikes, spikeKey{rid, pid})
}

func (s *Store) EnsureRetailer(ctx context.Context, slug, name string) (int16, error) {
	var id int16
	err := s.pool.QueryRow(ctx, `
		INSERT INTO retailers (slug, name) VALUES ($1, $2)
		ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
		RETURNING id`, slug, name).Scan(&id)
	return id, err
}

type AcceptedRow struct {
	ProductID  int64
	PriceCents int64
	InStock    bool
	UpdatedAt  time.Time
}

type UpsertResult struct {
	Accepted []AcceptedRow
	// Spikes are items rejected by the relative price check, with the
	// held price for the quarantine record.
	Spikes []SpikeReject
	// SpikeOverrides counts prices accepted because they persisted
	// through spikeAcceptAfter consecutive spike rejections.
	SpikeOverrides int
	// StaleSkipped counts rows dropped by the updated_at guard.
	StaleSkipped int
	// NoTitle holds items for unknown products that carried no title, so
	// no product row could be created; the caller quarantines them.
	NoTitle []Normalized
}

type SpikeReject struct {
	Item      Normalized
	HeldCents int64
}

// UpsertBatch resolves product ids (creating products as needed), applies
// the spike check against held prices, and upserts offers with a
// last-write-wins guard on updated_at. One batch, three round trips.
func (s *Store) UpsertBatch(ctx context.Context, retailerID int16, items []Normalized) (UpsertResult, error) {
	var res UpsertResult
	if len(items) == 0 {
		return res, nil
	}

	// In-batch dedupe: last update per sku_norm wins by UpdatedAt.
	bySKU := make(map[string]Normalized, len(items))
	for _, it := range items {
		if prev, ok := bySKU[it.SKUNorm]; !ok || it.UpdatedAt.After(prev.UpdatedAt) {
			bySKU[it.SKUNorm] = it
		}
	}

	ids, err := s.resolveProducts(ctx, bySKU)
	if err != nil {
		return res, err
	}

	held, err := s.heldPrices(ctx, retailerID, ids)
	if err != nil {
		return res, err
	}

	type offerRow struct {
		pid int64
		it  Normalized
	}
	var accepted []offerRow
	for sku, it := range bySKU {
		pid, ok := ids[sku]
		if !ok {
			// Unknown product and no title to create one from.
			res.NoTitle = append(res.NoTitle, it)
			continue
		}
		if h, ok := held[pid]; ok && SpikeSuspect(h, it.PriceCents) {
			if !s.noteSpike(retailerID, pid) {
				res.Spikes = append(res.Spikes, SpikeReject{Item: it, HeldCents: h})
				continue
			}
			res.SpikeOverrides++
		} else {
			s.clearSpike(retailerID, pid)
		}
		accepted = append(accepted, offerRow{pid, it})
	}
	if len(accepted) == 0 {
		return res, nil
	}
	// Concurrent workers upsert overlapping product sets; a deterministic
	// row order keeps their lock acquisition aligned and deadlock-free.
	slices.SortFunc(accepted, func(a, b offerRow) int { return cmp.Compare(a.pid, b.pid) })

	var (
		pids    []int64
		prices  []int64
		urls    []string
		stocks  []bool
		updated []time.Time
		cats    []string
		brands  []string
	)
	for _, r := range accepted {
		pids = append(pids, r.pid)
		prices = append(prices, r.it.PriceCents)
		urls = append(urls, r.it.URL)
		stocks = append(stocks, r.it.InStock)
		updated = append(updated, r.it.UpdatedAt)
		cats = append(cats, r.it.Category)
		brands = append(brands, r.it.Brand)
		res.Accepted = append(res.Accepted, AcceptedRow{ProductID: r.pid, PriceCents: r.it.PriceCents, InStock: r.it.InStock, UpdatedAt: r.it.UpdatedAt})
	}

	// category/brand are denormalized for the collection indexes; an
	// update that lacks them keeps what the row already has.
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO offers (product_id, retailer_id, price_cents, url, in_stock, updated_at, category, brand)
		SELECT u.pid, $1, u.price, u.url, u.stock, u.updated, u.cat, u.brand
		FROM unnest($2::bigint[], $3::bigint[], $4::text[], $5::boolean[], $6::timestamptz[], $7::text[], $8::text[])
		     AS u(pid, price, url, stock, updated, cat, brand)
		ON CONFLICT (product_id, retailer_id) DO UPDATE SET
			price_cents = EXCLUDED.price_cents,
			url         = EXCLUDED.url,
			in_stock    = EXCLUDED.in_stock,
			updated_at  = EXCLUDED.updated_at,
			category    = COALESCE(NULLIF(EXCLUDED.category, ''), offers.category),
			brand       = COALESCE(NULLIF(EXCLUDED.brand, ''), offers.brand)
		WHERE offers.updated_at <= EXCLUDED.updated_at`,
		retailerID, pids, prices, urls, stocks, updated, cats, brands)
	if err != nil {
		return res, fmt.Errorf("upsert offers: %w", err)
	}
	res.StaleSkipped = len(pids) - int(tag.RowsAffected())
	return res, nil
}

// resolveProducts maps sku_norm to product id, inserting products that do
// not exist yet. A sku left unmapped afterward is an unknown product whose
// item carried no title: there was nothing to create a product from.
func (s *Store) resolveProducts(ctx context.Context, bySKU map[string]Normalized) (map[string]int64, error) {
	out := make(map[string]int64, len(bySKU))

	s.mu.Lock()
	var misses []string
	for sku := range bySKU {
		if id, ok := s.ids[sku]; ok {
			out[sku] = id
		} else {
			misses = append(misses, sku)
		}
	}
	s.mu.Unlock()
	if len(misses) == 0 {
		return out, nil
	}
	sort.Strings(misses) // deterministic insert order across workers

	var skus, titles, brands, cats, attrs []string
	for _, sku := range misses {
		it := bySKU[sku]
		if it.Title == "" {
			continue // may still exist in the DB from another retailer
		}
		a, _ := json.Marshal(it.Attrs)
		skus = append(skus, sku)
		titles = append(titles, it.Title)
		brands = append(brands, it.Brand)
		cats = append(cats, it.Category)
		attrs = append(attrs, string(a))
	}
	if len(skus) > 0 {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO products (sku_norm, title, brand, category, attrs)
			SELECT u.sku, u.title, u.brand, u.cat, u.attrs::jsonb
			FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
			     AS u(sku, title, brand, cat, attrs)
			ON CONFLICT (sku_norm) DO NOTHING`,
			skus, titles, brands, cats, attrs)
		if err != nil {
			return nil, fmt.Errorf("insert products: %w", err)
		}
	}

	rows, err := s.pool.Query(ctx, `SELECT sku_norm, id FROM products WHERE sku_norm = ANY($1)`, misses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	s.mu.Lock()
	for rows.Next() {
		var sku string
		var id int64
		if err := rows.Scan(&sku, &id); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.ids[sku] = id
		out[sku] = id
	}
	s.mu.Unlock()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) heldPrices(ctx context.Context, retailerID int16, ids map[string]int64) (map[int64]int64, error) {
	pids := make([]int64, 0, len(ids))
	for _, id := range ids {
		pids = append(pids, id)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT product_id, price_cents FROM offers WHERE retailer_id = $1 AND product_id = ANY($2)`,
		retailerID, pids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	held := make(map[int64]int64)
	for rows.Next() {
		var pid, cents int64
		if err := rows.Scan(&pid, &cents); err != nil {
			return nil, err
		}
		held[pid] = cents
	}
	return held, rows.Err()
}

type QuarantineRow struct {
	Reason string
	Raw    json.RawMessage
}

func (s *Store) Quarantine(ctx context.Context, retailerID int16, rows []QuarantineRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		raw := r.Raw
		if len(raw) == 0 {
			raw = json.RawMessage("{}")
		}
		batch.Queue(`INSERT INTO quarantine (retailer_id, reason, raw) VALUES ($1, $2, $3)`,
			retailerID, r.Reason, raw)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

func (s *Store) LoadCursor(ctx context.Context, retailerID int16) (string, error) {
	var cur string
	err := s.pool.QueryRow(ctx,
		`SELECT cursor FROM ingest_cursors WHERE retailer_id = $1`, retailerID).Scan(&cur)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return cur, err
}

func (s *Store) SaveCursor(ctx context.Context, retailerID int16, cursor string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ingest_cursors (retailer_id, cursor, updated_at) VALUES ($1, $2, now())
		ON CONFLICT (retailer_id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
		retailerID, cursor)
	return err
}

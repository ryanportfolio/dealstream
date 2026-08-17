package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// Collections are rule-based: a jsonb rule row filters the live catalog,
// so membership updates itself as prices and stock move. Results are
// cached briefly; the rules are the source of truth, not a member table.
type CollectionRules struct {
	Category      string `json:"category,omitempty"`
	Brand         string `json:"brand,omitempty"`
	MaxPriceCents int64  `json:"max_price_cents,omitempty"`
	MinOffers     int    `json:"min_offers,omitempty"`
}

type CollectionSummary struct {
	Slug  string `json:"slug"`
	Title string `json:"title"`
}

func (s *Server) handleCollections(w http.ResponseWriter, r *http.Request) {
	// No colon in this key: item keys below are "collection:<slug>" with
	// a caller-supplied slug, so the index key stays out of any prefix a
	// slug could ever be spliced into.
	data, err := s.Cache.GetOrFill(r.Context(), "collections-index", 5*time.Minute,
		func(ctx context.Context) (any, error) {
			rows, err := s.PG.Query(ctx, `SELECT slug, title FROM collections ORDER BY slug`)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			out := []CollectionSummary{}
			for rows.Next() {
				var c CollectionSummary
				if err := rows.Scan(&c.Slug, &c.Title); err != nil {
					return nil, err
				}
				out = append(out, c)
			}
			return map[string]any{"collections": out}, rows.Err()
		})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "collections unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (s *Server) handleCollection(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	// Rules change on deploy, prices drift by the minute; five minutes of
	// staleness is a fair trade for absorbing the cold-load cost.
	data, err := s.Cache.GetOrFill(r.Context(), "collection:"+slug, 5*time.Minute,
		func(ctx context.Context) (any, error) {
			return s.loadCollection(ctx, slug)
		})
	if err == errNotFound {
		writeError(w, http.StatusNotFound, "no such collection")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "collection unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// Collection membership is computed by walking the in-stock price index
// in ascending order and stopping early, never by aggregating the whole
// catalog: a rule with no category filter would otherwise group offers
// for all 400k+ products on every cold load. The first offer seen for a
// product in price order is that product's best price.
const collectionSize = 100
const collectionScanCap = 2500

func (s *Server) loadCollection(ctx context.Context, slug string) (any, error) {
	var title string
	var rules CollectionRules
	err := s.PG.QueryRow(ctx,
		`SELECT title, rules FROM collections WHERE slug = $1`, slug).Scan(&title, &rules)
	// Missing row is a 404; any other failure is a 500, not an empty
	// catalog.
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}

	maxPrice := rules.MaxPriceCents
	if maxPrice == 0 {
		maxPrice = 1 << 40
	}
	// Filters hit the denormalized offer columns so the composite
	// partial indexes serve the walk; products is only joined for titles.
	rows, err := s.PG.Query(ctx, `
		SELECT o.product_id, o.price_cents, p.title, p.brand, p.category
		FROM offers o
		JOIN products p ON p.id = o.product_id
		WHERE o.in_stock AND o.price_cents <= $1
		  AND ($2 = '' OR o.category = $2)
		  AND ($3 = '' OR o.brand = $3)
		ORDER BY o.price_cents
		LIMIT $4`, maxPrice, rules.Category, rules.Brand, collectionScanCap)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// A min_offers rule filters on counts fetched after the walk, so the
	// walk must keep every candidate in the scan window: stopping at
	// collectionSize first would under-fill the page whenever the filter
	// rejects some of the earliest hits.
	target := collectionSize
	if rules.MinOffers > 1 {
		target = collectionScanCap
	}
	seen := map[int64]int{}
	items := []SearchResult{}
	for rows.Next() {
		var (
			id, price int64
			sr        SearchResult
		)
		if err := rows.Scan(&id, &price, &sr.Title, &sr.Brand, &sr.Category); err != nil {
			return nil, err
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if len(items) == target {
			break
		}
		seen[id] = len(items)
		sr.ID, sr.BestCents = id, &price
		items = append(items, sr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// One bounded query fills in offer counts for the selected products.
	if len(items) > 0 {
		ids := make([]int64, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		crows, err := s.PG.Query(ctx,
			`SELECT product_id, count(*) FROM offers WHERE product_id = ANY($1) GROUP BY product_id`, ids)
		if err != nil {
			return nil, err
		}
		defer crows.Close()
		for crows.Next() {
			var id int64
			var n int
			if err := crows.Scan(&id, &n); err != nil {
				return nil, err
			}
			items[seen[id]].OfferCount = n
		}
		if err := crows.Err(); err != nil {
			return nil, err
		}
	}

	if rules.MinOffers > 1 {
		kept := items[:0]
		for _, it := range items {
			if it.OfferCount >= rules.MinOffers {
				kept = append(kept, it)
			}
		}
		items = kept
	}
	if len(items) > collectionSize {
		items = items[:collectionSize]
	}
	return map[string]any{"slug": slug, "title": title, "items": items}, nil
}

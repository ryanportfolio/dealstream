package api

import (
	"context"
	"net/http"
	"time"
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
	data, err := s.Cache.GetOrFill(r.Context(), "collections:index", time.Minute,
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
	data, err := s.Cache.GetOrFill(r.Context(), "collection:"+slug, time.Minute,
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

func (s *Server) loadCollection(ctx context.Context, slug string) (any, error) {
	var title string
	var rules CollectionRules
	err := s.PG.QueryRow(ctx,
		`SELECT title, rules FROM collections WHERE slug = $1`, slug).Scan(&title, &rules)
	if err != nil {
		return nil, errNotFound
	}

	minOffers := max(rules.MinOffers, 1)
	sql := `
		SELECT p.id, p.title, p.brand, p.category,
		       min(o.price_cents) FILTER (WHERE o.in_stock) AS best_price,
		       count(o.*) AS offer_count
		FROM products p
		JOIN offers o ON o.product_id = p.id
		WHERE ($1 = '' OR p.category = $1)
		  AND ($2 = '' OR p.brand = $2)
		GROUP BY p.id
		HAVING min(o.price_cents) FILTER (WHERE o.in_stock) IS NOT NULL
		   AND ($3 = 0 OR min(o.price_cents) FILTER (WHERE o.in_stock) <= $3)
		   AND count(o.*) >= $4
		ORDER BY best_price
		LIMIT 100`
	rows, err := s.PG.Query(ctx, sql, rules.Category, rules.Brand, rules.MaxPriceCents, minOffers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []SearchResult{}
	for rows.Next() {
		var sr SearchResult
		if err := rows.Scan(&sr.ID, &sr.Title, &sr.Brand, &sr.Category, &sr.BestCents, &sr.OfferCount); err != nil {
			return nil, err
		}
		items = append(items, sr)
	}
	return map[string]any{"slug": slug, "title": title, "items": items}, rows.Err()
}

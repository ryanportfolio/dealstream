package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/ryanportfolio/dealstream/internal/metrics"
)

// A deal is a price dislocation: one retailer selling well below the
// median of the others, right now. That definition needs no history, so
// it works from day one; discount-vs-30-day-median can join it once
// enough history accumulates.
type Deal struct {
	ProductID   int64  `json:"product_id"`
	Title       string `json:"title"`
	Brand       string `json:"brand"`
	Category    string `json:"category"`
	BestCents   int64  `json:"best_price_cents"`
	MedianCents int64  `json:"median_price_cents"`
	Retailer    string `json:"retailer"`
	SavingsPct  int    `json:"savings_pct"`
}

const dealsKey = "deals:top"

func (s *Server) handleDeals(w http.ResponseWriter, r *http.Request) {
	limit := clamp(r.URL.Query().Get("limit"), 50, 500)

	data, err := s.Redis.Get(r.Context(), dealsKey).Bytes()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "deals not materialized yet")
		return
	}
	s.Cache.Hits.Add(1)

	var deals []Deal
	if err := json.Unmarshal(data, &deals); err != nil {
		writeError(w, http.StatusInternalServerError, "bad deals payload")
		return
	}
	if len(deals) > limit {
		deals = deals[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"deals": deals})
}

// RefreshDealsLoop rematerializes the deals list on an interval. The
// query scans all offers grouped per product; that cost is paid once per
// interval, never per request.
func (s *Server) RefreshDealsLoop(ctx context.Context, every time.Duration) {
	for {
		start := time.Now()
		n, err := s.refreshDeals(ctx)
		if err != nil {
			log.Printf("deals: refresh failed: %v", err)
			metrics.DealsRefresh.WithLabelValues("error").Observe(time.Since(start).Seconds())
		} else {
			log.Printf("deals: materialized %d in %s", n, time.Since(start).Round(time.Millisecond))
			metrics.DealsRefresh.WithLabelValues("ok").Observe(time.Since(start).Seconds())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

func (s *Server) refreshDeals(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	rows, err := s.PG.Query(ctx, `
		WITH spread AS (
			SELECT o.product_id,
			       min(o.price_cents) AS best,
			       percentile_cont(0.5) WITHIN GROUP (ORDER BY o.price_cents) AS median,
			       count(*) AS offers
			FROM offers o
			WHERE o.in_stock
			GROUP BY o.product_id
			HAVING count(*) >= 3
		)
		SELECT sp.product_id, p.title, p.brand, p.category,
		       sp.best, sp.median::bigint,
		       (SELECT r.slug FROM offers o2 JOIN retailers r ON r.id = o2.retailer_id
		        WHERE o2.product_id = sp.product_id AND o2.price_cents = sp.best AND o2.in_stock
		        LIMIT 1) AS retailer,
		       round(100 * (sp.median - sp.best) / sp.median)::int AS savings_pct
		FROM spread sp
		JOIN products p ON p.id = sp.product_id
		WHERE sp.median > 0 AND (sp.median - sp.best) / sp.median >= 0.15
		ORDER BY savings_pct DESC, sp.best
		LIMIT 500`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	deals := []Deal{}
	for rows.Next() {
		var d Deal
		if err := rows.Scan(&d.ProductID, &d.Title, &d.Brand, &d.Category,
			&d.BestCents, &d.MedianCents, &d.Retailer, &d.SavingsPct); err != nil {
			return 0, err
		}
		deals = append(deals, d)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	data, err := json.Marshal(deals)
	if err != nil {
		return 0, err
	}
	// No TTL: stale deals beat no deals if a refresh cycle fails.
	if err := s.Redis.Set(ctx, dealsKey, data, 0).Err(); err != nil {
		return 0, err
	}
	return len(deals), nil
}

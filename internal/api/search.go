package api

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type SearchResult struct {
	ID         int64  `json:"id"`
	Title      string `json:"title"`
	Brand      string `json:"brand"`
	Category   string `json:"category"`
	BestCents  *int64 `json:"best_price_cents"`
	OfferCount int    `json:"offer_count"`
}

// candidateCap bounds how many matched products a single search will
// aggregate offers for. Price sorting is exact within this set. The
// original unbounded query aggregated offers for every match; a broad
// term matched thousands of products and one such query monopolized a
// connection for seconds.
const candidateCap = 1000

// handleSearch is full-text over titles plus structured filters, cached
// for 30 seconds. Price filters and sorting apply to the best in-stock
// offer of each candidate.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	text := strings.TrimSpace(q.Get("q"))
	limit := clamp(q.Get("limit"), 20, 100)
	offset := clamp(q.Get("offset"), 0, 10_000)

	key := strings.Join([]string{
		"search", text, q.Get("category"), q.Get("brand"),
		q.Get("min_price"), q.Get("max_price"), q.Get("sort"),
		strconv.Itoa(limit), strconv.Itoa(offset),
	}, "\x1f")

	data, err := s.Cache.GetOrFill(r.Context(), key, 30*time.Second,
		func(ctx context.Context) (any, error) {
			return s.runSearch(ctx, q, text, limit, offset)
		})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (s *Server) runSearch(ctx context.Context, q url.Values, text string, limit, offset int) (any, error) {
	where := []string{"TRUE"}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if text != "" {
		where = append(where, "to_tsvector('simple', p.title) @@ websearch_to_tsquery('simple', "+arg(text)+")")
	}
	if c := q.Get("category"); c != "" {
		where = append(where, "p.category = "+arg(c))
	}
	if b := q.Get("brand"); b != "" {
		where = append(where, "p.brand = "+arg(b))
	}

	having := []string{"min(o.price_cents) FILTER (WHERE o.in_stock) IS NOT NULL"}
	if v, err := strconv.ParseInt(q.Get("min_price"), 10, 64); err == nil {
		having = append(having, "min(o.price_cents) FILTER (WHERE o.in_stock) >= "+arg(v))
	}
	if v, err := strconv.ParseInt(q.Get("max_price"), 10, 64); err == nil {
		having = append(having, "min(o.price_cents) FILTER (WHERE o.in_stock) <= "+arg(v))
	}

	order := "id"
	switch q.Get("sort") {
	case "price_asc":
		order = "best_price ASC"
	case "price_desc":
		order = "best_price DESC"
	}

	sql := `
		WITH hits AS (
			SELECT p.id, p.title, p.brand, p.category
			FROM products p
			WHERE ` + strings.Join(where, " AND ") + `
			ORDER BY p.id
			LIMIT ` + arg(candidateCap) + `
		)
		SELECT h.id, h.title, h.brand, h.category,
		       min(o.price_cents) FILTER (WHERE o.in_stock) AS best_price,
		       count(o.*) AS offer_count
		FROM hits h
		JOIN offers o ON o.product_id = h.id
		GROUP BY h.id, h.title, h.brand, h.category
		HAVING ` + strings.Join(having, " AND ") + `
		ORDER BY ` + order + `
		LIMIT ` + arg(limit) + ` OFFSET ` + arg(offset)

	rows, err := s.PG.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []SearchResult{}
	for rows.Next() {
		var sr SearchResult
		if err := rows.Scan(&sr.ID, &sr.Title, &sr.Brand, &sr.Category, &sr.BestCents, &sr.OfferCount); err != nil {
			return nil, err
		}
		results = append(results, sr)
	}
	return map[string]any{"results": results, "limit": limit, "offset": offset, "capped_at": candidateCap}, rows.Err()
}

func clamp(s string, def, max int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return def
	}
	return min(v, max)
}

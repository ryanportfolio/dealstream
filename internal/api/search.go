package api

import (
	"net/http"
	"strconv"
	"strings"
)

type SearchResult struct {
	ID         int64  `json:"id"`
	Title      string `json:"title"`
	Brand      string `json:"brand"`
	Category   string `json:"category"`
	BestCents  *int64 `json:"best_price_cents"`
	OfferCount int    `json:"offer_count"`
}

// handleSearch is full-text over titles plus structured filters. Price
// filters apply to the best in-stock offer, which is also what sorting
// by price uses.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	text := strings.TrimSpace(q.Get("q"))

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

	order := "p.id"
	switch q.Get("sort") {
	case "price_asc":
		order = "best_price ASC"
	case "price_desc":
		order = "best_price DESC"
	}

	limit := clamp(q.Get("limit"), 20, 100)
	offset := clamp(q.Get("offset"), 0, 10_000)

	sql := `
		SELECT p.id, p.title, p.brand, p.category,
		       min(o.price_cents) FILTER (WHERE o.in_stock) AS best_price,
		       count(o.*) AS offer_count
		FROM products p
		JOIN offers o ON o.product_id = p.id
		WHERE ` + strings.Join(where, " AND ") + `
		GROUP BY p.id
		HAVING ` + strings.Join(having, " AND ") + `
		ORDER BY ` + order + `
		LIMIT ` + arg(limit) + ` OFFSET ` + arg(offset)

	rows, err := s.PG.Query(r.Context(), sql, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search failed")
		return
	}
	defer rows.Close()

	results := []SearchResult{}
	for rows.Next() {
		var sr SearchResult
		if err := rows.Scan(&sr.ID, &sr.Title, &sr.Brand, &sr.Category, &sr.BestCents, &sr.OfferCount); err != nil {
			writeError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		results = append(results, sr)
	}
	if rows.Err() != nil {
		writeError(w, http.StatusInternalServerError, "search failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "limit": limit, "offset": offset})
}

func clamp(s string, def, max int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return def
	}
	return min(v, max)
}

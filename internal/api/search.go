package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
// aggregate offers for. The original unbounded query aggregated offers
// for every match; a broad term matched thousands of products and one
// such query monopolized a connection for seconds. Candidates are the
// best-ranked matches, price sorting is exact within them, and the
// response says when the cap truncated the match set.
const candidateCap = 1000

// handleSearch is full-text over titles plus structured filters, cached
// for 30 seconds. Price filters and sorting apply to the best in-stock
// offer of each candidate.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	text := strings.TrimSpace(q.Get("q"))
	limit := clamp(q.Get("limit"), 20, 100)
	offset := clamp(q.Get("offset"), 0, 10_000)

	// The key hashes the raw parameters: user text joined with any
	// separator could be forged to collide with another query's key.
	sum := sha256.Sum256([]byte(strings.Join([]string{
		text, q.Get("category"), q.Get("brand"),
		q.Get("min_price"), q.Get("max_price"), q.Get("sort"),
		strconv.Itoa(limit), strconv.Itoa(offset),
	}, "\x1f")))
	key := "search:" + hex.EncodeToString(sum[:])

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

	// Candidate order decides which matches survive the cap. For text
	// queries that is relevance rank: an id order would keep an arbitrary
	// lowest-id slice of a broad match and price-sort only that.
	candOrder := "p.id"
	if text != "" {
		ta := arg(text)
		where = append(where, "to_tsvector('simple', p.title) @@ websearch_to_tsquery('simple', "+ta+")")
		candOrder = "ts_rank(to_tsvector('simple', p.title), websearch_to_tsquery('simple', " + ta + ")) DESC, p.id"
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
			ORDER BY ` + candOrder + `
			LIMIT ` + arg(candidateCap) + `
		), n AS (SELECT count(*) AS c FROM hits)
		SELECT h.id, h.title, h.brand, h.category,
		       min(o.price_cents) FILTER (WHERE o.in_stock) AS best_price,
		       count(o.*) AS offer_count, n.c
		FROM hits h CROSS JOIN n
		JOIN offers o ON o.product_id = h.id
		GROUP BY h.id, h.title, h.brand, h.category, n.c
		HAVING ` + strings.Join(having, " AND ") + `
		ORDER BY ` + order + `
		LIMIT ` + arg(limit) + ` OFFSET ` + arg(offset)

	rows, err := s.PG.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []SearchResult{}
	var hitCount int64
	for rows.Next() {
		var sr SearchResult
		if err := rows.Scan(&sr.ID, &sr.Title, &sr.Brand, &sr.Category, &sr.BestCents, &sr.OfferCount, &hitCount); err != nil {
			return nil, err
		}
		results = append(results, sr)
	}
	// truncated says the match set outran the candidate cap, so results
	// beyond it exist but are not price-sorted in. Only known when the
	// page is non-empty; capped_at documents the bound either way.
	return map[string]any{
		"results": results, "limit": limit, "offset": offset,
		"capped_at": candidateCap, "truncated": hitCount == candidateCap,
	}, rows.Err()
}

func clamp(s string, def, max int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return def
	}
	return min(v, max)
}

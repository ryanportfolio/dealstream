package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// handleSimilar is a pgvector KNN over attribute-hash embeddings: cheap,
// deterministic, and explainable. Results carry the live best offer so
// the response is usable as a "similar deals" rail directly.
func (s *Server) handleSimilar(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad product id")
		return
	}

	data, err := s.Cache.GetOrFill(r.Context(), "similar:"+r.PathValue("id"), 5*time.Minute,
		func(ctx context.Context) (any, error) {
			return s.loadSimilar(ctx, id)
		})
	if err == errNotFound {
		writeError(w, http.StatusNotFound, "no such product or not embedded yet")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "similar failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (s *Server) loadSimilar(ctx context.Context, id int64) (any, error) {
	var embedded bool
	err := s.PG.QueryRow(ctx,
		`SELECT embedding IS NOT NULL FROM products WHERE id = $1`, id).Scan(&embedded)
	// Missing row or missing embedding is a 404; anything else (database
	// down, timeout) is a 500, not an empty catalog.
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	if !embedded {
		return nil, errNotFound
	}

	rows, err := s.PG.Query(ctx, `
		WITH target AS (SELECT embedding FROM products WHERE id = $1),
		neighbors AS (
			SELECT p.id, p.title, p.brand, p.category,
			       p.embedding <=> (SELECT embedding FROM target) AS distance
			FROM products p
			WHERE p.id <> $1 AND p.embedding IS NOT NULL
			ORDER BY p.embedding <=> (SELECT embedding FROM target)
			LIMIT 12
		)
		SELECT n.id, n.title, n.brand, n.category, round(n.distance::numeric, 4),
		       min(o.price_cents) FILTER (WHERE o.in_stock),
		       count(o.*)
		FROM neighbors n
		JOIN offers o ON o.product_id = n.id
		GROUP BY n.id, n.title, n.brand, n.category, n.distance
		ORDER BY n.distance`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type similar struct {
		SearchResult
		Distance float64 `json:"distance"`
	}
	items := []similar{}
	for rows.Next() {
		var it similar
		if err := rows.Scan(&it.ID, &it.Title, &it.Brand, &it.Category, &it.Distance,
			&it.BestCents, &it.OfferCount); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return map[string]any{"product_id": id, "similar": items}, rows.Err()
}

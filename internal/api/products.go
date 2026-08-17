package api

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

type Offer struct {
	Retailer   string    `json:"retailer"`
	PriceCents int64     `json:"price_cents"`
	InStock    bool      `json:"in_stock"`
	URL        string    `json:"url"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Product struct {
	ID       int64             `json:"id"`
	SKU      string            `json:"sku"`
	Title    string            `json:"title"`
	Brand    string            `json:"brand"`
	Category string            `json:"category"`
	Attrs    map[string]string `json:"attrs"`
	Offers   []Offer           `json:"offers"`
}

func (s *Server) handleProduct(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad product id")
		return
	}

	data, err := s.Cache.GetOrFill(r.Context(), "product:"+r.PathValue("id"), 30*time.Second,
		func(ctx context.Context) (any, error) {
			return s.loadProduct(ctx, id)
		})
	if err == errNotFound {
		writeError(w, http.StatusNotFound, "no such product")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

var errNotFound = &notFoundError{}

type notFoundError struct{}

func (*notFoundError) Error() string { return "not found" }

func (s *Server) loadProduct(ctx context.Context, id int64) (*Product, error) {
	var p Product
	err := s.PG.QueryRow(ctx,
		`SELECT id, sku_norm, title, brand, category, attrs FROM products WHERE id = $1`, id).
		Scan(&p.ID, &p.SKU, &p.Title, &p.Brand, &p.Category, &p.Attrs)
	if err != nil {
		return nil, errNotFound
	}

	rows, err := s.PG.Query(ctx, `
		SELECT r.slug, o.price_cents, o.in_stock, o.url, o.updated_at
		FROM offers o JOIN retailers r ON r.id = o.retailer_id
		WHERE o.product_id = $1
		ORDER BY o.price_cents`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var o Offer
		if err := rows.Scan(&o.Retailer, &o.PriceCents, &o.InStock, &o.URL, &o.UpdatedAt); err != nil {
			return nil, err
		}
		p.Offers = append(p.Offers, o)
	}
	return &p, rows.Err()
}

type HistoryPoint struct {
	Day       string `json:"day"`
	Retailer  string `json:"retailer"`
	MinCents  int64  `json:"min_cents"`
	MaxCents  int64  `json:"max_cents"`
	LastCents int64  `json:"last_cents"`
}

// handleHistory reads the ClickHouse daily rollup. Days is capped at a
// year; the rollup keeps this O(days × retailers) regardless of how many
// raw observations exist.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad product id")
		return
	}
	days := clamp(r.URL.Query().Get("days"), 30, 365)

	rows, err := s.CH.Query(r.Context(), `
		SELECT toString(day), retailer_id,
		       minMerge(min_price), maxMerge(max_price), argMaxMerge(last_price)
		FROM price_daily
		WHERE product_id = ? AND day >= today() - ?
		GROUP BY day, retailer_id
		ORDER BY day, retailer_id`, uint64(id), days)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "history query failed")
		return
	}
	defer rows.Close()

	names, err := s.retailerNames(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "retailers unavailable")
		return
	}

	points := []HistoryPoint{}
	for rows.Next() {
		var (
			day               string
			retailerID        uint16
			minP, maxP, lastP int64
		)
		if err := rows.Scan(&day, &retailerID, &minP, &maxP, &lastP); err != nil {
			writeError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		points = append(points, HistoryPoint{
			Day: day, Retailer: names[int16(retailerID)],
			MinCents: minP, MaxCents: maxP, LastCents: lastP,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"product_id": id, "days": days, "points": points})
}

// retailerNames is cached in process: eight rows that change only when a
// retailer is onboarded do not justify a database query per history
// request.
func (s *Server) retailerNames(ctx context.Context) (map[int16]string, error) {
	s.retailersMu.Lock()
	defer s.retailersMu.Unlock()
	if s.retailers != nil && time.Since(s.retailersAt) < 5*time.Minute {
		return s.retailers, nil
	}

	out := map[int16]string{}
	rows, err := s.PG.Query(ctx, `SELECT id, slug FROM retailers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int16
		var slug string
		if err := rows.Scan(&id, &slug); err != nil {
			return nil, err
		}
		out[id] = slug
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	s.retailers, s.retailersAt = out, time.Now()
	return out, nil
}

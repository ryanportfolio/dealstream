package feedgen

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	U       *Universe
	Streams map[string]*Stream
	// mess uses its own rng: mess should be random per response, unlike
	// the product universe, which must be deterministic.
	rng *rand.Rand
}

func NewServer(u *Universe, streams map[string]*Stream) *Server {
	return &Server{U: u, Streams: streams, rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))}
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /r/{slug}/catalog", s.withRetailer(s.handleCatalog))
	mux.HandleFunc("GET /r/{slug}/offers", s.withRetailer(s.handleOffers))
	mux.HandleFunc("GET /r/{slug}/health", s.withRetailer(s.handleHealth))
	mux.HandleFunc("POST /admin/retailers/{slug}/status", s.handleSetStatus)
	mux.HandleFunc("GET /admin/state", s.handleState)
	return mux
}

func (s *Server) withRetailer(next func(http.ResponseWriter, *http.Request, *Stream)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, ok := s.Streams[r.PathValue("slug")]
		if !ok {
			http.Error(w, "unknown retailer", http.StatusNotFound)
			return
		}
		switch st.Status() {
		case "dead":
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		case "degraded":
			time.Sleep(time.Duration(1500+s.rng.IntN(2500)) * time.Millisecond)
			if s.rng.Float64() < 0.10 {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
		}
		next(w, r, st)
	}
}

// handleCatalog pages the full carried catalog by product index. cursor is
// the next index to scan; a missing next_cursor means the sync is done.
func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request, st *Stream) {
	cursor, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
	limit := pageLimit(r, st.Cfg.PageSizeMax)

	items := make([]map[string]any, 0, limit)
	i := cursor
	for ; i < s.U.Size && len(items) < limit; i++ {
		if !s.U.Carried(i, st.Cfg.Slug, st.Cfg.CarryRate) {
			continue
		}
		cur := st.Current(i)
		items = s.appendItem(items, st, i, cur.PriceCents, cur.InStock, time.Now().UTC())
	}
	resp := map[string]any{"items": items}
	if i < s.U.Size {
		resp["next_cursor"] = i
	}
	writeJSON(w, resp)
}

// handleOffers serves the incremental change feed. since is the last seq
// the client processed; cursors older than the buffer get 410 and must
// full-resync via /catalog.
func (s *Server) handleOffers(w http.ResponseWriter, r *http.Request, st *Stream) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	limit := pageLimit(r, st.Cfg.PageSizeMax)

	updates, gone := st.Since(since, limit)
	if gone {
		http.Error(w, "cursor expired, full resync required", http.StatusGone)
		return
	}
	items := make([]map[string]any, 0, len(updates))
	next := since
	for _, u := range updates {
		items = s.appendItem(items, st, u.Product, u.PriceCents, u.InStock, u.At)
		next = u.Seq
	}
	writeJSON(w, map[string]any{"items": items, "next": next})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request, st *Stream) {
	writeJSON(w, map[string]any{"status": st.Status()})
}

func (s *Server) handleSetStatus(w http.ResponseWriter, r *http.Request) {
	st, ok := s.Streams[r.PathValue("slug")]
	if !ok {
		http.Error(w, "unknown retailer", http.StatusNotFound)
		return
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !validStatus(body.Status) {
		http.Error(w, "status must be active|degraded|dead", http.StatusBadRequest)
		return
	}
	st.SetStatus(body.Status)
	writeJSON(w, map[string]any{"slug": st.Cfg.Slug, "status": body.Status})
}

func (s *Server) handleState(w http.ResponseWriter, _ *http.Request) {
	out := map[string]any{}
	for slug, st := range s.Streams {
		seq, buffered, diverged := st.Stats()
		out[slug] = map[string]any{
			"status": st.Status(), "seq": seq, "buffered": buffered, "diverged": diverged,
		}
	}
	writeJSON(w, out)
}

// appendItem serializes one offer with the retailer's quirks applied, and
// may append a near-duplicate after it.
func (s *Server) appendItem(items []map[string]any, st *Stream, i int, priceCents int64, inStock bool, at time.Time) []map[string]any {
	q := st.Cfg.Quirks
	p := s.U.Product(i)

	if s.rng.Float64() < q.BadPriceRate {
		priceCents *= 100 // upstream unit bug: dollars sent as cents
	}
	currency := "USD"
	if s.rng.Float64() < q.WrongCurrencyRate {
		currency = []string{"EUR", "GBP", "CAD"}[s.rng.IntN(3)]
	}
	if s.rng.Float64() < q.StaleTimestampRate {
		at = at.Add(-time.Duration(1+s.rng.IntN(48)) * time.Hour)
	}

	item := map[string]any{
		"sku":        styleSKU(p.SKU, q.SKUStyle),
		"title":      p.Title,
		"brand":      p.Brand,
		"category":   p.Category,
		"attrs":      p.Attrs,
		"currency":   currency,
		"in_stock":   inStock,
		"updated_at": at.Format(time.RFC3339Nano),
		"url":        fmt.Sprintf("https://%s.example/p/%s", st.Cfg.Slug, strings.ToLower(p.SKU)),
	}
	format := q.PriceFormat
	if s.rng.Float64() < q.DriftRate {
		format = []string{"cents", "float", "string"}[s.rng.IntN(3)]
	}
	switch format {
	case "float":
		item["price"] = float64(priceCents) / 100
	case "string":
		item["price"] = fmt.Sprintf("%d.%02d", priceCents/100, priceCents%100)
	default:
		item["price_cents"] = priceCents
	}
	if s.rng.Float64() < q.MissingFieldRate {
		delete(item, []string{"title", "currency", "updated_at"}[s.rng.IntN(3)])
	}
	items = append(items, item)

	if s.rng.Float64() < q.DupeRate {
		dupe := make(map[string]any, len(item))
		for k, v := range item {
			dupe[k] = v
		}
		dupe["sku"] = mangleSKU(p.SKU, s.rng)
		items = append(items, dupe)
	}
	return items
}

func styleSKU(sku, style string) string {
	switch style {
	case "lower":
		return strings.ToLower(sku)
	case "dashed":
		return strings.ReplaceAll(sku, "DS-", "DS--")
	default:
		return sku
	}
}

func mangleSKU(sku string, rng *rand.Rand) string {
	switch rng.IntN(3) {
	case 0:
		return strings.ToLower(sku)
	case 1:
		return strings.ReplaceAll(sku, "-", "_")
	default:
		return sku + " "
	}
}

func validStatus(s string) bool {
	return s == "active" || s == "degraded" || s == "dead"
}

func pageLimit(r *http.Request, max int) int {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > max {
		return max
	}
	return limit
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// Package api serves the read side: search, product detail, price history,
// deals, and collections. Postgres is the source of truth, ClickHouse
// answers history, Redis holds materialized deals and short-TTL caches.
package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ryanportfolio/dealstream/internal/metrics"
)

type Server struct {
	PG    *pgxpool.Pool
	CH    driver.Conn
	Redis *redis.Client
	Cache *Cache

	retailersMu sync.Mutex
	retailers   map[int16]string
	retailersAt time.Time
}

func NewServer(pg *pgxpool.Pool, ch driver.Conn, rdb *redis.Client) *Server {
	return &Server{PG: pg, CH: ch, Redis: rdb, Cache: NewCache(rdb)}
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /search", instrument("search", s.handleSearch))
	mux.HandleFunc("GET /products/{id}", instrument("product", s.handleProduct))
	mux.HandleFunc("GET /products/{id}/history", instrument("history", s.handleHistory))
	mux.HandleFunc("GET /products/{id}/similar", instrument("similar", s.handleSimilar))
	mux.HandleFunc("GET /deals", instrument("deals", s.handleDeals))
	mux.HandleFunc("GET /collections", instrument("collections", s.handleCollections))
	mux.HandleFunc("GET /collections/{slug}", instrument("collection", s.handleCollection))
	return mux
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func instrument(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(rec, r)
		metrics.HTTPDuration.
			WithLabelValues(route, strconv.Itoa(rec.status/100)+"xx").
			Observe(time.Since(start).Seconds())
	}
}

// handleIndex makes the bare domain useful: a JSON map of the API for
// anyone who lands on the root instead of a README link.
func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "dealstream",
		"source":  "https://github.com/ryanportfolio/dealstream",
		"endpoints": []string{
			"/deals?limit=50",
			"/search?q=drill&sort=price_asc&limit=20",
			"/products/{id}",
			"/products/{id}/history?days=30",
			"/products/{id}/similar",
			"/collections",
			"/collections/{slug}",
			"/healthz",
		},
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{"postgres": "ok", "clickhouse": "ok", "redis": "ok"}
	code := http.StatusOK
	if err := s.PG.Ping(ctx); err != nil {
		checks["postgres"], code = err.Error(), http.StatusServiceUnavailable
	}
	if err := s.CH.Ping(ctx); err != nil {
		checks["clickhouse"], code = err.Error(), http.StatusServiceUnavailable
	}
	if err := s.Redis.Ping(ctx).Err(); err != nil {
		checks["redis"], code = err.Error(), http.StatusServiceUnavailable
	}
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(checks)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

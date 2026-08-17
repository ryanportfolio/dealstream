// Package api serves the read side: search, product detail, price history,
// deals, and collections. Postgres is the source of truth, ClickHouse
// answers history, Redis holds materialized deals and short-TTL caches.
package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	PG    *pgxpool.Pool
	CH    driver.Conn
	Redis *redis.Client
	Cache *Cache
}

func NewServer(pg *pgxpool.Pool, ch driver.Conn, rdb *redis.Client) *Server {
	return &Server{PG: pg, CH: ch, Redis: rdb, Cache: NewCache(rdb)}
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /search", s.handleSearch)
	mux.HandleFunc("GET /products/{id}", s.handleProduct)
	mux.HandleFunc("GET /products/{id}/history", s.handleHistory)
	mux.HandleFunc("GET /deals", s.handleDeals)
	mux.HandleFunc("GET /collections", s.handleCollections)
	mux.HandleFunc("GET /collections/{slug}", s.handleCollection)
	return mux
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

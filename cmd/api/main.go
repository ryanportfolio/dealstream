// api serves search, product detail, price history, deals, and
// collections over HTTP.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ryanportfolio/dealstream/internal/api"
	"github.com/ryanportfolio/dealstream/internal/config"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dealsEvery := flag.Duration("deals-every", 5*time.Minute, "deals rematerialization interval")
	flag.Parse()

	if err := config.LoadDotenv(".env"); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, config.MustGet("PG_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	chconn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.MustGet("CH_ADDR")},
		Auth: clickhouse.Auth{
			Database: config.MustGet("CH_DB"),
			Username: config.MustGet("CH_USER"),
			Password: config.MustGet("CH_PASSWORD"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer chconn.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     config.MustGet("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	defer rdb.Close()

	srv := api.NewServer(pool, chconn, rdb)
	go srv.RefreshDealsLoop(ctx, *dealsEvery)

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Mux()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
	}()

	log.Printf("api: listening on %s", *addr)
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

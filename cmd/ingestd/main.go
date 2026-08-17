// ingestd polls every configured retailer feed and maintains the canonical
// catalog (Postgres), the price history (ClickHouse), and the quarantine.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanportfolio/dealstream/internal/config"
	"github.com/ryanportfolio/dealstream/internal/ingest"
	"github.com/ryanportfolio/dealstream/internal/metrics"
)

func main() {
	feedBase := flag.String("feed", "http://127.0.0.1:8081", "feedgen base URL")
	cfgPath := flag.String("config", "config/feedgen.json", "retailer list (slugs and names)")
	poll := flag.Duration("poll", 2*time.Second, "incremental poll interval")
	metricsAddr := flag.String("metrics", ":9101", "metrics listen address")
	flag.Parse()
	metrics.Serve(*metricsAddr)

	if err := config.LoadDotenv(".env"); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	var cfg struct {
		Retailers []struct {
			Slug string `json:"slug"`
			Name string `json:"name"`
		} `json:"retailers"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatal(err)
	}

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

	store := ingest.NewStore(pool)
	ch := ingest.NewCHWriter(chconn)
	client := ingest.NewClient(*feedBase)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch.Run(ctx)
	}()

	for _, r := range cfg.Retailers {
		id, err := store.EnsureRetailer(ctx, r.Slug, r.Name)
		if err != nil {
			log.Fatalf("register %s: %v", r.Slug, err)
		}
		w := &ingest.Worker{
			Slug: r.Slug, RetailerID: id,
			Client: client, Store: store, CH: ch,
			PageSize: 500, Poll: *poll,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Run(ctx)
		}()
	}

	log.Printf("ingestd: %d retailers, feed %s", len(cfg.Retailers), *feedBase)
	<-ctx.Done()
	wg.Wait()
	log.Printf("ingestd: drained, %d events unflushed", ch.Pending())
}

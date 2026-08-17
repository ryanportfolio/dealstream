// dbstat prints quick row counts and freshness numbers for sanity checks.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/ryanportfolio/dealstream/internal/config"
)

func main() {
	if err := config.LoadDotenv(".env"); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pg, err := pgx.Connect(ctx, config.MustGet("PG_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close(ctx)

	for _, q := range []struct{ label, sql string }{
		{"products", "SELECT count(*) FROM products"},
		{"offers", "SELECT count(*) FROM offers"},
		{"quarantine", "SELECT count(*) FROM quarantine"},
		{"offers by retailer", `SELECT string_agg(r.slug || '=' || c.n::text, ' ' ORDER BY r.slug)
			FROM (SELECT retailer_id, count(*) n FROM offers GROUP BY retailer_id) c
			JOIN retailers r ON r.id = c.retailer_id`},
		{"quarantine by reason", `SELECT string_agg(reason || '=' || n::text, ' ' ORDER BY n DESC)
			FROM (SELECT reason, count(*) n FROM quarantine GROUP BY reason) q`},
		{"cursors", `SELECT string_agg(r.slug || ':' || ic.cursor, ' ' ORDER BY r.slug)
			FROM ingest_cursors ic JOIN retailers r ON r.id = ic.retailer_id`},
	} {
		var out *string
		if err := pg.QueryRow(ctx, q.sql).Scan(&out); err != nil {
			log.Fatalf("%s: %v", q.label, err)
		}
		if out == nil {
			empty := "(none)"
			out = &empty
		}
		fmt.Printf("%-22s %s\n", q.label, *out)
	}

	ch, err := clickhouse.Open(&clickhouse.Options{
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
	defer ch.Close()

	var events uint64
	if err := ch.QueryRow(ctx, "SELECT count() FROM price_events").Scan(&events); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%-22s %d\n", "ch price_events", events)
}

// migrate applies the SQL files under db/postgres and db/clickhouse in
// filename order. Files are idempotent by convention (IF NOT EXISTS) except
// 001_init.sql for Postgres, which fails loudly on a non-empty database
// rather than guessing.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/ryanportfolio/dealstream/internal/config"
)

func main() {
	if err := config.LoadDotenv(".env"); err != nil {
		log.Fatalf("read .env: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := migratePostgres(ctx); err != nil {
		log.Fatalf("postgres: %v", err)
	}
	if err := migrateClickHouse(ctx); err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	log.Println("migrations applied")
}

func migratePostgres(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, config.MustGet("PG_DSN"))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	files, err := sqlFiles("db/postgres")
	if err != nil {
		return err
	}
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		log.Printf("postgres: applied %s", f)
	}
	return nil
}

func migrateClickHouse(ctx context.Context) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.MustGet("CH_ADDR")},
		Auth: clickhouse.Auth{
			Database: config.MustGet("CH_DB"),
			Username: config.MustGet("CH_USER"),
			Password: config.MustGet("CH_PASSWORD"),
		},
		TLS: tlsIfSet(),
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	files, err := sqlFiles("db/clickhouse")
	if err != nil {
		return err
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		// clickhouse-go executes one statement per call.
		for _, stmt := range strings.Split(string(data), ";") {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", f, err)
			}
		}
		log.Printf("clickhouse: applied %s", f)
	}
	return nil
}

func tlsIfSet() *tls.Config {
	if os.Getenv("CH_TLS") == "1" {
		return &tls.Config{}
	}
	return nil
}

func sqlFiles(dir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

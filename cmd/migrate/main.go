// migrate applies the SQL files under db/postgres and db/clickhouse in
// filename order, once each. Applied filenames (for both stores) are
// recorded in Postgres schema_migrations.
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

	pg, err := pgx.Connect(ctx, config.MustGet("PG_DSN"))
	if err != nil {
		log.Fatalf("postgres: connect: %v", err)
	}
	defer pg.Close(ctx)
	if _, err := pg.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		filename text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		log.Fatalf("postgres: migrations table: %v", err)
	}

	if err := migratePostgres(ctx, pg); err != nil {
		log.Fatalf("postgres: %v", err)
	}
	if err := migrateClickHouse(ctx, pg); err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	log.Println("migrations applied")
}

func applied(ctx context.Context, pg *pgx.Conn, f string) (bool, error) {
	var exists bool
	err := pg.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = $1)`, filepath.ToSlash(f)).Scan(&exists)
	return exists, err
}

func record(ctx context.Context, pg *pgx.Conn, f string) error {
	_, err := pg.Exec(ctx, `INSERT INTO schema_migrations (filename) VALUES ($1)`, filepath.ToSlash(f))
	return err
}

func migratePostgres(ctx context.Context, pg *pgx.Conn) error {
	files, err := sqlFiles("db/postgres")
	if err != nil {
		return err
	}
	for _, f := range files {
		if done, err := applied(ctx, pg, f); err != nil {
			return err
		} else if done {
			continue
		}
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := pg.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		if err := record(ctx, pg, f); err != nil {
			return err
		}
		log.Printf("postgres: applied %s", f)
	}
	return nil
}

func migrateClickHouse(ctx context.Context, pg *pgx.Conn) error {
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
		if done, err := applied(ctx, pg, f); err != nil {
			return err
		} else if done {
			continue
		}
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
		if err := record(ctx, pg, f); err != nil {
			return err
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

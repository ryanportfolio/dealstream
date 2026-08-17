// embed backfills product embeddings, then (re)builds the IVFFlat index.
// The index is created here rather than in a migration so it trains its
// centroids on real vectors, not an empty table. Safe to re-run: only
// NULL embeddings are computed, and the index build is skipped unless
// -reindex is passed or the index is missing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanportfolio/dealstream/internal/config"
	"github.com/ryanportfolio/dealstream/internal/embed"
)

func main() {
	batch := flag.Int("batch", 2000, "rows per update batch")
	reindex := flag.Bool("reindex", false, "drop and rebuild the vector index")
	flag.Parse()

	if err := config.LoadDotenv(".env"); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, config.MustGet("PG_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	start := time.Now()
	total := 0
	for {
		n, err := backfillBatch(ctx, pool, *batch)
		if err != nil {
			log.Fatal(err)
		}
		if n == 0 {
			break
		}
		total += n
		if total%20000 < *batch {
			log.Printf("embedded %d products", total)
		}
	}
	log.Printf("backfill done: %d products in %s", total, time.Since(start).Round(time.Second))

	if err := ensureIndex(ctx, pool, *reindex); err != nil {
		log.Fatal(err)
	}
}

func backfillBatch(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, title, brand, category, attrs
		FROM products WHERE embedding IS NULL
		ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var ids []int64
	var vecs []string
	for rows.Next() {
		var (
			id                     int64
			title, brand, category string
			attrs                  map[string]string
		)
		if err := rows.Scan(&id, &title, &brand, &category, &attrs); err != nil {
			return 0, err
		}
		ids = append(ids, id)
		vecs = append(vecs, vectorLiteral(embed.Vector(title, brand, category, attrs)))
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	_, err = pool.Exec(ctx, `
		UPDATE products p SET embedding = u.vec::vector
		FROM unnest($1::bigint[], $2::text[]) AS u(id, vec)
		WHERE p.id = u.id`, ids, vecs)
	return len(ids), err
}

func ensureIndex(ctx context.Context, pool *pgxpool.Pool, reindex bool) error {
	if reindex {
		if _, err := pool.Exec(ctx, `DROP INDEX IF EXISTS products_embedding_idx`); err != nil {
			return err
		}
	}
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'products_embedding_idx')`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		log.Println("vector index present")
		return nil
	}
	log.Println("building IVFFlat index (lists=200)...")
	start := time.Now()
	// 200 lists ≈ sqrt of the ~430k row count, biased low for recall.
	_, err := pool.Exec(ctx, `
		CREATE INDEX products_embedding_idx ON products
		USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200)`)
	if err != nil {
		return err
	}
	log.Printf("index built in %s", time.Since(start).Round(time.Second))
	return nil
}

func vectorLiteral(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%g", x)
	}
	b.WriteByte(']')
	return b.String()
}

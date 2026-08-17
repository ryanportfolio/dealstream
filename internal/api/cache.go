package api

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ryanportfolio/dealstream/internal/metrics"
)

// Cache is a read-through JSON cache. Redis being down degrades to
// straight database reads; it never takes a request down with it.
// Concurrent misses on the same key collapse into one fill: without
// that, every cold popular key sends a stampede of identical queries at
// Postgres at once.
type Cache struct {
	rdb *redis.Client

	mu       sync.Mutex
	inflight map[string]*flight
}

// flight is one in-progress fill; followers wait on done and read the
// leader's result.
type flight struct {
	done chan struct{}
	data []byte
	err  error
}

func NewCache(rdb *redis.Client) *Cache {
	return &Cache{rdb: rdb, inflight: make(map[string]*flight)}
}

// GetOrFill returns the cached JSON at key, or runs fill, stores the
// result with ttl, and returns it. If the leader's request is cancelled
// mid-fill, followers get its error and simply retry as new leaders.
func (c *Cache) GetOrFill(ctx context.Context, key string, ttl time.Duration, fill func(context.Context) (any, error)) ([]byte, error) {
	if data, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		metrics.CacheOps.WithLabelValues("hit").Inc()
		return data, nil
	}
	metrics.CacheOps.WithLabelValues("miss").Inc()

	c.mu.Lock()
	if f, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.done:
			return f.data, f.err
		}
	}
	f := &flight{done: make(chan struct{})}
	c.inflight[key] = f
	c.mu.Unlock()

	f.data, f.err = c.fillAndSet(ctx, key, ttl, fill)
	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
	close(f.done)
	return f.data, f.err
}

func (c *Cache) fillAndSet(ctx context.Context, key string, ttl time.Duration, fill func(context.Context) (any, error)) ([]byte, error) {
	v, err := fill(ctx)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	// Best effort: a failed SET is a cache miss next time, nothing more.
	c.rdb.Set(ctx, key, data, ttl)
	return data, nil
}

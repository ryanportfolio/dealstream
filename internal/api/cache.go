package api

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache is a read-through JSON cache. Redis being down degrades to
// straight database reads; it never takes a request down with it.
type Cache struct {
	rdb    *redis.Client
	Hits   atomic.Int64
	Misses atomic.Int64
}

func NewCache(rdb *redis.Client) *Cache {
	return &Cache{rdb: rdb}
}

// GetOrFill returns the cached JSON at key, or runs fill, stores the
// result with ttl, and returns it.
func (c *Cache) GetOrFill(ctx context.Context, key string, ttl time.Duration, fill func(context.Context) (any, error)) ([]byte, error) {
	if data, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		c.Hits.Add(1)
		return data, nil
	}
	c.Misses.Add(1)

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

func writeCached(ctx context.Context, c *Cache, key string, ttl time.Duration, fill func(context.Context) (any, error)) ([]byte, error) {
	return c.GetOrFill(ctx, key, ttl, fill)
}

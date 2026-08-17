package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ErrGone means the incremental cursor aged out of the feed's buffer and
// only a full catalog resync can recover.
var ErrGone = errors.New("cursor expired")

type Client struct {
	base string
	http *http.Client
}

func NewClient(base string) *Client {
	return &Client{base: base, http: &http.Client{Timeout: 30 * time.Second}}
}

// Page is a feed response. Items stay raw: an item that fails to parse is
// quarantined individually instead of failing the page.
type Page struct {
	Items      []json.RawMessage `json:"items"`
	NextCursor *int              `json:"next_cursor"`
	Next       uint64            `json:"next"`
	AsOfSeq    uint64            `json:"as_of_seq"`
}

func (c *Client) Catalog(ctx context.Context, slug string, cursor, limit int) (Page, error) {
	return c.get(ctx, fmt.Sprintf("%s/r/%s/catalog?cursor=%d&limit=%d", c.base, slug, cursor, limit))
}

func (c *Client) Offers(ctx context.Context, slug string, since uint64, limit int) (Page, error) {
	return c.get(ctx, fmt.Sprintf("%s/r/%s/offers?since=%d&limit=%d", c.base, slug, since, limit))
}

func (c *Client) get(ctx context.Context, url string) (Page, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Page{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Page{}, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusGone:
		return Page{}, ErrGone
	case resp.StatusCode != http.StatusOK:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return Page{}, fmt.Errorf("feed returned %d: %s", resp.StatusCode, body)
	}

	var p Page
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return Page{}, fmt.Errorf("decode page: %w", err)
	}
	return p, nil
}

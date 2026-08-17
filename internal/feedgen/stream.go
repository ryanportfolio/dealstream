package feedgen

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ryanportfolio/dealstream/internal/metrics"
)

// Quirks describes how a retailer's feed misbehaves. Rates are per item
// serialized, except DriftRate which is per item and switches the price
// wire format mid-feed.
type Quirks struct {
	PriceFormat        string  `json:"price_format"` // cents | float | string
	SKUStyle           string  `json:"sku_style"`    // canonical | lower | dashed
	DupeRate           float64 `json:"dupe_rate"`
	BadPriceRate       float64 `json:"bad_price_rate"`
	MissingFieldRate   float64 `json:"missing_field_rate"`
	WrongCurrencyRate  float64 `json:"wrong_currency_rate"`
	StaleTimestampRate float64 `json:"stale_timestamp_rate"`
	DriftRate          float64 `json:"drift_rate"`
}

type RetailerCfg struct {
	Slug          string  `json:"slug"`
	Name          string  `json:"name"`
	CarryRate     float64 `json:"carry_rate"`
	UpdatesPerSec float64 `json:"updates_per_sec"`
	PageSizeMax   int     `json:"page_size_max"`
	Quirks        Quirks  `json:"quirks"`
}

type offerState struct {
	PriceCents int64
	InStock    bool
}

type Update struct {
	Seq        uint64
	Product    int
	PriceCents int64
	InStock    bool
	At         time.Time
}

const maxBuffered = 200_000

// Stream is one retailer's live feed: a capped buffer of recent updates
// plus the current state of every offer that has diverged from its
// deterministic base. Consumers page with a cursor; falling behind the
// buffer horizon is a real condition they must handle (HTTP 410).
type Stream struct {
	Cfg RetailerCfg

	mu      sync.Mutex
	u       *Universe
	rng     *rand.Rand
	buf     []Update
	nextSeq uint64
	deltas  map[int]offerState
	status  string // active | degraded | dead
	carry   float64
}

func NewStream(u *Universe, cfg RetailerCfg) *Stream {
	return &Stream{
		Cfg:     cfg,
		u:       u,
		rng:     rand.New(rand.NewPCG(u.Seed, stringHash(cfg.Slug))),
		deltas:  make(map[int]offerState),
		status:  "active",
		carry:   cfg.CarryRate,
		nextSeq: 1,
	}
}

// Tick emits n price/stock updates for randomly chosen carried offers.
func (s *Stream) Tick(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == "dead" {
		return
	}
	now := time.Now().UTC()
	for range n {
		i, ok := s.pickCarried()
		if !ok {
			continue
		}
		cur, exists := s.deltas[i]
		if !exists {
			cur = offerState{PriceCents: s.u.RetailerBaseCents(i, s.Cfg.Slug), InStock: s.u.Product(i).InStock}
		}
		// Mostly small price moves; occasionally a stock flip instead.
		if s.rng.Float64() < 0.15 {
			cur.InStock = !cur.InStock
		} else {
			step := 0.92 + s.rng.Float64()*0.16 // ×0.92..1.08
			cur.PriceCents = int64(float64(cur.PriceCents) * step)
			if cur.PriceCents < 99 {
				cur.PriceCents = 99
			}
			cur.PriceCents = cur.PriceCents - cur.PriceCents%100 + 99
		}
		s.deltas[i] = cur
		s.buf = append(s.buf, Update{Seq: s.nextSeq, Product: i, PriceCents: cur.PriceCents, InStock: cur.InStock, At: now})
		s.nextSeq++
		metrics.FeedUpdates.WithLabelValues(s.Cfg.Slug).Inc()
	}
	if len(s.buf) > maxBuffered {
		s.buf = s.buf[len(s.buf)-maxBuffered:]
	}
}

func (s *Stream) pickCarried() (int, bool) {
	for range 32 {
		i := s.rng.IntN(s.u.Size)
		if s.u.Carried(i, s.Cfg.Slug, s.carry) {
			return i, true
		}
	}
	return 0, false
}

// Current returns the offer state for product i, base or diverged.
func (s *Stream) Current(i int) offerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.deltas[i]; ok {
		return st
	}
	return offerState{PriceCents: s.u.RetailerBaseCents(i, s.Cfg.Slug), InStock: s.u.Product(i).InStock}
}

// Since returns up to limit updates with Seq > since. gone reports an
// unusable cursor: aged out of the buffer, or ahead of the head (the feed
// restarted and reset its sequence) — both require a full resync.
func (s *Stream) Since(since uint64, limit int) (out []Update, gone bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if since > s.nextSeq-1 {
		return nil, true
	}
	if len(s.buf) > 0 && since+1 < s.buf[0].Seq {
		return nil, true
	}
	if len(s.buf) == 0 && since+1 < s.nextSeq {
		return nil, true
	}
	// Binary search would do; linear from the back is fine at this size.
	start := len(s.buf)
	for start > 0 && s.buf[start-1].Seq > since {
		start--
	}
	end := min(start+limit, len(s.buf))
	return append([]Update(nil), s.buf[start:end]...), false
}

func (s *Stream) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *Stream) SetStatus(st string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = st
}

func (s *Stream) Stats() (seq uint64, buffered, diverged int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSeq - 1, len(s.buf), len(s.deltas)
}

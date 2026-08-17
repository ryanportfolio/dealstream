package feedgen

import (
	"math/rand/v2"
	"strings"
	"testing"
)

func testUniverse() *Universe {
	return &Universe{Seed: 42, Size: 100_000}
}

func TestProductDeterministic(t *testing.T) {
	a, b := testUniverse(), testUniverse()
	for _, i := range []int{0, 1, 999, 99_999} {
		pa, pb := a.Product(i), b.Product(i)
		if pa.SKU != pb.SKU || pa.Title != pb.Title || pa.BaseCents != pb.BaseCents {
			t.Fatalf("product %d not deterministic: %+v vs %+v", i, pa, pb)
		}
	}
}

func TestPriceWithinCategoryBand(t *testing.T) {
	u := testUniverse()
	for i := range 10_000 {
		p := u.Product(i)
		if p.BaseCents < 399 || p.BaseCents > 129_999 {
			t.Fatalf("product %d price %d outside all category bands", i, p.BaseCents)
		}
		if p.BaseCents%100 != 99 {
			t.Fatalf("product %d price %d not .99-styled", i, p.BaseCents)
		}
	}
}

func TestCarryRateApproximate(t *testing.T) {
	u := testUniverse()
	const rate = 0.30
	carried := 0
	for i := range u.Size {
		if u.Carried(i, "testmart", rate) {
			carried++
		}
	}
	got := float64(carried) / float64(u.Size)
	if got < rate-0.01 || got > rate+0.01 {
		t.Fatalf("carry rate %.3f, want ~%.2f", got, rate)
	}
}

func TestRetailerOffsetBounded(t *testing.T) {
	u := testUniverse()
	for i := range 5_000 {
		base := u.Product(i).BaseCents
		rp := u.RetailerBaseCents(i, "testmart")
		diff := float64(rp-base) / float64(base)
		if diff < -0.081 || diff > 0.081 {
			t.Fatalf("product %d retailer offset %.3f outside ±8%%", i, diff)
		}
	}
}

func TestStreamCursorSemantics(t *testing.T) {
	u := testUniverse()
	st := NewStream(u, RetailerCfg{Slug: "testmart", CarryRate: 0.5, PageSizeMax: 100})
	st.Tick(50)

	all, gone := st.Since(0, 1000)
	if gone {
		t.Fatal("fresh cursor reported gone")
	}
	if len(all) != 50 {
		t.Fatalf("want 50 updates, got %d", len(all))
	}
	for i, up := range all {
		if up.Seq != uint64(i+1) {
			t.Fatalf("seq gap at %d: %d", i, up.Seq)
		}
	}

	tail, _ := st.Since(45, 1000)
	if len(tail) != 5 || tail[0].Seq != 46 {
		t.Fatalf("since=45 want seqs 46..50, got %d items starting %d", len(tail), tail[0].Seq)
	}

	page, _ := st.Since(0, 10)
	if len(page) != 10 {
		t.Fatalf("limit not applied: %d", len(page))
	}
}

func TestStreamCursorExpiry(t *testing.T) {
	u := testUniverse()
	st := NewStream(u, RetailerCfg{Slug: "testmart", CarryRate: 0.5})
	// Overflow the buffer so seq 1 ages out.
	for range maxBuffered/1000 + 2 {
		st.Tick(1000)
	}
	if _, gone := st.Since(0, 10); !gone {
		t.Fatal("expired cursor not reported gone")
	}
	seq, buffered, _ := st.Stats()
	if buffered > maxBuffered {
		t.Fatalf("buffer exceeded cap: %d", buffered)
	}
	if _, gone := st.Since(seq, 10); gone {
		t.Fatal("live cursor reported gone")
	}
}

func TestStreamCursorAheadOfHead(t *testing.T) {
	// A consumer whose cursor outruns the stream (feedgen restarted and
	// reset its sequence) must be told to resync, not left polling empty
	// pages forever.
	u := testUniverse()
	st := NewStream(u, RetailerCfg{Slug: "testmart", CarryRate: 0.5})
	st.Tick(10)
	if _, gone := st.Since(121905, 10); !gone {
		t.Fatal("cursor ahead of head not reported gone")
	}
	if _, gone := st.Since(10, 10); gone {
		t.Fatal("head cursor reported gone")
	}
}

func TestMangleSKUNormalizesBack(t *testing.T) {
	// Every mangled variant must normalize to the same canonical key the
	// ingester derives, or dedupe cannot work.
	rng := rand.New(rand.NewPCG(1, 2))
	norm := func(s string) string {
		s = strings.ToUpper(strings.TrimSpace(s))
		return strings.Map(func(r rune) rune {
			if r == '-' || r == '_' || r == ' ' {
				return -1
			}
			return r
		}, s)
	}
	sku := "DS-00001234"
	for range 100 {
		if norm(mangleSKU(sku, rng)) != norm(sku) {
			t.Fatalf("mangled sku does not normalize back")
		}
	}
	for _, style := range []string{"canonical", "lower", "dashed"} {
		if norm(styleSKU(sku, style)) != norm(sku) {
			t.Fatalf("style %s does not normalize back", style)
		}
	}
}

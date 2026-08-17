// Package feedgen simulates multi-retailer product feeds. The product
// universe is a pure function of (seed, index): nothing is stored, so a
// million-offer catalog costs no memory and every restart serves identical
// data. Only live price changes (deltas) are kept in memory.
package feedgen

import (
	"fmt"
	"math"
)

type Product struct {
	Index     int
	SKU       string // canonical form; retailers mangle formatting on the wire
	Title     string
	Brand     string
	Category  string
	Attrs     map[string]string
	BaseCents int64
	InStock   bool
}

type Universe struct {
	Seed uint64
	Size int
}

// splitmix64 is the standard finalizer; good enough avalanche for
// deterministic attribute derivation.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (u *Universe) hash(parts ...uint64) uint64 {
	h := u.Seed
	for _, p := range parts {
		h = splitmix64(h ^ p)
	}
	return h
}

var categories = []struct {
	name     string
	minCents int64
	maxCents int64
	nouns    []string
}{
	{"audio", 1499, 89999, []string{"Earbuds", "Headphones", "Speaker", "Soundbar", "Turntable", "Microphone"}},
	{"kitchen", 999, 44999, []string{"Blender", "Kettle", "Toaster", "Air Fryer", "Grinder", "Skillet"}},
	{"outdoors", 1999, 129999, []string{"Tent", "Backpack", "Sleeping Bag", "Trekking Poles", "Camp Stove", "Hammock"}},
	{"tools", 899, 69999, []string{"Drill", "Impact Driver", "Multimeter", "Socket Set", "Sander", "Rotary Tool"}},
	{"home", 1299, 99999, []string{"Desk Lamp", "Air Purifier", "Humidifier", "Vacuum", "Fan", "Space Heater"}},
	{"apparel", 799, 24999, []string{"Rain Jacket", "Hoodie", "Running Shoes", "Beanie", "Gloves", "Vest"}},
	{"toys", 499, 19999, []string{"Building Set", "RC Car", "Puzzle", "Plush", "Board Game", "Drone"}},
	{"beauty", 399, 15999, []string{"Serum", "Moisturizer", "Hair Dryer", "Trimmer", "Cleanser", "Sunscreen"}},
}

var brands = []string{
	"Arvello", "Brightpeak", "Cindercrest", "Dunmore", "Everquist", "Fernwell",
	"Graniteer", "Hollowvale", "Ironquill", "Juniperline", "Kestwick", "Lumenor",
	"Marrowfield", "Nordbeck", "Oakhurst", "Pinefold", "Quarrow", "Ridgeline",
	"Saltmere", "Thornbury", "Umberly", "Vantorre", "Westrell", "Yarrowino",
}

var adjectives = []string{
	"Pro", "Ultra", "Compact", "Classic", "Elite", "Prime", "Core", "Max",
	"Slim", "Heavy-Duty", "Portable", "Wireless", "Precision", "Everyday",
}

var colors = []string{"Black", "White", "Slate", "Navy", "Forest", "Sand", "Crimson"}

// Product derives the full product record for an index. Same seed, same
// index, same product, forever.
func (u *Universe) Product(i int) Product {
	h := u.hash(uint64(i))
	cat := categories[h%uint64(len(categories))]
	brand := brands[u.hash(uint64(i), 1)%uint64(len(brands))]
	adj := adjectives[u.hash(uint64(i), 2)%uint64(len(adjectives))]
	noun := cat.nouns[u.hash(uint64(i), 3)%uint64(len(cat.nouns))]
	model := 100 + u.hash(uint64(i), 4)%900

	// Log-uniform price inside the category band, so cheap items dominate
	// the way real catalogs do.
	span := math.Log(float64(cat.maxCents) / float64(cat.minCents))
	frac := float64(u.hash(uint64(i), 5)%1e6) / 1e6
	base := int64(float64(cat.minCents) * math.Exp(span*frac))
	base = base - base%100 + 99 // .99 pricing

	return Product{
		Index:     i,
		SKU:       fmt.Sprintf("DS-%08d", i),
		Title:     fmt.Sprintf("%s %s %s %d", brand, adj, noun, model),
		Brand:     brand,
		Category:  cat.name,
		Attrs:     map[string]string{"color": colors[u.hash(uint64(i), 6)%uint64(len(colors))]},
		BaseCents: base,
		InStock:   u.hash(uint64(i), 7)%100 < 95,
	}
}

// Carried reports whether retailer r stocks product i, at roughly rate.
// Membership is stable across restarts.
func (u *Universe) Carried(i int, retailer string, rate float64) bool {
	h := u.hash(uint64(i), stringHash(retailer))
	return float64(h%1e9)/1e9 < rate
}

// RetailerBaseCents is the retailer's regular price for a product: the
// universe base plus a stable per-retailer offset of up to ±8%.
func (u *Universe) RetailerBaseCents(i int, retailer string) int64 {
	p := u.Product(i)
	h := u.hash(uint64(i), stringHash(retailer), 8)
	offset := (float64(h%1600) - 800) / 10000 // -0.08..0.08
	return p.BaseCents + int64(float64(p.BaseCents)*offset)
}

func stringHash(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}

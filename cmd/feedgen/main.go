// feedgen serves simulated retailer feeds. See config/feedgen.json for the
// universe size and per-retailer mess profiles.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ryanportfolio/dealstream/internal/feedgen"
)

type cfg struct {
	Seed         uint64                `json:"seed"`
	UniverseSize int                   `json:"universe_size"`
	Retailers    []feedgen.RetailerCfg `json:"retailers"`
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	cfgPath := flag.String("config", "config/feedgen.json", "config file")
	flag.Parse()

	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	var c cfg
	if err := json.Unmarshal(data, &c); err != nil {
		log.Fatalf("parse %s: %v", *cfgPath, err)
	}

	u := &feedgen.Universe{Seed: c.Seed, Size: c.UniverseSize}
	streams := make(map[string]*feedgen.Stream, len(c.Retailers))
	for _, rc := range c.Retailers {
		st := feedgen.NewStream(u, rc)
		streams[rc.Slug] = st
		go tickLoop(st, rc.UpdatesPerSec)
	}

	log.Printf("feedgen: %d retailers, universe %d, listening on %s", len(streams), u.Size, *addr)
	log.Fatal(http.ListenAndServe(*addr, feedgen.NewServer(u, streams).Mux()))
}

// tickLoop spreads a retailer's update rate over 250ms ticks, carrying the
// fractional remainder so low rates still emit.
func tickLoop(st *feedgen.Stream, perSec float64) {
	const interval = 250 * time.Millisecond
	perTick := perSec * interval.Seconds()
	var carry float64
	for range time.Tick(interval) {
		carry += perTick
		n := int(carry)
		carry -= float64(n)
		st.Tick(n)
	}
}

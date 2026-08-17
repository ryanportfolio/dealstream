package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"
)

// The showcase layer renders browser-friendly HTML for the same routes
// the API serves. It works by running the real handler against a buffer
// and rendering the recorded JSON, so the HTML view can never drift from
// the API payload: they are the same bytes. curl keeps getting JSON
// (content negotiation on the Accept header), and ?format=json shows the
// raw payload in a browser.

// wantsHTML decides the response shape. An explicit format param wins;
// otherwise browsers (Accept: text/html) get the showcase.
func wantsHTML(r *http.Request) bool {
	switch r.URL.Query().Get("format") {
	case "json":
		return false
	case "html":
		return true
	}
	return strings.Contains(r.Header.Get("Accept"), "text/html")
}

type bodyRecorder struct {
	header http.Header
	buf    bytes.Buffer
	status int
}

func newBodyRecorder() *bodyRecorder {
	return &bodyRecorder{header: http.Header{}, status: http.StatusOK}
}

func (r *bodyRecorder) Header() http.Header         { return r.header }
func (r *bodyRecorder) WriteHeader(code int)        { r.status = code }
func (r *bodyRecorder) Write(b []byte) (int, error) { return r.buf.Write(b) }

// showcase wraps a JSON handler with an HTML renderer. The handler runs
// either way; render receives the JSON it produced.
func (s *Server) showcase(render func(w http.ResponseWriter, r *http.Request, body []byte), next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !wantsHTML(r) {
			next(w, r)
			return
		}
		rec := newBodyRecorder()
		next(rec, r)
		if rec.status != http.StatusOK {
			renderErrorPage(w, r, rec.status, rec.buf.Bytes())
			return
		}
		render(w, r, rec.buf.Bytes())
	}
}

func renderErrorPage(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	var e struct {
		Error string `json:"error"`
	}
	json.Unmarshal(body, &e)
	if e.Error == "" {
		e.Error = http.StatusText(status)
	}
	renderPage(w, r, status, "error", map[string]any{"Status": status, "Message": e.Error})
}

// renderPage executes the named content template inside the base layout.
func renderPage(w http.ResponseWriter, r *http.Request, status int, name string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["JSONHref"] = jsonHref(r)
	data["View"] = name
	var buf bytes.Buffer
	if err := pages.ExecuteTemplate(&buf, name, data); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	w.Write(buf.Bytes())
}

// jsonHref is the current URL with format=json, the toggle target.
func jsonHref(r *http.Request) string {
	q := r.URL.Query()
	q.Set("format", "json")
	return r.URL.Path + "?" + q.Encode()
}

// Typed mirrors of the JSON payloads, for unmarshaling recorded bodies.

type dealsPayload struct {
	Deals []Deal `json:"deals"`
}

type searchPayload struct {
	Results   []SearchResult `json:"results"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	CappedAt  int            `json:"capped_at"`
	Truncated bool           `json:"truncated"`
}

type similarPayload struct {
	ProductID int64 `json:"product_id"`
	Similar   []struct {
		SearchResult
		Distance float64 `json:"distance"`
	} `json:"similar"`
}

type historyPayload struct {
	ProductID int64          `json:"product_id"`
	Days      int            `json:"days"`
	Points    []HistoryPoint `json:"points"`
}

type collectionsPayload struct {
	Collections []CollectionSummary `json:"collections"`
}

type collectionPayload struct {
	Slug  string         `json:"slug"`
	Title string         `json:"title"`
	Items []SearchResult `json:"items"`
}

func (s *Server) renderDeals(w http.ResponseWriter, r *http.Request, body []byte) {
	var p dealsPayload
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	renderPage(w, r, http.StatusOK, "deals", map[string]any{"Deals": p.Deals})
}

func (s *Server) renderSearch(w http.ResponseWriter, r *http.Request, body []byte) {
	var p searchPayload
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	renderPage(w, r, http.StatusOK, "search", map[string]any{
		"Results": p.Results, "Truncated": p.Truncated,
		"Query": r.URL.Query().Get("q"), "Sort": r.URL.Query().Get("sort"),
	})
}

func (s *Server) renderProduct(w http.ResponseWriter, r *http.Request, body []byte) {
	var p Product
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	renderPage(w, r, http.StatusOK, "product", map[string]any{"P": p})
}

func (s *Server) renderSimilar(w http.ResponseWriter, r *http.Request, body []byte) {
	var p similarPayload
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	renderPage(w, r, http.StatusOK, "similar", map[string]any{"ProductID": p.ProductID, "Similar": p.Similar})
}

func (s *Server) renderCollections(w http.ResponseWriter, r *http.Request, body []byte) {
	var p collectionsPayload
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	renderPage(w, r, http.StatusOK, "collections", map[string]any{"Collections": p.Collections})
}

func (s *Server) renderCollection(w http.ResponseWriter, r *http.Request, body []byte) {
	var p collectionPayload
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	renderPage(w, r, http.StatusOK, "collection", map[string]any{"Title": p.Title, "Slug": p.Slug, "Items": p.Items})
}

// chartLine is one retailer's price line in the history SVG. Dots mark
// the vertices: with sparse history (day one has a single point per
// retailer) a polyline alone renders nothing.
type chartLine struct {
	Retailer string
	Color    string
	Points   string
	Dots     []chartDot
}

type chartDot struct{ X, Y string }

var lineColors = []string{"#3ddc97", "#e8b04b", "#6aa3f0", "#d4718e", "#9a7ff0", "#5bc8c4", "#c2955b", "#8fb04e"}

const chartW, chartH = 660, 200

func (s *Server) renderHistory(w http.ResponseWriter, r *http.Request, body []byte) {
	var p historyPayload
	if err := json.Unmarshal(body, &p); err != nil {
		writeError(w, http.StatusInternalServerError, "render failed")
		return
	}
	lines, lo, hi, firstDay, lastDay := buildChart(p.Points)
	renderPage(w, r, http.StatusOK, "history", map[string]any{
		"ProductID": p.ProductID, "Days": p.Days, "Points": p.Points,
		"Lines": lines, "Lo": lo, "Hi": hi,
		"FirstDay": firstDay, "LastDay": lastDay,
		"ChartW": chartW, "ChartH": chartH,
	})
}

// buildChart turns daily rollup points into per-retailer SVG polylines of
// the day's last price.
func buildChart(points []HistoryPoint) (lines []chartLine, lo, hi int64, firstDay, lastDay string) {
	if len(points) == 0 {
		return nil, 0, 0, "", ""
	}
	daySet := map[string]bool{}
	byRetailer := map[string]map[string]int64{}
	lo, hi = points[0].LastCents, points[0].LastCents
	for _, pt := range points {
		daySet[pt.Day] = true
		if byRetailer[pt.Retailer] == nil {
			byRetailer[pt.Retailer] = map[string]int64{}
		}
		byRetailer[pt.Retailer][pt.Day] = pt.LastCents
		lo, hi = min(lo, pt.LastCents), max(hi, pt.LastCents)
	}
	if hi == lo {
		hi = lo + 1
	}
	days := make([]string, 0, len(daySet))
	for d := range daySet {
		days = append(days, d)
	}
	sort.Strings(days)
	firstDay, lastDay = days[0], days[len(days)-1]
	retailers := make([]string, 0, len(byRetailer))
	for name := range byRetailer {
		retailers = append(retailers, name)
	}
	sort.Strings(retailers)

	xStep := float64(chartW) / float64(max(len(days)-1, 1))
	for i, name := range retailers {
		var b strings.Builder
		var dots []chartDot
		for di, day := range days {
			cents, ok := byRetailer[name][day]
			if !ok {
				continue
			}
			x := float64(di) * xStep
			if len(days) == 1 {
				x = chartW / 2 // one lone day sits centered, not clipped at the edge
			}
			y := float64(chartH) - float64(cents-lo)/float64(hi-lo)*float64(chartH-16) - 8
			fmt.Fprintf(&b, "%.1f,%.1f ", x, y)
			if len(days) <= 60 {
				dots = append(dots, chartDot{X: fmt.Sprintf("%.1f", x), Y: fmt.Sprintf("%.1f", y)})
			}
		}
		lines = append(lines, chartLine{Retailer: name, Color: lineColors[i%len(lineColors)], Points: strings.TrimSpace(b.String()), Dots: dots})
	}
	return lines, lo, hi, firstDay, lastDay
}

func (s *Server) renderLanding(w http.ResponseWriter, r *http.Request) {
	renderPage(w, r, http.StatusOK, "landing", nil)
}

var pages = template.Must(template.New("").Funcs(template.FuncMap{
	"usd": func(cents int64) string {
		return fmt.Sprintf("$%d.%02d", cents/100, cents%100)
	},
	"usdPtr": func(cents *int64) string {
		if cents == nil {
			return "out of stock"
		}
		return fmt.Sprintf("$%d.%02d", *cents/100, *cents%100)
	},
	"stamp": func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04 UTC") },
	// barPct scales best against median for the deals savings bar.
	"barPct": func(best, median int64) int {
		if median <= 0 {
			return 100
		}
		return int(best * 100 / median)
	},
}).Parse(pageTemplates))

const pageTemplates = `
{{define "head"}}<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dealstream</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='7' fill='%2312151b'/%3E%3Cpath d='M8 22 L14 12 L19 18 L24 8' fill='none' stroke='%233ddc97' stroke-width='3' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700;800&family=Martian+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{
--bg:#0c0e12;--panel:#12151b;--panel2:#171b22;--line:#222835;--hair:#1a1f28;
--ink:#e4e8ee;--mut:#828b9a;--faint:#565e6b;
--acc:#3ddc97;--acc-dim:rgba(61,220,151,.13);--amber:#e8b04b;
--sans:'Archivo',system-ui,sans-serif;--mono:'Martian Mono',ui-monospace,Consolas,monospace;
}
*{box-sizing:border-box;margin:0}
html{color-scheme:dark}
body{background:var(--bg);color:var(--ink);font:400 13px/1.6 var(--mono);padding:0 20px 72px}
.wrap{max-width:1020px;margin:0 auto}
::selection{background:var(--acc);color:#08130e}

header{display:flex;align-items:center;gap:26px;padding:20px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
.brand{font:800 20px/1 var(--sans);color:var(--ink);text-decoration:none;letter-spacing:-.02em}
.brand em{font-style:normal;color:var(--acc)}
nav{display:flex;gap:20px;flex-wrap:wrap}
nav a{color:var(--mut);text-decoration:none;font-size:12.5px;letter-spacing:.02em}
nav a:hover{color:var(--ink)}
nav a.on{color:var(--ink);border-bottom:1px solid var(--acc);padding-bottom:2px}
.spacer{flex:1}
.raw{color:var(--mut);text-decoration:none;font-size:11.5px;border:1px solid var(--line);border-radius:6px;padding:5px 12px;transition:all .15s}
.raw:hover{color:var(--ink);border-color:var(--mut)}

h1{font:700 30px/1.15 var(--sans);letter-spacing:-.02em;margin:34px 0 8px}
.sub{color:var(--mut);margin-bottom:24px;font-size:12.5px;max-width:640px}
.sub a{color:var(--mut)}
.label{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--faint)}

.overflow{overflow-x:auto}
table{width:100%;border-collapse:collapse;margin:6px 0 14px}
th{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--faint);font-weight:400;text-align:left;padding:8px 12px;border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:10px 12px;border-bottom:1px solid var(--hair);vertical-align:middle;font-variant-numeric:tabular-nums}
td.r,th.r{text-align:right}
tbody tr{transition:background .1s}
tr:hover td{background:var(--panel)}
a{color:var(--ink)}
td a{text-decoration:none;border-bottom:1px solid transparent}
td a:hover{border-bottom-color:var(--mut)}
.mut{color:var(--mut)}
.acc{color:var(--acc)}
.tname{font:600 13.5px var(--sans);letter-spacing:0}
.badge{background:var(--acc-dim);color:var(--acc);border-radius:5px;padding:2px 8px;font-size:11.5px;white-space:nowrap}
.pill{display:inline-block;background:var(--panel2);border-radius:5px;padding:2px 9px;color:var(--mut);font-size:11px;white-space:nowrap}

.bar{width:96px;height:5px;background:var(--panel2);border-radius:3px;overflow:hidden;display:inline-block;vertical-align:middle}
.bar i{display:block;height:100%;background:var(--acc);border-radius:3px}

.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:14px;margin:26px 0}
.card{border:1px solid var(--line);border-radius:10px;padding:20px 20px 18px;background:linear-gradient(180deg,var(--panel2),var(--panel));display:block;text-decoration:none;transition:border-color .15s,transform .15s}
.card:hover{border-color:var(--acc);transform:translateY(-1px)}
.card b{display:block;font:600 16px var(--sans);color:var(--ink);margin-bottom:7px}
.card span{color:var(--mut);font-size:12px;line-height:1.55}

.stats{display:flex;gap:0;border:1px solid var(--line);border-radius:10px;overflow:hidden;margin:30px 0 8px;flex-wrap:wrap}
.stat{flex:1;min-width:150px;padding:18px 22px;background:var(--panel);border-right:1px solid var(--line)}
.stat:last-child{border-right:0}
.stat b{display:block;font:700 24px/1.1 var(--sans);letter-spacing:-.01em}
.stat span{color:var(--faint);font-size:10.5px;letter-spacing:.1em;text-transform:uppercase}

.flow{display:flex;align-items:stretch;gap:10px;margin:26px 0;flex-wrap:wrap}
.node{border:1px solid var(--line);border-radius:8px;padding:12px 16px;background:var(--panel)}
.node b{display:block;font:600 13px var(--sans)}
.node span{color:var(--faint);font-size:10.5px}
.arrow{align-self:center;color:var(--faint)}

form{display:flex;gap:10px;margin:22px 0;flex-wrap:wrap}
input,select{background:var(--panel);border:1px solid var(--line);border-radius:8px;color:var(--ink);padding:10px 14px;font:400 13px var(--mono);outline:none;transition:border-color .15s}
input:focus,select:focus{border-color:var(--acc)}
input[type=text]{flex:1;min-width:230px}
button{background:var(--acc);border:0;border-radius:8px;color:#08130e;font:600 13px var(--sans);padding:10px 22px;cursor:pointer}
button:hover{filter:brightness(1.08)}

.chart{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:18px;margin:14px 0}
.chart svg{display:block;width:100%;height:auto}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:11.5px;color:var(--mut);margin-bottom:12px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.axis{display:flex;justify-content:space-between;color:var(--faint);font-size:10.5px;margin-top:8px}
.note{color:var(--amber);font-size:12px;margin:10px 0}

footer{margin-top:44px;color:var(--faint);font-size:11px;border-top:1px solid var(--line);padding-top:16px;line-height:1.8}
footer a{color:var(--mut);text-decoration:none;border-bottom:1px dotted var(--faint)}
footer a:hover{color:var(--ink)}
</style></head><body><div class="wrap">
<header>
<a class="brand" href="/">deal<em>stream</em></a>
<nav>
<a href="/deals" {{if eq .View "deals"}}class="on"{{end}}>deals</a>
<a href="/search?q=drill&sort=price_asc" {{if eq .View "search"}}class="on"{{end}}>search</a>
<a href="/collections" {{if or (eq .View "collections") (eq .View "collection")}}class="on"{{end}}>collections</a>
<a href="https://github.com/ryanportfolio/dealstream">source</a>
</nav>
<span class="spacer"></span>
<a class="raw" href="{{.JSONHref}}">raw json</a>
</header>
{{end}}

{{define "foot"}}
<footer>every page renders the exact JSON the API serves at the same URL · <a href="{{.JSONHref}}">raw json</a> · Go on Postgres, ClickHouse, Redis · <a href="https://github.com/ryanportfolio/dealstream">how it works</a></footer>
</div></body></html>
{{end}}

{{define "landing"}}{{template "head" .}}
<h1>a deals catalog<br>built to be operated</h1>
<p class="sub">eight simulated retailer feeds, deliberately messy, ingested live into one clean catalog · every number on this page is real and moving</p>
<div class="stats">
<div class="stat"><b>1.1M</b><span>offers</span></div>
<div class="stat"><b>427k</b><span>products</span></div>
<div class="stat"><b>8M+</b><span>price observations</span></div>
<div class="stat"><b>8</b><span>retailer feeds</span></div>
</div>
<div class="flow">
<div class="node"><b>feedgen</b><span>8 messy feeds</span></div>
<span class="arrow">&rarr;</span>
<div class="node"><b>ingestd</b><span>validate · dedupe · quarantine</span></div>
<span class="arrow">&rarr;</span>
<div class="node"><b>postgres</b><span>catalog</span></div>
<div class="node"><b>clickhouse</b><span>price history</span></div>
<div class="node"><b>redis</b><span>deals · caches</span></div>
<span class="arrow">&rarr;</span>
<div class="node"><b>api</b><span>this page</span></div>
</div>
<div class="cards">
<a class="card" href="/deals"><b>deals</b><span>cross-retailer price dislocations, one retailer selling well under the median of the others, right now</span></a>
<a class="card" href="/search?q=drill&sort=price_asc"><b>search</b><span>full-text over 427k products with exact price sorting</span></a>
<a class="card" href="/collections"><b>collections</b><span>rule-based sets computed from live prices, membership moves as prices do</span></a>
<a class="card" href="/products/11508"><b>a product</b><span>one product merged across retailers, with price history and vector-similar neighbors</span></a>
</div>
<p class="sub">the pipeline behind this page validates, dedupes, and quarantines every feed item, and reports its own health · the <a href="https://github.com/ryanportfolio/dealstream">README</a> has the measurements</p>
{{template "foot" .}}{{end}}

{{define "deals"}}{{template "head" .}}
<h1>deals</h1>
<p class="sub">one retailer selling well below the median of the others · recomputed every few minutes from live offers</p>
<div class="overflow"><table>
<thead><tr><th>product</th><th>category</th><th class="r">best</th><th class="r">median</th><th>spread</th><th class="r">saves</th><th>at</th></tr></thead>
<tbody>{{range .Deals}}<tr>
<td><a class="tname" href="/products/{{.ProductID}}">{{.Title}}</a><br><span class="mut">{{.Brand}}</span></td>
<td><span class="pill">{{.Category}}</span></td>
<td class="r acc">{{usd .BestCents}}</td>
<td class="r mut">{{usd .MedianCents}}</td>
<td><span class="bar"><i style="width:{{barPct .BestCents .MedianCents}}%"></i></span></td>
<td class="r"><span class="badge">&minus;{{.SavingsPct}}%</span></td>
<td class="mut">{{.Retailer}}</td>
</tr>{{end}}</tbody>
</table></div>
{{template "foot" .}}{{end}}

{{define "search"}}{{template "head" .}}
<h1>search</h1>
<p class="sub">full-text over product titles · price filters and sorting apply to the best in-stock offer</p>
<form action="/search" method="get">
<input type="text" name="q" value="{{.Query}}" placeholder="drill, speaker, tent, kettle&hellip;">
<select name="sort">
<option value="" {{if eq .Sort ""}}selected{{end}}>relevance</option>
<option value="price_asc" {{if eq .Sort "price_asc"}}selected{{end}}>price low to high</option>
<option value="price_desc" {{if eq .Sort "price_desc"}}selected{{end}}>price high to low</option>
</select>
<button>search</button>
</form>
{{if .Truncated}}<p class="note">broad match, showing the 1,000 best-ranked candidates</p>{{end}}
<div class="overflow"><table>
<thead><tr><th>product</th><th>brand</th><th>category</th><th class="r">best price</th><th class="r">offers</th></tr></thead>
<tbody>{{range .Results}}<tr>
<td><a class="tname" href="/products/{{.ID}}">{{.Title}}</a></td>
<td class="mut">{{.Brand}}</td>
<td><span class="pill">{{.Category}}</span></td>
<td class="r acc">{{usdPtr .BestCents}}</td>
<td class="r mut">{{.OfferCount}}</td>
</tr>{{else}}<tr><td colspan="5" class="mut">nothing matched</td></tr>{{end}}</tbody>
</table></div>
{{template "foot" .}}{{end}}

{{define "product"}}{{template "head" .}}
<h1>{{.P.Title}}</h1>
<p class="sub">{{.P.Brand}} · <span class="pill">{{.P.Category}}</span> · sku {{.P.SKU}}{{range $k, $v := .P.Attrs}} · {{$k}} {{$v}}{{end}}</p>
<p class="sub"><a href="/products/{{.P.ID}}/history">price history</a> · <a href="/products/{{.P.ID}}/similar">similar items</a></p>
<div class="overflow"><table>
<thead><tr><th>retailer</th><th class="r">price</th><th>stock</th><th>updated</th></tr></thead>
<tbody>{{range .P.Offers}}<tr>
<td>{{.Retailer}}</td>
<td class="r acc">{{usd .PriceCents}}</td>
<td>{{if .InStock}}in stock{{else}}<span class="mut">out of stock</span>{{end}}</td>
<td class="mut">{{stamp .UpdatedAt}}</td>
</tr>{{end}}</tbody>
</table></div>
{{template "foot" .}}{{end}}

{{define "history"}}{{template "head" .}}
<h1>price history</h1>
<p class="sub">daily rollup from ClickHouse for <a href="/products/{{.ProductID}}">product {{.ProductID}}</a> · last {{.Days}} days · lines are each retailer's closing price</p>
{{if .Lines}}
<div class="chart">
<div class="legend">{{range .Lines}}<span><span class="dot" style="background:{{.Color}}"></span>{{.Retailer}}</span>{{end}}
<span class="spacer"></span><span>{{usd .Lo}} &ndash; {{usd .Hi}}</span></div>
<svg viewBox="0 0 {{.ChartW}} {{.ChartH}}" preserveAspectRatio="none" role="img" aria-label="daily last price per retailer">
<line x1="0" y1="0" x2="{{.ChartW}}" y2="0" stroke="#1a1f28" stroke-width="1"/>
<line x1="0" y1="{{.ChartH}}" x2="{{.ChartW}}" y2="{{.ChartH}}" stroke="#1a1f28" stroke-width="1"/>
<line x1="0" y1="100" x2="{{.ChartW}}" y2="100" stroke="#1a1f28" stroke-width="1" stroke-dasharray="3 5"/>
{{range .Lines}}<polyline points="{{.Points}}" fill="none" stroke="{{.Color}}" stroke-width="1.7" stroke-linejoin="round"/>{{end}}
{{range .Lines}}{{$c := .Color}}{{range .Dots}}<circle cx="{{.X}}" cy="{{.Y}}" r="3" fill="{{$c}}"/>{{end}}{{end}}
</svg>
<div class="axis"><span>{{.FirstDay}}</span><span>{{.LastDay}}</span></div>
</div>
{{end}}
<div class="overflow"><table>
<thead><tr><th>day</th><th>retailer</th><th class="r">low</th><th class="r">high</th><th class="r">last</th></tr></thead>
<tbody>{{range .Points}}<tr>
<td>{{.Day}}</td><td class="mut">{{.Retailer}}</td>
<td class="r">{{usd .MinCents}}</td><td class="r">{{usd .MaxCents}}</td><td class="r acc">{{usd .LastCents}}</td>
</tr>{{else}}<tr><td colspan="5" class="mut">no observations yet</td></tr>{{end}}</tbody>
</table></div>
{{template "foot" .}}{{end}}

{{define "similar"}}{{template "head" .}}
<h1>similar items</h1>
<p class="sub">pgvector nearest neighbors of <a href="/products/{{.ProductID}}">product {{.ProductID}}</a> by attribute-hash embedding · smaller distance is closer</p>
<div class="overflow"><table>
<thead><tr><th>product</th><th>brand</th><th>category</th><th class="r">best price</th><th class="r">distance</th></tr></thead>
<tbody>{{range .Similar}}<tr>
<td><a class="tname" href="/products/{{.ID}}">{{.Title}}</a></td>
<td class="mut">{{.Brand}}</td>
<td><span class="pill">{{.Category}}</span></td>
<td class="r acc">{{usdPtr .BestCents}}</td>
<td class="r mut">{{printf "%.4f" .Distance}}</td>
</tr>{{end}}</tbody>
</table></div>
{{template "foot" .}}{{end}}

{{define "collections"}}{{template "head" .}}
<h1>collections</h1>
<p class="sub">rule-based sets computed from live prices · no hand-picked members, membership moves as prices do</p>
<div class="cards">
{{range .Collections}}<a class="card" href="/collections/{{.Slug}}"><b>{{.Title}}</b><span>/collections/{{.Slug}}</span></a>{{end}}
</div>
{{template "foot" .}}{{end}}

{{define "collection"}}{{template "head" .}}
<h1>{{.Title}}</h1>
<p class="sub">membership recomputed from live offers · best price first</p>
<div class="overflow"><table>
<thead><tr><th>product</th><th>brand</th><th>category</th><th class="r">best price</th><th class="r">offers</th></tr></thead>
<tbody>{{range .Items}}<tr>
<td><a class="tname" href="/products/{{.ID}}">{{.Title}}</a></td>
<td class="mut">{{.Brand}}</td>
<td><span class="pill">{{.Category}}</span></td>
<td class="r acc">{{usdPtr .BestCents}}</td>
<td class="r mut">{{.OfferCount}}</td>
</tr>{{end}}</tbody>
</table></div>
{{template "foot" .}}{{end}}

{{define "error"}}{{template "head" .}}
<h1>{{.Status}}</h1>
<p class="sub">{{.Message}}</p>
<p class="sub">try <a href="/deals">deals</a> or <a href="/search?q=drill">search</a></p>
{{template "foot" .}}{{end}}
`

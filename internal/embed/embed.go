// Package embed builds product embeddings from catalog attributes using
// signed feature hashing: no model download, fully deterministic, and
// explainable (two products are close because they share tokens, weighted
// by field). Good enough for "similar deals"; swapping in a learned
// embedding only changes this package.
package embed

import (
	"hash/fnv"
	"math"
	"strings"
)

const Dim = 256

// Field weights: category dominates (a drill is never similar to a serum),
// brand groups, title tokens discriminate within a category.
const (
	weightCategory = 3.0
	weightBrand    = 2.0
	weightTitle    = 1.0
	weightAttr     = 0.5
)

func Vector(title, brand, category string, attrs map[string]string) []float32 {
	v := make([]float32, Dim)
	add := func(token string, weight float64) {
		if token == "" {
			return
		}
		h := fnv.New64a()
		h.Write([]byte(token))
		sum := h.Sum64()
		idx := sum % Dim
		sign := 1.0
		if (sum>>63)&1 == 1 {
			sign = -1.0
		}
		v[idx] += float32(sign * weight)
	}

	add("cat:"+strings.ToLower(category), weightCategory)
	add("brand:"+strings.ToLower(brand), weightBrand)
	for _, tok := range Tokenize(title) {
		// The brand usually leads the title; skip it there so it is not
		// double counted against the weighted brand feature.
		if strings.EqualFold(tok, brand) {
			continue
		}
		add("t:"+tok, weightTitle)
	}
	for k, val := range attrs {
		add("a:"+strings.ToLower(k)+"="+strings.ToLower(val), weightAttr)
	}

	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm > 0 {
		inv := float32(1 / math.Sqrt(norm))
		for i := range v {
			v[i] *= inv
		}
	}
	return v
}

// Tokenize lowercases and splits on non-alphanumerics, dropping single
// characters and bare numbers (model numbers pull unrelated products
// together).
func Tokenize(s string) []string {
	var out []string
	var b strings.Builder
	flush := func() {
		tok := b.String()
		b.Reset()
		if len(tok) < 2 || isNumeric(tok) {
			return
		}
		out = append(out, tok)
	}
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return out
}

func isNumeric(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

package embed

import (
	"math"
	"testing"
)

func cosine(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot // inputs are L2-normalized
}

func TestDeterministicAndNormalized(t *testing.T) {
	a := Vector("Arvello Pro Drill 512", "Arvello", "tools", map[string]string{"color": "Slate"})
	b := Vector("Arvello Pro Drill 512", "Arvello", "tools", map[string]string{"color": "Slate"})
	var norm float64
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("not deterministic")
		}
		norm += float64(a[i]) * float64(a[i])
	}
	if math.Abs(norm-1) > 1e-5 {
		t.Fatalf("not unit norm: %f", norm)
	}
}

func TestSimilarityOrdering(t *testing.T) {
	drill := Vector("Arvello Pro Drill 512", "Arvello", "tools", nil)
	drill2 := Vector("Nordbeck Compact Drill 210", "Nordbeck", "tools", nil)
	sander := Vector("Nordbeck Compact Sander 300", "Nordbeck", "tools", nil)
	serum := Vector("Lumenor Everyday Serum 118", "Lumenor", "beauty", nil)

	sameTool := cosine(drill, drill2)
	crossTool := cosine(drill, sander)
	crossCategory := cosine(drill, serum)

	if sameTool <= crossCategory || crossTool <= crossCategory {
		t.Fatalf("category weighting broken: drill/drill2=%.3f drill/sander=%.3f drill/serum=%.3f",
			sameTool, crossTool, crossCategory)
	}
	if sameTool <= 0.2 {
		t.Fatalf("same-category same-noun similarity too low: %.3f", sameTool)
	}
}

func TestTokenize(t *testing.T) {
	got := Tokenize("Arvello Heavy-Duty Drill 512")
	want := []string{"arvello", "heavy", "duty", "drill"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

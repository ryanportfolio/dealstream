package feedgen

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func testServer() *Server {
	u := testUniverse()
	cfg := RetailerCfg{Slug: "testmart", CarryRate: 0.5, PageSizeMax: 100}
	return NewServer(u, map[string]*Stream{"testmart": NewStream(u, cfg)})
}

func getJSON(t *testing.T, s *Server, path string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Mux().ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	if rec.Code != 200 {
		t.Fatalf("GET %s: status %d", path, rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET %s: bad json: %v", path, err)
	}
	return body
}

func TestCatalogNegativeCursorClamped(t *testing.T) {
	// A negative cursor must not index products outside the universe; the
	// deterministic hash would fabricate phantoms for them.
	s := testServer()
	neg := getJSON(t, s, "/r/testmart/catalog?cursor=-500&limit=5")
	zero := getJSON(t, s, "/r/testmart/catalog?cursor=0&limit=5")
	negItems, zeroItems := neg["items"].([]any), zero["items"].([]any)
	if len(negItems) == 0 || len(zeroItems) == 0 {
		t.Fatal("expected items on first page")
	}
	if negItems[0].(map[string]any)["sku"] != zeroItems[0].(map[string]any)["sku"] {
		t.Fatal("negative cursor served different products than cursor=0")
	}
}

func TestOffersCarryEpochAndHasMore(t *testing.T) {
	s := testServer()
	s.Streams["testmart"].Tick(30)

	page := getJSON(t, s, "/r/testmart/offers?since=0&limit=10")
	if _, ok := page["epoch"]; !ok {
		t.Fatal("offers response missing epoch")
	}
	if page["has_more"] != true {
		t.Fatal("20 remaining updates but has_more not true")
	}

	cat := getJSON(t, s, "/r/testmart/catalog?limit=5")
	if cat["epoch"] != page["epoch"] {
		t.Fatalf("catalog epoch %v != offers epoch %v", cat["epoch"], page["epoch"])
	}

	drained := getJSON(t, s, "/r/testmart/offers?since=30&limit=10")
	if drained["has_more"] != false {
		t.Fatal("drained feed reported has_more")
	}
}

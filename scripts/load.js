// k6 load mix approximating catalog-browse traffic. Run:
//   k6 run scripts/load.js
// Env: API (default http://127.0.0.1:8080), VUS, DURATION.
import http from "k6/http";
import { check, sleep } from "k6";
import { Trend } from "k6/metrics";

const API = __ENV.API || "http://127.0.0.1:8080";

// Random product ids hit gaps on purpose; a 404 is correct behavior, so
// only 5xx and network errors count as failures.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 404 }));

export const options = {
  vus: Number(__ENV.VUS || 30),
  duration: __ENV.DURATION || "5m",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(99)<1500"],
  },
};

const byRoute = {
  search: new Trend("route_search", true),
  product: new Trend("route_product", true),
  history: new Trend("route_history", true),
  similar: new Trend("route_similar", true),
  deals: new Trend("route_deals", true),
  collection: new Trend("route_collection", true),
};

const QUERIES = ["drill", "speaker", "tent", "kettle", "headphones", "vacuum", "jacket", "drone", "sander", "lamp"];
const COLLECTIONS = ["audio-under-50", "camp-kit", "kitchen-upgrades", "tool-bench", "under-20"];

// Product ids are dense from the seed; sampling a wide range gives a
// realistic mix of hits and cache misses.
function productID() {
  return 1 + Math.floor(Math.random() * 420000);
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default function () {
  const r = Math.random();
  let res, route;
  if (r < 0.35) {
    route = "search";
    res = http.get(`${API}/search?q=${pick(QUERIES)}&sort=price_asc&limit=20`);
  } else if (r < 0.6) {
    route = "product";
    res = http.get(`${API}/products/${productID()}`);
  } else if (r < 0.72) {
    route = "history";
    res = http.get(`${API}/products/${productID()}/history?days=30`);
  } else if (r < 0.82) {
    route = "similar";
    res = http.get(`${API}/products/${productID()}/similar`);
  } else if (r < 0.92) {
    route = "deals";
    res = http.get(`${API}/deals?limit=50`);
  } else {
    route = "collection";
    res = http.get(`${API}/collections/${pick(COLLECTIONS)}`);
  }
  // 404s are expected for ids that fell in gaps; anything 5xx is not.
  check(res, { "not 5xx": (r) => r.status < 500 });
  byRoute[route].add(res.timings.duration);
  sleep(0.3 + Math.random() * 0.4);
}

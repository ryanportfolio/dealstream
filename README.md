# dealstream

A deals-catalog backend built to be operated, not just shipped. Go services
ingest multi-retailer price feeds into Postgres, Redis, and ClickHouse, serve
search, collections, price history, and similar-deal recommendations, and
report their own health through Prometheus and Grafana.

Work in progress. Numbers, dashboards, and the full writeup land as milestones
finish.

## Layout

- `cmd/feedgen` fake retailer APIs with configurable data mess
- `cmd/ingestd` poll, validate, quarantine, upsert
- `cmd/api` search, collections, history, recommendations
- `db/` Postgres and ClickHouse schemas
- `deploy/` docker compose stack and observability config

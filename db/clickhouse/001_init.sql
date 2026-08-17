-- Append-only price observations. One row per (offer, poll) where the
-- price or stock state changed. Queries are per-product time ranges and
-- catalog-wide aggregates, hence the ORDER BY.

CREATE TABLE IF NOT EXISTS dealstream.price_events (
    product_id   UInt64,
    retailer_id  UInt16,
    price_cents  Int64,
    currency     FixedString(3),
    in_stock     Bool,
    observed_at  DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (product_id, retailer_id, observed_at);

-- Daily rollup for fast history charts over long ranges.
CREATE MATERIALIZED VIEW IF NOT EXISTS dealstream.price_daily
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (product_id, retailer_id, day)
AS SELECT
    product_id,
    retailer_id,
    toDate(observed_at)          AS day,
    minState(price_cents)        AS min_price,
    maxState(price_cents)        AS max_price,
    argMaxState(price_cents, observed_at) AS last_price
FROM dealstream.price_events
GROUP BY product_id, retailer_id, day;

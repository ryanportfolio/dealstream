-- Canonical catalog. Offers are the unit of ingestion: one row per
-- (product, retailer). Price history lives in ClickHouse, not here.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE retailers (
    id          smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug        text NOT NULL UNIQUE,
    name        text NOT NULL,
    status      text NOT NULL DEFAULT 'active'  -- active | degraded | dead
);

CREATE TABLE products (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku_norm    text NOT NULL UNIQUE,           -- normalized cross-retailer key
    title       text NOT NULL,
    brand       text NOT NULL,
    category    text NOT NULL,
    attrs       jsonb NOT NULL DEFAULT '{}',
    embedding   vector(256),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX products_category_idx ON products (category);
CREATE INDEX products_brand_idx ON products (brand);
CREATE INDEX products_title_trgm_idx ON products USING gin (to_tsvector('simple', title));

CREATE TABLE offers (
    product_id  bigint NOT NULL REFERENCES products (id),
    retailer_id smallint NOT NULL REFERENCES retailers (id),
    price_cents bigint NOT NULL,
    currency    char(3) NOT NULL DEFAULT 'USD',
    url         text NOT NULL,
    in_stock    boolean NOT NULL DEFAULT true,
    first_seen  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, retailer_id)
);

CREATE INDEX offers_retailer_updated_idx ON offers (retailer_id, updated_at);
CREATE INDEX offers_price_idx ON offers (price_cents) WHERE in_stock;

-- Rows that failed validation. Kept, not dropped: quarantine rate is a
-- first-class metric and the rows are replayable after a fix.
CREATE TABLE quarantine (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    retailer_id smallint,
    reason      text NOT NULL,
    raw         jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX quarantine_reason_idx ON quarantine (reason, received_at);

CREATE TABLE ingest_cursors (
    retailer_id smallint PRIMARY KEY REFERENCES retailers (id),
    cursor      text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE collections (
    id          int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug        text NOT NULL UNIQUE,
    title       text NOT NULL,
    -- rule-based membership, e.g. {"category": "audio", "max_price_cents": 5000}
    rules       jsonb NOT NULL
);

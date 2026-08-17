-- Collections ask for "cheapest in-stock offers in a category (or
-- brand)". With category only on products, that plan probes hundreds of
-- thousands of product rows to find a few thousand matches (measured:
-- 19.6s for outdoors). Category and brand are immutable-ish product
-- facts, so they are denormalized onto offers at write time and indexed
-- with price.

ALTER TABLE offers ADD COLUMN category text NOT NULL DEFAULT '';
ALTER TABLE offers ADD COLUMN brand    text NOT NULL DEFAULT '';

UPDATE offers o
SET category = p.category, brand = p.brand
FROM products p
WHERE p.id = o.product_id;

CREATE INDEX offers_category_price_idx ON offers (category, price_cents) WHERE in_stock;
CREATE INDEX offers_brand_price_idx    ON offers (brand, price_cents)    WHERE in_stock;

-- Starter rule-based collections. Idempotent.

INSERT INTO collections (slug, title, rules) VALUES
  ('audio-under-50',   'Audio under $50',        '{"category": "audio", "max_price_cents": 5000}'),
  ('camp-kit',         'Camp kit',               '{"category": "outdoors"}'),
  ('kitchen-upgrades', 'Kitchen upgrades',       '{"category": "kitchen", "min_offers": 3}'),
  ('tool-bench',       'Tool bench',             '{"category": "tools"}'),
  ('ridgeline-gear',   'Ridgeline gear',         '{"brand": "Ridgeline"}'),
  ('under-20',         'Everything under $20',   '{"max_price_cents": 2000, "min_offers": 4}')
ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title, rules = EXCLUDED.rules;

-- Basalt SQL rewrite of rest.bsl (golden: examples/golden/rest.plan)
-- BSL: read http "url" @[page, page_param, page_size, total_field, retries, ...]
--
-- PAGINATE BY / RETRY ON are the promoted REST clauses (migration.md §6);
-- rarer knobs stay in the WITH (...) bag. WHERE on a REST source runs in
-- basalt after the fetch (no pushdown to REST).

LOAD INTO 'examples/out.csv' AS
SELECT id, name
FROM HTTP('https://api.example.com/v1/items')
  PAGINATE BY page (param = 'page', size = 100, total = 'count')
  RETRY 3 ON (429, 503)
  WITH (prefetch = 4, timeout_ms = 30000)
WHERE active = 'true';

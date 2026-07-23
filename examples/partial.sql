-- Basalt SQL rewrite of partial.bsl (golden: examples/golden/partial.plan)
-- BSL: write sr stream_load items upsert on id partial cols (status, amount)

CREATE CONNECTION sr TYPE starrocks OPTIONS (
  fe_host  = '10.140.0.7',
  fe_port  = 9030,
  be_url   = 'http://10.140.0.10:8040',
  database = 'bronze'
);

LOAD INTO sr.items
  USING stream_load
  UPSERT ON (id) PARTIAL COLS (status, amount)
AS
SELECT * FROM 'examples/in.csv';

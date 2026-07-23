-- Basalt SQL rewrite of upsert_infer.bsl (golden: examples/golden/upsert_infer.plan)
-- BSL: write sr stream_load SA1010 upsert   (bare: infer the PK at plan time)
--
-- Bare UPSERT (no ON) asks the runtime to infer the key from the source
-- table's PK metadata — requires a table read on a SQL source that exposes it.

CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  database = 'totvs'
);

CREATE CONNECTION sr TYPE starrocks OPTIONS (
  fe_host  = '10.140.0.7',
  fe_port  = 9030,
  be_url   = 'http://10.140.0.10:8040',
  database = 'bronze'
);

LOAD INTO sr.SA1010
  USING stream_load
  UPSERT
AS
SELECT * FROM erp.dbo.SA1010;

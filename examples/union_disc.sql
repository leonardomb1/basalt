-- Basalt SQL rewrite of union_disc.bsl (golden: examples/golden/union_disc.plan)
-- BSL: union erp tables "SELECT name, SUBSTRING(...) ..." @[tag = ..., where = ...]
--
-- Discovered union: one branch per discovery-query row — first column names the
-- table, second column is the branch tag. AS (table_name, CT2_EMPRESA) names the
-- output tag column; PUSHDOWN pushes the raw predicate into every branch.

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

LOAD INTO sr.CT2_UNIFIED
  USING stream_load
  UPSERT ON (CT2_EMPRESA, R_E_C_N_O_)
AS
SELECT *
FROM EACH TABLE OF (erp.QUERY($$SELECT name, SUBSTRING(name, 4, 2) FROM sys.tables WHERE name LIKE 'CT2%'$$))
  AS (table_name, CT2_EMPRESA)
  PUSHDOWN($$D_E_L_E_T_ <> '*'$$);

-- Basalt SQL rewrite of union.bsl (golden: examples/golden/union.plan)
-- BSL: union from erp table ... as "01" ... @[tag = CT2_EMPRESA, canon = CT2010]
--
-- The tag column is a literal ('01' AS CT2_EMPRESA), the canon is ANCHOR SCHEMA,
-- reconciliation is by name (UNION ALL BY NAME). Credentials by convention:
-- erp -> ERP_USER/ERP_PASS, sr -> SR_USER/SR_PASS (same vars the BSL named).

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
  SPLIT BY (R_E_C_N_O_)
AS
SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
UNION ALL BY NAME
SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
ANCHOR SCHEMA erp.dbo.CT2010;

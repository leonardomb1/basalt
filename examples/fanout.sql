-- Basalt SQL rewrite of fanout.bsl (golden: examples/golden/fanout.plan)
-- BSL: for name, pk in job.tables @[mode = parallel, on_error = continue]
--        match pk is empty => {...} _ => {...} end
--
-- FOR EACH ROW OF over a JSON-param path; per-row dispatch via the CASE
-- statement (guard form). ${...} interpolation in targets/keys/raw SQL is
-- unchanged from BSL.

CREATE ENDPOINT '/fanout';

PARAM job JSON FROM BODY;

CREATE CONNECTION crm TYPE sqlserver OPTIONS (
  host     = 'crm.internal',
  database = 'crm'
);

CREATE CONNECTION sr TYPE starrocks OPTIONS (
  fe_host  = '10.140.0.7',
  fe_port  = 9030,
  be_url   = 'http://10.140.0.10:8040',
  database = 'bronze'
);

FOR EACH ROW OF ($job.tables) AS (name, pk)
  PARALLEL ON ERROR CONTINUE
  CASE
    WHEN pk IS EMPTY THEN
      LOAD INTO sr.'crm_${lower(name)}' USING stream_load AS
      SELECT * FROM crm.QUERY($$SELECT * FROM ${name}$$);
    ELSE
      LOAD INTO sr.'crm_${lower(name)}' USING stream_load
        UPSERT ON ('${pk}') AS
      SELECT *, now() AS extraction_timestamp
      FROM crm.QUERY($$SELECT * FROM ${name}$$);
  END CASE
END FOR;

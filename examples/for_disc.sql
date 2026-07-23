-- Basalt SQL rewrite of for_disc.bsl (golden: examples/golden/for_disc.plan)
-- BSL: for name, pk in crm query "SELECT ..." @[mode = sequential, on_error = stop]
--
-- Fan-out over a DISCOVERY QUERY (vs the JSON-param form in fanout.sql): the
-- query's first N columns bind to the N loop variables positionally.

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

FOR EACH ROW OF (crm.QUERY($$SELECT name, pk FROM meta_tables$$)) AS (name, pk)
  SEQUENTIAL ON ERROR STOP
  LOAD INTO sr.'crm_${lower(name)}' USING stream_load
    UPSERT ON ('${pk}') AS
  SELECT * FROM crm.QUERY($$SELECT * FROM ${name}$$);
END FOR;

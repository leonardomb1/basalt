-- Basalt SQL rewrite of ingest.bsl (golden: examples/golden/ingest.plan)
-- BSL: @http(path = "/ingest"), read request | select ... | filter | write append
--
-- Notable deltas vs the BSL original (all intentional, per migration.md):
--   * FROM BODY declares the schema (BSL `read request` inferred it at runtime);
--     the declaration is the endpoint's contract (§9).
--   * Credentials by convention: connection `sr` reads SR_USER / SR_PASS from
--     the environment — exactly the vars the BSL script named via env()/secret().
--   * WHERE precedes SELECT, so it filters on the body column `event`, not the
--     projected alias `kind` (plan delta: filter before project — see README).

CREATE ENDPOINT '/ingest';

CREATE CONNECTION sr TYPE starrocks OPTIONS (
  fe_host  = '10.140.0.7',
  fe_port  = 9030,
  be_url   = 'http://10.140.0.10:8040',
  database = 'test'
);

LOAD INTO sr.events_log USING stream_load APPEND AS
SELECT event         AS kind,
       CAST(value AS INT) AS value,
       now()         AS ingested_at
FROM BODY (
  event STRING,
  value STRING
)
WHERE event != 'debug';

-- Basalt SQL rewrite of except.bsl (golden: examples/golden/except.plan)
-- BSL: select * except (note)   (EXCLUDE is the DuckDB spelling; both accepted)

LOAD INTO 'examples/out.csv' AS
SELECT * EXCLUDE (note)
FROM 'examples/in.csv';

-- Basalt SQL rewrite of distinct.bsl (golden: examples/golden/distinct.plan)
-- BSL: select status | distinct on status

LOAD INTO 'examples/out.csv' AS
SELECT DISTINCT status
FROM 'examples/in.csv';

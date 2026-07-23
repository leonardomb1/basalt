-- Basalt SQL rewrite of agg.bsl (golden: examples/golden/agg.plan)
-- BSL: aggregate n = count(), total = sum(cast(amount as int)) by status

LOAD INTO 'examples/out.csv' AS
SELECT status,
       COUNT(*)                 AS n,
       SUM(CAST(amount AS INT)) AS total
FROM 'examples/in.csv'
GROUP BY status;

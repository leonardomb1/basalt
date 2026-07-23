-- Basalt SQL rewrite of csv_demo.bsl (golden: examples/golden/csv_demo.plan)
-- BSL: filter status == "paid" | select ..., label = if(...) | limit 2

LOAD INTO 'examples/out.csv' AS
SELECT id,
       amount,
       CASE WHEN amount IS NULL THEN 'n/a'
            ELSE concat('$', amount) END AS label,
       note
FROM 'examples/in.csv'
WHERE status = 'paid'
LIMIT 2;

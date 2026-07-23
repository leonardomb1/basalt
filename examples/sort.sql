-- Basalt SQL rewrite of sort.bsl (golden: examples/golden/sort.plan)
-- BSL: filter amount is not null | select id, amt = cast(...) | sort amt desc

LOAD INTO 'examples/out.csv' AS
SELECT id, CAST(amount AS INT) AS amt
FROM 'examples/in.csv'
WHERE amount IS NOT NULL
ORDER BY amt DESC;

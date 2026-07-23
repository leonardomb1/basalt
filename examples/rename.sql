-- Basalt SQL rewrite of rename.bsl (golden: examples/golden/rename.plan)
-- BSL: select * rename (amount as amt)

LOAD INTO 'examples/out.csv' AS
SELECT * RENAME (amount AS amt)
FROM 'examples/in.csv';

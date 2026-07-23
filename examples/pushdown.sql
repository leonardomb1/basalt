-- Basalt SQL rewrite of pushdown.bsl (golden: examples/golden/pushdown.plan)
-- BSL: read erp table dbo.SC5010 @[where = "D_E_L_E_T_ <> '*'"] | filter | select
--
-- The raw @[where] fragment becomes the explicit PUSHDOWN($$...$$) clause
-- (verbatim to the source, dollar-quoted so the '*' needs no escaping).
-- `WHERE valor > 0` and the projection are implicit-pushdown candidates:
-- if the translator covers them they descend ANDed / as the column list,
-- otherwise they run in basalt. check -s shows which.

CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  database = 'totvs'
);

LOAD INTO 'examples/out.csv' AS
SELECT filial, num, valor, SUBSTR(filial, 1, 2) AS empresa
FROM erp.dbo.SC5010
  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
WHERE valor > 0;

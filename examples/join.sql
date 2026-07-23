-- Basalt SQL rewrite of join.bsl (golden: examples/golden/join.plan)
-- BSL: let paid = ... ; read ... | join left paid on id = id

LOAD INTO 'examples/out.csv' AS
WITH paid AS (
  SELECT id, amount
  FROM 'examples/in.csv'
  WHERE status = 'paid'
)
SELECT t.id, t.note, p.amount
FROM 'examples/in.csv' t
LEFT JOIN paid p ON t.id = p.id;

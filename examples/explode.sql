-- Basalt SQL rewrite of explode.bsl (golden: examples/golden/explode.plan)
-- BSL: explode note as piece on ","

LOAD INTO 'examples/out.csv' AS
SELECT id, piece
FROM 'examples/in.csv'
CROSS JOIN UNNEST(SPLIT(note, ',')) AS piece;

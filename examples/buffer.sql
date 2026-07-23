-- Basalt SQL rewrite of buffer.bsl (golden: examples/golden/buffer.plan)
-- BSL: read erp table dbo.SC5010 @[buffer]
--
-- WITH (buffer) fully drains + closes the source before opening the sink
-- (avoids slow-consumer query aborts, e.g. Dataverse).

CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  database = 'totvs'
);

LOAD INTO 'examples/out.csv' AS
SELECT filial, num
FROM erp.dbo.SC5010
  WITH (buffer);

-- TPC-H Q12: shipping modes and order priority. One inner join plus a
-- column-to-column temporal comparison (l_commitdate < l_receiptdate), which
-- no statistics can prune, and conditional aggregation.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH ord AS (
  SELECT o_orderkey, o_orderpriority
  FROM IDENTIFIER($data || '/orders.parquet')
)
SELECT l.l_shipmode,
       SUM(CASE WHEN o.o_orderpriority = '1-URGENT'
                  OR o.o_orderpriority = '2-HIGH'  THEN 1 ELSE 0 END) AS high_line_count,
       SUM(CASE WHEN o.o_orderpriority <> '1-URGENT'
                 AND o.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM IDENTIFIER($data || '/lineitem.parquet') l
JOIN ord o ON l.l_orderkey = o.o_orderkey
WHERE l.l_commitdate  <  l.l_receiptdate
  AND l.l_shipdate    <  l.l_commitdate
  AND l.l_receiptdate >= '1994-01-01'
  AND l.l_receiptdate <  '1995-01-01'
  AND l.l_shipmode IN ('MAIL', 'SHIP')
GROUP BY l.l_shipmode
ORDER BY l.l_shipmode;

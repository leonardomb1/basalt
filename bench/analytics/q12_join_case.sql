-- TPC-H Q12: join, IN list, date comparisons between columns, CASE in aggregates.
SELECT l.l_shipmode,
       sum(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
       sum(CASE WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM {orders} o
JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey
WHERE l.l_shipmode IN ('MAIL', 'SHIP')
  AND l.l_commitdate < l.l_receiptdate AND l.l_shipdate < l.l_commitdate
  AND l.l_receiptdate >= CAST('1994-01-01' AS DATE) AND l.l_receiptdate < CAST('1995-01-01' AS DATE)
GROUP BY l.l_shipmode
ORDER BY l.l_shipmode;

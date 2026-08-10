-- DuckDB reference for bench/tpch/q12.sql.
COPY (
  SELECT l.l_shipmode,
         sum(CASE WHEN o.o_orderpriority = '1-URGENT'
                    OR o.o_orderpriority = '2-HIGH'  THEN 1 ELSE 0 END) AS high_line_count,
         sum(CASE WHEN o.o_orderpriority <> '1-URGENT'
                   AND o.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
  FROM read_parquet('@DATA@/lineitem.parquet') l
  JOIN (SELECT o_orderkey, o_orderpriority
        FROM read_parquet('@DATA@/orders.parquet')) o ON l.l_orderkey = o.o_orderkey
  WHERE l.l_commitdate  <  l.l_receiptdate
    AND l.l_shipdate    <  l.l_commitdate
    AND l.l_receiptdate >= '1994-01-01'
    AND l.l_receiptdate <  '1995-01-01'
    AND l.l_shipmode IN ('MAIL', 'SHIP')
  GROUP BY l.l_shipmode
  ORDER BY l.l_shipmode
) TO '@OUT@' (FORMAT csv, HEADER);

-- DuckDB reference for bench/tpch/q03.sql.
COPY (
  SELECT l.l_orderkey,
         sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
         o.o_orderdate,
         o.o_shippriority
  FROM read_parquet('@DATA@/lineitem.parquet') l
  JOIN (SELECT o_orderkey, o_custkey, o_orderdate, o_shippriority
        FROM read_parquet('@DATA@/orders.parquet')
        WHERE o_orderdate < '1995-03-15') o ON l.l_orderkey = o.o_orderkey
  JOIN (SELECT c_custkey
        FROM read_parquet('@DATA@/customer.parquet')
        WHERE c_mktsegment = 'BUILDING') c ON o.o_custkey = c.c_custkey
  WHERE l.l_shipdate > '1995-03-15'
  GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
  ORDER BY revenue DESC, o.o_orderdate
  LIMIT 10
) TO '@OUT@' (FORMAT csv, HEADER);

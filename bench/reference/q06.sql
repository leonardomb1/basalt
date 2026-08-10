-- DuckDB reference for bench/tpch/q06.sql.
COPY (
  SELECT sum(l_extendedprice * l_discount) AS revenue
  FROM read_parquet('@DATA@/lineitem.parquet')
  WHERE l_shipdate >= '1994-01-01'
    AND l_shipdate <  '1995-01-01'
    AND l_discount >= 0.05
    AND l_discount <= 0.07
    AND l_quantity <  24
) TO '@OUT@' (FORMAT csv, HEADER);

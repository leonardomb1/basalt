-- DuckDB reference for bench/tpch/q14.sql. Emits the two SUMs rather than their
-- ratio, matching the basalt script.
COPY (
  SELECT sum(CASE WHEN p.p_type LIKE 'PROMO%'
                  THEN l.l_extendedprice * (1 - l.l_discount)
                  ELSE 0 END)                        AS promo_revenue,
         sum(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
  FROM read_parquet('@DATA@/lineitem.parquet') l
  JOIN (SELECT p_partkey, p_type
        FROM read_parquet('@DATA@/part.parquet')) p ON l.l_partkey = p.p_partkey
  WHERE l.l_shipdate >= '1995-09-01'
    AND l.l_shipdate <  '1995-10-01'
) TO '@OUT@' (FORMAT csv, HEADER);

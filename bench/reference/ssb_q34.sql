-- DuckDB reference for bench/ssb/q34.sql.
COPY (
SELECT c.c_city, s.s_city, d.d_year, sum(lo_revenue) AS revenue
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey, d_year FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_yearmonth = 'Dec1997') d ON lo_orderdate = d_datekey
JOIN (SELECT c_custkey, c_city FROM read_parquet('@DATA@/ssb/customer.parquet') WHERE c_city IN ('UNITED KI1', 'UNITED KI5')) c ON lo_custkey = c_custkey
JOIN (SELECT s_suppkey, s_city FROM read_parquet('@DATA@/ssb/supplier.parquet') WHERE s_city IN ('UNITED KI1', 'UNITED KI5')) s ON lo_suppkey = s_suppkey
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year, revenue DESC
) TO '@OUT@' (FORMAT csv, HEADER);

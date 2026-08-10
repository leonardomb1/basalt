-- DuckDB reference for bench/ssb/q31.sql.
COPY (
SELECT c.c_nation, s.s_nation, d.d_year, sum(lo_revenue) AS revenue
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey, d_year FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_year >= 1992 AND d_year <= 1997) d ON lo_orderdate = d_datekey
JOIN (SELECT c_custkey, c_nation FROM read_parquet('@DATA@/ssb/customer.parquet') WHERE c_region = 'ASIA') c ON lo_custkey = c_custkey
JOIN (SELECT s_suppkey, s_nation FROM read_parquet('@DATA@/ssb/supplier.parquet') WHERE s_region = 'ASIA') s ON lo_suppkey = s_suppkey
GROUP BY c.c_nation, s.s_nation, d.d_year
ORDER BY d.d_year, revenue DESC
) TO '@OUT@' (FORMAT csv, HEADER);

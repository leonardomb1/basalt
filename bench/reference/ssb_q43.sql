-- DuckDB reference for bench/ssb/q43.sql.
COPY (
SELECT d.d_year, s.s_city, p.p_brand1, sum(lo_revenue - lo_supplycost) AS profit
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey, d_year FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_year IN (1997, 1998)) d ON lo_orderdate = d_datekey
JOIN (SELECT c_custkey FROM read_parquet('@DATA@/ssb/customer.parquet') WHERE c_region = 'AMERICA') c ON lo_custkey = c_custkey
JOIN (SELECT s_suppkey, s_city FROM read_parquet('@DATA@/ssb/supplier.parquet') WHERE s_nation = 'UNITED STATES') s ON lo_suppkey = s_suppkey
JOIN (SELECT p_partkey, p_brand1 FROM read_parquet('@DATA@/ssb/part.parquet') WHERE p_category = 'MFGR#14') p ON lo_partkey = p_partkey
GROUP BY d.d_year, s.s_city, p.p_brand1
ORDER BY d.d_year, s.s_city, p.p_brand1
) TO '@OUT@' (FORMAT csv, HEADER);

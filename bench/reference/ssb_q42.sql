-- DuckDB reference for bench/ssb/q42.sql.
COPY (
SELECT d.d_year, s.s_nation, p.p_category, sum(lo_revenue - lo_supplycost) AS profit
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey, d_year FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_year IN (1997, 1998)) d ON lo_orderdate = d_datekey
JOIN (SELECT c_custkey FROM read_parquet('@DATA@/ssb/customer.parquet') WHERE c_region = 'AMERICA') c ON lo_custkey = c_custkey
JOIN (SELECT s_suppkey, s_nation FROM read_parquet('@DATA@/ssb/supplier.parquet') WHERE s_region = 'AMERICA') s ON lo_suppkey = s_suppkey
JOIN (SELECT p_partkey, p_category FROM read_parquet('@DATA@/ssb/part.parquet') WHERE p_mfgr IN ('MFGR#1', 'MFGR#2')) p ON lo_partkey = p_partkey
GROUP BY d.d_year, s.s_nation, p.p_category
ORDER BY d.d_year, s.s_nation, p.p_category
) TO '@OUT@' (FORMAT csv, HEADER);

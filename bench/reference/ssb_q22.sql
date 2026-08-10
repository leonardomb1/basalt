-- DuckDB reference for bench/ssb/q22.sql.
COPY (
SELECT sum(lo_revenue) AS revenue, d.d_year, p.p_brand1
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey, d_year FROM read_parquet('@DATA@/ssb/date.parquet')) d ON lo_orderdate = d_datekey
JOIN (SELECT s_suppkey FROM read_parquet('@DATA@/ssb/supplier.parquet') WHERE s_region = 'ASIA') s ON lo_suppkey = s_suppkey
JOIN (SELECT p_partkey, p_brand1 FROM read_parquet('@DATA@/ssb/part.parquet') WHERE p_brand1 BETWEEN 'MFGR#221' AND 'MFGR#228') p ON lo_partkey = p_partkey
GROUP BY d.d_year, p.p_brand1
ORDER BY d.d_year, p.p_brand1
) TO '@OUT@' (FORMAT csv, HEADER);

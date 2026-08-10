-- SSB Q4.1 fact x 4 dimensions, profit by year and nation
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q41.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey, d_year FROM IDENTIFIER($data || '/ssb/date.parquet')),
     c AS (SELECT c_custkey, c_nation FROM IDENTIFIER($data || '/ssb/customer.parquet') WHERE c_region = 'AMERICA'),
     s AS (SELECT s_suppkey FROM IDENTIFIER($data || '/ssb/supplier.parquet') WHERE s_region = 'AMERICA'),
     p AS (SELECT p_partkey FROM IDENTIFIER($data || '/ssb/part.parquet') WHERE p_mfgr IN ('MFGR#1', 'MFGR#2'))
SELECT d.d_year, c.c_nation, SUM(lo_revenue - lo_supplycost) AS profit
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
JOIN c ON lo_custkey = c_custkey
JOIN s ON lo_suppkey = s_suppkey
JOIN p ON lo_partkey = p_partkey
GROUP BY d.d_year, c.c_nation
ORDER BY d.d_year, c.c_nation;

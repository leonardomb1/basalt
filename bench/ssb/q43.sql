-- SSB Q4.3 profit by year, city and brand
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q43.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey, d_year FROM IDENTIFIER($data || '/ssb/date.parquet') WHERE d_year IN (1997, 1998)),
     c AS (SELECT c_custkey FROM IDENTIFIER($data || '/ssb/customer.parquet') WHERE c_region = 'AMERICA'),
     s AS (SELECT s_suppkey, s_city FROM IDENTIFIER($data || '/ssb/supplier.parquet') WHERE s_nation = 'UNITED STATES'),
     p AS (SELECT p_partkey, p_brand1 FROM IDENTIFIER($data || '/ssb/part.parquet') WHERE p_category = 'MFGR#14')
SELECT d.d_year, s.s_city, p.p_brand1, SUM(lo_revenue - lo_supplycost) AS profit
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
JOIN c ON lo_custkey = c_custkey
JOIN s ON lo_suppkey = s_suppkey
JOIN p ON lo_partkey = p_partkey
GROUP BY d.d_year, s.s_city, p.p_brand1
ORDER BY d.d_year, s.s_city, p.p_brand1;

-- SSB Q2.2 brand range
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q22.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey, d_year FROM IDENTIFIER($data || '/ssb/date.parquet')),
     s AS (SELECT s_suppkey FROM IDENTIFIER($data || '/ssb/supplier.parquet') WHERE s_region = 'ASIA'),
     p AS (SELECT p_partkey, p_brand1 FROM IDENTIFIER($data || '/ssb/part.parquet') WHERE p_brand1 BETWEEN 'MFGR#221' AND 'MFGR#228')
SELECT SUM(lo_revenue) AS revenue, d.d_year, p.p_brand1
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
JOIN s ON lo_suppkey = s_suppkey
JOIN p ON lo_partkey = p_partkey
GROUP BY d.d_year, p.p_brand1
ORDER BY d.d_year, p.p_brand1;

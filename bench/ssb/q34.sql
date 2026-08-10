-- SSB Q3.4 one month
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q34.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey, d_year FROM IDENTIFIER($data || '/ssb/date.parquet') WHERE d_yearmonth = 'Dec1997'),
     c AS (SELECT c_custkey, c_city FROM IDENTIFIER($data || '/ssb/customer.parquet') WHERE c_city IN ('UNITED KI1', 'UNITED KI5')),
     s AS (SELECT s_suppkey, s_city FROM IDENTIFIER($data || '/ssb/supplier.parquet') WHERE s_city IN ('UNITED KI1', 'UNITED KI5'))
SELECT c.c_city, s.s_city, d.d_year, SUM(lo_revenue) AS revenue
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
JOIN c ON lo_custkey = c_custkey
JOIN s ON lo_suppkey = s_suppkey
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year, revenue DESC;

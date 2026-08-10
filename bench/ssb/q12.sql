-- SSB Q1.2 tighter date and discount window
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q12.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey FROM IDENTIFIER($data || '/ssb/date.parquet') WHERE d_yearmonthnum = 199401)
SELECT SUM(lo_extendedprice * lo_discount) AS revenue
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
WHERE lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35;

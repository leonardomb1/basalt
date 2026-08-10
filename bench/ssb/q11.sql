-- SSB Q1.1 fact x date, selective discount/quantity filter
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q11.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey FROM IDENTIFIER($data || '/ssb/date.parquet') WHERE d_year = 1993)
SELECT SUM(lo_extendedprice * lo_discount) AS revenue
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
WHERE lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25;

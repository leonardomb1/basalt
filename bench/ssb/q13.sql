-- SSB Q1.3 single week
-- Dimensions are lifted into CTEs: basalt requires the right side of a join
-- to be a CTE. The reference in bench/reference/ssb_q13.sql mirrors them inline.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH d AS (SELECT d_datekey FROM IDENTIFIER($data || '/ssb/date.parquet') WHERE d_weeknuminyear = 6 AND d_year = 1994)
SELECT SUM(lo_extendedprice * lo_discount) AS revenue
FROM IDENTIFIER($data || '/ssb/lineorder.parquet')
JOIN d ON lo_orderdate = d_datekey
WHERE lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 26 AND 35;

-- TPC-H Q6: forecasting revenue change. Pure scan + selective conjunctive
-- filter, single scalar aggregate. The most SIMD-friendly shape in the set and
-- the one that exposes row-group pruning and temporal comparison.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM IDENTIFIER($data || '/lineitem.parquet')
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate <  '1995-01-01'
  AND l_discount >= 0.05
  AND l_discount <= 0.07
  AND l_quantity <  24;

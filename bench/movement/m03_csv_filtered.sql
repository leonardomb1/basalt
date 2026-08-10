-- Movement M3: csv -> csv with a selective filter. Isolates how much work the
-- engine wastes materialising rows it then discards: same input as M2, ~2% of the
-- rows out. A selection-vector/late-materialisation change should move M3 while
-- leaving M2 alone.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
SELECT l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_shipdate
FROM IDENTIFIER($data || '/lineitem.csv')
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate <  '1994-04-01'
  AND l_discount >= 0.05;

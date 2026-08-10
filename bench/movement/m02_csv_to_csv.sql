-- Movement M2: csv -> csv, whole table, no compute. Same projection as M1, so the
-- difference between the two is purely the read path: parquet decode vs CSV parse.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
SELECT l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_shipdate
FROM IDENTIFIER($data || '/lineitem.csv');

-- Movement M1: parquet -> csv, whole table, no compute. This is the shape basalt
-- exists for and that no TPC-H query measures: decode every row, serialise every
-- row, write it. Six columns rather than sixteen so the sink is not the only thing
-- being timed.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
SELECT l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_shipdate
FROM IDENTIFIER($data || '/lineitem.parquet');

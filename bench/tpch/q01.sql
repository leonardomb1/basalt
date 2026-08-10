-- TPC-H Q1: pricing summary report. Single-table scan, filter, GROUP BY on two
-- low-cardinality string keys, eight aggregates.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
SELECT l_returnflag,
       l_linestatus,
       SUM(l_quantity)                                       AS sum_qty,
       SUM(l_extendedprice)                                  AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity)                                       AS avg_qty,
       AVG(l_extendedprice)                                  AS avg_price,
       AVG(l_discount)                                       AS avg_disc,
       COUNT(*)                                              AS count_order
FROM IDENTIFIER($data || '/lineitem.parquet')
WHERE l_shipdate <= '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

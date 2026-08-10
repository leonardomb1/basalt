-- TPC-H Q3: shipping priority. Two inner joins, filters on both dimensions,
-- GROUP BY on a high-cardinality integer key, sort + limit tail.
-- Basalt requires the right side of a join to be a CTE, so the dimensions are
-- lifted into CTEs and lineitem stays the probe side.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH cust AS (
  SELECT c_custkey
  FROM IDENTIFIER($data || '/customer.parquet')
  WHERE c_mktsegment = 'BUILDING'
),
ord AS (
  SELECT o_orderkey, o_custkey, o_orderdate, o_shippriority
  FROM IDENTIFIER($data || '/orders.parquet')
  WHERE o_orderdate < '1995-03-15'
)
SELECT l.l_orderkey,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       o.o_orderdate,
       o.o_shippriority
FROM IDENTIFIER($data || '/lineitem.parquet') l
JOIN ord  o ON l.l_orderkey = o.o_orderkey
JOIN cust c ON o.o_custkey  = c.c_custkey
WHERE l.l_shipdate > '1995-03-15'
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
ORDER BY revenue DESC, o.o_orderdate
LIMIT 10;

-- TPC-H Q3: three-way join, group, top 10.
SELECT l.l_orderkey, sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue, o.o_orderdate, o.o_shippriority
FROM {customer} c
JOIN {orders} o ON o.o_custkey = c.c_custkey
JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey
WHERE c.c_mktsegment = 'BUILDING'
  AND o.o_orderdate < CAST('1995-03-15' AS DATE)
  AND l.l_shipdate > CAST('1995-03-15' AS DATE)
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
ORDER BY revenue DESC, o.o_orderdate
LIMIT 10;

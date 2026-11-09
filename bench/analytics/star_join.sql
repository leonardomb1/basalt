-- lineitem first, three dimensions each keyed on a lineitem column, all filtered:
-- the part and supplier joins' filters reach the lineitem scan.
SELECT count(*) AS n, sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM {lineitem} l
JOIN {orders} o ON o.o_orderkey = l.l_orderkey
JOIN {part} p ON p.p_partkey = l.l_partkey
JOIN {supplier} s ON s.s_suppkey = l.l_suppkey
WHERE o.o_orderdate >= CAST('1995-01-01' AS DATE)
  AND o.o_orderdate < CAST('1996-01-01' AS DATE)
  AND p.p_brand = 'Brand#23'
  AND s.s_nationkey = 7;

-- TPC-H Q18: IN over a grouped HAVING subquery, a three-way join, top 100.
SELECT c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity) AS qty
FROM {customer} c
JOIN {orders} o ON o.o_custkey = c.c_custkey
JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey
WHERE o.o_orderkey IN (SELECT l_orderkey FROM {lineitem} GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
GROUP BY c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice
ORDER BY o.o_totalprice DESC, o.o_orderdate
LIMIT 100;

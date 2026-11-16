-- TPC-H Q5: six tables in a chain of joins, two of them keyed on two columns.
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM {customer} c
JOIN {orders} o ON c.c_custkey = o.o_custkey
JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey
JOIN {supplier} s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey
JOIN {nation} n ON s.s_nationkey = n.n_nationkey
JOIN {region} r ON n.n_regionkey = r.r_regionkey
WHERE r.r_name = 'ASIA' AND o.o_orderdate >= CAST('1994-01-01' AS DATE) AND o.o_orderdate < CAST('1995-01-01' AS DATE)
GROUP BY n_name
ORDER BY revenue DESC;

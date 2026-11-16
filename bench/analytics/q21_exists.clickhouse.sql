-- No correlated EXISTS here: Q21 decorrelated. Another supplier on the order exists
-- when the order has more than one; none of them late when the late suppliers are
-- the line's own alone.
SELECT s_name, count(*) AS numwait
FROM {supplier} s
JOIN {lineitem} l1 ON s.s_suppkey = l1.l_suppkey
JOIN {orders} o ON o.o_orderkey = l1.l_orderkey
JOIN {nation} n ON s.s_nationkey = n.n_nationkey
JOIN (SELECT l_orderkey, count(DISTINCT l_suppkey) AS nsupp,
             count(DISTINCT CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS nlate
      FROM {lineitem} GROUP BY l_orderkey) a ON a.l_orderkey = l1.l_orderkey
WHERE o.o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND n.n_name = 'SAUDI ARABIA'
  AND a.nsupp > 1 AND a.nlate = 1
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100;

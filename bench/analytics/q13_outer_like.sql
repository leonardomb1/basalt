-- TPC-H Q13: left join with a NOT LIKE condition, then a group over a group.
SELECT c_count, count(*) AS custdist
FROM (SELECT c.c_custkey AS k, count(o.o_orderkey) AS c_count
      FROM {customer} c
      LEFT JOIN {orders} o ON c.c_custkey = o.o_custkey AND o.o_comment NOT LIKE '%special%requests%'
      GROUP BY c.c_custkey) t
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;

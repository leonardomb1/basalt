-- TPC-H Q13; ClickHouse fills an outer join's missing side with defaults, not NULL, unless join_use_nulls is set.
SELECT c_count, count(*) AS custdist
FROM (SELECT c.c_custkey AS k, count(o.o_orderkey) AS c_count
      FROM {customer} c
      LEFT JOIN {orders} o ON c.c_custkey = o.o_custkey AND o.o_comment NOT LIKE '%special%requests%'
      GROUP BY c.c_custkey) t
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC

SETTINGS join_use_nulls = 1;

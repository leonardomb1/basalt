-- TPC-H Q6: a selective filter over one scan, one sum.
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM {lineitem}
WHERE l_shipdate >= CAST('1994-01-01' AS DATE) AND l_shipdate < CAST('1995-01-01' AS DATE)
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

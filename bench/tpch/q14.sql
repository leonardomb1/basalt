-- TPC-H Q14: promotion effect. Join to a dimension on a high-cardinality key,
-- a LIKE prefix predicate, and arithmetic over two aggregates.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH prt AS (
  SELECT p_partkey, p_type
  FROM IDENTIFIER($data || '/part.parquet')
)
SELECT SUM(CASE WHEN p.p_type LIKE 'PROMO%'
                THEN l.l_extendedprice * (1 - l.l_discount)
                ELSE 0 END)                             AS promo_revenue,
       SUM(l.l_extendedprice * (1 - l.l_discount))      AS total_revenue
FROM IDENTIFIER($data || '/lineitem.parquet') l
JOIN prt p ON l.l_partkey = p.p_partkey
WHERE l.l_shipdate >= '1995-09-01'
  AND l.l_shipdate <  '1995-10-01';

-- A high-cardinality distinct count.
SELECT count(DISTINCT l_partkey) AS parts, count(DISTINCT l_orderkey) AS orders FROM {lineitem};

-- A top-N sort over the whole table.
SELECT l_orderkey, l_linenumber, l_extendedprice FROM {lineitem} ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 10;

-- DuckDB reference for bench/ssb/q13.sql.
COPY (
SELECT sum(lo_extendedprice * lo_discount) AS revenue
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_weeknuminyear = 6 AND d_year = 1994) d ON lo_orderdate = d_datekey
WHERE lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 26 AND 35
) TO '@OUT@' (FORMAT csv, HEADER);

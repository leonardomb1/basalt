-- DuckDB reference for bench/tpcds/q03.sql — the spec form, verbatim joins.
COPY (
SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, sum(ss_ext_sales_price) sum_agg
FROM read_parquet('@DATA@/tpcds/date_dim.parquet') dt,
     read_parquet('@DATA@/tpcds/store_sales.parquet'),
     read_parquet('@DATA@/tpcds/item.parquet') item
WHERE dt.d_date_sk = ss_sold_date_sk AND ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128 AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100
) TO '@OUT@' (FORMAT csv, HEADER);

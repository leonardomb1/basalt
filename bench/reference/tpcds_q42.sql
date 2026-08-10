-- DuckDB reference for bench/tpcds/q42.sql — the spec form, verbatim joins.
COPY (
SELECT dt.d_year, item.i_category_id, item.i_category, sum(ss_ext_sales_price) AS revenue
FROM read_parquet('@DATA@/tpcds/date_dim.parquet') dt,
     read_parquet('@DATA@/tpcds/store_sales.parquet'),
     read_parquet('@DATA@/tpcds/item.parquet') item
WHERE dt.d_date_sk = ss_sold_date_sk AND ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1 AND dt.d_moy = 11 AND dt.d_year = 2000
GROUP BY dt.d_year, item.i_category_id, item.i_category
ORDER BY revenue DESC, dt.d_year, item.i_category_id, item.i_category
LIMIT 100
) TO '@OUT@' (FORMAT csv, HEADER);

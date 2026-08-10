-- TPC-DS q96 — late-evening store traffic, three dimensions
--
-- Ported from the spec's comma-join form to basalt's explicit joins with the
-- dimensions as CTEs. Predicates and grouping are unchanged.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH td AS (SELECT t_time_sk FROM IDENTIFIER($data || '/tpcds/time_dim.parquet') WHERE t_hour = 20 AND t_minute >= 30),
     hd AS (SELECT hd_demo_sk FROM IDENTIFIER($data || '/tpcds/household_demographics.parquet') WHERE hd_dep_count = 7),
     st AS (SELECT s_store_sk FROM IDENTIFIER($data || '/tpcds/store.parquet') WHERE s_store_name = 'ese')
SELECT COUNT(*) AS n
FROM IDENTIFIER($data || '/tpcds/store_sales.parquet')
JOIN td ON ss_sold_time_sk = t_time_sk
JOIN hd ON ss_hdemo_sk = hd_demo_sk
JOIN st ON ss_store_sk = s_store_sk
ORDER BY n
LIMIT 100;

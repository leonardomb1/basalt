# 2026-11-17: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.14.8, duckdb v2.0.0, clickhouse 26.11.1.2310, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 27 ms | 48 |
| clickhouse | 49 ms | 214 |
| polars | 128 ms | 136 |
| datafusion | 93 ms | 118 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 23 ms | 65 ms | 69 ms | 62 ms | 67 ms |
| median | 89 ms | 60 ms | 72 ms | 155 ms | 94 ms |
| pivot | 21 ms | 12 ms | 19 ms | 19 ms | 13 ms |
| q01_agg | 59 ms | 39 ms | 71 ms | 60 ms | 68 ms |
| q03_join_topn | 56 ms | 42 ms | 78 ms | 26 ms | 45 ms |
| q05_join_chain | 59 ms | 43 ms | 77 ms | 24 ms | 67 ms |
| q06_filter | 23 ms | 24 ms | 23 ms | 18 ms | 28 ms |
| q12_join_case | 48 ms | 31 ms | 49 ms | 17 ms | 39 ms |
| q13_outer_like | 85 ms | 57 ms | 59 ms | 38 ms | 46 ms |
| q16_distinct_notin | 21 ms | 20 ms | 24 ms | 14 ms | 18 ms |
| q18_in_having | 106 ms | 58 ms | 74 ms | 60 ms | 145 ms |
| q21_exists | 125 ms | 81 ms | 118 ms | 114 ms | 132 ms |
| rollup | 36 ms | 28 ms | 56 ms | 84 ms | 55 ms |
| star_join | 51 ms | 47 ms | 48 ms | 28 ms | 71 ms |
| string_group | 25 ms | 21 ms | 53 ms | 39 ms | 27 ms |
| string_like | 50 ms | 79 ms | 55 ms | 61 ms | 77 ms |
| topn_sort | 21 ms | 23 ms | 19 ms | 28 ms | 19 ms |
| window_running | 66 ms | 64 ms | 58 ms | 73 ms | 111 ms |
| window_topk | 45 ms | 47 ms | 44 ms | 38 ms | 55 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 95 / 58 | 418 / 216 | 486 / 269 | 376 / 149 | 330 / 144 |
| median | 201 / 142 | 310 / 170 | 483 / 265 | 1095 / 526 | 817 / 311 |
| pivot | 138 / 78 | 140 / 57 | 307 / 139 | 276 / 86 | 219 / 71 |
| q01_agg | 241 / 176 | 237 / 117 | 458 / 246 | 489 / 175 | 302 / 138 |
| q03_join_topn | 183 / 105 | 318 / 133 | 534 / 287 | 237 / 89 | 404 / 137 |
| q05_join_chain | 186 / 118 | 329 / 137 | 448 / 246 | 252 / 90 | 466 / 176 |
| q06_filter | 129 / 89 | 209 / 81 | 321 / 141 | 239 / 79 | 274 / 95 |
| q12_join_case | 144 / 91 | 244 / 105 | 337 / 185 | 218 / 74 | 325 / 118 |
| q13_outer_like | 277 / 156 | 372 / 195 | 465 / 262 | 377 / 133 | 381 / 141 |
| q16_distinct_notin | 88 / 48 | 177 / 69 | 288 / 143 | 224 / 73 | 290 / 80 |
| q18_in_having | 281 / 133 | 284 / 156 | 471 / 265 | 470 / 173 | 750 / 356 |
| q21_exists | 225 / 144 | 321 / 182 | 627 / 369 | 681 / 285 | 863 / 370 |
| rollup | 150 / 111 | 208 / 80 | 396 / 218 | 607 / 248 | 265 / 121 |
| star_join | 191 / 111 | 389 / 161 | 422 / 206 | 234 / 88 | 490 / 200 |
| string_group | 175 / 109 | 138 / 65 | 416 / 229 | 527 / 165 | 201 / 83 |
| string_like | 140 / 113 | 354 / 201 | 507 / 241 | 277 / 128 | 282 / 142 |
| topn_sort | 115 / 80 | 221 / 86 | 289 / 144 | 348 / 103 | 247 / 78 |
| window_running | 327 / 180 | 274 / 171 | 389 / 217 | 397 / 167 | 389 / 225 |
| window_topk | 130 / 88 | 313 / 161 | 305 / 183 | 316 / 115 | 357 / 152 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 641 ms | 837 ms | 470 ms | 618 ms | 736 ms |
| parquet_to_parquet | 165 ms | 513 ms | 696 ms | 503 ms | 1080 ms |
| csv_to_parquet | 1117 ms | 947 ms | 1035 ms | 824 ms | 1178 ms |
| csvgz_to_parquet | 1671 ms | 2033 ms | 1275 ms | 1292 ms | 3241 ms |
| ndjson_to_parquet | 935 ms | 1161 ms | 1808 ms | 1369 ms | 1687 ms |
| orders_transform | 66 ms | 39 ms | 66 ms | 36 ms | 77 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 784 / 536 | 1870 / 1487 | 1281 / 986 | 1098 / 815 | 1064 / 724 |
| parquet_to_parquet | 15 / 14 | 1437 / 1173 | 1850 / 1537 | 1045 / 746 | 799 / 609 |
| csv_to_parquet | 1424 / 1018 | 1692 / 1254 | 2087 / 1336 | 1274 / 1038 | 554 / 452 |
| csvgz_to_parquet | 347 / 309 | 1380 / 841 | 1319 / 927 | 1915 / 1251 | 406 / 339 |
| ndjson_to_parquet | 806 / 585 | 1800 / 1074 | 3126 / 1742 | 2389 / 1247 | 554 / 464 |
| orders_transform | 219 / 137 | 223 / 119 | 374 / 239 | 276 / 101 | 302 / 150 |

## Database: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 472 ms | 380 ms | 976 ms | 1347 ms | n/a |
| parquet_to_pg | 572 ms | 1283 ms | 3392 ms | 2132 ms | n/a |
| mysql_to_parquet | 814 ms | 3827 ms | 896 ms | 981 ms | n/a |
| parquet_to_mysql | 6913 ms | 17.1 s | 10.6 s | n/a | n/a |
| mssql_to_parquet | 1359 ms | n/a | n/a | 6571 ms | n/a |
| parquet_to_mssql | 973 ms | n/a | n/a | n/a | n/a |
| pg_to_mysql | 5388 ms | 17.2 s | 11.5 s | n/a | n/a |

## Database: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 509 / 175 | 648 / 326 | 763 / 463 | 815 / 256 | n/a |
| parquet_to_pg | 462 / 330 | 337 / 254 | 716 / 685 | 874 / 709 | n/a |
| mysql_to_parquet | 164 / 111 | 194 / 173 | 807 / 472 | 820 / 265 | n/a |
| parquet_to_mysql | 464 / 379 | 338 / 260 | 926 / 792 | n/a | n/a |
| mssql_to_parquet | 133 / 109 | n/a | n/a | 791 / 199 | n/a |
| parquet_to_mssql | 429 / 348 | n/a | n/a | n/a | n/a |
| pg_to_mysql | 47 / 44 | 111 / 109 | 632 / 603 | n/a | n/a |

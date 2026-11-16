# 2026-11-16: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.14.7, duckdb v2.0.0, clickhouse 26.11.1.2310, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 28 ms | 50 |
| clickhouse | 49 ms | 215 |
| polars | 126 ms | 139 |
| datafusion | 90 ms | 118 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 23 ms | 68 ms | 58 ms | 64 ms | 65 ms |
| median | 91 ms | 57 ms | 74 ms | 173 ms | 92 ms |
| pivot | 59 ms | 12 ms | 17 ms | 16 ms | 15 ms |
| q01_agg | 58 ms | 38 ms | 78 ms | 60 ms | 72 ms |
| q03_join_topn | 54 ms | 39 ms | 79 ms | 27 ms | 48 ms |
| q05_join_chain | 189 ms | 43 ms | 68 ms | 27 ms | 68 ms |
| q06_filter | 23 ms | 24 ms | 29 ms | 18 ms | 30 ms |
| q12_join_case | 46 ms | 31 ms | 57 ms | 21 ms | 44 ms |
| q13_outer_like | 83 ms | 58 ms | 59 ms | 44 ms | 48 ms |
| q16_distinct_notin | 22 ms | 19 ms | 30 ms | 16 ms | 20 ms |
| q18_in_having | 104 ms | 56 ms | 79 ms | 62 ms | 150 ms |
| q21_exists | 268 ms | 79 ms | 116 ms | 117 ms | 137 ms |
| rollup | 138 ms | 27 ms | 52 ms | 87 ms | 59 ms |
| star_join | 51 ms | 44 ms | 47 ms | 28 ms | 75 ms |
| string_group | 25 ms | 19 ms | 61 ms | 41 ms | 29 ms |
| string_like | 48 ms | 77 ms | 60 ms | 65 ms | 71 ms |
| topn_sort | 21 ms | 21 ms | 12 ms | 32 ms | 21 ms |
| window_running | 132 ms | 61 ms | 66 ms | 74 ms | 114 ms |
| window_topk | 44 ms | 44 ms | 49 ms | 40 ms | 63 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 96 / 57 | 424 / 224 | 478 / 245 | 372 / 155 | 331 / 144 |
| median | 197 / 143 | 311 / 171 | 487 / 264 | 1086 / 553 | 812 / 300 |
| pivot | 61 / 52 | 141 / 61 | 299 / 137 | 277 / 87 | 216 / 65 |
| q01_agg | 223 / 171 | 229 / 115 | 440 / 250 | 487 / 176 | 312 / 146 |
| q03_join_topn | 180 / 107 | 317 / 131 | 537 / 285 | 239 / 87 | 402 / 141 |
| q05_join_chain | 902 / 554 | 320 / 138 | 443 / 233 | 250 / 87 | 460 / 179 |
| q06_filter | 126 / 81 | 218 / 87 | 324 / 158 | 235 / 77 | 274 / 91 |
| q12_join_case | 143 / 86 | 243 / 106 | 345 / 206 | 221 / 80 | 333 / 118 |
| q13_outer_like | 273 / 157 | 368 / 192 | 464 / 257 | 382 / 132 | 378 / 140 |
| q16_distinct_notin | 87 / 49 | 177 / 66 | 283 / 151 | 216 / 72 | 286 / 81 |
| q18_in_having | 273 / 130 | 289 / 159 | 484 / 289 | 485 / 179 | 793 / 374 |
| q21_exists | 960 / 633 | 309 / 178 | 632 / 363 | 660 / 279 | 853 / 357 |
| rollup | 40 / 38 | 199 / 89 | 389 / 209 | 613 / 246 | 274 / 122 |
| star_join | 195 / 117 | 395 / 167 | 423 / 202 | 231 / 88 | 494 / 199 |
| string_group | 175 / 113 | 148 / 67 | 407 / 237 | 381 / 124 | 202 / 81 |
| string_like | 136 / 109 | 347 / 202 | 500 / 254 | 273 / 124 | 277 / 134 |
| topn_sort | 117 / 83 | 211 / 83 | 288 / 123 | 353 / 109 | 241 / 79 |
| window_running | 225 / 155 | 277 / 171 | 393 / 237 | 390 / 160 | 388 / 224 |
| window_topk | 132 / 89 | 310 / 159 | 305 / 187 | 316 / 112 | 365 / 155 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 623 ms | 820 ms | 469 ms | 609 ms | 726 ms |
| parquet_to_parquet | 153 ms | 504 ms | 706 ms | 500 ms | 1076 ms |
| csv_to_parquet | 1091 ms | 1080 ms | 1014 ms | 810 ms | 1184 ms |
| csvgz_to_parquet | 1646 ms | 2037 ms | 1281 ms | 1310 ms | 3237 ms |
| ndjson_to_parquet | 949 ms | 1133 ms | 1817 ms | 1381 ms | 1683 ms |
| orders_transform | 62 ms | 39 ms | 67 ms | 36 ms | 77 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 781 / 539 | 1710 / 1406 | 1336 / 1003 | 1053 / 801 | 1089 / 760 |
| parquet_to_parquet | 15 / 14 | 1385 / 1121 | 1850 / 1468 | 999 / 713 | 822 / 621 |
| csv_to_parquet | 1414 / 1018 | 1808 / 1371 | 2104 / 1344 | 1280 / 1033 | 561 / 454 |
| csvgz_to_parquet | 321 / 279 | 1271 / 847 | 1372 / 973 | 1902 / 1244 | 405 / 344 |
| ndjson_to_parquet | 826 / 601 | 1563 / 1000 | 3169 / 1765 | 2393 / 1252 | 564 / 476 |
| orders_transform | 215 / 135 | 231 / 124 | 359 / 231 | 283 / 105 | 317 / 153 |

## Database: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 472 ms | 384 ms | 975 ms | 1338 ms | n/a |
| parquet_to_pg | 556 ms | 1238 ms | 3364 ms | 2135 ms | n/a |
| mysql_to_parquet | 809 ms | 3835 ms | 900 ms | 987 ms | n/a |
| parquet_to_mysql | 5054 ms | 16.7 s | 10.1 s | n/a | n/a |
| mssql_to_parquet | 1320 ms | n/a | n/a | 6570 ms | n/a |
| parquet_to_mssql | 976 ms | n/a | n/a | n/a | n/a |
| pg_to_mysql | 7131 ms | 17.2 s | 11.5 s | n/a | n/a |

## Database: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 510 / 177 | 733 / 331 | 758 / 463 | 824 / 257 | n/a |
| parquet_to_pg | 432 / 323 | 333 / 254 | 727 / 638 | 873 / 708 | n/a |
| mysql_to_parquet | 164 / 110 | 198 / 176 | 753 / 460 | 822 / 271 | n/a |
| parquet_to_mysql | 462 / 355 | 334 / 259 | 923 / 847 | n/a | n/a |
| mssql_to_parquet | 131 / 110 | n/a | n/a | 764 / 200 | n/a |
| parquet_to_mssql | 443 / 347 | n/a | n/a | n/a | n/a |
| pg_to_mysql | 49 / 47 | 124 / 122 | 647 / 614 | n/a | n/a |

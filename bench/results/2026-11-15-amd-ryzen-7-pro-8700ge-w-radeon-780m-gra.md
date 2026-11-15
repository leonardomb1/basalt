# 2026-11-15: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.14.2, duckdb v2.0.0, clickhouse 26.11.1.2310, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 28 ms | 50 |
| clickhouse | 49 ms | 213 |
| polars | 126 ms | 138 |
| datafusion | 91 ms | 116 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 22 ms | 66 ms | 66 ms | 62 ms | 68 ms |
| median | 92 ms | 60 ms | 71 ms | 156 ms | 95 ms |
| q01_agg | 59 ms | 39 ms | 72 ms | 58 ms | 76 ms |
| q03_join_topn | 54 ms | 42 ms | 72 ms | 28 ms | 49 ms |
| q06_filter | 23 ms | 24 ms | 22 ms | 16 ms | 30 ms |
| q12_join_case | 47 ms | 32 ms | 49 ms | 18 ms | 44 ms |
| q13_outer_like | 84 ms | 57 ms | 61 ms | 40 ms | 46 ms |
| q16_distinct_notin | 22 ms | 19 ms | 23 ms | 16 ms | 19 ms |
| q18_in_having | 103 ms | 55 ms | 72 ms | 61 ms | 148 ms |
| star_join | 50 ms | 47 ms | 47 ms | 29 ms | 73 ms |
| string_group | 26 ms | 21 ms | 53 ms | 37 ms | 27 ms |
| string_like | 50 ms | 76 ms | 52 ms | 59 ms | 72 ms |
| topn_sort | 21 ms | 21 ms | 12 ms | 20 ms | 20 ms |
| window_topk | 43 ms | 43 ms | 42 ms | 39 ms | 54 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 96 / 58 | 410 / 214 | 469 / 244 | 426 / 158 | 325 / 145 |
| median | 196 / 141 | 315 / 172 | 479 / 264 | 1093 / 519 | 802 / 306 |
| q01_agg | 237 / 172 | 231 / 115 | 456 / 243 | 493 / 179 | 309 / 152 |
| q03_join_topn | 182 / 108 | 316 / 131 | 536 / 270 | 232 / 86 | 399 / 140 |
| q06_filter | 125 / 88 | 216 / 89 | 322 / 148 | 242 / 79 | 269 / 96 |
| q12_join_case | 141 / 83 | 251 / 110 | 343 / 189 | 225 / 76 | 328 / 119 |
| q13_outer_like | 283 / 162 | 372 / 195 | 465 / 261 | 390 / 128 | 374 / 137 |
| q16_distinct_notin | 86 / 47 | 188 / 73 | 281 / 137 | 222 / 73 | 282 / 81 |
| q18_in_having | 275 / 129 | 277 / 151 | 472 / 270 | 482 / 180 | 783 / 377 |
| star_join | 192 / 115 | 390 / 163 | 419 / 206 | 238 / 89 | 505 / 202 |
| string_group | 175 / 111 | 143 / 69 | 415 / 224 | 446 / 139 | 200 / 80 |
| string_like | 136 / 109 | 348 / 202 | 500 / 247 | 277 / 129 | 283 / 139 |
| topn_sort | 113 / 80 | 213 / 83 | 282 / 121 | 291 / 87 | 236 / 76 |
| window_topk | 130 / 89 | 310 / 159 | 301 / 181 | 314 / 116 | 355 / 148 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 440 ms | 814 ms | 471 ms | 624 ms | 722 ms |
| parquet_to_parquet | 671 ms | 493 ms | 679 ms | 508 ms | 1072 ms |
| csv_to_parquet | 1009 ms | 919 ms | 1020 ms | 834 ms | 1165 ms |
| csvgz_to_parquet | 1596 ms | 2014 ms | 1249 ms | 1267 ms | 3244 ms |
| ndjson_to_parquet | 867 ms | 1156 ms | 1804 ms | 1352 ms | 1674 ms |
| orders_transform | 46 ms | 39 ms | 66 ms | 42 ms | 78 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 786 / 599 | 1796 / 1414 | 1250 / 966 | 1145 / 845 | 1082 / 765 |
| parquet_to_parquet | 1236 / 958 | 1405 / 1107 | 1823 / 1450 | 1084 / 760 | 841 / 629 |
| csv_to_parquet | 1404 / 1027 | 1907 / 1348 | 2073 / 1320 | 1299 / 1058 | 544 / 446 |
| csvgz_to_parquet | 349 / 321 | 1433 / 910 | 1260 / 913 | 1942 / 1258 | 411 / 348 |
| ndjson_to_parquet | 823 / 609 | 1639 / 1024 | 3116 / 1750 | 2398 / 1232 | 564 / 465 |
| orders_transform | 223 / 149 | 231 / 127 | 364 / 236 | 266 / 95 | 323 / 164 |

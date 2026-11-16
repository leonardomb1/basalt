# 2026-11-16: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.14.6, duckdb v2.0.0, clickhouse 26.11.1.2310, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 27 ms | 50 |
| clickhouse | 48 ms | 215 |
| polars | 125 ms | 136 |
| datafusion | 90 ms | 118 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 23 ms | 68 ms | 68 ms | 62 ms | 69 ms |
| median | 95 ms | 59 ms | 71 ms | 162 ms | 96 ms |
| pivot | 58 ms | 13 ms | 18 ms | 21 ms | 18 ms |
| q01_agg | 57 ms | 42 ms | 74 ms | 64 ms | 77 ms |
| q03_join_topn | 56 ms | 43 ms | 70 ms | 30 ms | 50 ms |
| q05_join_chain | 187 ms | 45 ms | 70 ms | 27 ms | 67 ms |
| q06_filter | 23 ms | 25 ms | 23 ms | 18 ms | 29 ms |
| q12_join_case | 47 ms | 31 ms | 56 ms | 21 ms | 48 ms |
| q13_outer_like | 84 ms | 60 ms | 58 ms | 41 ms | 46 ms |
| q16_distinct_notin | 22 ms | 18 ms | 25 ms | 18 ms | 21 ms |
| q18_in_having | 102 ms | 56 ms | 83 ms | 60 ms | 149 ms |
| q21_exists | 266 ms | 81 ms | 119 ms | 115 ms | 143 ms |
| rollup | 137 ms | 28 ms | 47 ms | 86 ms | 62 ms |
| star_join | 52 ms | 46 ms | 56 ms | 30 ms | 74 ms |
| string_group | 25 ms | 21 ms | 52 ms | 40 ms | 28 ms |
| string_like | 49 ms | 77 ms | 56 ms | 63 ms | 75 ms |
| topn_sort | 21 ms | 23 ms | 13 ms | 35 ms | 21 ms |
| window_running | 131 ms | 63 ms | 63 ms | 76 ms | 116 ms |
| window_topk | 45 ms | 46 ms | 51 ms | 39 ms | 56 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 95 / 58 | 425 / 223 | 482 / 267 | 423 / 161 | 337 / 148 |
| median | 199 / 144 | 310 / 172 | 485 / 264 | 1100 / 531 | 867 / 316 |
| pivot | 61 / 52 | 137 / 56 | 300 / 134 | 279 / 90 | 218 / 68 |
| q01_agg | 223 / 168 | 242 / 121 | 458 / 254 | 491 / 180 | 302 / 148 |
| q03_join_topn | 185 / 110 | 314 / 129 | 535 / 266 | 233 / 85 | 402 / 142 |
| q05_join_chain | 897 / 548 | 314 / 135 | 448 / 239 | 252 / 89 | 456 / 176 |
| q06_filter | 130 / 92 | 219 / 85 | 323 / 144 | 244 / 81 | 277 / 94 |
| q12_join_case | 140 / 81 | 239 / 99 | 337 / 196 | 225 / 80 | 317 / 118 |
| q13_outer_like | 290 / 174 | 368 / 192 | 468 / 263 | 378 / 126 | 389 / 139 |
| q16_distinct_notin | 95 / 48 | 176 / 67 | 283 / 141 | 223 / 74 | 295 / 86 |
| q18_in_having | 277 / 132 | 286 / 157 | 480 / 289 | 441 / 163 | 756 / 355 |
| q21_exists | 951 / 634 | 333 / 191 | 634 / 373 | 653 / 278 | 895 / 388 |
| rollup | 40 / 38 | 194 / 90 | 394 / 196 | 582 / 212 | 281 / 126 |
| star_join | 195 / 113 | 373 / 158 | 426 / 213 | 238 / 92 | 509 / 199 |
| string_group | 179 / 118 | 145 / 68 | 425 / 235 | 516 / 161 | 206 / 80 |
| string_like | 138 / 110 | 358 / 203 | 514 / 242 | 278 / 127 | 278 / 135 |
| topn_sort | 113 / 76 | 216 / 80 | 286 / 127 | 358 / 110 | 241 / 79 |
| window_running | 224 / 154 | 276 / 169 | 396 / 227 | 399 / 167 | 389 / 228 |
| window_topk | 130 / 89 | 307 / 159 | 309 / 191 | 318 / 112 | 353 / 147 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 604 ms | 791 ms | 461 ms | 618 ms | 727 ms |
| parquet_to_parquet | 155 ms | 495 ms | 714 ms | 512 ms | 1078 ms |
| csv_to_parquet | 1080 ms | 962 ms | 1012 ms | 821 ms | 1162 ms |
| csvgz_to_parquet | 1672 ms | 2018 ms | 1260 ms | 1293 ms | 3229 ms |
| ndjson_to_parquet | 927 ms | 1161 ms | 1808 ms | 1386 ms | 1671 ms |
| orders_transform | 59 ms | 39 ms | 68 ms | 35 ms | 80 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 761 / 528 | 1672 / 1396 | 1414 / 1073 | 1133 / 835 | 1082 / 750 |
| parquet_to_parquet | 15 / 14 | 1419 / 1158 | 1829 / 1444 | 1082 / 752 | 796 / 624 |
| csv_to_parquet | 1392 / 1013 | 2149 / 1415 | 2112 / 1367 | 1285 / 1049 | 544 / 446 |
| csvgz_to_parquet | 349 / 298 | 1219 / 804 | 1290 / 919 | 1921 / 1254 | 418 / 355 |
| ndjson_to_parquet | 799 / 580 | 1579 / 1044 | 3159 / 1770 | 2388 / 1236 | 570 / 477 |
| orders_transform | 215 / 138 | 225 / 122 | 360 / 231 | 273 / 102 | 328 / 163 |

## Database: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 852 ms | 394 ms | 971 ms | 1340 ms | n/a |
| parquet_to_pg | 538 ms | 1288 ms | 3388 ms | 2133 ms | n/a |
| mysql_to_parquet | 783 ms | 3823 ms | 903 ms | 1001 ms | n/a |
| parquet_to_mysql | 5818 ms | 16.2 s | 10.2 s | n/a | n/a |
| mssql_to_parquet | 1348 ms | n/a | n/a | 6535 ms | n/a |
| parquet_to_mssql | 1048 ms | n/a | n/a | n/a | n/a |
| pg_to_mysql | 6255 ms | 14.8 s | 10.4 s | n/a | n/a |

## Database: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 150 / 106 | 724 / 341 | 792 / 472 | 823 / 253 | n/a |
| parquet_to_pg | 452 / 330 | 333 / 256 | 741 / 654 | 869 / 705 | n/a |
| mysql_to_parquet | 164 / 113 | 185 / 163 | 802 / 468 | 831 / 266 | n/a |
| parquet_to_mysql | 466 / 349 | 338 / 263 | 924 / 847 | n/a | n/a |
| mssql_to_parquet | 133 / 110 | n/a | n/a | 848 / 199 | n/a |
| parquet_to_mssql | 430 / 351 | n/a | n/a | n/a | n/a |
| pg_to_mysql | 17 / 17 | 118 / 117 | 690 / 654 | n/a | n/a |

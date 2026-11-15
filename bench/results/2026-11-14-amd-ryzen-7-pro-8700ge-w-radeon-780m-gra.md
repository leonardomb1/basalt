# 2026-11-14: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.14.0, duckdb v2.0.0, clickhouse 26.11.1.2310, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 26 ms | 51 |
| clickhouse | 42 ms | 219 |
| polars | 125 ms | 137 |
| datafusion | 87 ms | 120 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 23 ms | 66 ms | 60 ms | 57 ms | 67 ms |
| median | 93 ms | 55 ms | 71 ms | 157 ms | 93 ms |
| q01_agg | 61 ms | 39 ms | 74 ms | 61 ms | 77 ms |
| q03_join_topn | 53 ms | 40 ms | 76 ms | 26 ms | 49 ms |
| q06_filter | 24 ms | 25 ms | 23 ms | 18 ms | 31 ms |
| q12_join_case | 46 ms | 31 ms | 50 ms | 17 ms | 44 ms |
| q13_outer_like | 83 ms | 57 ms | 60 ms | 40 ms | 46 ms |
| q16_distinct_notin | 22 ms | 20 ms | 24 ms | 15 ms | 21 ms |
| q18_in_having | 103 ms | 57 ms | 79 ms | 60 ms | 150 ms |
| star_join | 50 ms | 47 ms | 48 ms | 29 ms | 75 ms |
| string_group | 26 ms | 20 ms | 53 ms | 37 ms | 29 ms |
| string_like | 47 ms | 80 ms | 65 ms | 61 ms | 72 ms |
| topn_sort | 22 ms | 22 ms | 23 ms | 36 ms | 20 ms |
| window_topk | 43 ms | 44 ms | 43 ms | 36 ms | 59 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 97 / 60 | 418 / 225 | 489 / 265 | 369 / 151 | 339 / 147 |
| median | 198 / 142 | 318 / 175 | 488 / 271 | 1095 / 546 | 803 / 317 |
| q01_agg | 237 / 181 | 241 / 125 | 459 / 257 | 523 / 196 | 320 / 157 |
| q03_join_topn | 181 / 110 | 315 / 137 | 535 / 299 | 232 / 85 | 403 / 144 |
| q06_filter | 125 / 87 | 213 / 85 | 322 / 152 | 245 / 80 | 274 / 99 |
| q12_join_case | 142 / 85 | 244 / 106 | 343 / 201 | 221 / 76 | 334 / 120 |
| q13_outer_like | 275 / 161 | 368 / 197 | 471 / 268 | 397 / 139 | 378 / 143 |
| q16_distinct_notin | 86 / 47 | 176 / 72 | 287 / 146 | 225 / 74 | 291 / 89 |
| q18_in_having | 278 / 132 | 281 / 154 | 476 / 295 | 435 / 173 | 763 / 371 |
| star_join | 195 / 114 | 393 / 166 | 429 / 219 | 242 / 90 | 502 / 204 |
| string_group | 175 / 116 | 145 / 67 | 428 / 241 | 514 / 163 | 206 / 83 |
| string_like | 138 / 110 | 354 / 206 | 514 / 280 | 274 / 126 | 282 / 141 |
| topn_sort | 113 / 76 | 219 / 87 | 293 / 149 | 386 / 122 | 245 / 82 |
| window_topk | 132 / 91 | 307 / 155 | 315 / 197 | 320 / 113 | 360 / 157 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 424 ms | 778 ms | 460 ms | 618 ms | 730 ms |
| parquet_to_parquet | 679 ms | 505 ms | 713 ms | 500 ms | 1085 ms |
| csv_to_parquet | 1014 ms | 1052 ms | 1035 ms | 807 ms | 1174 ms |
| csvgz_to_parquet | 5973 ms | 2031 ms | 1255 ms | 1311 ms | 3225 ms |
| ndjson_to_parquet | 3022 ms | 1224 ms | 1808 ms | 1352 ms | 1661 ms |
| orders_transform | 45 ms | 42 ms | 71 ms | 35 ms | 76 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 771 / 590 | 1689 / 1407 | 1339 / 1036 | 1081 / 805 | 1092 / 766 |
| parquet_to_parquet | 1204 / 935 | 1382 / 1118 | 1991 / 1524 | 1067 / 755 | 789 / 614 |
| csv_to_parquet | 1521 / 1063 | 1873 / 1338 | 2037 / 1307 | 1294 / 1056 | 563 / 452 |
| csvgz_to_parquet | 87 / 86 | 1423 / 865 | 1344 / 964 | 1896 / 1241 | 426 / 355 |
| ndjson_to_parquet | 293 / 271 | 1741 / 1095 | 3077 / 1711 | 2391 / 1235 | 564 / 475 |
| orders_transform | 220 / 151 | 231 / 125 | 381 / 251 | 272 / 99 | 311 / 161 |

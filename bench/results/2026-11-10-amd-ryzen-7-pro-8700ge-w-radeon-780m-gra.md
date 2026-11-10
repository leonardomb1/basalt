# 2026-11-10: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.13.4 (7c4954d), duckdb v2.0.0, clickhouse 26.10.2.103, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 28 ms | 51 |
| clickhouse | 45 ms | 239 |
| polars | 106 ms | 137 |
| datafusion | 80 ms | 116 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 22 ms | 66 ms | 88 ms | 56 ms | 68 ms |
| median | 97 ms | 58 ms | 120 ms | 160 ms | 96 ms |
| q01_agg | 59 ms | 42 ms | 105 ms | 55 ms | 74 ms |
| q03_join_topn | 55 ms | 38 ms | 109 ms | 29 ms | 49 ms |
| q06_filter | 23 ms | 22 ms | 60 ms | 17 ms | 29 ms |
| q12_join_case | 47 ms | 31 ms | 87 ms | 20 ms | 41 ms |
| q13_outer_like | 82 ms | 56 ms | 99 ms | 41 ms | 46 ms |
| q16_distinct_notin | 21 ms | 20 ms | 50 ms | 15 ms | 21 ms |
| q18_in_having | 103 ms | 55 ms | 135 ms | 58 ms | 148 ms |
| star_join | 51 ms | 45 ms | 103 ms | 28 ms | 75 ms |
| string_group | 26 ms | 19 ms | 90 ms | 41 ms | 30 ms |
| string_like | 49 ms | 78 ms | 96 ms | 63 ms | 72 ms |
| topn_sort | 21 ms | 22 ms | 37 ms | 33 ms | 21 ms |
| window_topk | 44 ms | 40 ms | 83 ms | 37 ms | 56 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 95 / 59 | 418 / 216 | 596 / 275 | 364 / 145 | 329 / 149 |
| median | 197 / 143 | 317 / 173 | 618 / 297 | 1092 / 567 | 870 / 326 |
| q01_agg | 241 / 185 | 238 / 121 | 523 / 263 | 500 / 186 | 308 / 155 |
| q03_join_topn | 182 / 107 | 313 / 129 | 654 / 323 | 238 / 91 | 402 / 153 |
| q06_filter | 123 / 87 | 213 / 85 | 395 / 208 | 238 / 81 | 277 / 100 |
| q12_join_case | 143 / 86 | 245 / 108 | 425 / 245 | 219 / 78 | 321 / 119 |
| q13_outer_like | 271 / 161 | 368 / 198 | 663 / 329 | 375 / 135 | 395 / 151 |
| q16_distinct_notin | 87 / 48 | 176 / 69 | 338 / 184 | 208 / 73 | 280 / 85 |
| q18_in_having | 279 / 130 | 293 / 158 | 669 / 423 | 421 / 170 | 778 / 389 |
| star_join | 195 / 114 | 383 / 164 | 552 / 268 | 239 / 93 | 507 / 215 |
| string_group | 175 / 112 | 143 / 66 | 494 / 248 | 507 / 169 | 205 / 88 |
| string_like | 138 / 110 | 361 / 207 | 685 / 272 | 276 / 134 | 280 / 145 |
| topn_sort | 113 / 79 | 224 / 89 | 392 / 182 | 384 / 122 | 241 / 84 |
| window_topk | 132 / 90 | 308 / 151 | 380 / 235 | 307 / 115 | 344 / 152 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 405 ms | 636 ms | 457 ms | 472 ms | 712 ms |
| parquet_to_parquet | 688 ms | 447 ms | 802 ms | 464 ms | 1096 ms |
| csv_to_parquet | 1039 ms | 965 ms | 1147 ms | 792 ms | 1165 ms |
| csvgz_to_parquet | 5929 ms | 1986 ms | 1303 ms | 1242 ms | 3253 ms |
| ndjson_to_parquet | 3111 ms | 1097 ms | 1863 ms | 1351 ms | 1656 ms |
| orders_transform | 46 ms | 37 ms | 83 ms | 32 ms | 79 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 748 / 587 | 1683 / 1328 | 1762 / 1341 | 1187 / 846 | 1075 / 725 |
| parquet_to_parquet | 1223 / 941 | 1332 / 1056 | 2409 / 1850 | 1061 / 740 | 756 / 603 |
| csv_to_parquet | 1525 / 1070 | 2082 / 1456 | 2825 / 1747 | 1271 / 1048 | 546 / 442 |
| csvgz_to_parquet | 86 / 86 | 1335 / 880 | 2207 / 1437 | 1920 / 1252 | 415 / 345 |
| ndjson_to_parquet | 293 / 271 | 1709 / 1080 | 4302 / 2374 | 2396 / 1239 | 554 / 469 |
| orders_transform | 221 / 147 | 226 / 118 | 504 / 305 | 278 / 106 | 313 / 164 |

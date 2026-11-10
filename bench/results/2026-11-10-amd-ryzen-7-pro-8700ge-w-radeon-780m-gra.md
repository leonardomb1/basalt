# 2026-11-10: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.13.3, duckdb v2.0.0, clickhouse 26.10.2.103, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 27 ms | 50 |
| clickhouse | 45 ms | 232 |
| polars | 105 ms | 135 |
| datafusion | 81 ms | 116 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 22 ms | 68 ms | 83 ms | 60 ms | 67 ms |
| median | 95 ms | 59 ms | 102 ms | 165 ms | 92 ms |
| q01_agg | 61 ms | 41 ms | 100 ms | 70 ms | 69 ms |
| q03_join_topn | 102 ms | 41 ms | 117 ms | 30 ms | 47 ms |
| q06_filter | 23 ms | 25 ms | 68 ms | 19 ms | 28 ms |
| q12_join_case | 46 ms | 32 ms | 78 ms | 21 ms | 40 ms |
| q13_outer_like | 84 ms | 57 ms | 95 ms | 42 ms | 46 ms |
| q16_distinct_notin | 21 ms | 21 ms | 57 ms | 17 ms | 19 ms |
| q18_in_having | 105 ms | 58 ms | 132 ms | 66 ms | 147 ms |
| star_join | 51 ms | 47 ms | 103 ms | 28 ms | 73 ms |
| string_group | 25 ms | 21 ms | 86 ms | 42 ms | 29 ms |
| string_like | 49 ms | 79 ms | 99 ms | 64 ms | 75 ms |
| topn_sort | 21 ms | 23 ms | 54 ms | 29 ms | 18 ms |
| window_topk | 44 ms | 47 ms | 83 ms | 37 ms | 58 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 101 / 64 | 422 / 224 | 595 / 277 | 383 / 163 | 331 / 155 |
| median | 201 / 143 | 313 / 171 | 657 / 322 | 1078 / 546 | 828 / 316 |
| q01_agg | 219 / 167 | 238 / 116 | 505 / 256 | 793 / 275 | 304 / 151 |
| q03_join_topn | 228 / 148 | 314 / 132 | 657 / 329 | 236 / 89 | 408 / 153 |
| q06_filter | 127 / 85 | 217 / 86 | 402 / 208 | 236 / 80 | 273 / 101 |
| q12_join_case | 139 / 87 | 248 / 107 | 428 / 234 | 219 / 77 | 307 / 120 |
| q13_outer_like | 268 / 164 | 371 / 194 | 658 / 338 | 385 / 137 | 368 / 149 |
| q16_distinct_notin | 89 / 48 | 187 / 75 | 345 / 197 | 226 / 74 | 290 / 88 |
| q18_in_having | 277 / 133 | 282 / 152 | 619 / 383 | 461 / 190 | 755 / 372 |
| star_join | 194 / 114 | 384 / 157 | 560 / 269 | 244 / 95 | 499 / 205 |
| string_group | 175 / 115 | 146 / 67 | 486 / 257 | 496 / 162 | 203 / 89 |
| string_like | 137 / 106 | 367 / 208 | 627 / 279 | 272 / 130 | 292 / 144 |
| topn_sort | 113 / 71 | 218 / 84 | 383 / 197 | 324 / 107 | 242 / 82 |
| window_topk | 129 / 86 | 308 / 160 | 389 / 224 | 323 / 122 | 352 / 162 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 417 ms | 657 ms | 486 ms | 489 ms | 718 ms |
| parquet_to_parquet | 689 ms | 447 ms | 779 ms | 469 ms | 1085 ms |
| csv_to_parquet | 1055 ms | 884 ms | 1127 ms | 768 ms | 1154 ms |
| csvgz_to_parquet | 5896 ms | 2013 ms | 1296 ms | 1254 ms | 3252 ms |
| ndjson_to_parquet | 3083 ms | 1110 ms | 1897 ms | 1363 ms | 1674 ms |
| orders_transform | 45 ms | 39 ms | 84 ms | 33 ms | 73 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 716 / 562 | 1657 / 1316 | 1855 / 1433 | 1078 / 782 | 1102 / 741 |
| parquet_to_parquet | 1222 / 932 | 1424 / 1135 | 2544 / 2018 | 997 / 729 | 821 / 629 |
| csv_to_parquet | 1509 / 1078 | 1688 / 1212 | 2914 / 1835 | 1273 / 1053 | 559 / 457 |
| csvgz_to_parquet | 86 / 85 | 1381 / 812 | 2316 / 1487 | 1910 / 1252 | 392 / 335 |
| ndjson_to_parquet | 293 / 272 | 1937 / 1128 | 3856 / 2229 | 2394 / 1250 | 552 / 467 |
| orders_transform | 224 / 157 | 230 / 120 | 493 / 304 | 275 / 104 | 318 / 162 |

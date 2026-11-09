# 2026-11-09: SF1, 12 threads, best of 3

Machine: 13th Gen Intel(R) Core(TM) i5-1345U, Linux 6.6.87.2-microsoft-standard-WSL2. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.13.3, duckdb v2.0.0, clickhouse 26.10.2.103, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 2 ms | 16 |
| duckdb | 33 ms | 40 |
| clickhouse | 49 ms | 190 |
| polars | 138 ms | 61 |
| datafusion | 102 ms | 114 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 51 ms | 144 ms | 175 ms | 129 ms | 120 ms |
| median | 148 ms | 98 ms | 156 ms | 316 ms | 174 ms |
| q01_agg | 140 ms | 80 ms | 158 ms | 148 ms | 163 ms |
| q03_join_topn | 185 ms | 76 ms | 199 ms | 61 ms | 94 ms |
| q06_filter | 43 ms | 41 ms | 72 ms | 29 ms | 48 ms |
| q12_join_case | 88 ms | 54 ms | 137 ms | 36 ms | 81 ms |
| q13_outer_like | 182 ms | 105 ms | 163 ms | 78 ms | 83 ms |
| q16_distinct_notin | 39 ms | 39 ms | 76 ms | 37 ms | 44 ms |
| q18_in_having | 226 ms | 108 ms | 180 ms | 125 ms | 284 ms |
| star_join | 89 ms | 91 ms | 164 ms | 55 ms | 163 ms |
| string_group | 52 ms | 29 ms | 120 ms | 73 ms | 45 ms |
| string_like | 75 ms | 142 ms | 120 ms | 97 ms | 109 ms |
| topn_sort | 43 ms | 44 ms | 58 ms | 55 ms | 36 ms |
| window_topk | 101 ms | 83 ms | 110 ms | 80 ms | 109 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 120 / 51 | 283 / 148 | 517 / 262 | 289 / 118 | 380 / 178 |
| median | 260 / 138 | 235 / 136 | 492 / 273 | 1088 / 532 | 766 / 341 |
| q01_agg | 326 / 204 | 181 / 87 | 423 / 237 | 465 / 172 | 342 / 172 |
| q03_join_topn | 270 / 144 | 233 / 95 | 578 / 335 | 130 / 58 | 411 / 170 |
| q06_filter | 157 / 79 | 161 / 70 | 320 / 181 | 130 / 55 | 306 / 112 |
| q12_join_case | 209 / 100 | 174 / 75 | 358 / 216 | 124 / 53 | 344 / 148 |
| q13_outer_like | 380 / 188 | 255 / 139 | 485 / 273 | 277 / 100 | 365 / 153 |
| q16_distinct_notin | 84 / 51 | 105 / 47 | 278 / 172 | 118 / 53 | 279 / 96 |
| q18_in_having | 208 / 125 | 219 / 114 | 496 / 310 | 331 / 139 | 699 / 356 |
| star_join | 237 / 119 | 293 / 117 | 471 / 265 | 139 / 61 | 494 / 229 |
| string_group | 251 / 110 | 73 / 42 | 358 / 204 | 324 / 107 | 220 / 90 |
| string_like | 199 / 136 | 308 / 177 | 496 / 252 | 202 / 93 | 316 / 154 |
| topn_sort | 143 / 71 | 171 / 71 | 306 / 165 | 229 / 73 | 264 / 89 |
| window_topk | 119 / 83 | 220 / 115 | 312 / 206 | 219 / 94 | 350 / 175 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 718 ms | 1369 ms | 794 ms | 755 ms | 1612 ms |
| parquet_to_parquet | 1249 ms | 930 ms | 1575 ms | 1050 ms | 1740 ms |
| csv_to_parquet | 2083 ms | 2231 ms | 2427 ms | 1472 ms | 2259 ms |
| csvgz_to_parquet | 7832 ms | 3602 ms | 2365 ms | 2417 ms | 4336 ms |
| ndjson_to_parquet | 4985 ms | 2892 ms | 3841 ms | 2760 ms | 4039 ms |
| orders_transform | 86 ms | 74 ms | 146 ms | 72 ms | 122 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 983 / 686 | 1910 / 1499 | 1467 / 953 | 1024 / 691 | 771 / 601 |
| parquet_to_parquet | 1633 / 1184 | 1580 / 1206 | 2005 / 1513 | 1115 / 786 | 809 / 617 |
| csv_to_parquet | 1669 / 1206 | 1881 / 1306 | 2565 / 1605 | 1216 / 891 | 502 / 416 |
| csvgz_to_parquet | 78 / 76 | 1716 / 874 | 1894 / 1259 | 1633 / 1071 | 355 / 298 |
| ndjson_to_parquet | 344 / 315 | 2183 / 1253 | 3365 / 2002 | 2390 / 1246 | 524 / 447 |
| orders_transform | 297 / 135 | 156 / 86 | 407 / 256 | 178 / 68 | 329 / 178 |

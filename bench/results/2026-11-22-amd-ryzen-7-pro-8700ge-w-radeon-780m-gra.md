# 2026-11-22: SF1, 8 threads, best of 3

Machine: AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics, Linux 6.12.113+deb13-amd64. Data: TPC-H SF1 as Parquet (`bench.py gen`).

Versions: basalt 0.14.20, duckdb v2.0.0, clickhouse 26.11.1.2310, polars 2.1.0, datafusion 55.0.0.

Every engine runs as a process; a query's time is its wall time less the engine's
startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average
(sampled every millisecond); Polars and DataFusion include Python and the library.

## Startup

| engine | wall | peak memory (MB) |
|---|---|---|
| basalt | 1 ms | 15 |
| duckdb | 27 ms | 50 |
| clickhouse | 46 ms | 210 |
| polars | 128 ms | 130 |
| datafusion | 91 ms | 116 |

## Analytics: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 23 ms | 73 ms | 65 ms | 73 ms | 85 ms |
| median | 100 ms | 59 ms | 71 ms | 199 ms | 130 ms |
| pivot | 20 ms | 14 ms | 23 ms | 18 ms | 15 ms |
| q01_agg | 61 ms | 39 ms | 76 ms | 58 ms | 76 ms |
| q03_join_topn | 55 ms | 40 ms | 71 ms | 25 ms | 49 ms |
| q05_join_chain | 60 ms | 43 ms | 75 ms | 24 ms | 68 ms |
| q06_filter | 23 ms | 25 ms | 23 ms | 13 ms | 29 ms |
| q12_join_case | 46 ms | 31 ms | 51 ms | 17 ms | 44 ms |
| q13_outer_like | 86 ms | 58 ms | 68 ms | 39 ms | 44 ms |
| q16_distinct_notin | 23 ms | 20 ms | 26 ms | 13 ms | 21 ms |
| q18_in_having | 107 ms | 57 ms | 81 ms | 57 ms | 157 ms |
| q21_exists | 125 ms | 85 ms | 127 ms | 113 ms | 151 ms |
| rollup | 35 ms | 30 ms | 56 ms | 86 ms | 55 ms |
| star_join | 50 ms | 47 ms | 49 ms | 28 ms | 74 ms |
| string_group | 26 ms | 21 ms | 66 ms | 38 ms | 29 ms |
| string_like | 50 ms | 81 ms | 55 ms | 60 ms | 76 ms |
| topn_sort | 21 ms | 23 ms | 15 ms | 23 ms | 20 ms |
| window_running | 68 ms | 66 ms | 65 ms | 72 ms | 125 ms |
| window_topk | 45 ms | 48 ms | 43 ms | 38 ms | 58 ms |

## Analytics: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| distinct_count | 97 / 61 | 281 / 153 | 467 / 241 | 345 / 144 | 330 / 155 |
| median | 199 / 145 | 316 / 169 | 477 / 259 | 1077 / 499 | 695 / 334 |
| pivot | 149 / 85 | 139 / 57 | 297 / 135 | 281 / 86 | 215 / 68 |
| q01_agg | 237 / 184 | 228 / 114 | 474 / 262 | 489 / 183 | 299 / 146 |
| q03_join_topn | 182 / 109 | 311 / 130 | 533 / 268 | 237 / 86 | 408 / 141 |
| q05_join_chain | 189 / 119 | 328 / 136 | 439 / 234 | 241 / 87 | 474 / 179 |
| q06_filter | 126 / 88 | 210 / 84 | 321 / 145 | 240 / 78 | 270 / 91 |
| q12_join_case | 142 / 89 | 247 / 107 | 331 / 191 | 220 / 77 | 331 / 121 |
| q13_outer_like | 270 / 161 | 363 / 192 | 460 / 273 | 382 / 131 | 381 / 134 |
| q16_distinct_notin | 91 / 48 | 170 / 67 | 284 / 140 | 213 / 73 | 284 / 80 |
| q18_in_having | 277 / 130 | 286 / 155 | 473 / 283 | 431 / 165 | 774 / 386 |
| q21_exists | 227 / 149 | 335 / 184 | 633 / 372 | 676 / 290 | 884 / 416 |
| rollup | 167 / 114 | 208 / 94 | 393 / 212 | 594 / 229 | 269 / 122 |
| star_join | 194 / 117 | 398 / 165 | 415 / 201 | 235 / 91 | 502 / 201 |
| string_group | 179 / 113 | 141 / 68 | 412 / 241 | 506 / 156 | 197 / 78 |
| string_like | 138 / 111 | 358 / 202 | 517 / 244 | 278 / 130 | 284 / 139 |
| topn_sort | 115 / 80 | 215 / 86 | 285 / 126 | 301 / 90 | 240 / 79 |
| window_running | 337 / 186 | 286 / 181 | 388 / 229 | 396 / 164 | 383 / 230 |
| window_topk | 132 / 88 | 313 / 158 | 307 / 182 | 315 / 113 | 355 / 151 |

## Movement: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 655 ms | 853 ms | 493 ms | 658 ms | 733 ms |
| parquet_to_parquet | 164 ms | 549 ms | 733 ms | 505 ms | 1101 ms |
| csv_to_parquet | 1093 ms | 1087 ms | 1049 ms | 855 ms | 1177 ms |
| csvgz_to_parquet | 1646 ms | 2170 ms | 1257 ms | 1288 ms | 3235 ms |
| ndjson_to_parquet | 990 ms | 1294 ms | 1830 ms | 1352 ms | 1686 ms |
| orders_transform | 59 ms | 40 ms | 69 ms | 37 ms | 77 ms |

## Movement: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| parquet_to_csv | 779 / 539 | 1548 / 1269 | 1287 / 989 | 1082 / 805 | 1042 / 736 |
| parquet_to_parquet | 15 / 14 | 1372 / 1101 | 1798 / 1342 | 975 / 711 | 781 / 606 |
| csv_to_parquet | 1376 / 1016 | 1720 / 1245 | 2018 / 1278 | 1231 / 985 | 547 / 440 |
| csvgz_to_parquet | 326 / 284 | 1228 / 816 | 1272 / 913 | 1818 / 1161 | 403 / 350 |
| ndjson_to_parquet | 757 / 562 | 1369 / 963 | 3114 / 1752 | 2395 / 1237 | 581 / 473 |
| orders_transform | 223 / 129 | 221 / 123 | 368 / 239 | 282 / 108 | 307 / 155 |

## Database: time

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 482 ms | 401 ms | 990 ms | 1320 ms | n/a |
| parquet_to_pg | 565 ms | 1287 ms | 3407 ms | 2291 ms | n/a |
| mysql_to_parquet | 768 ms | 3816 ms | 903 ms | 1016 ms | n/a |
| parquet_to_mysql | 5922 ms | 16.9 s | 12.1 s | n/a | n/a |
| mssql_to_parquet | 1320 ms | n/a | n/a | 6514 ms | n/a |
| parquet_to_mssql | 984 ms | n/a | n/a | n/a | n/a |
| pg_to_mysql | 5709 ms | 17.7 s | 12.2 s | n/a | n/a |

## Database: memory, peak / average MB

| task | basalt | duckdb | clickhouse | polars | datafusion |
|---|---|---|---|---|---|
| pg_to_parquet | 471 / 174 | 707 / 343 | 753 / 461 | 812 / 255 | n/a |
| parquet_to_pg | 434 / 326 | 345 / 260 | 708 / 643 | 849 / 689 | n/a |
| mysql_to_parquet | 182 / 113 | 193 / 172 | 771 / 435 | 753 / 257 | n/a |
| parquet_to_mysql | 463 / 392 | 325 / 256 | 783 / 673 | n/a | n/a |
| mssql_to_parquet | 127 / 107 | n/a | n/a | 761 / 200 | n/a |
| parquet_to_mssql | 438 / 347 | n/a | n/a | n/a | n/a |
| pg_to_mysql | 47 / 44 | 119 / 118 | 622 / 552 | n/a | n/a |

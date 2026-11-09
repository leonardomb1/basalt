# Benchmarks

basalt against DuckDB, ClickHouse Local, Polars and DataFusion on TPC-H data:
startup, query time, and memory, peak and average. Results are committed in
[`results/`](results/), one file per run, dated and naming the machine; the
README's table comes from the latest.

## What is measured

- **Every engine runs as a process,** one per query and repetition, and engines
  take turns on each repetition, so warm caches and heat favour none. basalt,
  DuckDB and ClickHouse run as their command-line programs; Polars and DataFusion,
  which have none, as a small Python process that imports the library, registers
  the tables and runs the query.
- **Startup** is that same process running `SELECT 1`, best of three or more.
- **Query time** is the process's wall time, best of `-n`, less the engine's
  startup, so a library that takes 300 ms to import is not charged for it on
  every query.
- **Memory** is the process's resident memory: the peak, from the kernel when it
  exits, and the average of samples taken from `/proc` every millisecond while it
  runs. For Polars and DataFusion it includes Python and the library.
- **Threads** are the same for every engine (`--threads`, default all cores):
  `basalt -j`, DuckDB's `threads`, ClickHouse's `max_threads`,
  `POLARS_MAX_THREADS`, DataFusion's target partitions. Memory limits are each
  engine's default.
- **Answers** are checked before anything is timed: `check` runs every query on
  every engine at SF0.01 and compares the rows with DuckDB's, numbers within a
  relative 1e-4; a movement task's output is read back and counted.

## The suites

[`analytics/`](analytics/): one query per file, each a different path through an
engine: TPC-H Q1, Q3, Q6, Q12, Q13, Q16 and Q18, a star join, `count(DISTINCT)`,
`LIKE` over long strings, grouping by strings, a top-N sort, a top-k window and
an exact median. A table is written `{lineitem}`, which each engine fills in its
own way; `<query>.<engine>.sql` replaces the query for one engine where its
dialect differs, saying why at its head (ClickHouse's outer joins fill defaults
rather than NULL; its `median` is approximate).

[`movement.json`](movement.json): `lineitem` (6 million rows at SF1) from Parquet
to CSV and to Parquet, from CSV, gzipped CSV and NDJSON to Parquet, and a filtered,
computed projection of `orders` to Parquet; each engine's statement written out,
as each writes files its own way.

## Running

The engines that are libraries come through `uv`; DuckDB and ClickHouse are their
binaries, on `PATH` or given with `--duckdb` and `--clickhouse`. basalt is
`--basalt`, a release build with the CI's flags:

```sh
zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl -Dcpu=x86_64_v3+aes+pclmul -Dstrip=true
B="uv run --with duckdb --with polars --with datafusion --with pyarrow python bench/bench.py"
$B gen --sf 0.01 --data <data>/tpch-sf0.01
$B gen --sf 1 --data <data>/tpch-sf1
$B check --sf 0.01 --data <data>/tpch-sf0.01 --basalt zig-out/bin/basalt
$B run --sf 1 --data <data>/tpch-sf1 --basalt zig-out/bin/basalt -n 3
```

`gen` lays the data out as `parquet/`, `csv/` and `ndjson/`, about 3.7 GB at SF1,
and keeps what is already there; it refuses SF10 and above without `--big`.
`--engine` and `--filter` narrow a run; `-v` shows a differing answer.

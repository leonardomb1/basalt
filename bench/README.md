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
  relative 1e-4 and a NULL however an engine spells it in CSV; a movement task's
  output is read back and counted, and a database task's target table counted
  against the table it moved.

## The suites

[`analytics/`](analytics/): one query per file, each a different path through an
engine: TPC-H Q1, Q3, Q5, Q6, Q12, Q13, Q16, Q18 and Q21, a star join,
`count(DISTINCT)`, `LIKE` over long strings, grouping by strings, a top-N sort, a
top-k window, a running total with `LAG`, `PIVOT`, `ROLLUP` and an exact median. A
table is written `{lineitem}`, which each engine fills in its own way;
`<query>.<engine>.sql` replaces the query for one engine where its dialect differs,
saying why at its head (ClickHouse's outer joins fill defaults rather than NULL,
its `median` is approximate and its LAG is `lagInFrame`; PIVOT is conditional sums
where there is no PIVOT, ROLLUP a union in Polars, Q21 decorrelated where a
correlated EXISTS is not taken).

[`movement.json`](movement.json): `lineitem` (6 million rows at SF1) from Parquet
to CSV and to Parquet, from CSV, gzipped CSV and NDJSON to Parquet, and a filtered,
computed projection of `orders` to Parquet; each engine's statement written out,
as each writes files its own way.

[`database.json`](database.json): `orders` (1.5 million rows at SF1) out of and
into PostgreSQL, MySQL and SQL Server, to and from Parquet, and PostgreSQL to MySQL.
Each engine moves it the way it can: DuckDB through its `postgres` and `mysql`
extensions, ClickHouse through its `postgresql()` and `mysql()` table functions,
Polars reading through ConnectorX and writing to PostgreSQL through ADBC;
DataFusion has no database connector, and SQL Server only basalt writes. A task
that writes starts from an empty table, made before each run and not timed.

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
`--engine` and `--filter` narrow a run, which then writes no results file; `-v`
shows a differing answer.

The database suite runs only with `--db`, against servers that are up: by default
the integration tests' containers, each setting from `BENCH_PG_HOST`,
`BENCH_MY_PORT`, `BENCH_MS_PASSWORD` and so on. `gen --db` loads `bench_orders`
into each with basalt; Polars needs ConnectorX and the PostgreSQL ADBC driver.

```sh
docker compose -f tests/integration/compose.yaml up -d --wait postgres mysql mssql
B="uv run --with duckdb --with polars --with datafusion --with pyarrow --with connectorx --with adbc-driver-postgresql python bench/bench.py"
$B gen --sf 1 --db --data <data>/tpch-sf1 --basalt zig-out/bin/basalt
$B check --db --sf 0.01 --data <data>/tpch-sf0.01 --basalt zig-out/bin/basalt
$B run --db --sf 1 --data <data>/tpch-sf1 --basalt zig-out/bin/basalt -n 3
```

## Comparing two builds, and scaling

```sh
python3 bench/bench.py ab --a <baseline basalt> --b <candidate basalt> --data <data>/tpch-sf1 -n 10
python3 bench/bench.py ab --a <basalt> --data <data>/tpch-sf1 -n 10            # the noise floor
python3 bench/bench.py scale --basalt <basalt> --data <data>/tpch-sf1 --threads-list 1,2,4,8,16
```

`--suite database` takes the database tasks into `ab` and `scale`, whose `all`
is analytics and movement. `ab` runs two basalt builds in the order A B B A on every task, so drift favours
neither, and prints each one's median wall time, CPU time (user and system) and
page faults, with the ratio of B's median wall time to A's and a 95% bootstrap
interval: a change is real only when the interval leaves 1. Running it with one
build gives the machine's noise floor; measure it before trusting a difference.
`scale` runs one build at each thread count, printing the median wall time, the
speedup over the first count and the total CPU time against the first's, which
separates work a wider run adds from cores it waits on. Neither samples memory, so
nothing wakes beside the process measured. Both need only Python's standard
library.

Prefer a bare-metal machine with identical cores for numbers that decide a change:
a laptop's mixed performance and efficiency cores, its thermal limits and a VM's
missing hardware counters spread the same run by a third or more.


# Benchmarks

Two harnesses, deliberately separate:

| step | what it gates |
| --- | --- |
| `zig build bench` | the explicit SIMD kernels in `exec/simd.zig`, against equivalent scalar loops |
| `zig build bench-e2e` | the **engine**: committed scripts through the real CLI, timed end to end and checked against DuckDB |

`bench-e2e` exists because performance claims about a query engine are worthless
without a reproducible number attached. Every optimisation should move a line in
its table, and the table should be re-run before and after — not argued about.

## Fixtures

TPC-H SF1, generated once. They are **not committed** (~1.3 GB):

```sh
mkdir -p ~/basalt-bench-data && cd ~/basalt-bench-data
duckdb tpch.db < /path/to/basalt/bench/gen.sql
export BASALT_BENCH_DATA=$PWD
```

DuckDB is the fixture generator and the reference implementation. It is not a
dependency of basalt — without it on `PATH` the harness still runs and simply
drops the reference column.

## Running

```sh
zig build -Doptimize=ReleaseFast                 # REQUIRED — see below
zig build bench-e2e -Doptimize=ReleaseFast -- --reps 3
zig build bench-e2e -Doptimize=ReleaseFast -- --filter tpch --json
```

`--data <dir>` overrides `$BASALT_BENCH_DATA`; `--out <dir>` is scratch for the
sinks (default `/tmp/basalt-bench-out` — keep it on a tmpfs so disk write variance
stays out of the measurement); `--bin <path>` picks the binary; `--reps <n>` sets
timed runs after one warmup; `--filter <s>` selects by name substring.

**Build ReleaseFast or the numbers are fiction.** `zig build` defaults to Debug,
and a Debug basalt is 15–170× slower — enough to invent regressions that do not
exist. The harness cannot detect this, so it is on you.

## The two suites

`tpch-*` — **compute**: scan, filter, join, GROUP BY, sort. What DuckDB is built
for, and therefore the honest place to measure the gap. Each has a DuckDB
reference in `reference/` that must stay semantically identical, including the CSV
sink, so a comparison is whole-pipeline rather than basalt-writes-a-file versus
duckdb-prints-a-table.

`move-*` — **movement**: read every row, serialise every row, write it, no
compute. This is what basalt is *for* and what no analytics benchmark measures. No
reference: there is no interesting answer to compare, only throughput. `m02` and
`m03` share a projection so the difference between them is only the filter — the
line to watch when changing how filtered rows are materialised.

Deviations from the TPC-H spec, all applied to both sides equally: `q14` emits its
two SUMs rather than their ratio, and dimension tables are lifted into CTEs
because basalt requires the right side of a join to be a CTE. Interval arithmetic
is spelled as the literal date it computes.

## Reading the output

```
query      suite      basalt    duckdb     ratio   rows  result
tpch-q01   tpch        353ms      49ms     7.20x      4   ok
```

`ratio` under 1 means basalt is faster. `result` is `ok` when the output matched
DuckDB field by field — numerically where both sides parse as numbers, within
`1e-9` relative, since float SUM order differs between engines and bit-equality is
the wrong bar. **A fast wrong answer is not a result**: treat any mismatch as
outranking every timing on the table.

Timings are wall clock of the child process, so they include CLI startup. That is
the honest number for a movement tool, but it is a fixed tax that matters most on
the queries with tiny results, so the harness measures and prints each engine's
startup separately.

## Baseline

First run, recorded so later work has something to beat. basalt 0.5.7, DuckDB
1.5.5, TPC-H SF1, AMD Ryzen 7 PRO 8700GE (16 threads), 61 GB RAM, fixtures in page
cache, sinks on tmpfs, min of 3.

| query | basalt | duckdb | ratio |
| --- | --- | --- | --- |
| tpch-q01 | 353 ms | 49 ms | 7.2× |
| tpch-q03 | 636 ms | 41 ms | 15.5× |
| tpch-q06 | 460 ms | 24 ms | 19.2× |
| tpch-q12 | 665 ms | 29 ms | 22.9× |
| tpch-q14 | 357 ms | 39 ms | 9.2× |

| query | basalt | rows out | rate |
| --- | --- | --- | --- |
| move-m01 (parquet→csv) | 1716 ms | 6,001,215 | 3.5 M rows/s |
| move-m02 (csv→csv) | 2809 ms | 6,001,215 | 2.1 M rows/s |
| move-m03 (csv→csv, 2% kept) | 886 ms | 122,005 | — |

Where the compute time goes, from `--explain` actuals (exclusive, summed across
lanes, so these are CPU not wall):

- **q01** aggregate 609 ms > scan 453 ms > filter 92 ms. The aggregate — 5.9 M rows
  into **four** groups keyed by two short strings — is the single largest cost in
  the query. It should be nearly free, and it is not, because grouping compares and
  hashes string bytes.
- **q06** scan 262 ms, filter 193 ms for a predicate that keeps 1.9% of rows.
- **q14** scan 265 ms dominates; the join over 76 K surviving rows is 38 ms.

### Since the baseline

- **q06 457 ms → 121 ms** (18.3× → 5.0×). Not a kernel change: the parallel parquet
  path used to refuse an ungrouped aggregate and hand it to the serial driver, so
  `SELECT SUM(x) FROM t WHERE ...` ran on one core. `-j 1` and `-j 16` were within
  a millisecond of each other.

Worth knowing when reading any row above: **q03, q12 and q14 are still serial**, and
that is most of their gap. `classifyAggPipeline` rejects any pipeline containing a
join, so join-then-aggregate has no parallel path at all. Compare thread counts
before concluding anything about a query's per-row cost:

```sh
for j in 1 4 16; do zig build run -Doptimize=ReleaseFast -- run bench/tpch/q12.sql \
  -p data=$BASALT_BENCH_DATA -p out=/tmp/q12 -j $j; done
```

And `m03` versus `m02` isolates the sink: identical input and projection, 2% of the
rows written, 886 ms against 2809 ms. Roughly 1.9 s of `m02` is materialising and
serialising rows — about twice what parsing the 773 MB input costs. For the
movement suite the **output path**, not the source, is the bottleneck.

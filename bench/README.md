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

## The suites

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

`ssb-*` — **star schema**: one fact table joined to two, three or four dimensions
with the filters on the dimensions. This is the shape most warehouse queries actually
have, and the one the join-chain fan-out exists for. Fixtures are SSB derived from
TPC-H SF1 by `bench/ssb_gen.sql` (the standard derivation: `lineitem`+`orders`
denormalised into a `lineorder` fact, dimensions widened with region/nation/city, a
generated date dimension). Not `ssb-dbgen` output, so absolute numbers are not
comparable to published SSB results — both engines read identical files, which is
what makes the comparison valid.

`tpcds-*` — **five of the 99 TPC-DS queries** (3, 42, 52, 55, 96), ported from the
spec's comma-join form to explicit joins. Only five were ported because, when the
suite was written, only 23 of the 99 were expressible in the dialect. Subqueries,
window functions, `EXISTS`, `INTERSECT` and `EXCEPT` have all landed since, so most of
the rest are now portable; `ROLLUP` and `GROUPING SETS` are still missing. Porting
more is open work. Generate the fixtures with `bench/tpcds_gen.sql`.

Deviations from the TPC-H spec, all applied to both sides equally: `q14` emits its
two SUMs rather than their ratio, and dimension tables sit in CTEs, which basalt
once required of a join's right side and no longer does. Interval arithmetic is
spelled as the literal date it computes.

## Reading the output

```
query      suite      basalt    duckdb     ratio   rows  result
tpch-q01   tpch        372ms     100ms     3.72x      4  ok
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

## Latest run

basalt 0.11.9 against DuckDB 1.5.1, Intel i7-13700H (20 threads), fixtures in page
cache, min of 3. Every query matched DuckDB.

| query | basalt | duckdb | ratio |
| --- | --- | --- | --- |
| tpch-q01 | 372 ms | 100 ms | 3.72× |
| tpch-q03 | 217 ms | 76 ms | 2.86× |
| tpch-q06 | 118 ms | 41 ms | 2.88× |
| tpch-q12 | 239 ms | 43 ms | 5.56× |
| tpch-q14 | 163 ms | 60 ms | 2.72× |
| ssb-q11 … q43 (13) | 102–281 ms | 38–91 ms | 1.86–3.54× |
| tpcds-q03, q42, q52, q55, q96 | 67–115 ms | 27–48 ms | 1.81–3.19× |

| query | basalt | rows out | rate |
| --- | --- | --- | --- |
| move-m01 (parquet→csv) | 208 ms | 6,001,215 | 28.9 M rows/s |
| move-m02 (csv→csv) | 715 ms | 6,001,215 | 8.4 M rows/s |
| move-m03 (csv→csv, 2% kept) | 581 ms | 122,005 | — |

## Diagnosing a slow line

Compare thread counts before concluding anything about a query's per-row cost. A
time that stays flat from `-j 1` to `-j 16` means the shape never reached a
parallel path, not that the engine is slow per row:

```sh
for j in 1 4 16; do zig build run -Doptimize=ReleaseFast -- run bench/tpch/q12.sql \
  -p data=$BASALT_BENCH_DATA -p out=/tmp/q12 -j $j; done
```

`m03` against `m02` isolates the sink: the same input and projection, with 2% of
the rows written, so their difference is what materialising and serialising rows
costs. `basalt run --trace` and a `zig build -Dstats=true` build break a run down further
([Command line](../docs/tools/cli.md)).

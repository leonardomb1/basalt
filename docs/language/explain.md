# DESCRIBE, SHOW TABLES and EXPLAIN

## DESCRIBE and SHOW TABLES

`DESCRIBE <source>;` prints one row per column — `column`, `type`, `nullable`
— for a file, a `conn.schema.table`, a `conn.QUERY($$...$$)` or a whole query
(`DESCRIBE SELECT ...`, CTEs included). The types are the engine's, i.e. what a
sink would receive; a table or query is asked for no rows, so it is cheap on a
large one. `SHOW TABLES FROM <conn>[.<schema>] [LIKE 'pattern'];` lists a SQL
source's tables and views from its `information_schema` (`table_schema`,
`table_name`, `table_type`), system schemas left out; on an `http` connection
it lists the declared resources ([HTTP APIs](../connectors/http.md)). Both are ordinary result
statements: `basalt run --format json -c "DESCRIBE erp.dbo.SC5010;"` gives a
program the schema, and the REPL's `\d` and `\dt` are the same statements.

## `EXPLAIN`

`EXPLAIN <statement>` prints the plan instead of running it.
`EXPLAIN ANALYZE <statement>` runs it and prints a profile: a summary, then the
operator tree with what each operator actually cost — its own time (its inputs'
taken out), its rows in and out, and across lanes the least and most any lane
spent. See [Reading EXPLAIN ANALYZE](#reading-explain-analyze).

Both print a summary, then the same tree: root first, the scan deepest, which is
the nesting a pull pipeline has, each operator numbered and shown with its SQL.
The summary names the sink and the path; `EXPLAIN` annotates each operator with
the schema it yields, `EXPLAIN ANALYZE` with what it measured.

```console
$ basalt run -c "EXPLAIN SELECT g, COUNT(*) AS c FROM 'x.parquet' GROUP BY g;"
Summary
  Sink: stdout (default)
  Path: morsel-parallel candidate (per-lane partials, combined)
plan
└─ aggregate (#1)  by g; count(*)
   │  schema: g:string?  c:int
   └─ scan (#2)  parquet x.parquet
         schema: g:string?  v:int?

$ basalt run -j 4 -c "EXPLAIN ANALYZE SELECT g, COUNT(*) AS c FROM 'x.parquet' WHERE v > 0 GROUP BY g;"
Summary
  Wall time: 21.40ms   Operator time: 38.62ms (summed over 4 lanes)
  Rows read: 500.0K (500000)   Rows out: 601   Path: parquet parallel aggregate, 6 row groups
  Sink: stdout, not written: EXPLAIN ANALYZE moves no data
  Row groups: 6 read, 0 skipped
  Columns read: 2 of 2
  fold on the lanes: 19.87ms
  merge the lanes' groups: 412.0us
  emit and write the groups: 96.3us
  Top operators by own time:
    1. aggregate (#1): 24.10ms (62.4%)
    2. scan (#3): 11.35ms (29.4%)
    3. filter (#2): 3.17ms (8.2%)
plan (actuals)
└─ aggregate (#1)  by g; count(*)
   │  time: 24.10ms (62.4%) [min=3.92ms, max=8.31ms]  skew 1.4x
   │  rows: 499.6K (499612) -> 2.4K (2404)   batches: 6   rows per lane [min=601, max=601]
   └─ filter (#2)  v > 0
      │  time: 3.17ms (8.2%) [min=0.71ms, max=0.88ms]
      │  rows: 500.0K (500000) -> 499.6K (499612) (99.9% kept)   batches: 6
      └─ scan (#3)  parquet x.parquet
            time: 11.35ms (29.4%) [min=2.48ms, max=3.21ms]
            rows: 500.0K (500000)   batches: 6   rows per lane [min=83.3K (83333), max=166.7K (166667)]
```

`EXPLAIN`'s `Path:` line names the fan-out the plan is eligible for: `split-parallel`
for a SQL read divided into key ranges, `morsel-parallel` for a local CSV divided
into byte-range chunks or a parquet divided into row groups, `serial` for
everything else (a CSV over HTTP is fetched whole, so it parses serially). Both
stay "candidate" because what settles it is inside the source — a table's key and
size, a CSV that quotes a newline, a parquet with one row group — and neither
`EXPLAIN` nor `check` reads that far.

A source whose schema only the source itself can describe — a database table, a
remote object — reads `schema: unresolved`, said once at the scan rather than
repeated down the tree. Neither form connects.

A SQL read prints the WHERE it will send on a `pushdown:` line under its scan
([Pushdown](pushdown.md)). A join prints its right side's read as its `<build>`
scan, with that read's own `pushdown:` — so a CTE or table function joined in
shows that it filters at the source:

```console
$ basalt run -c "... EXPLAIN SELECT o.id, c.nm FROM pg.orders o JOIN active(1) c ON o.cid = c.cid;"
Summary
  Sink: stdout (default)
  Path: split-parallel candidate
plan
└─ project (#1)  id, nm
   └─ join (#2)  inner on cid = cid
      ├─ <probe> scan (#3)  postgres table orders
      │     schema: unresolved
      └─ <build> scan (#4)  postgres table customers (via binding __tvf1_active)
            pushdown: ("active" = 1)
```

`EXPLAIN` is an ordinary statement: it goes anywhere a terminal `SELECT` or a
`LOAD INTO` goes, and explains that one query against whatever the statements
above it declared — connections, CTEs, `PARAM`s, `LET`s, functions. Everything
before and after it runs normally, at full parallelism, and a script may hold
several. A script that *opens* with `EXPLAIN` still explains the whole script,
offline: no params are bound and nothing runs, which is the form to reach for
when the point is to inspect a job rather than run one.

A plan printed by an `EXPLAIN` **statement** goes to **stderr**, like the
`EXPLAIN ANALYZE` tree and `PRINT`: the statements around it may be writing
rows to stdout, and stdout is the data contract ([Command line](../tools/cli.md)). The whole-script
`EXPLAIN` prefix prints to stdout instead — there the plan is the invocation's
only output, so `basalt run -c "EXPLAIN ..." > plan.txt` still captures it.

```console
$ basalt run -c "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'db', database = 'erp');
                 EXPLAIN SELECT SUM(v) AS v FROM pg.public.t;"
```

`EXPLAIN ANALYZE` moves no data. The pipeline runs — that is where the numbers
come from — but every sink is discarded: a terminal `SELECT` prints its plan
instead of its rows, and a `LOAD` writes nothing, creates no table and commits
no blob. A database can wrap an explained `INSERT` in `BEGIN ... ROLLBACK`; a
load into a remote lake has no undo, so it is never performed.

It runs at the run's `-j`, on whichever path the pipeline takes: every
parallel path folds the chains its lanes build into one tree, so the plan prints
the same way serial or fanned out, with the figures of the path that ran. `-j 1`
gives a serial profile. The clock is read once per batch, and a run without
`EXPLAIN ANALYZE` collects nothing.

## Reading EXPLAIN ANALYZE

The **summary** comes first:

- **Wall time** is the pipeline from start to finish. **Operator time** is every
  operator's own time added up, over every lane: at `-j 8` eight lanes' work
  fills one wall second, so it can exceed the wall time, and a wall time far
  above it is time spent outside the operators — connecting, waiting on a
  source, writing.
- **Path** is how the pipeline ran: `serial`, a parallel path and how its input
  divided (`parquet parallel aggregate, 6 row groups`, `postgres split-parallel,
  8 key ranges`), or that the source ran the aggregate or the `ORDER BY ...
  LIMIT` itself.
- **Row groups** read and skipped by their statistics, **Columns read** of
  those the files and tables offer, and how much **spilled to disk** at most.
- **Sent to** is each request a source was given, as sent: a SQL read's query
  with the `WHERE` that descended into it, an HTTP read's URL. A split path's
  query is shown once, before its key ranges are added. This is where pushdown
  shows.
- The **phases** outside any operator: opening the sources, a parallel path's
  merge and the writing of its result.
- **Top operators by own time**, with each one's share of the operator time.

Then the **tree**, root first, the scan deepest; `<probe>` and `<build>` mark a
join's two sides, and a parallel breaker's tail (`HAVING`, `ORDER BY ... LIMIT`)
stands above a scan of *the lanes' partial results, merged*, the lanes'
operators below it. Each node shows:

- its kind, its number, and what it does in SQL: the predicate, the keys, the
  columns, the file or table and the `WHERE` it pushed down;
- `time:` its own time and share. Past 30% of the operator time a node is red,
  past 15% yellow, on a terminal without `NO_COLOR`. With lanes,
  `[min=…, max=…]` is the least and most a lane spent on it, and `skew 2.3x`
  calls out a lane far above the lanes' mean (1.5x or more, on a node of 5% or
  more): one lane holding the run up, from uneven row groups or ranges;
- `rows:` in and out — a join counts its probe side — with the share kept by a
  filter, join, distinct or limit, the batches, and with lanes the rows per lane;
- what the operator knows besides: a join's `build rows` and `build size` (its
  shared index counted once, not per lane), `sorted runs spilled`, and
  `spilled, partition levels` for a join, aggregate or distinct past
  `--op-memory`; and the choice the planner made, such as which side of a join
  is held in memory and on what estimates.

Row counts are exact. Times are what the run cost under the profile; reading the
clock per batch adds little, but a run of many tiny batches pays for it.

`EXPLAIN COSTS` is rejected at parse time — there is no cost model to report.

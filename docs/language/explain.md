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
`EXPLAIN ANALYZE <statement>` runs it and prints the operator tree with the
time and row count each stage actually cost — time is *exclusive*, so a
stage's figure excludes its inputs.

Both print the same tree: root first, the scan deepest, which is the nesting a
pull pipeline has. `EXPLAIN` annotates each operator with its detail and output
schema, `EXPLAIN ANALYZE` with what it measured.

```console
$ basalt run -c "EXPLAIN SELECT g, COUNT(*) AS c FROM 'x.parquet' GROUP BY g;"
plan
  write  stdout  (default)
    aggregate  1 agg(s), 1 group(s)
      schema: g:string?  c:int
      scan  parquet  x.parquet
        schema: g:string?  v:int?
  physical: morsel-parallel candidate (per-lane partials, combined)

$ basalt run -c "EXPLAIN ANALYZE SELECT g, COUNT(*) AS c FROM 'x.parquet' GROUP BY g;"
plan (actuals, exclusive time)
  aggregate      13.1ms          601 rows        2 batches
    scan          6.2ms       500000 rows        6 batches
```

The `physical:` line names the fan-out the plan is eligible for: `split-parallel`
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
([Pushdown](pushdown.md)). A join prints its right side's read on a `right` line under it, with that
read's own `pushdown:` — so a CTE or table function joined in shows that it
filters at the source:

```console
$ basalt run -c "... EXPLAIN SELECT o.id, c.nm FROM pg.orders o JOIN active(1) c ON o.cid = c.cid;"
plan
  write  stdout  (default)
    select  id, nm
      join  inner __tvf1_active
        right  scan  postgres  table customers (via binding __tvf1_active)
          pushdown: ("active" = 1)
        scan  postgres  table orders
          schema: unresolved
  physical: split-parallel candidate
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

It also runs serially, whatever `-j` says, so the tree is one operator tree
rather than a different report per parallel path. Read the timings as a serial
profile: the row counts and the shape are exact, the durations are not what the
same query costs at full parallelism.

`EXPLAIN COSTS` is rejected at parse time — there is no cost model to report.

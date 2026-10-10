# Aggregates

`GROUP BY` folds rows into one per group; the aggregate functions are listed
under [Queries](queries.md#operators). End to end:

```sql
SELECT region,
       COUNT(*)                AS orders,
       COUNT(DISTINCT customer) AS customers,
       SUM(amount)             AS revenue
FROM 'orders.parquet'
WHERE placed_at >= '2026-01-01'
GROUP BY region
HAVING COUNT(*) > 100
ORDER BY revenue DESC
LIMIT 10;
```

A **plan-time constant** may sit in the list alongside the aggregates, since it is
one value for the whole query — a literal, arithmetic or `||` over literals, a
call like `now()`, or a `$param` / `$let`. That is how an aggregate result carries
a run id or a tenant tag:

```sql
PARAM tag STRING DEFAULT 'acme';
LET run_ts = now();
SELECT $tag AS tenant, $run_ts AS loaded_at, region, COUNT(*) AS orders
FROM 'orders.parquet' GROUP BY region;
```

A plain *column* still may not: it has no single value per group, so it must be
wrapped in an aggregate or named in `GROUP BY`.

An expression is fine when every column in it is covered: by a grouping key, or
by a grouping *expression* it repeats, which then counts as one value per group as
SQL has it. The same holds in `HAVING` and `ORDER BY`, and an aggregate may sit
inside such an expression:

```sql
SELECT upper(region) AS r, lower(upper(region)) || '-x' AS tag,
       upper(region) || ': ' || COUNT(*) AS label
FROM 'orders.parquet'
GROUP BY upper(region)
HAVING lower(upper(region)) <> 'north';
```

`lower(region)` there is refused: `region` itself is not grouped, only
`upper(region)` is.

`ORDER BY` may also sort by an aggregate the `SELECT` list does not compute —
`ORDER BY MAX(amount) DESC`, `ORDER BY SUM(amount) / COUNT(*)` — which is
computed beside the others and dropped after the `LIMIT`, as an aggregate only
`HAVING` names is. Under `SELECT DISTINCT` it must be one of the list's, as SQL
requires, and after a `UNION` it must name an output column.

## FILTER

Any aggregate takes `FILTER (WHERE <condition>)`, folding only the rows where the
condition holds — several slices of a group in one pass, where a `WHERE` would need
a query each:

```sql
SELECT region,
       COUNT(*)                                       AS orders,
       COUNT(*)    FILTER (WHERE status = 'returned') AS returns,
       SUM(amount) FILTER (WHERE amount > 0)          AS revenue,
       COUNT(DISTINCT customer) FILTER (WHERE placed_at >= '2026-01-01') AS new_year_customers
FROM 'orders.parquet'
GROUP BY region
HAVING SUM(amount) FILTER (WHERE amount > 0) > 1000;
```

A null condition counts as false. Every aggregate skips nulls, so `agg(x) FILTER
(WHERE c)` is read as `agg(if(c, x, null))` and `COUNT(*) FILTER (WHERE c)` as
`count_if(c)`: the result is the same, and a filtered aggregate runs wherever a
plain one does — on the parallel lanes, spilling, and over a
[window](windows.md#filter). An aggregate with no matching row is null (`COUNT` 0).

**A float `SUM` is reproducible for a given `-j`, not across values of it.** The
lanes each total their own slice and the slices are added in a fixed order, so
rerunning the same command writes the same bytes; but `-j` decides how the input
is cut, and float addition is not associative, so `-j 4` and `-j 8` can differ in
the last bits (as can either from `-j 1`). `SUM` over an `INT` or a `DECIMAL` is
exact and identical everywhere (a `DECIMAL(p, s)` sums as `DECIMAL(38, s)`, as DuckDB types it) — `SUM(CAST(amount AS DECIMAL(18,2)))` is the way
to total money you intend to compare or checksum. `COUNT`, `MIN` and `MAX` are
exact too, as are `count_if`, `bool_and`/`bool_or` and the `bit_*` aggregates.
The variances and deviations combine their lanes the same fixed way, so they
share `SUM`'s float caveat: `ROUND` them before comparing runs at different `-j`.

**`SUM` over a text column is exact**, as SQL sums exact numbers — a CSV column of
quoted numbers (`"1500"`, `"12.50"`), read as text, still totals to the cent. Each
value is read strictly as an exact number: digits, an optional sign and at most
one `.`. The result's type is fixed at plan time from the values the CSV reader
sampled to type the file (its first 1024 rows): `INT` when they are whole,
`DECIMAL(38, s)` when the most decimal places among them is `s`. Text with no
sample to go by — a database's text column, or an expression such as
`SUM(trim(v))` — sums as an `INT`.

A value the sum cannot add exactly fails the run, naming the cast that would:
`'1.255'` past a sum typed `DECIMAL(38, 2)` (trailing zeros aside — `'1.500'` is
`1.50`) asks for `SUM(CAST(v AS DECIMAL(38, 3)))`, and exponent form (`'1e3'`),
`nan` or `inf` for `SUM(CAST(v AS FLOAT))`. `AVG` and the other numeric
aggregates read text as `FLOAT`. The same rules hold for `SUM(...) OVER (...)`.

**Past `--op-memory`, a `GROUP BY` spills to disk** (see
[Spilling to disk](../tools/cli.md#spilling-to-disk)): groups it already holds keep
folding in memory, and rows of new groups go to disk by a hash of their key, to be
aggregated once the input ends. Every result is exact — a float `SUM` included,
since each group still sees its rows in input order — but the groups come out in
another order, so add `ORDER BY` if the order matters. The disk it may use is
bounded by `--spill-cap`. Spilling happens on a `-j 1` run and on any aggregate
the parallel lanes do not take; the lanes' own per-lane tables do not spill.

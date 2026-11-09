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

## Collecting and ordered aggregates

| Aggregate | Answers |
|---|---|
| `string_agg(x, sep [ORDER BY y])` | the values that are not null as one text, each after its row's `sep`; NULL over none |
| `array_agg(x [ORDER BY y])`, `list_agg` | every value, nulls too, as a `LIST`, which `UNNEST` and `l[i]` read back; NULL over no rows |
| `any_value(x)` | the first value that is not null |
| `arg_max(v, by)`, `max_by` / `arg_min(v, by)`, `min_by` | `v` of the row with the greatest or least `by`, the first such row on a tie; a null `by` never wins |
| `percentile_cont(f) WITHIN GROUP (ORDER BY x)` | the value a fraction `f` (0 to 1) of the way along, interpolated between two rows; a `FLOAT`, `x` numeric |
| `percentile_disc(f) WITHIN GROUP (ORDER BY x)` | the first value at or past that place, of `x`'s own type |

```sql
SELECT customer,
       string_agg(item, ', ' ORDER BY placed_at)        AS items,
       array_agg(amount ORDER BY placed_at DESC)[1]     AS latest_amount,
       arg_max(status, placed_at)                       AS latest_status,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
FROM 'orders.parquet'
GROUP BY customer;
```

`string_agg` and `array_agg` take their `ORDER BY` inside the call or, as SQL Server
writes it, in `WITHIN GROUP (ORDER BY y)`, and take `DISTINCT`. Without an `ORDER BY`
they go by input order, ties too.

The answer of `string_agg`, `array_agg`, `any_value`, `arg_max` and `arg_min` can
depend on the order rows arrive in, so a query holding one reads its input on one
lane, whatever `-j` says, and gives the same answer at every `-j` and when it spills.
`string_agg`, `array_agg` and the percentiles hold each group's values until the end,
counted against `--op-memory`. None of these takes `OVER (...)`, and none descends to
a SQL source: the source's rows are read and folded here.

## ROLLUP, CUBE and GROUPING SETS

Several groupings in one query, each input row counted once per grouping:

```sql
SELECT region, status, COUNT(*) AS orders, GROUPING(region, status) AS level
FROM 'orders.parquet'
GROUP BY ROLLUP(region, status);
```

- `ROLLUP(a, b)` is the groupings `(a, b)`, `(a)` and `()`, the last the grand total.
- `CUBE(a, b)` is every combination: `(a, b)`, `(a)`, `(b)`, `()`.
- `GROUPING SETS ((a, b), (c), ())` lists them, and may hold a `ROLLUP` or `CUBE`.
- `ROLLUP((a, b), c)` takes `a, b` as one item, and a plain key beside them,
  `GROUP BY x, ROLLUP(a)`, is in every grouping.

A key a grouping leaves out is NULL in its rows, apart from a group whose own key is
NULL: `GROUPING(a)` is 1 in the first and 0 in the second, and `GROUPING(a, b)` makes
a number of those bits, the last key the lowest. A key keeps its value inside an
aggregate (`MAX(region)` over the grand total). Over no input rows a grouping of no
keys still answers one row, as a plain aggregate does; one with keys answers none.
Such a query runs on one lane.

## PIVOT and UNPIVOT

`PIVOT` turns a column's values into columns, grouped on every input column it does
not use, so a derived table is how to say which those are. `UNPIVOT` does the reverse.
Both follow `FROM` and its joins, before `WHERE`.

```sql
SELECT * FROM (SELECT region, status, amount FROM 'orders.parquet') t
PIVOT (SUM(amount) AS total, COUNT(*) AS n FOR status IN ('open', 'done' AS closed)) AS p
WHERE p.region <> 'x';

SELECT * FROM 'orders.parquet'
UNPIVOT (amount FOR quarter IN (q1, q2 AS 'second')) AS u;
```

Each `PIVOT` value becomes a column named by what it is written as or its `AS`, with
`_` and the aggregate's alias after it when it has one, or its name when there are
several; a value other than a text or a whole number needs an `AS`. The values are
listed, so the columns are known before the query runs; a value no row holds is a
column of NULLs, or 0 under `COUNT`. A column the input lacks is refused. It runs on
one lane and reads every column of its input.

`UNPIVOT` makes a row per listed column, with its name or `AS` label and its value,
leaving out null values unless `UNPIVOT INCLUDE NULLS`. The listed columns must share
a type; cast them first when they do not.

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

**`AVG` is a `FLOAT`** whatever it averages, and so are `MEDIAN`, the variances
and the deviations — a deliberate difference from PostgreSQL, whose `avg` of an
INT or `numeric` column is a `numeric`. For PostgreSQL's exact mean, divide
the exact sum: `SUM(x) * 1.0 / COUNT(x)` over an INT column, `SUM(x) /
COUNT(x)` over a DECIMAL one (an exact decimal quotient, see
[division](../reference/expressions.md)). `SUM(x) / COUNT(x)` over an INT
column is integer division and truncates, as in PostgreSQL.

**Past `--op-memory`, a `GROUP BY` spills to disk** (see
[Spilling to disk](../tools/cli.md#spilling-to-disk)): groups it already holds keep
folding in memory, and rows of new groups go to disk by a hash of their key, to be
aggregated once the input ends. Every result is exact — a float `SUM` included,
since each group still sees its rows in input order — but the groups come out in
another order, so add `ORDER BY` if the order matters. The disk it may use is
bounded by `--spill-cap`. On parallel lanes, a `GROUP BY` whose groups pass
`--op-memory` reads its input again with each row sent to one of 16 files by its
key, and the lanes aggregate the files side by side, each with a sixteenth of
`--op-memory`: the result is the one `-j 1` gives, and the same at every `-j`. An
aggregate with no `GROUP BY` that outgrows memory runs on one lane instead.

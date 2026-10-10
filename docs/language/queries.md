# Queries

A query is a `SELECT`, with `WITH` bindings ahead of it if it needs them. On its
own it prints its rows; after `LOAD INTO … AS` it writes them ([LOAD INTO](load-into.md)).

```sql
WITH pedidos AS (                          -- CTE = named binding
  SELECT filial, num, valor, obra
  FROM erp.dbo.SC5010
    PUSHDOWN($$D_E_L_E_T_ <> '*'$$)        -- raw predicate, verbatim to the source
  WHERE valor > 0                          -- translated predicate
), obras AS (                              -- a join's right side is a CTE
  SELECT codigo_obra, nome_obra FROM erp.dbo.obras
)
SELECT p.filial, p.num, o.nome_obra
FROM pedidos p
LEFT JOIN obras o ON p.obra = o.codigo_obra
ORDER BY p.num DESC
LIMIT 100 OFFSET 20;
```

## Operators

| clause | plan stage |
|--------|-----------|
| `WHERE <expr>` | filter |
| `SELECT a, expr AS x` | projection |
| `SELECT * EXCLUDE (a, b)` / `EXCEPT` | all-but projection |
| `SELECT * RENAME (a AS b)` | rename projection |
| `COUNT(*) / SUM / AVG / MIN / MAX ... GROUP BY k` | aggregate (every other item must be a group key, aliased or not, an expression whose columns are all covered by grouping keys or grouping expressions it repeats (`lower(upper(g))` under `GROUP BY upper(g)`; see [Aggregates](aggregates.md)), or a plan-time constant; a `GROUP BY` with no aggregate in `SELECT` or `HAVING` and no window function is refused — use `SELECT DISTINCT`). A numeric aggregate refuses a non-numeric argument at plan time, and casts text per row — so a CSV column read as text still sums, and text that is not a number fails the run. `SUM` over text is exact ([Aggregates](aggregates.md)): an `INT`, or a `DECIMAL(38, s)` when the column's sampled values show `s` decimal places; `AVG` over text is a `FLOAT` |
| `ROUND(AVG(x), 2)`, `SUM(a)/COUNT(*)` | an aggregate inside an expression: the calls are computed by the aggregate, the arithmetic around them by a projection after it |
| `COUNT(DISTINCT x)` | aggregate — combines freely with other aggregates; ignores nulls |
| `MEDIAN(x)` | aggregate — a float; the mean of the two middle values on an even count; ignores nulls. A NaN orders above every number, as in `ORDER BY` and Postgres, so it is the median only when the middle falls on one. Holds every value of the group until the end, so it is the one aggregate that is not O(1) per group. Engine-side only (never pushed down) |
| `count_if(cond)` | aggregate — the rows where `cond` is true, as an `INT`; `0` for no rows, never null |
| `bool_and(cond)` / `bool_or(cond)` | aggregate — whether `cond` held for every row / for any row; nulls ignored, null when the group has no non-null value |
| `bit_and(x)` / `bit_or(x)` / `bit_xor(x)` | aggregate — the bitwise fold of an `INT` column; nulls ignored, null when there is nothing to fold |
| `var_samp(x)` / `var_pop(x)`, `stddev_samp(x)` / `stddev_pop(x)` | aggregate — the sample and population variance and standard deviation, as floats; nulls ignored. `variance` and `stddev` are the **sample** ones, as in Postgres, DuckDB, Trino and SQL Server (MySQL and StarRocks read them as population). A sample statistic of fewer than two values is null; a population one of a single value is `0` |
| `SUM(x) FILTER (WHERE cond)` | any aggregate over only the rows where `cond` holds — under `GROUP BY`, over the whole table or over a window; see [Aggregates](aggregates.md#filter) |
| `f(...) OVER (PARTITION BY .. ORDER BY .. [frame])` | window — a value per row from its partition and frame, anywhere in a `SELECT` item; see [Window functions](windows.md) |
| `HAVING <expr>` | filter after the aggregate; aggregate calls in it refer to the columns it produced, including ones the `SELECT` list never asked for |
| `ORDER BY a DESC, b` | sort — by columns, expressions (`ORDER BY x - 1`, `ORDER BY upper(s)`) or positions (`ORDER BY 2`); nulls last in both directions; `NULLS FIRST`/`NULLS LAST` are not accepted |
| `LIMIT n [OFFSET m]` | limit — `n` and `m` are constants: an integer, a `$param`, a `LET`, a loop variable, a scalar subquery or an expression of them (`LIMIT $page_size OFFSET ($page - 1) * $page_size`), evaluated once before the query runs; a column is refused, and so is a negative value; a fraction rounds to a whole number as PostgreSQL rounds it (`LIMIT 2.5` is 3, an exact number rounding half away from zero, a float to the nearest even), and text must spell a whole number. `LIMIT ALL` and `LIMIT NULL` are no limit, `OFFSET` may come first or stand alone, and the standard spelling `OFFSET m ROWS FETCH FIRST n ROWS ONLY` (`NEXT` for `FIRST`, `ROW` for `ROWS`, `n` 1 when left out) means the same; `WITH TIES` is not supported |
| `SELECT * EXCEPT (a, b)` / `EXCLUDE` | every column but those; a name not present is ignored, so one list serves tables that differ. Right after a union (`EACH TABLE OF`, `UNION ALL BY NAME`) the names are dropped *before* the branches are reconciled, so a column one table carries with an incompatible type can be excepted instead of failing the load. A name may be `IDENTIFIER(<expr>)` — a `$param` or loop variable rendered at run time, `'a, b'` excluding both and `''` nothing |
| `SELECT DISTINCT` / `DISTINCT ON (a, b)` | distinct — under a plain `DISTINCT` an `ORDER BY` column must be one the SELECT list outputs (`SELECT DISTINCT g … ORDER BY v` is refused, as in SQL); `ON` keys are input columns: they need not be in the SELECT list, and may be ones it renames (`DISTINCT ON (grp) grp AS k`). `DISTINCT ON` keeps the first row per key in `ORDER BY` order when there is one (`ORDER BY k, ts DESC` keeps the latest), else the first in input order |
| `CROSS JOIN UNNEST(SPLIT(tags, ',')) AS tag` | explode (also `UNNEST(col)`) |
| `CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag` | explode a JSON array: one row per element — strings unquoted, objects and arrays as JSON text, a JSON `null` as null. A null or `null` cell gives no rows; an object or scalar is an error |
| `[INNER\|LEFT [OUTER]\|RIGHT [OUTER]\|FULL [OUTER]\|CROSS\|SEMI\|ANTI] JOIN <source> [AS] x ON a = b [AND ...]` | join — the right side a CTE, `(SELECT ...)`, table function, path or connection table; `JOIN LATERAL f(x.col)` passes a column to a table function ([user functions](user-functions.md)); see [Joins](joins.md) |

Row order without `ORDER BY` is not defined in SQL, and `GROUP BY` returns groups
in hash-partition order. A pipeline that only filters, projects or joins keeps the
source's order at any `-j` — to the terminal, a CSV, a parquet or an Arrow file —
so the same run writes the same rows in the same order; lanes still read in
parallel, and their output is put back in order before it is written. A parquet
file's row groups follow those lanes' units (each lane encodes its own), so its
layout, not its rows, can differ from a `-j 1` run. Loading into a database table
does not order its rows (a table has none). `DISTINCT` keeps the first row per
key in input order at any `-j`. Add `ORDER BY` whenever the order is part of
the answer.

An `ORDER BY` whose input passes `--op-memory` sorts it in pieces: each piece is
written as a sorted run to the scratch directory and the runs are merged as rows
are read out. The result is the same, ties included, and the disk it takes is
bounded by `--spill-cap` ([Spilling to disk](../tools/cli.md#spilling-to-disk)).

A `GROUP BY` or `DISTINCT` whose keys outgrow `--op-memory` spills to disk the
same way. The rows are the same — `DISTINCT ON` still keeps the first row per
key — but their order without `ORDER BY` is not that of an in-memory run.

## Naming, `GROUP BY` and `ORDER BY`

A plain column keeps its own name, as written in the source (`SELECT MyCol`
is `MyCol`). A computed `SELECT` item does not need `AS`; without one its name
is basalt's choice, since SQL leaves it to the implementation: the text of its
expression with its tokens lowercased and run together, no spaces — `COUNT(*)`
becomes `count(*)`, `ClientIP - 1` becomes `clientip-1`, `CAST(x AS VARCHAR)`
becomes `cast(xasvarchar)` and `$p + 1` becomes `p+1` (a `$name` without its
`$`). A string literal keeps its quotes and its case, so `upper('cat')` and
`upper(cat)` are two names. A number keeps its digits (`1.50`). An explicit
alias always wins, and is the way to give a computed column a name you rely on.

A result may repeat a name, as SQL allows: `SELECT a, a` and two unaliased
`COUNT(*)` print two columns of the same name. `ORDER BY` or `GROUP BY` on such
a name is ambiguous and refused, unless every item of that name is the same
expression (`SELECT x AS a, x AS a ... ORDER BY a`). A CTE, derived table or
subquery may repeat a name too, and so may a CSV header: `*` over it passes both
columns through, and only a reference to the repeated name is refused, as
ambiguous (`SELECT a FROM (SELECT x AS a, y AS a FROM t) d`). A `LOAD INTO`
target, file or table, takes each column by its name, so there a repeat is
refused, naming it, whether written in the SELECT list or brought in by `*`.
Two plain columns of the same name from a join's two sides are not a repeat:
the later is named `_r` — see [Joins](joins.md).

A bare name in `GROUP BY` is an input column first, as in SQL, and a SELECT
item's alias only when no input column has that name: `SELECT upper(g) AS g,
COUNT(*) ... GROUP BY g` groups by the column `g`, and so does `SELECT g,
upper(g) AS g ... GROUP BY g`. `ORDER BY` reads a bare name the other way round,
as the output column first (`SELECT -v AS v ... ORDER BY v` sorts by `-v`). The
parser does not see a file's or table's columns, so an alias whose expression does
not read a column of its own name (`SELECT g AS v ... GROUP BY v`) is taken as the
alias even when the input has a column `v`, where SQL would group by that column —
give such an alias another name.

`GROUP BY`, `ORDER BY` and `HAVING` may repeat an expression instead of its alias:
it binds to the one column the projection already produced when it is the same
expression — same columns, same literals to the letter, spacing and keyword case
aside. A column is the same however it is written: `upper(t.g)` under
`FROM 'x.csv' t` repeats `upper(g)`, and so does `upper("g")`. One that differs is a new expression: `ORDER BY upper(s)` beside
`SELECT upper('s')` sorts by the column `s`, uppercased.

```sql
SELECT AdvEngineID, COUNT(*) FROM hits
GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;      -- binds to count(*)

SELECT DATE_TRUNC('minute', EventTime) AS m, COUNT(*) AS c FROM hits
GROUP BY DATE_TRUNC('minute', EventTime);          -- binds to m
```

- **A subquery in `FROM` is a derived table** — `FROM (SELECT ...) x` — and works in a
  join's right side too: `JOIN (SELECT ...) y ON ...`. It lowers to exactly what
  `WITH x AS (...)` produces, so it costs nothing extra to run; the alias is optional.
- **Column order is the `SELECT` list's**, not the aggregate's. `SELECT k, SUM(x), k2`
  writes `k, sum, k2`; the engine folds grouping keys before aggregates internally and
  reorders the projection back on the way out.
- `GROUP BY <n>` is positional — it names the *n*-th `SELECT` item, which may not
  be an aggregate.
- `GROUP BY <expr>` accepts a computed key (`GROUP BY ClientIP - 1`). A key the
  `SELECT` list does not name groups the rows but is not written out:
  `SELECT COUNT(*) AS c ... GROUP BY k` has the one column `c`.
- `ORDER BY <n>` is positional too — it sorts by the *n*-th `SELECT` item, so the
  list may not be `*` before it.
- `ORDER BY` may name a column the `SELECT` list does not project, or an
  expression over the columns (`ORDER BY price - discount`). It is carried
  through the projection as a hidden column and dropped after the `LIMIT`, so
  sorting by it costs nothing in the output. In a grouped query `ORDER BY` may
  also name an aggregate the `SELECT` list leaves out (`ORDER BY MAX(x)`,
  `ORDER BY SUM(x) / COUNT(*) DESC`), computed beside the others and dropped
  the same way; under `SELECT DISTINCT` or after a set operation it must repeat
  one the list computes.

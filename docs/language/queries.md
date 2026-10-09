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
| `COUNT(*) / SUM / AVG / MIN / MAX ... GROUP BY k` | aggregate (every other item must be a group key, aliased or not, or a plan-time constant; a `GROUP BY` with no aggregate and no window function is refused — use `SELECT DISTINCT`). A numeric aggregate refuses a non-numeric argument at plan time, and casts text per row — so a CSV column read as text still sums, and text that is not a number fails the run |
| `ROUND(AVG(x), 2)`, `SUM(a)/COUNT(*)` | an aggregate inside an expression: the calls are computed by the aggregate, the arithmetic around them by a projection after it |
| `COUNT(DISTINCT x)` | aggregate — combines freely with other aggregates; ignores nulls |
| `MEDIAN(x)` | aggregate — a float; the mean of the two middle values on an even count; ignores nulls. Holds every value of the group until the end, so it is the one aggregate that is not O(1) per group. Engine-side only (never pushed down) |
| `count_if(cond)` | aggregate — the rows where `cond` is true, as an `INT`; `0` for no rows, never null |
| `bool_and(cond)` / `bool_or(cond)` | aggregate — whether `cond` held for every row / for any row; nulls ignored, null when the group has no non-null value |
| `bit_and(x)` / `bit_or(x)` / `bit_xor(x)` | aggregate — the bitwise fold of an `INT` column; nulls ignored, null when there is nothing to fold |
| `var_samp(x)` / `var_pop(x)`, `stddev_samp(x)` / `stddev_pop(x)` | aggregate — the sample and population variance and standard deviation, as floats; nulls ignored. `variance` and `stddev` are the **sample** ones, as in Postgres, DuckDB, Trino and SQL Server (MySQL and StarRocks read them as population). A sample statistic of fewer than two values is null; a population one of a single value is `0` |
| `SUM(x) FILTER (WHERE cond)` | any aggregate over only the rows where `cond` holds — under `GROUP BY`, over the whole table or over a window; see [Aggregates](aggregates.md#filter) |
| `f(...) OVER (PARTITION BY .. ORDER BY .. [frame])` | window — a value per row from its partition and frame, anywhere in a `SELECT` item; see [Window functions](windows.md) |
| `HAVING <expr>` | filter after the aggregate; aggregate calls in it refer to the columns it produced, including ones the `SELECT` list never asked for |
| `ORDER BY a DESC, b` | sort — nulls last in both directions; `NULLS FIRST`/`NULLS LAST` are not accepted |
| `LIMIT n [OFFSET m]` | limit |
| `SELECT * EXCEPT (a, b)` / `EXCLUDE` | every column but those; a name not present is ignored, so one list serves tables that differ. Right after a union (`EACH TABLE OF`, `UNION ALL BY NAME`) the names are dropped *before* the branches are reconciled, so a column one table carries with an incompatible type can be excepted instead of failing the load. A name may be `IDENTIFIER(<expr>)` — a `$param` or loop variable rendered at run time, `'a, b'` excluding both and `''` nothing |
| `SELECT DISTINCT` / `DISTINCT ON (a, b)` | distinct — `ON` keys are input columns: they need not be in the SELECT list, and may be ones it renames (`DISTINCT ON (grp) grp AS k`). `DISTINCT ON` keeps the first row per key in `ORDER BY` order when there is one (`ORDER BY k, ts DESC` keeps the latest), else the first in input order |
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

A computed `SELECT` item does not need `AS`. Without one it is named after the
text of its expression, lowercased and stripped of spaces — `COUNT(*)` becomes
`count(*)`, `ClientIP - 1` becomes `clientip-1`. An explicit alias always wins.

The same naming is what lets `GROUP BY` and `ORDER BY` repeat an expression
instead of its alias: both sides render the expression the same way, so they
bind to the one column the projection already produced.

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
- `GROUP BY <n>` is positional — it names the *n*-th `SELECT` item.
- `GROUP BY <expr>` accepts a computed key (`GROUP BY ClientIP - 1`).
- `ORDER BY` may name a column the `SELECT` list does not project. It is
  carried through the projection as a hidden column and dropped after the
  `LIMIT`, so sorting by an unselected column costs nothing in the output.

# Window functions

A window function computes a value for each row from the rows around it — its
**partition** (`PARTITION BY`), in an order (`ORDER BY`), within a **frame** —
without folding them into one, as `GROUP BY` would:

```sql
SELECT conta, data, valor,
       ROW_NUMBER() OVER (PARTITION BY conta ORDER BY data DESC)          AS recencia,
       SUM(valor)   OVER (PARTITION BY conta ORDER BY data)               AS saldo,
       AVG(valor)   OVER (PARTITION BY conta ORDER BY data
                          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)       AS media_7,
       valor - LAG(valor) OVER (PARTITION BY conta ORDER BY data)         AS variacao
FROM 'movimentos.csv';
```

## Functions

| function | value |
|----------|-------|
| `ROW_NUMBER()` | 1, 2, 3, … within the partition; ties numbered in input order |
| `RANK()` | the row's rank, ties sharing it and leaving a gap (1, 2, 2, 4) |
| `DENSE_RANK()` | the same without gaps (1, 2, 2, 3) |
| `PERCENT_RANK()` | `(rank - 1) / (rows in partition - 1)`, a float from 0 to 1; 0 for a lone row |
| `CUME_DIST()` | the share of the partition ordered at or before the row (its peers included), a float in (0, 1] |
| `NTILE(n)` | which of `n` buckets of near-equal size the row falls in, 1 to `n`; the first `rows mod n` buckets hold one row more |
| `LAG(x[, n[, default]])` / `LEAD(x[, n[, default]])` | `x` from `n` rows before / after (default 1), within the partition; past its edge the `default`, else null |
| `FIRST_VALUE(x)` / `LAST_VALUE(x)` | `x` at the frame's first / last row |
| `NTH_VALUE(x, n)` | `x` at the frame's `n`-th row, null if the frame is shorter |
| every aggregate | `SUM`, `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`, `AVG`, `MIN`, `MAX`, `MEDIAN`, `count_if`, `bool_and` / `bool_or`, `bit_and` / `bit_or` / `bit_xor`, `var_samp` / `variance` / `var_pop`, `stddev_samp` / `stddev` / `stddev_pop` — over the frame, as [under `GROUP BY`](queries.md#operators) |

- The ranking and distribution functions, `NTILE`, `LAG` and `LEAD` need an
  `ORDER BY` inside `OVER (...)` — it is what they number by — and take no frame;
  writing one is an error rather than being ignored.
- `NTILE`'s bucket count and `NTH_VALUE`'s position are positive whole numbers;
  `LAG`/`LEAD`'s offset a non-negative one. Their `default` is a constant — a
  literal, a `$param` or an expression over them — cast to the column's type
  (`LAG(amount, 1, 0)`).
- `DISTINCT` is accepted inside `COUNT` only.
- The names are not reserved: a column called `rank` still reads as a column.

## The window: PARTITION BY, ORDER BY

`PARTITION BY` is optional; without it the whole input is one partition, and its
nulls group together as in `GROUP BY`. `ORDER BY` sorts within the partition, `DESC`
per key; **nulls sort last** either way (`NULLS FIRST` / `NULLS LAST` are not
accepted). Keys and arguments may be any expression — `PARTITION BY substr(conta,
1, 3)`, `ORDER BY qtd * preco`, `SUM(qtd * preco)` — computed into hidden columns
before the window runs.

A window used more than once can be named after `HAVING`:

```sql
SELECT conta, data, valor,
       SUM(valor) OVER w                                 AS saldo,
       AVG(valor) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS media_3,
       RANK()     OVER w                                 AS ordem
FROM 'movimentos.csv'
WINDOW w AS (PARTITION BY conta ORDER BY data);
```

`OVER w` takes the named window whole; `OVER (w ...)` may add an `ORDER BY` it lacks
and a frame, never a `PARTITION BY`, and cannot extend a window that has a frame.
One named window may build on another (`WINDOW a AS (PARTITION BY conta), b AS (a
ORDER BY data)`).

## Frames

The frame is the slice of the partition an aggregate or a value function reads:

```sql ignore
{ ROWS | RANGE } <start>                       -- ends at CURRENT ROW
{ ROWS | RANGE } BETWEEN <start> AND <end>
```

| bound | `ROWS` | `RANGE` |
|-------|--------|---------|
| `UNBOUNDED PRECEDING` | the partition's first row | the same |
| `<n> PRECEDING` | `n` rows before | rows whose key is at least the current key minus `n` |
| `CURRENT ROW` | this row | this row's **peers** — every row with an equal `ORDER BY` key |
| `<n> FOLLOWING` | `n` rows after | rows whose key is at most the current key plus `n` |
| `UNBOUNDED FOLLOWING` | the partition's last row | the same |

- **The default frame**: without `ORDER BY`, the whole partition (a share of the
  total); with it, `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — everything
  up to the current row *and its peers* (a running total in which ties share a
  value). So `LAST_VALUE(x) OVER (ORDER BY t)` is the last of the current row's
  peers, not of the partition; write `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
  FOLLOWING` for that.
- `ROWS` counts positions; `RANGE` compares values:

  ```sql ignore
  -- ties do NOT share: 10, 30, 50 over v = 10, 20, 20
  SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  -- ties DO share (the default): 10, 50, 50
  SUM(v) OVER (ORDER BY v)
  ```

- A frame is clipped at the partition's edges, and may be empty
  (`ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING` on the last row): an aggregate over
  no rows is null (`COUNT` is 0), a value function null.
- A start after its end is refused where it is written (`ROWS BETWEEN CURRENT ROW
  AND 1 PRECEDING`, `ROWS 2 FOLLOWING`, `ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING`),
  as are `UNBOUNDED FOLLOWING` as a start and `UNBOUNDED PRECEDING` as an end. A
  `ROWS` offset is a whole number of rows. `GROUPS` frames and frame exclusion
  (`EXCLUDE …`) are not supported.
- A `RANGE` offset (`<n> PRECEDING` / `FOLLOWING`) needs exactly one `ORDER BY` key,
  a number, date or timestamp — `check` refuses any other. Over a number the offset
  is in its units (`RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW` over a price). Over
  a date or timestamp it is a **number of days**, or an `INTERVAL '<n>' DAY | HOUR |
  MINUTE | SECOND` (also written `INTERVAL '<n> days'`); a `TIME` key takes only the
  interval. With `DESC` "preceding" means larger keys, as the order runs. A row
  whose key is null has its peers, the other nulls, as its offset frame, and a
  non-null row's offset never reaches the nulls (`UNBOUNDED FOLLOWING` does):

  ```sql
  SELECT data, valor,
         SUM(valor) OVER (ORDER BY data RANGE BETWEEN 6 PRECEDING AND CURRENT ROW)                 AS semana,
         COUNT(*)   OVER (ORDER BY data RANGE BETWEEN INTERVAL '36' HOUR PRECEDING AND CURRENT ROW) AS recentes
  FROM 'movimentos.csv';
  ```

- Without `ORDER BY` a `ROWS` frame counts in input order, as the rows arrive.

## IGNORE NULLS

`LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` and `NTH_VALUE` take `IGNORE NULLS` —
after the call, as in the standard, or inside it — to look through null values.
`RESPECT NULLS`, the default, is accepted too. Carrying the last known value
forward:

```sql
SELECT conta, data,
       LAST_VALUE(saldo) IGNORE NULLS OVER (PARTITION BY conta ORDER BY data) AS ultimo_saldo,
       LAG(saldo IGNORE NULLS)        OVER (PARTITION BY conta ORDER BY data) AS saldo_anterior
FROM 'movimentos.csv';
```

`LAG(x, n) IGNORE NULLS` is the `n`-th non-null value before the row (offset 0 the
row's own value), `NTH_VALUE(x, n) IGNORE NULLS` the frame's `n`-th non-null one.

## FILTER

An aggregate takes `FILTER (WHERE <condition>)` — over a window and under `GROUP BY`
alike — and folds only the rows where the condition holds (a null condition counts
as false):

```sql
SELECT conta, data, valor,
       SUM(valor) FILTER (WHERE valor > 0) OVER (PARTITION BY conta ORDER BY data) AS entradas_ate_aqui,
       COUNT(*)   FILTER (WHERE valor < 0) OVER (PARTITION BY conta)              AS saidas_na_conta
FROM 'movimentos.csv';
```

See [Aggregates](aggregates.md#filter) for how it runs.

## Windows inside expressions

A window call can sit anywhere in a `SELECT` item: it is computed first under a
hidden name, then the expression around it — `ROW_NUMBER() OVER (...) + 1`, `valor -
LAG(valor) OVER (...)`, `ROUND(AVG(valor) OVER (...), 2)`, `CASE WHEN RANK() OVER
(...) = 1 THEN 'top' END`. `ORDER BY` may repeat a window item of the `SELECT` list
(or name its alias). Columns a window reads — keys, arguments, columns of the
expression around it — need not be projected.

A window function cannot be nested in another, nor fed to an aggregate, and is
refused in `WHERE`, `GROUP BY`, `HAVING` and `ON`, as in standard SQL: a row's window
is known only once the rows it filters are. Filter on one through a derived table
or a CTE:

```sql
WITH ranked AS (
  SELECT conta, data, valor,
         ROW_NUMBER() OVER (PARTITION BY conta ORDER BY data DESC) AS rn
  FROM 'movimentos.csv')
SELECT conta, data, valor FROM ranked WHERE rn <= 3;
```

That shape is recognized: a lone `ROW_NUMBER` filtered with `rn <= k` (or `<`, `=`)
keeps only each partition's best `k` rows as they stream, rather than sorting them
all.

## Several windows

Functions with the same `PARTITION BY` and `ORDER BY` share one window stage, each
with its own frame. A different one is a stage of its own, chained after the first
and sorting its own input; the columns still come out in the `SELECT` list's order.
Without an outer `ORDER BY` the rows come out in the last stage's order — add one
when it matters.

## Over a GROUP BY

With `GROUP BY` the windows run after the aggregate and `HAVING`, over one row per
group, and may read the aggregates — the share of the total, a ranking of groups:

```sql
SELECT conta,
       SUM(valor)                                        AS total,
       ROUND(100.0 * SUM(valor) / SUM(SUM(valor)) OVER (), 1) AS pct,
       RANK() OVER (ORDER BY SUM(valor) DESC)            AS posicao
FROM 'movimentos.csv'
GROUP BY conta;
```

A column a window reads there must be a grouping key or inside an aggregate.

## Types

| function | type |
|----------|------|
| `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `COUNT`, `count_if` | `INT`, never null |
| `PERCENT_RANK`, `CUME_DIST` | `FLOAT`, never null |
| `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `MIN`, `MAX` | the column's type |
| `SUM` | `INT` over ints, an exact `DECIMAL` of the column's scale over decimals, else `FLOAT` |
| `AVG`, `MEDIAN`, the variances and deviations | `FLOAT` |
| `bool_and`, `bool_or` | `BOOL` |
| `bit_and`, `bit_or`, `bit_xor` | `INT` |

All but the never-null ones are nullable: an empty frame, or one of nothing but
nulls, has no value.

## How it runs

A window is a **breaker**: a row's value is not known until its whole partition has
arrived, so memory is bounded by the input, as for `ORDER BY`, `DISTINCT` and
`GROUP BY` (a window does not spill). Each stage sorts once, then resolves every
row's frame by two forward-moving edges, so a frame of any size costs the same:
an aggregate adds the rows entering and takes back the rows leaving — `MIN`/`MAX`
through a monotonic queue, the variances by Welford's update and its inverse (exact
over integers), `COUNT(DISTINCT)` by per-value counts. `MEDIAN`, `bit_and` and
`bit_or` cannot take a row back: a frame that only grows adds to them, any other is
recomputed. A float `SUM` or `AVG` over a sliding frame can differ from a fresh
total in the last bits.

`DISTINCT ON (...)` remains the cheaper way to keep one row per key — without an
`ORDER BY` it streams, where `ROW_NUMBER() ... = 1` would not.

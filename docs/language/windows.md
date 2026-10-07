# Window functions

`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG(col[, n])` / `LEAD(col[, n])`, and
`SUM(col)` / `COUNT(*|col)` / `MIN(col)` / `MAX(col)` / `AVG(col)` over `PARTITION BY` /
`ORDER BY`:

```sql
SELECT conta, data, valor,
       ROW_NUMBER() OVER (PARTITION BY conta ORDER BY data DESC) AS recencia
FROM 'movimentos.csv';
```

- `ORDER BY` inside `OVER (...)` is required for the ranking and offset functions — it
  is what the numbering is by. An aggregate does not need it: **without `ORDER BY` its
  frame is the whole partition** (share-of-total), and **with it the frame is everything
  up to and including the current row's peers** (a running total). Those are the two
  frames standard SQL defaults to, so no frame syntax is needed to reach either.
- Ties share a running total: two rows with equal `ORDER BY` values both read the total
  *including both*, which is what `RANGE` framing specifies.
- `PARTITION BY` is optional; without it the whole input is one partition.
- A window function must be the **whole** select item: `ROW_NUMBER() OVER (...) + 1` is
  not accepted, because a window is a stage rather than an expression.
- Its column is **appended** to the projection. Columns the window itself names — the
  partition keys, the order keys and the function's argument — do **not** have to be
  projected: they are carried through hidden and dropped afterwards, so
  `SELECT LAG(v) OVER (PARTITION BY k ORDER BY t) AS prev FROM 't.csv'` returns `prev`
  alone.
- Several window functions in one `SELECT` may share one window, each writing the
  same `PARTITION BY` and `ORDER BY` in its own `OVER (...)` — there is no named
  `WINDOW` clause — and each may frame it its own way (a moving sum beside a
  running total). A different `PARTITION BY` or `ORDER BY` is
  refused; write the second as a separate query or wrap the first in a derived table.
  A window function's argument, and its `PARTITION BY` and `ORDER BY` keys, are plain
  columns: compute an expression in a CTE first.
- `MIN`/`MAX` answer a value from the column and keep its type; `SUM` over a
  `DECIMAL` is an exact `DECIMAL` of the column's scale, as the `SUM` aggregate is,
  and an `INT` over ints; `AVG` is always a float;
  all of them are nullable, since a peer group of nothing but nulls has no answer.
- The names are not reserved: a column called `rank` still reads as a column.
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` and
  `ROWS BETWEEN <n> PRECEDING AND CURRENT ROW` set an explicit frame, which counts
  **rows** rather than peers. That is the difference worth knowing:

  ```sql ignore
  -- ties do NOT share: 10, 30, 50 over v = 10, 20, 20
  SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  -- ties DO share (the default): 10, 50, 50
  SUM(v) OVER (ORDER BY v)
  ```

  A bounded frame is a moving window — `AVG(v) OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING
  AND CURRENT ROW)` is a three-row moving average, clipped at the partition's start. A
  `FOLLOWING` end bound is not accepted; the frame always ends at the current row.
- A frame applies to an aggregate. `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG` and `LEAD`
  do not take one, and writing one is an error rather than being ignored.
- `LAG`/`LEAD` read a plain column and an optional literal offset (default 1). Looking
  past the edge of the row's own partition yields **null** — there is no third
  `default` argument — so the column is always nullable.

Because a window function cannot sit inside an expression, compose one by wrapping it in
a derived table. Change detection reads:

```sql
SELECT conta, valor - anterior AS delta
FROM (SELECT conta, valor,
             LAG(valor) OVER (PARTITION BY conta ORDER BY data) AS anterior
      FROM 'movimentos.csv') m
WHERE anterior IS NOT NULL;
```

A window is a **breaker**: a row's number is not known until its whole partition has
arrived, so memory is bounded by the input, as it already is for `ORDER BY`, `DISTINCT`
and `GROUP BY`. `DISTINCT ON (...)` remains the cheaper way to keep one row per key —
without an `ORDER BY` it streams, where `ROW_NUMBER() ... = 1` would not.

# Subqueries

`IN (SELECT ...)` and the scalar subquery are supported as sugar over machinery
that already existed — each is rewritten at parse time into the join or constant
it always denoted, so nothing here adds a new execution path:

```sql
-- Planned as a SEMI JOIN against the (anonymous) subquery binding.
SELECT * FROM 'facts.csv' WHERE x IN (SELECT k FROM 't.csv');

-- NOT IN plans as an ANTI JOIN with SQL's NULL rule — see below.
SELECT * FROM 'facts.csv' WHERE x NOT IN (SELECT k FROM 't.csv');

-- A scalar subquery runs ONCE, before the outer query, and its single cell is
-- spliced in as a constant — so the comparison pushes down to the source like
-- any literal. This is the incremental-extraction idiom:
SELECT * FROM conn.orders WHERE ts > (SELECT max(ts) FROM 'lake/orders.parquet');

-- The same thing, named — useful when several statements share the value:
LET hi = (SELECT max(ts) FROM 'lake/orders.parquet');
SELECT * FROM conn.orders WHERE ts > $hi;
```

The subquery of `IN` must produce exactly one named column, and the test must sit
as a **top-level AND condition of `WHERE`** — under an `OR`, a `NOT` or a `CASE`
it cannot be lifted into a join without changing what the predicate means, so
those shapes are parse errors (spell them as an explicit join). The left side
must be a plain column. The literal-list form `IN (1, 2, 3)` is unchanged.

A scalar query — inline or as `LET x = (SELECT ...);` — must produce exactly one
column; one row is the value, zero rows read as `NULL` (standard SQL), and more
than one row is an error. The inline form is evaluated per *statement*, not per
row: it may not reference columns of the outer query.

**`NOT IN` is standard three-valued `NOT IN`.** A single NULL from the subquery
makes `x NOT IN (...)` unknown for every row, so no row survives; a NULL `x`
survives only an empty subquery. To keep the rows that match no *non-null* key —
what an extraction script usually means — say so in the subquery:
`WHERE k IS NOT NULL`.

`EXISTS` has no spelling: the uncorrelated form is degenerate (all rows or none)
and the useful form is correlated. A **correlated** subquery — one referencing a
column of the outer query — has no equivalent here and is not planned: it needs
either decorrelation in an optimiser or per-row execution of the inner query, and
the second is fatal for a streaming engine. Rewrite it as a join — the equality
correlation `EXISTS` usually carries is exactly `IN` on that key.

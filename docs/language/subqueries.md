# Subqueries

`IN (SELECT ...)`, `EXISTS (...)` and the scalar subquery are supported as sugar
over machinery that already existed — each is rewritten at parse time into the
join, aggregate or constant it always denoted, so nothing here adds a new
execution path, and `EXPLAIN` shows the joins it became:

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

The subquery of `IN` must produce exactly one named column — a `UNION`,
`INTERSECT` or `EXCEPT` of such queries is named by its first — and the test must sit
as a **top-level AND condition of `WHERE`** — under an `OR`, a `NOT` or a `CASE`
it cannot be lifted into a join without changing what the predicate means, so
those shapes are parse errors (spell them as an explicit join). The left side
must be a plain column. The literal-list form `IN (1, 2, 3)` is unchanged.

A scalar query — inline or as `LET x = (SELECT ...);` — must produce exactly one
column; one row is the value, zero rows read as `NULL` (standard SQL), and more
than one row is an error.

**`NOT IN` is standard three-valued `NOT IN`.** A single NULL from the subquery
makes `x NOT IN (...)` unknown for every row, so no row survives; a NULL `x`
survives only an empty subquery. To keep the rows that match no *non-null* key —
what an extraction script usually means — say so in the subquery:
`WHERE k IS NOT NULL`.

## EXISTS

`EXISTS (subquery)` is true when the subquery returns a row; what it selects does
not matter. Without a reference to the outer query it is a constant, evaluated
once like a scalar subquery, and may stand anywhere a scalar subquery may:

```sql
SELECT * FROM 'facts.csv'
WHERE EXISTS (SELECT 1 FROM 't.csv' WHERE k > 100);
```

## Correlated subqueries

A subquery in the `SELECT` list or `WHERE` may reference columns of the query
directly around it, by that query's table name or alias. A name without a table
is always the subquery's own column, so an outer column is always written
qualified. The correlation is lifted out of the subquery's `WHERE` and the
subquery becomes one join — never a query per row:

```sql
-- Customers with an order: a SEMI JOIN on the correlation key.
SELECT c.id, c.name FROM 'customers.csv' c
WHERE EXISTS (SELECT 1 FROM 'orders.csv' o WHERE o.cust = c.id);

-- Customers without one: an ANTI JOIN.
SELECT c.id FROM 'customers.csv' c
WHERE NOT EXISTS (SELECT 1 FROM 'orders.csv' o WHERE o.cust = c.id AND o.day > c.since);

-- Per-row aggregates: a LEFT JOIN to the subquery grouped by the key.
SELECT c.id,
       (SELECT count(*) FROM 'orders.csv' o WHERE o.cust = c.id) AS orders,
       (SELECT max(o.amount) FROM 'orders.csv' o WHERE o.cust = c.id AND o.region = c.region) AS top
FROM 'customers.csv' c;

-- IN with a correlation: a SEMI JOIN on (tier, region) = (cust, region).
SELECT c.id FROM 'customers.csv' c
WHERE c.tier IN (SELECT o.cust FROM 'orders.csv' o WHERE o.region = c.region);
```

What each form becomes:

| Form | Plan | Rows |
|---|---|---|
| `EXISTS (...)` / `NOT EXISTS (...)` | semi / anti join on the correlation keys | an outer row whose key is NULL matches nothing: `EXISTS` false, `NOT EXISTS` true |
| `x IN (SELECT y ...)` | semi join on the keys and `x = y` | as `IN` per outer row |
| `x NOT IN (SELECT y ...)` | anti join on the keys, testing `x = y OR x IS NULL OR y IS NULL` | three-valued per outer row: a NULL `y` in that row's set, or a NULL `x` against a non-empty set, drops the row |
| `(SELECT agg(...) ...)` | LEFT JOIN to the aggregate grouped by the keys | a row that matches nothing gets the aggregate over no rows: `COUNT` 0, `coalesce(sum(v), 0)` 0, the others NULL |
| `(SELECT v ...)` | LEFT JOIN to the rows grouped by the keys | NULL for no row; more than one row for an outer row fails the run with *more than one row returned by a subquery used as an expression* |

A correlation is a condition of the subquery's `WHERE` (or of an inner join's `ON`
inside it). An equality between a column of the subquery and one of the outer
query — `o.cust = c.id`, either way round, as many as you like — is a join key.
Any other condition (`o.day > c.since`) is allowed in `EXISTS`, `NOT EXISTS`, `IN`
and `NOT IN`, where it decides which key matches count; a subquery correlated
only by such conditions runs as a nested-loop join. A scalar subquery needs at
least one equality; besides it, a condition on the outer row alone
(`... WHERE o.cust = c.id AND c.active`) is allowed and, where it fails, gives the
subquery's value over no rows. The two sides of an equality must compare: an
`int` against a `string` is refused, as in an `ON`.

A correlated `EXISTS`, `IN` or `NOT IN` sits as a top-level AND condition of
`WHERE`, like an uncorrelated `IN`. A correlated scalar subquery may stand
anywhere in a `SELECT` item or a `WHERE` expression — under `OR`, in a `CASE`,
inside an aggregate's argument (`sum((SELECT ...))`).

Refused, each with a message naming the reason:

- a correlated scalar subquery with a condition between the two queries that is
  not an equality, or with no equality at all — join the two queries and
  aggregate with `GROUP BY` instead;
- an outer column in the subquery's `SELECT` list, `HAVING`, `ORDER BY` or a join of
  its own, or reached from a derived table or CTE inside it — and from a subquery
  nested two levels deep, which may reference only the query directly around it;
- a correlated subquery in `ON`, `GROUP BY`, `HAVING`, `ORDER BY` or `LIMIT` of the
  outer query, and a derived table in `FROM` or `JOIN` that references the tables
  beside it (there is no `LATERAL` subquery; `JOIN LATERAL` takes a
  [table function](user-functions.md));
- a correlated scalar subquery in the `SELECT` list of a grouped query, outside an
  aggregate — it is evaluated per input row;
- `LIMIT`, `OFFSET`, `GROUP BY`, a window or a set operation inside a correlated
  subquery (except `LIMIT n` in `EXISTS`, which changes nothing): each would apply
  per outer row, which one join cannot. Pick a single row with `max` or `min`;
- a correlated `EXISTS` over an aggregate (an ungrouped aggregate always returns a
  row), and a correlated `IN` over one (compare with a scalar subquery);
- a correlated `EXISTS` outside a top-level AND of `WHERE` (an uncorrelated one may
  stand anywhere).

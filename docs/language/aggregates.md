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

**A float `SUM` is reproducible for a given `-j`, not across values of it.** The
lanes each total their own slice and the slices are added in a fixed order, so
rerunning the same command writes the same bytes; but `-j` decides how the input
is cut, and float addition is not associative, so `-j 4` and `-j 8` can differ in
the last bits (as can either from `-j 1`). `SUM` over an `INT` or a `DECIMAL` is
exact and identical everywhere — `SUM(CAST(amount AS DECIMAL(18,2)))` is the way
to total money you intend to compare or checksum. `COUNT`, `MIN` and `MAX` are
exact too, as are `count_if`, `bool_and`/`bool_or` and the `bit_*` aggregates.
The variances and deviations combine their lanes the same fixed way, so they
share `SUM`'s float caveat: `ROUND` them before comparing runs at different `-j`.

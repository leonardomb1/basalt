# UNION, INTERSECT and EXCEPT

`UNION ALL` is SQL's: branches line up **by position**, under the first branch's
column names, and must have the same number of columns. Types widen per column
(an int meeting a float is a float); a pair with no common type is an error.
`UNION` without `ALL` also removes duplicate rows — from everything to its left, so
`a UNION b UNION ALL c` deduplicates `a ∪ b` and then appends `c`.

```sql
SELECT id, amount FROM 'eu.csv'
UNION ALL
SELECT id, total FROM 'us.csv'      -- `total` lands under `amount`
ORDER BY id;
```

`INTERSECT` keeps the rows found in both branches, `EXCEPT` those of the first
found nowhere in the second; both line columns up by position and remove
duplicates. Two NULLs count as the same value there, unlike in a join's `ON`.
`INTERSECT` binds tighter than `UNION` and `EXCEPT` (`a UNION b INTERSECT c` is
`a UNION (b INTERSECT c)`). `INTERSECT ALL` and `EXCEPT ALL` are not supported.

`BY NAME` lines branches up by column name instead, and is what reconciling N
similar tables needs. It applies to `UNION` only, and a chain is one or the other.

A `BY NAME` branch may be **any query** — a file, a filter, a projection, an
aggregate — not only `SELECT ['tag' AS c,] t.* FROM <conn>.<table>`. That shape
is the reconciliation case the feature was built for (N similar tables aligned
by name) and it still gets the `tag` column and table discovery; a general
branch is built like any other pipeline and reconciled the same way.

Alignment is **by column name**: NULL-fill missing, drop extra, and widen types
where one holds the other (an int meeting a float is a float), as DuckDB's
`UNION ALL BY NAME` does. Unlike DuckDB, types with no common one (a date against
a string) are an error rather than cast to text: `CAST` one side, or `EXCEPT` the
column. `UNION BY NAME` also deduplicates.

```sql
-- explicit branches: the tag is just a literal column
SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
UNION ALL BY NAME
SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
ANCHOR SCHEMA erp.dbo.CT2010;          -- schema authority (optional)
```

```sql
-- discovered: one branch per row of a raw 2-column query (table, tag)
SELECT *
FROM EACH TABLE OF (erp.QUERY($$SELECT name, SUBSTRING(name,4,2) FROM sys.tables WHERE name LIKE 'CT2%'$$))
  AS (table_name, CT2_EMPRESA)         -- 2nd name = output tag column
  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)      -- raw predicate on EVERY branch
  ANCHOR SCHEMA erp.dbo.CT2010;
```

The discovery source may also be a full basalt `SELECT`, executed in-engine at
plan time (its translatable `WHERE` prefix still descends to the source as
usual). The connection the *discovered tables* live in is inferred from the
query's leading read when it names a connection; `IN <conn>` overrides it:

```sql
SELECT *
FROM EACH TABLE OF (SELECT name, substr(name, 4, 2) FROM erp.sys.tables WHERE name LIKE 'CT2%')
  AS (table_name, CT2_EMPRESA)
  ANCHOR SCHEMA erp.dbo.CT2010;
```

JSON form (array of `{table, tag}` objects, e.g. from a request body):
`FROM EACH TABLE OF ($job.tables) IN erp AS (table_name, tag)` — element keys
remappable via `WITH (table_field = ..., tag_field = ..., tag_substr = '4,2')`.

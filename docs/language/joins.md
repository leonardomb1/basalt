# Joins

Joins are hash equi-joins: the right side is materialized and indexed once, the
left side streams through. The right side is a CTE, a `(SELECT ...)`, a table
function, or any source a `FROM` reads — a path (`JOIN 'smb://fs/x.xlsx' x`, its
`WITH (...)` after the alias) or a connection's table (`JOIN sr.db.t AS t`),
read as `(SELECT * FROM it)` would be. A key is an `=` between a value of each
side, `AND`-combined for composite keys, written in either order; a null key
never matches. A key may be computed — `trim(t.code) = CAST(x.code AS string)`,
each value naming one side's columns — and each side then computes it before the
join, out of sight of `SELECT *`. Columns need no table when the names say which
side they are: `trim(code) = CAST(cr AS varchar)` finds `code` and `cr` in the two
sides' columns, in either order, and is refused as ambiguous only when both
values' columns exist on both sides. Keys of two types (a spreadsheet's number
against a table's text) do not join until one is cast. The rest of an `ON` is a
condition: one naming only the right side, or no column (`1 = 1`), narrows the
right side before the join — right for an outer join too, and pushed down to a
SQL source as that side's `WHERE` — so `JOIN sr.t AS t ON t.D_E_L_E_T_ <> '*'
AND t.k = x.k` reads only live rows. Any other (the left side alone, the two
sides compared otherwise than by `=`) filters the joined rows, which only an
inner join means; another kind says so. `CROSS JOIN <cte>` takes no `ON`.
Right-side columns that collide with a left name come back suffixed `_r`, and
`_r2`, `_r3`, … if that name is taken too — that is the name `SELECT *` shows. A
qualified reference needs no suffix: with `FROM t a JOIN r b`, `b.amt` is the
right side's `amt` everywhere in the query (`SELECT`, `WHERE`, `GROUP BY`,
`ORDER BY`, a later join's `ON`), and `SELECT b.amt` calls its output `amt`
unless `a.amt` is already in the list. A pipeline shaped `read | filters | join
| filters | write` probes in parallel under `-j` — over local CSV/Parquet
morsels, and over key-range splits for a splittable SQL source. A chain of
joins followed by `GROUP BY` fans out the same way (`read | filters |
join+ | filters | aggregate | sort/limit | write`). Right and full joins stay
serial in every case: they have to emit the build rows nothing matched, and each
lane would emit those from its own copy of the match tracking. The build side is
fully resident; past 4 GiB the run fails fast instead of eating the host — raise
the ceiling per join with `WITH (max_build = '16GB')` on the join clause, filter
the CTE, or flip the join.

## Key pushdown

When one side of a join is a SQL table, the other side's key values narrow its
read, so joining a small sheet to a large table reads only the table's matching
rows:

```sql
CREATE CONNECTION sr TYPE starrocks OPTIONS (host = 'fe', database = 'erp');
SELECT s.cr, c.name
FROM 'centers.xlsx' s
JOIN sr.erp.cost_centers c ON trim(c.code) = CAST(s.cr AS varchar);
-- the table is read as … WHERE TRIM(`code`) IN ('1005', '1048')
```

A SQL right side takes the left side's keys: the left side is read first, into
memory, and replayed into the join. Past 100,000 rows or 64 MB the read-ahead
stops and the right side is read in full, so a large left side costs nothing
extra. A local file of up to 64 MB joined to a SQL table runs serially even
under `-j`, so the table can take its keys; a bigger one keeps its lanes. Otherwise a SQL left read right before the first join takes the right
side's keys once that side is indexed, under `-j` too, where every split reads
with them. A side takes keys only where the join never outputs its unmatched
rows — the right side of an inner, left, semi or anti join, the left side of an
inner, right or semi one — and not under `NOT IN`. A side with anything but
filters and selects between its read and the join, or a key that is no
function of the read's columns, takes nothing.

Up to 1,000 distinct values go as `IN (…)`; past that a number, decimal, date or
timestamp key goes as its `>= min AND <= max` range, and a text key as nothing,
since the database's collation may order text otherwise. A value the dialect
cannot spell exactly drops that key's predicate, never just the value: the
read may return more rows than match, which the join discards, never fewer. An
empty side reads nothing (`WHERE 1 = 0`). `EXPLAIN` names the side that takes
the keys; `WITH (key_pushdown = false)` on the join turns it off.

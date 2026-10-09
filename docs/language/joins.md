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

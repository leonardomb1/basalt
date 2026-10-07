# Pushdown

A read from a SQL source asks the source to do as much of the query as it can
answer exactly — the filter, the columns, a whole aggregate, a top-N — so less
crosses the wire. The answer never changes, only where the work happens;
`EXPLAIN` shows what descended on the scan's `pushdown:` line.

- **`PUSHDOWN(<expr>)`** — a raw predicate sent verbatim into the generated
  source query's `WHERE`. The argument is a string expression: a `$$...$$`
  literal (`PUSHDOWN($$D_E_L_E_T_ <> '*'$$)`), a loop-var value
  (`PUSHDOWN($where)`), or one built with `||`. ANDed with whatever the
  translated `WHERE` pushes down. Empty ⇒ no clause. Syntax errors surface at
  the source at runtime (permanent, exit 1).
- **Projection pushdown** — a table read asks the source only for the columns
  the pipeline provably needs (`SELECT a, b FROM erp.t` is sent as
  `SELECT [a], [b] FROM t`), so a narrow read of a 300-column table moves only
  the columns it reads. `SELECT * EXCEPT (...)` — after a table read or a union of
  table reads — first asks the source for the table's shape and no rows, then
  names every column but the excepted ones, so a column nobody wants never
  leaves the server. A plain
  `SELECT *` still fetches everything; a split-parallel read adds its key column
  to the list.
- **Implicit pushdown** — the contiguous `WHERE` (filter) prefix directly after
  a SQL table/query read is translated into that source query's `WHERE`
  automatically, across a join too: a filter naming only columns the probe
  side already had is moved below the join first, and a filter on a join *key* also
  gains a twin on the other side's key — so `FROM fact JOIN dim ON fact.k = dim.k
  WHERE dim.k = '…'` prunes the fact table at the source. Both moves are refused
  where they would change the answer: never below a `RIGHT`/`FULL` join (there the
  probe side is the one that gets null-extended), never when a referenced column
  could have come from the right side, and the key twin only under an inner join.
  `EXPLAIN` shows where the filter ended up. Translatable: comparisons,
  `AND`/`OR`/`NOT`, `IS [NOT] NULL`/`EMPTY`, `IN`, `LIKE`, `CASE`/`IF`, `CAST` (but
  not one converting text to a number — the sources disagree about what
  `CAST('abc' AS INT)` means, NULL in StarRocks and MySQL against an error in
  Postgres and here, so descending it would make the answer depend on whether it
  descended; put the expression in a raw `QUERY(...)` to ask for the source's own
  coercion), and the portable string
  functions (`lower upper trim substr replace concat coalesce starts_with
  ends_with contains`). A `$param`, LET or loop variable descends
  as its value — `WHERE D2_EMISSAO >= $since` sends `>= '20240105'`, with a
  query LET's value decided first. Untranslatable pieces (arithmetic,
  `now()`/`today()`, user funcs) stay in the engine — the filter is always
  kept, so results never change, only how much crosses the wire. `EXPLAIN`
  prints the descended predicate on a `pushdown:` line. A CTE, derived table or
  table function the query *starts from* counts as that read, and so does a chain
  of them, each reading the one before: every `WHERE` along it descends as if
  written inline (`(via binding b, a)` in `EXPLAIN`), except in a binding
  holding a window function, which stays apart so a `WHERE rn = 1` over it can run
  as a top-N. A filter written after a `SELECT` list — the query's own `WHERE`
  over a CTE, derived table or table function — moves in front of it when every
  column it names is one that list passes through or renames, rewritten in the
  source's names: `SELECT num FROM paid_orders($d) WHERE num > 5`, with the body
  selecting `C5_NUM AS num`, sends `"C5_NUM" > 5`. A conjunct naming a computed
  column (`x * 2 AS y`) stays above the list, the others still move. Rows a
  moved filter removes are no longer evaluated by the list, so a computed column
  that would have failed on one of them no longer fails the query. A CTE,
  derived table or table function read as a `JOIN`'s right side is readied the
  same way — its own WHERE descends into its read and only the columns it uses
  are asked for — so `JOIN itens($filial) i` reads that branch's rows, not the
  table. A filter written *after* an inner join that names only the right side's
  columns, each by its alias (`WHERE i.valor > 0`), joins that read too — and
  descends with it. After a `LEFT`, `RIGHT` or `FULL` join it stays above the
  join, since there it also decides the unmatched rows; so does a condition
  naming both sides, or a bare column name, which could be either side's.
- **Text comparisons follow the column's collation**, so the source never keeps
  fewer rows than basalt would. basalt compares text byte by byte; a source
  compares by collation — case-insensitive by default on SQL Server and MySQL,
  and SQL Server ignores trailing spaces (`'ab   ' = 'ab'`) under every
  collation, binary ones too. Sent as written, `code >= 'B'` lost `'a…'` rows on
  a case-insensitive server, and `D2_DOC > '000123'` lost the padded
  `'000123   '` on a Protheus `char(n)`. So, asking the source's catalog once
  per run and table, and only when a text comparison is in play:
  - `=`, `IN`, `LIKE` and the prefix/suffix/contains tests descend under any
    collation — folding and padding only let the source match more, and the
    engine re-applies the filter.
  - `<`, `<=`, `>`, `>=` on text descend only where the column compares bytes (a
    `_BIN2` collation on SQL Server, or `_BIN` on a `char`/`varchar`; `_bin`
    on MySQL; on Postgres `C`, `POSIX`, the builtin provider, or any libc
    collation on a musl build — as the server reports it, never an ICU one;
    StarRocks always) and the literal is printable ASCII; where the collation
    also pads, `>` is sent as `>= 'x' OR col LIKE 'x%'` and `<` as `<=`,
    which keeps every row basalt keeps. Elsewhere they stay in the engine.
  - `<>`, `NOT (… = …)` and `IS NOT EMPTY` on text negate an equality the
    collation widens, so they descend only where it compares bytes — on
    SQL Server with an exact form, `NOT (col = 'x' AND DATALENGTH(col) = 1)`.
  - `length()` and `strpos()` never descend (MySQL's `LENGTH` counts bytes, SQL
    Server's `LEN` drops trailing spaces), nor a `LIKE` pattern holding `_` (a
    byte in a SQL Server `varchar` under a UTF-8 collation); SQL Server's `[` is
    escaped, and on SQL Server a literal outside printable ASCII stays in the
    engine.

  `EXPLAIN` does not connect, so it says `text comparisons decided by the
  collation at run time` where the catalog will decide.
- **Whole-aggregate pushdown** — `read <sql> | filters | GROUP BY` descends as
  one grouped query when every filter translates, the group keys are bare
  columns, and the aggregates are `COUNT[(DISTINCT)] SUM MIN MAX` with types
  the engine can pin via explicit casts (`AVG`, summed floats/decimals, and
  collation-dependent string extremes deliberately stay engine-side — the
  result must be bit-identical, not merely close). `HAVING`/sort/limit still
  run in the engine on the tiny grouped result.
  Nothing re-applies the `WHERE` over a grouped result, so each filter must
  keep *exactly* basalt's rows: a text comparison descends only where the
  column's collation compares bytes — on a Protheus binary collation as
  `D2_FILIAL = '01' AND DATALENGTH(D2_FILIAL) = 2`, since SQL Server would also
  count `'01 '`. The top-N descent below holds its filters to the same rule.
  When an aggregate over a SQL source does *not* descend, every matching row
  is streamed to the engine to be grouped — the run log says so in a `warn`
  line that names the rule that refused it (`the WHERE predicate does not
  translate exactly to mysql SQL (…)`, ``group key `x` is renamed by the
  aggregate``, …).
- **`LIMIT` and top-N pushdown** — `read <sql> | filters | [SELECT] | [ORDER BY]
  | LIMIT n [OFFSET m]` asks the source for `n + m` rows: `LIMIT` on
  postgres/mysql/StarRocks, `TOP` on sqlserver. Without `ORDER BY` any `n + m`
  rows are an answer, so it always descends. With one, the source orders them
  first — nulls last in both directions, as the engine does (`NULLS LAST`;
  a leading `k IS NULL` key on mysql/StarRocks; a `CASE` key on sqlserver) —
  and the engine still sorts, offsets and cuts what arrives, so the final order
  is its own. It descends only when the answer cannot change:
  - every `WHERE` before the limit translates (the rules above) — the source counts
    rows after its own filter, so one left here would thin the capped set;
  - each `ORDER BY` key is a source column, as-is or renamed by the `SELECT`
    (`SELECT id AS k … ORDER BY k`), not a computed one;
  - each key is a number, date, time or timestamp. A string key stays
    engine-side: its order is the source's collation — case-folded by default
    on mysql and sqlserver, space-padded for `char(n)` — where the engine
    compares bytes, the same reason string `MIN`/`MAX` do not descend.

  Rows tied on every key at the cut are interchangeable either way: which of
  them the engine keeps already depends on the order rows arrive in, which a
  SQL source never promises. A `DISTINCT`, a join or an aggregate before the
  limit is not this shape (the aggregate has its own descent above). The
  capped read runs as one statement, so it is not split into key ranges under
  `-j`. A top-N that does not descend streams every matching row here to be
  sorted, and the run log says so in a `warn` line naming the rule. `EXPLAIN`
  shows it on the scan's `pushdown:` line — `order by id desc limit 1000 (if
  the keys are numeric or temporal)`, since analysis does not connect to learn
  the key's type — and `physical: serial (top-N pushed, sorts at most 1000
  rows)` in place of `materializes`.

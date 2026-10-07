# FOR EACH and the CASE statement

Plan-time fan-out — one pipeline (or dispatch) per row of a discovery source.
A catalog of tables, each read and loaded under a per-row name:

```sql
FOR EACH ROW OF ($tables) AS (name, where)
  PARALLEL ON ERROR CONTINUE           -- or SEQUENTIAL / ON ERROR STOP
  LOAD INTO sr.IDENTIFIER('fluig_' || lower($name))
    USING stream_load UPSERT AS        -- bare UPSERT: PK inferred from source
  SELECT *, now() AS extraction_timestamp
  FROM fluig.dbo.IDENTIFIER($name)     -- a per-row TABLE read
  PUSHDOWN($where);                    -- raw predicate value ("" ⇒ no WHERE)
END FOR;
```

- Sources: a raw discovery query (`conn.QUERY($$...$$)`, first N columns → N
  loop vars positionally), an in-engine `SELECT` query (any basalt query, run
  once at plan time; first N columns → N loop vars positionally), or a JSON
  param path (`$tables`, `$job.tables`, …; object fields bound to the loop
  vars by name, a missing field ⇒ `""`).
- The body holds queries (`LOAD INTO` / `SELECT`, each with its own `WITH`),
  nested `FOR EACH ROW OF`, the `CASE` statement, `CALL`, `PRINT`, `EXPLAIN`
  and `THROW`. Declarations — `PARAM`, `LET`, `CREATE CONNECTION`, `CREATE
  FUNCTION` — belong at the top level; `check` and `run` refuse one in a body
  alike. A top-level `CASE` arm may declare a connection, so a script can pick
  its endpoint per environment; a `PARAM`, `LET` or function there is refused.
- Loops nest. The inner loop's discovery source is rendered with the outer
  row (`FOR EACH ROW OF (erp.QUERY($$SELECT ... WHERE t = '${name}'$$))`), and
  the inner body sees both rows' variables, the innermost winning a shared
  name. Each `PARALLEL` loop fans out over its own rows.
- A `WITH` inside a body is rendered per row like the query that reads it,
  and is scoped to the body: after the loop the name means whatever it meant
  before. At the top level a `WITH` is visible from its statement onwards, so
  two statements may reuse a CTE name and each reads its own.
- Loop variables may be typed: `AS (name, port:INT)`.
- A loop variable is also an ordinary expression **value**: `SELECT $name AS
  empresa`, `WHERE $port > 1000` (typed vars compare as their declared type).
  Only `$name` is the loop variable: a bare `name` in the body is still the
  source column of that name, as it is beside a PARAM.
- The `CASE` **statement** (`... THEN <statements> ... END CASE`) dispatches
  whole pipelines per row — subject form (`CASE $env WHEN 'prod', 'staging'
  THEN ... END CASE`) and the guard form. `END CASE` distinguishes it from the
  CASE **expression** ([Expressions](../reference/expressions.md)). Use it when the branches are *different pipelines*
  (different sources/sinks); for choosing a *value*, put the conditional in the
  expression (`IDENTIFIER(if($pk = '', $name || 'id', $pk))`).

## Dynamic names — `$var`, `IDENTIFIER()`, `||`

Loop variables (and params) are referenced with `$` — `$name`, `$where` —
resolved by name per row. A *name* is computed from them by an ordinary string
expression, and **`IDENTIFIER(<string-expr>)`** turns that string into a table
or object reference (the precedent is Snowflake / Databricks `IDENTIFIER`).
`||` is string concat; `lower()`, `if()`, `concat()` compose as usual.

| you want | write |
|---|---|
| a per-row source table | `FROM conn.schema.IDENTIFIER($name)` |
| a per-row file path | `FROM IDENTIFIER('dir/' \|\| $name \|\| '.csv')` (the extension must be literal) |
| a computed sink name | `LOAD INTO conn.IDENTIFIER('pre_' \|\| lower($name))` |
| a per-row sink file | `LOAD INTO IDENTIFIER('dir/' \|\| $name \|\| '.csv')` (extension literal, as above — it picks the writer) |
| a raw predicate value | `PUSHDOWN($where)` |
| a conditional key | `UPSERT ON (IDENTIFIER(if($pk = '', $name \|\| 'id', $pk)))` |
| a per-row column *name* | `SELECT IDENTIFIER($col), COUNT(*) ... GROUP BY IDENTIFIER($col) ORDER BY IDENTIFIER($col)` |
| a per-row column value | `SELECT $name AS empresa` — a plain expression, no quoting |

In an expression or a name position (`SELECT` list, `WHERE`, `GROUP BY`,
`ORDER BY`, `DISTINCT ON`) `IDENTIFIER(<string-expr>)` is a **column** whose
name is computed per row — or once, from a `PARAM`, outside any loop. The
column is only known when the row renders it, so `check` validates the stages
before the first dynamic name and leaves the rest to `run`, which reports an
unknown column per row (``for-each row c=nope: unknown field `nope```).

`IDENTIFIER($name)` resolves to a **table** read, so bare `UPSERT` still infers
the PK from source metadata — a raw `QUERY(...)` read cannot. This is why the
catalog holds only `{name, where}`, never a PK.

## Raw `${...}` interpolation (raw SQL bodies only)

Inside a raw `QUERY($$...$$)` or `PUSHDOWN($$...$$)` literal, `${var}` /
`${ <expr> }` still splices loop values into the SQL text (C#-style: nested
string literals in the hole need no escaping) — `QUERY($$SELECT ${cols} FROM
${name}$$)`. Prefer `$var` + `IDENTIFIER()` everywhere a *name* is meant;
reach for `${...}` only when you are literally building a raw SQL string.

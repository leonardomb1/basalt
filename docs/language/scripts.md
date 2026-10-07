# Scripts

A script is a sequence of `;`-terminated statements:

```sql ignore
@include 'lib.sql';               -- top of file only; C-style, relative to this file
CREATE ENDPOINT '/x' DOC '...';   -- only for HTTP mode; absent = batch
PARAM ...;                        -- request/CLI inputs
LET name = <expr>;                -- sealed plan-time constant
THROW 'msg' WHEN <condition>;     -- fail the plan on the script's own invariant
CREATE CONNECTION ...;            -- named data endpoints
CREATE FUNCTION f(a) AS <expr>;   -- scalar functions (inlined at plan time)
CREATE FUNCTION p(a) AS ... END;  -- statement functions, invoked with CALL
CREATE FUNCTION t(a) RETURNS TABLE AS SELECT ...;  -- table functions, read with FROM t('x')
LOAD INTO ... AS <query>;         -- output pipeline(s)
<query>;                          -- terminal SELECT = print to stdout
CALL p('x');                      -- run a statement function
FOR EACH ROW OF (...) ... END FOR;
CASE ... END CASE;                -- plan-time dispatch
PRINT <expr>;                     -- progress line on stderr, via the run log
```

`@include 'file.sql';` splices another script's declarations ahead of this one
at plan time. It stands on a line of its own — nothing may follow it there but a
`--` comment. Each included file is parsed separately (errors report the
included file's own path and line), includes may nest (depth 16, cycles
rejected), and paths resolve relative to the including file. A file is spliced
in **once**, at its first include, like C's `#pragma once`: when `outliers.sql`
and `dispersion.sql` both include `stats.sql` and a script includes both,
`stats.sql` is in the program one time — its `CREATE FUNCTION`s are not defined
twice — and each library still sees what it declares. That holds for every
statement in it, so an included file's `LOAD INTO` runs once however many paths
reach it.

`THROW <message> [WHEN <condition>];` asserts what the engine cannot infer.
Both operands are ordinary expressions over `$params` and `$lets` ([Expressions](../reference/expressions.md)), so they
are decided at plan time: an absent or true condition aborts the script before a
row is read, with `message` as the error text verbatim; a false condition is a
no-op. `basalt check` rejects a script whose guard fires, so a bad invocation is
caught without connecting to anything. A fired guard is permanent, never
transient — exit `1`, never `75` ([exit codes](../tools/cli.md#exit-codes)), so a scheduler will not retry it.

```sql ignore
THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
THROW 'since must be an ISO date' WHEN $since <> '' AND length($since) < 10;
THROW 'unreachable branch';       -- unconditional, e.g. in a CASE arm
```

- **Batch is the silent default.** A script with no `CREATE ENDPOINT` runs once
  to completion ([exit codes](../tools/cli.md#exit-codes)).
- Keywords are case-insensitive; identifiers keep their case.
- Comments: `--` to end of line, `/* ... */` blocks.
- Strings are `'...'` (double `''` for a literal quote), and only `'...'`.
- **Quoted names** are `"..."` (ANSI): `"Exchange rate"` is the column of that
  name, not the text. It is the only way to name a column containing a space or
  spelling a keyword — `SELECT "select", "Valor Total" FROM ...` — and a quoted
  name is never read as a keyword. Double `""` for a literal quote inside one.
  A misspelled quoted name fails as `unknown field`, at plan time.
- **Raw SQL literals** use Postgres dollar-quoting: `$$...$$`, or
  `$tag$...$tag$` when the body contains `$$`. No escaping inside; `${...}`
  interpolation of loop vars still applies within them ([FOR EACH](for-each.md)).
- **Dynamic names** (per-row table/sink names, keys) use `$var` +
  `IDENTIFIER()` + `||`, not raw string interpolation — see [dynamic names](for-each.md#dynamic-names--var-identifier-).

**`PRINT <expr>;`** emits one progress line where it stands — the way a long
`FOR EACH` or `CALL` says what it is doing. The argument is an ordinary
expression (literals, `||`, `$params`, `$lets`, and inside a `FOR EACH` or
statement-function body the loop variables, bound per row); non-strings render
as they would in a sink. It writes to **stderr through the run log**, never
stdout — stdout is the data contract (`--format json` NDJSON rows or the summary
object), and a progress line there would corrupt it. A `PRINT` is shown whatever
`--log-level` says, since the script asked for it; only `-q` silences it. Under
`--log-format json` it is an NDJSON line with `"level":"print"` and the text as
its `msg`. `PRINT` is not an output pipeline — a script still needs a `LOAD INTO`
or a terminal query.

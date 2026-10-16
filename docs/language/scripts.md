# Scripts

A script is a sequence of `;`-terminated statements:

```sql ignore
@include 'lib.sql';               -- top of file only; C-style, relative to this file
@include 'lib/stats.sql' AS st;   -- a library: its functions as st.f(...)
@include 'github.com/org/repo/sql/kit.sql@v1.2.0' AS kit;  -- remote, pinned in basalt.sum
PARAM ...;                        -- CLI inputs
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

`@include 'lib/stats.sql' AS st;` includes a **library**: it may only declare
functions — scalar, statement and table — and any other statement in it (a
`LOAD INTO`, `SELECT`, `PARAM`, `LET`, connection or `CALL`) is an error naming
it, so including a library never runs anything. Its functions are namespaced
under the alias: `st.median(x)`, `CALL st.refresh(...)`, `FROM st.orders($since)`;
inside the library they call each other by their plain names, and outside it
the plain names do not exist, so two libraries may each define `clean()`. An
alias is private to the file that writes it, as a Go import is: a library's own
`@include 'util.sql' AS u` makes `u.f(...)` callable there and nowhere else, so
two libraries may each use `u` for different files, and a file it includes
without `AS` shares its namespace, as if written in it. One file may not give
two includes the same alias, nor use a connection's name as one. Without `AS`, a file is spliced whole, as above,
statements and all: the choice between running a file's statements and only
taking its functions is written at each include.

A path written as a repository — `github.com/owner/repo/path/file.sql@ref`,
`gitlab.com/group/repo/path/file.sql@ref`, or `host/group/repo.git/path/file.sql@ref`
for a self-hosted GitLab — is a **remote include**, fetched over HTTPS (no `git`
needed; `GITHUB_TOKEN` or `GITLAB_TOKEN` in the environment reads a private
repository). The `@ref` (a tag, branch or commit) is required. Each remote file
is pinned by its SHA-256 in `basalt.sum` beside the root script, as Go's `go.sum`
pins a module, and is refused until pinned: `basalt check --pin script.sql`
fetches what is not pinned and records it. A file whose content no longer
matches its pin — a moved tag or branch, a host serving something else — is
refused, never run. Pinned files are kept in a cache (`$BASALT_CACHE`, else
`$XDG_CACHE_HOME/basalt`, else `~/.cache/basalt`) and read from it with no
network, so a scheduled run does not depend on the host. A remote file's own
includes resolve inside the same repository and ref and cannot climb out of it
or name a local path. A remote file may be included with or without `AS`, with
the same meaning as a local one: commit `basalt.sum` with the script, so what
runs is what was reviewed.

`THROW <message> [WHEN <condition>];` asserts what the engine cannot infer.
Both operands are ordinary expressions over `$params` and `$lets` ([Expressions](../reference/expressions.md)), so they
are decided at plan time: an absent or true condition aborts the script before a
row is read, with `message` as the error text verbatim; a false or null
condition is a no-op, and one that is not a boolean is an error. That holds
wherever the `THROW` stands among the top-level statements. One that reads a
query `LET`, or sits in a `FOR EACH`, `CASE` or statement-function body, is
decided when reached instead, so the statements before it have run. `basalt check` rejects a script whose guard fires, so a bad invocation is
caught without connecting to anything. A fired guard is permanent, never
transient — exit `1`, never `75` ([exit codes](../tools/cli.md#exit-codes)), so a scheduler will not retry it.

```sql ignore
THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
THROW 'since must be an ISO date' WHEN $since <> '' AND length($since) < 10;
THROW 'unreachable branch';       -- unconditional, e.g. in a CASE arm
```

- A script runs once to completion ([exit codes](../tools/cli.md#exit-codes)).
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

# Scripts

A script is a sequence of `;`-terminated statements:

```sql ignore
@include 'lib.sql';               -- top of file only; runs it, its EXPORTs by name
@include 'lib/stats.sql' AS st;   -- a library: its EXPORTs as st.f(...), st.pg, $st.x
@include 'github.com/org/repo/sql/kit.sql@v1.2.0' AS kit;  -- remote, pinned in basalt.sum
PARAM ...;                        -- CLI inputs (EXPORT PARAM: shared with includers)
LET name = <expr>;                -- a variable, evaluated when it runs
THROW 'msg' WHEN <condition>;     -- fail the plan on the script's own invariant
CREATE CONNECTION ...;            -- named data endpoints
CREATE FUNCTION f(a) AS <expr>;   -- scalar functions (inlined at plan time)
CREATE FUNCTION p(a) AS BEGIN ... END;  -- statement functions, invoked with CALL
CREATE FUNCTION t(a) RETURNS TABLE AS SELECT ...;  -- table functions, read with FROM t('x')
LOAD INTO ... AS <query>;         -- output pipeline(s)
<query>;                          -- terminal SELECT = print to stdout
CALL p('x');                      -- run a statement function
CALL delta_optimize('lake/t/');   -- a built-in procedure (Delta maintenance)
CALL execute_on('pg', $$...$$);   -- SQL run on a database as written (setup)
FOR EACH ROW OF (...) ... END FOR;
CASE ... END CASE;                -- plan-time dispatch
PRINT <expr>;                     -- progress line on stderr, via the run log
```

`@include 'file.sql';` brings another script into this one at plan time. It
stands on a line of its own — nothing may follow it there but a `--` comment.
Each included file is parsed separately (errors report the included file's own
path and line), includes may nest (depth 16, cycles rejected), and paths resolve
relative to the including file, by the path it was included as — a symlinked
folder is not followed to its target, and the working directory plays no part:
`mods/m/fns.sql` including `'lib/sys.sql'` reads `mods/m/lib/sys.sql`.

**Every file is a module.** The functions, connections, PARAMs and LETs a file
declares are its own. Another file sees one only when it is declared `EXPORT` —
`EXPORT FUNCTION`, `EXPORT CONNECTION`, `EXPORT PARAM`, `EXPORT LET`, and
`EXPORT OR REPLACE ...` — and only when it includes that file itself: an export
does not pass through a file in between. When `main.sql` includes `a.sql`, which
includes `b.sql`, `main.sql` sees what `a.sql` exports and nothing of `b.sql`'s;
to use `b.sql`'s, it includes `b.sql` too. Naming what an included file declares
without `EXPORT`, or what reaches the file only through another, is an error
saying which file to change. So two files may each keep a private `pg` or
`clean()` without meeting, and a helper inside a library is never called, or
replaced, by accident from outside it. Messages, logs and `EXPLAIN` show every
name as its file wrote it, and a connection's default credentials (`PG_USER`,
`PG_PASS`) keep its declared name.

```sql ignore
-- lib/warehouse.sql
CREATE CONNECTION staging TYPE postgres OPTIONS (host = 'stg');   -- this file's own
EXPORT CONNECTION dw TYPE postgres OPTIONS (host = 'dw');
EXPORT FUNCTION cents(x) AS CAST(round(x * 100) AS BIGINT);

-- main.sql
@include 'lib/warehouse.sql';
LOAD INTO dw.public.sales AS SELECT id, cents(amount) AS amount FROM 'sales.csv';
```

What a file included without `AS` exports is used by its plain name. A name the
including file declares too, or that two such includes both export, is an error
at the `@include`, as which one a reference meant could not be told: include one
of them `AS` an alias. Such a file is also **run**: its statements — a `LOAD
INTO`, a `SELECT`, a `CALL` — are part of the program, ahead of the includer's.
A file is in the program **once**, at its first include, like C's `#pragma
once`: when `outliers.sql` and `dispersion.sql` both include `stats.sql` and a
script includes both, `stats.sql`'s functions are declared one time, its `LOAD
INTO` runs once however many paths reach it, and each includer still sees what
`stats.sql` exports.

`@include 'lib/stats.sql' AS st;` includes a **library**: it may only declare —
functions (scalar, statement and table), connections, PARAMs and LETs of a value
— and a statement that would run (a `LOAD INTO`, `SELECT`, `CALL`, `FOR EACH`, a
`LET` of a query) is an error naming it, so including a library never runs
anything. One that exports nothing is an error at its `@include`. Its exports
are used through the alias: `st.median(x)`, `CALL st.refresh(...)`, `FROM
st.orders($since)`, `st.dw.public.t`, `$st.region`, `${st.median(x)}` in a
string, `'smb://st.share/dir/f.csv'`; naming one it keeps private
(`st.helper(...)`) is an error listing what it exports. An alias is private to
the file that writes it, as a Go import is: a library's own `@include 'util.sql'
AS u` makes `u.f(...)` usable there and nowhere else, so two libraries may each
use `u` for different files. One file may not give two includes the same alias,
nor use a connection's name as one. The choice between running a file's
statements and only taking its declarations is written at each include.

A run binds the PARAMs the **root script** sees, as a Terraform root module's
variables: its own, those exported by the files it includes without `AS`, by
their names, and those its libraries export, as `alias.name` (`-p st.region=eu`).
Any other PARAM — private to its file, or exported only to a file in between —
takes its `DEFAULT`, and one without a `DEFAULT` is an error at check, as nothing
could bind it. `-p` for a PARAM the script does not expose is refused, naming the
file that declares it.

A path written as a repository — `github.com/owner/repo/path/file.sql@ref`,
`gitlab.com/group/repo/path/file.sql@ref`, or `host/group/repo.git/path/file.sql@ref`
for a self-hosted GitLab — is a **remote include**, fetched over HTTPS (no `git`
needed; `GITHUB_TOKEN` or `GITLAB_TOKEN` in the environment reads a private
repository; the GitLab token goes only to gitlab.com, or to a self-hosted GitLab
named in `GITLAB_HOST`). The `@ref` (a tag, branch or commit) is required. Each remote file
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

## Running SQL on a database

`CALL execute_on('<connection>', $$ ... $$)` sends SQL to a database connection
(postgres, mysql, sqlserver, starrocks or doris) as written, for the setup a
load needs: a `CREATE TABLE` with the types and keys the target wants, an index,
a `GRANT`, a `TRUNCATE`, a procedure. Several statements may go at once.
PostgreSQL runs them in one implicit transaction, so a failing one undoes those
before it. SQL Server runs them as one batch. MySQL runs them one by one, each
committed as it runs, and stops at the first that fails. A server that takes one
statement at a time (some StarRocks and Doris versions) refuses several with its
own message, so send one per call there.

A failing statement fails the run with the server's message, and the SQL is
never sent again. The SQL runs on a connection of its own, closed after, so a
`SET` or a temporary table it makes is gone for the reads and loads that follow.
The run log says how long it took and, on PostgreSQL and MySQL, how many rows
its statements changed, shown as a `PRINT` is. `check` and `EXPLAIN` send
nothing; `EXPLAIN ANALYZE` names what it would run.

The SQL is text: `${...}` holes are filled as in any string (loop variables,
`PARAM`s, `LET`s), and both arguments may be computed:
`CALL execute_on($target, 'TRUNCATE ' || $table)`. A `FUNCTION execute_on` that
the script declares is called instead.

```sql ignore
CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'db', user = 'etl', password = '${PG_PASS}', database = 'dw');
CALL execute_on('pg', $$
  CREATE TABLE IF NOT EXISTS sales (id bigint PRIMARY KEY, region text, amount numeric(12,2));
  CREATE INDEX IF NOT EXISTS sales_region ON sales (region);
$$);
LOAD INTO pg.public.sales UPSERT AS SELECT * FROM 'sales.parquet';
```


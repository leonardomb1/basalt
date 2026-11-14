# Parameters and LET

```sql
PARAM dias   INT DEFAULT 7;              -- -p dias=3
PARAM desde  TIMESTAMP;                  -- no default = required
PARAM job    JSON;                       -- -p job='{"tables":[...]}', a document
```

- Reference with `$`: `$dias`, `$desde`. JSON documents navigate by dotted
  path — `$job.tables`, `$job.source.host` — resolved to literals at plan time.
  A `JSON` param takes its document from `-p job='{"a":1}'` (or a kernel's
  `params`), else a string `DEFAULT`; a
  path to a key the document lacks is an error naming the key. `check` with no
  value bound reads every path as `null`.
- Safe navigation: `$job.filtro?.uf` — a missing intermediate resolves the
  whole path to `null` instead of erroring.
- Types: `BOOL INT FLOAT STRING BYTES DATE TIME TIMESTAMP DECIMAL(p,s) JSON`
  (common synonyms accepted: `INTEGER BIGINT DOUBLE TEXT VARCHAR(n) DATETIME
  NUMERIC ...`). A value bound from text — `-p`, a kernel's `params`, a
  default — is read as a `CAST` reads it and keeps the declared type:
  `-p d=2026-02-01` is a `DATE` (`date_add('day', 1, $d)` works), a date alone
  bound to a `TIMESTAMP` is its midnight, `TIME` takes `HH:MM[:SS[.ffffff]]`,
  and a `DECIMAL(10,2)` rounds `12.345` to `12.35`. Text that is not one fails
  the run, and `check`, naming the param and the value. `DECIMAL` alone is
  `DECIMAL(38,0)`; `DECIMAL(p)` without a scale is refused. `-p` must name a
  `PARAM` the script declares, once: a misspelt name fails `run` and `check`
  rather than running on the default, and a name given twice is a malformed
  command line. A kernel's `params` may carry names a script does not
  declare; those are ignored.

**`LET name = <expr>;`** declares a variable, as JavaScript's `let` does: its
expression is evaluated once, when the statement runs (in declaration order; it
may reference `$params` and earlier `$lets`), and `$name` reads the value it then
holds, never the expression again. A value from outside is a PARAM's to take: a
LET cannot be bound externally, and `-p name=...` is an error. A LET and a PARAM
may not share a name. `LET run_ts = now();` gives one consistent timestamp
across every pipeline of a run.

A LET may read an environment variable with `env('NAME')` (or `secret('NAME')`,
the same, as in a connection's options), which keeps a password out of the
script while a value is computed from it once before the run:

```sql
LET token = sha256(env('API_PASS') || strftime(now(), '%d/%m/%Y'));
SELECT * FROM HTTP('https://api.example.com/v1/items')
  WITH (header = 'X-Token: ${token}');
```

The variable is read when the script runs, never by `basalt check`, which types
the LET without it; one that is not set fails the run before any statement. A
query cannot call `env()` itself: read it into a LET and use `$name`.

`LET name = (SELECT ...);` binds the single cell of a query instead, run when
the statement is reached (so it may read anything the script can) — see
[Subqueries](subqueries.md). An expression LET cannot reference a query
LET: expression LETs fold before any query runs.

# Parameters and LET

```sql
PARAM dias   INT DEFAULT 7;              -- batch: -p dias=3 | http: query string
PARAM desde  TIMESTAMP;                  -- no default = required
PARAM job    JSON FROM BODY;             -- whole JSON body as a document
PARAM tenant STRING FROM HEADER('X-Tenant');
```

- Reference with `$`: `$dias`, `$desde`. JSON documents navigate by dotted
  path — `$job.tables`, `$job.source.host` — resolved to literals at plan time.
  A `JSON` param takes its document from `-p job='{"a":1}'` (or a kernel's
  `params`), else the request body in HTTP mode, else a string `DEFAULT`; a
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
  `DECIMAL(38,0)`; `DECIMAL(p)` without a scale is refused.
- Source defaults: scalars bind from the query string, `JSON` from the body.
  `FROM QUERY`, `FROM BODY` and `FROM HEADER('X-Name')` say so explicitly; a
  bare `FROM HEADER` reads the header named like the param.

**`LET name = <expr>;`** is PARAM's sealed sibling: a script-scoped constant
folded once at plan time (in declaration order; it may reference `$params` and
earlier `$lets`) and referenced as `$name`. It can never be bound externally —
`-p name=...` is an error, HTTP binding ignores it, and it is not part of an
endpoint's parameter surface. A LET and a PARAM may not share a name. `LET
run_ts = now();` gives one consistent timestamp across every pipeline of a run.

`LET name = (SELECT ...);` binds the single cell of a query instead, run when
the statement is reached (so it may read anything the script can) — see
[Subqueries](subqueries.md). An expression LET cannot reference a query
LET: expression LETs fold before any query runs.

# HTTP APIs

**HTTP connections** hold what every read of an API shares. After `OPTIONS`,
an `http` connection takes the REST source clauses ([Sources](../language/sources.md)) — `PAGINATE`, `RETRY`,
`WITH (...)` — as defaults each read starts from; a read's own clauses win:

```sql
CREATE CONNECTION gh TYPE http OPTIONS (base_url = 'https://api.github.com',
    auth = 'bearer', token = env('GH_TOKEN'))
  RETRY 3
  WITH (header = 'Accept: application/vnd.github+json');
```

`auth` is `bearer` (`token`), `basic` (`user`/`password`, which default to the
`NAME_USER`/`NAME_PASS` convention), `header` (`header_name`/`header_value`,
for API keys), `login_json` (`login_url`, the body's fields as `body_<name>`,
and `token_path`, `token_header`, `token_prefix` for where the token goes) or
`oauth2` (`login_url` — `token_url` is accepted too — `client_id`,
`client_secret`, `scope`). With no `auth`, no credentials are read at all.

A read names its endpoint in place, `conn.GET(path, name = value, ...)` or
`conn.POST(path, body = ...)`, each `name = value` a query parameter. To name an
endpoint once and read it like a table, make it a table function
([user functions](../language/user-functions.md)):

```sql ignore
CREATE FUNCTION repos(org) RETURNS TABLE AS
  SELECT * FROM gh.GET('/orgs/' || $org || '/repos', type = 'public')
  PAGINATE BY page (param = 'page', size = 100);

SELECT name, stargazers_count FROM repos('ziglang') WHERE NOT archived;
```

An http connection has no tables: `SHOW TABLES FROM gh` and `FROM gh.repos` are
errors that show the function form.

## Calling a function per row

A table function whose body reads an http connection is called with each row's
values through `JOIN LATERAL`, as an embeddings model, a geocoder or an enrichment
API is:

```sql ignore
CREATE CONNECTION ai TYPE http OPTIONS (base_url = 'https://api.openai.com',
    auth = 'bearer', token = env('OPENAI_API_KEY'));

CREATE FUNCTION embed(input) RETURNS TABLE AS
  SELECT CAST(embedding AS LIST(DOUBLE)) AS embedding
  FROM ai.POST('/v1/embeddings')
  WITH (body_type = 'json', body = '{"model": "text-embedding-3-small", "input": ${input}}',
        items = 'data', batch = 128, concurrency = 4);

LOAD INTO pg.public.doc_chunks UPSERT ON (doc_id, no) AS
SELECT d.doc_id, c.no, c.text, sha256(c.text) AS text_hash, e.embedding
FROM docs d
CROSS JOIN UNNEST(text_chunks(d.body, 1000, 200)) AS c
CROSS JOIN LATERAL embed(c.text) e
WHERE sha256(c.text) NOT IN (SELECT text_hash FROM pg.public.doc_chunks);
```

- Such a function is called per row when a call passes a row's value
  (`embed(c.text)`, any expression of the row) or its request holds a `${param}`
  hole; a call of constants (`FROM embed('hello')`) is then one request. Its body
  is the one read and a `SELECT` of its columns.
- `${input}` in a JSON body (`body_type = 'json'`) is the argument as JSON: a
  string quoted, a number bare, NULL as `null`. In the path or a form body it is
  the argument's text percent-encoded, in a query parameter its text (encoded as the
  URL is built), in a header its text, a line break in it failing the run;
  `'/geo/' || $addr` in the path is the same hole. A script's `PARAM` or loop
  variable fills its own `${name}` in the request as in any read, and never a
  parameter's hole of the same name. The rest of the request is sent as any read of the connection sends it:
  its auth, `RETRY` (a 429 or 5xx is retried, `Retry-After` honoured) and `items`,
  the path to the reply's items.
- Each column of the body's `SELECT` is a `CAST(... AS type)`, so the plan types it
  without calling the API; a field the reply lacks reads as NULL. The alias is the
  item, a struct: `e.embedding`.
- With `batch = n` up to n rows go in one request, `${input}` a JSON array of their
  values in input order, and the reply holds exactly one item per row, the k-th
  answering the k-th; a reply with another count fails the run. A parameter then
  goes only in the body. Without `batch` each row is its own request and takes
  every item its reply holds: none drops the row, which `LEFT JOIN LATERAL ... ON
  TRUE` keeps with NULLs instead.
- `concurrency` (default 4, up to 64) is how many requests are in flight; rows are
  cut into requests in input order and answers joined back in that order, so the
  rows out and which rows share a request do not depend on `-j` or timing. A
  statement that calls one runs on one lane: every call is paid for, and a
  parallel plan may sample or reread its input. Rows are read ahead only to fill
  the requests in flight, so a `LIMIT` may still have called for up to `batch` x
  `concurrency` rows more than it keeps. A CTE that calls one and is read more
  than once runs once, into memory, so no row is sent twice. `batch` and
  `concurrency` on a read that sends one request are an error.
- A `WHERE` condition that does not read the alias, an `IN` or `NOT IN` subquery
  among them, applies before the call (one reading an UNNEST's alias right after
  that UNNEST), so a rerun skips what its target already holds and pays only for
  new rows. Nothing is cached between runs.
- `check` and `EXPLAIN` call nothing. A request that still fails after its retries
  fails the run, naming the function and the request's row count.

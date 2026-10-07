# HTTP APIs

**HTTP connections** hold what every read of an API shares. After `OPTIONS`,
an `http` connection takes the REST source clauses ([Sources](../language/sources.md)) — `PAGINATE`, `RETRY`,
`WITH (...)` — as defaults each read starts from; a read's own clauses win:

```sql
CREATE CONNECTION gh TYPE http OPTIONS (base_url = 'https://api.github.com',
    auth = 'bearer', token = env('GH_TOKEN'))
  RETRY 3 ON (429, 503)
  WITH (header = 'Accept: application/vnd.github+json');
```

`auth` is `bearer` (`token`), `basic` (`user`/`password`, which default to the
`NAME_USER`/`NAME_PASS` convention), `header` (`header_name`/`header_value`,
for API keys), `login_json` (`login_url`, the body's fields as `body_<name>`,
and `token_path`, `token_header`, `token_prefix` for where the token goes) or
`oauth2` (`login_url` — `token_url` is accepted too — `client_id`,
`client_secret`, `scope`). With no `auth`, no credentials are read at all.

`CREATE RESOURCE conn.name AS GET(...) | POST(...) [PAGINATE ...] [RETRY ...]
[WITH (...)];` names one endpoint, so it reads like a table:

```sql
CREATE RESOURCE gh.repos AS GET('/orgs/ziglang/repos', type = 'public')
  PAGINATE BY page (param = 'page', size = 100);

SELECT name, stargazers_count FROM gh.repos WHERE NOT archived;
```

`SHOW TABLES FROM gh` lists a connection's resources (`resource`, `method`,
`path`) and `DESCRIBE gh.repos` fetches it to print its columns. Reading
`gh.name` that no `CREATE RESOURCE` declared is a plan-time error that says so.

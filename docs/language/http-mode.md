# HTTP mode

```sql
CREATE ENDPOINT '/eventos' DOC 'Recebe telemetria';

LOAD INTO sr.bronze.eventos USING stream_load AS
SELECT device_id, CAST(ts AS TIMESTAMP) AS ts, tipo, now() AS recebido_em
FROM BODY (
  device_id STRING NOT NULL,
  ts        STRING,
  tipo      STRING,
  payload   JSON
)
WHERE tipo IN ('leitura', 'alarme');
```

- `basalt serve <dir>` hosts every `*.sql` endpoint script in the folder,
  routed by the declared path, on port 8080 unless `--port` (`-p` here) says
  otherwise, listening on every interface unless `--host 127.0.0.1` (or another
  address) narrows it; `DOC` feeds the startup banner. SIGHUP, or `--watch` on a change,
  reloads the scripts. `GET /healthz` and `/readyz` answer `200` for a load
  balancer. `basalt run script.sql --port N` serves a single script the same
  way; without `--port`, `run` refuses an endpoint script instead of guessing
  a port. A port already in use is an error, not a second listener.
- A request body is capped at 64 MiB (**413** past it) and the request head
  at 64 KiB (**431**); a request that is not valid HTTP gets **400**. A body
  that sends nothing for 30 seconds gets **408**; a request with neither
  `Content-Length` nor chunked encoding has an empty body.
- Each connection gets its own thread, up to 64; past that a new connection
  is answered **503** + `Retry-After` at once, health probes included, so the
  client retries rather than queueing. Bodies are read in parallel, but
  pipeline runs go one at a time: a long run makes the next endpoint request
  (buffered accepts included) wait for it once its body is in, but never
  delays `/healthz` or another client's upload. For more throughput, run
  replicas behind the load balancer.
- A reload loads the new scripts on the side and swaps them in between runs:
  a run in flight finishes on its old script, later requests see the new
  ones, and the server keeps accepting connections throughout.
- **`FROM BODY (schema)`** declares the request contract. The body (JSON array
  or single object) is validated row by row: a missing/null `NOT NULL` column
  or an unreadable value rejects the request with a message naming the row —
  served as **422**. A value is read as `CAST` reads it: a `DATE`, `TIME` or
  `TIMESTAMP` from its ISO text, a `DECIMAL` from text or a JSON number, a
  `BOOL` from `true`/`false` (any case) or a number, `BYTES` from a string's
  bytes. Extra keys are dropped. `JSON` columns ride as text.
- **`FROM HEADER('X-Tenant')`** on a `PARAM` binds it from that request header
  (case-insensitive); bare `FROM HEADER` matches the param's own name. A param
  binds only from its declared source: `?tenant=` in the URL cannot stand in
  for, or override, a header or body param.
- Status contract: success → `200` + summary JSON; per-item failures → `207`;
  permanent error → `422`; transient → `503` + `Retry-After`.

## Durable buffer (WAL)

`ACCEPT ... INTO BUFFER` turns the endpoint into a queue: **200 means
"accepted durably"** (fsynced), and the load happens asynchronously.

```sql
CREATE ENDPOINT '/eventos'
  DOC 'Recebe telemetria; ack após persistir em disco'
  ACCEPT BODY (
    device_id STRING NOT NULL,
    ts        STRING,
    payload   JSON
  )
  INTO BUFFER 'eventos'
    AT '/var/lib/basalt/wal'
    SEGMENT 16 MB
    RETAIN UNTIL LOADED;          -- or: RETAIN 24 HOURS (allows reprocessing)

LOAD INTO sr.bronze.eventos USING stream_load AS
SELECT device_id, CAST(ts AS TIMESTAMP) AS ts, payload, now() AS recebido_em
FROM BUFFER 'eventos'
  FLUSH EVERY 5 SECONDS OR 50000 ROWS;
```

- Requests are validated against the `ACCEPT BODY` schema (422 naming the
  row), appended to append-only JSONL segments, and acked after one fsync
  (group commit: N rows, one sync).
- A flusher thread drains completed segments through the pipeline, one run
  per segment. The StarRocks label is derived from the segment name
  (`eventos-000042`), so a crash between "loaded" and "marked" replays the
  same label and the sink dedups — effectively exactly-once, no 2PC.
- Backpressure: buffer disk usage over the limit — `MAX 2 GB` in the clause,
  1 GiB by default — ⇒ `503 + Retry-After`; the client is the queue. Sizes are
  written in `KB`, `MB` or `GB`.
- Without `AT`, segments go to `wal/` under the working directory.
- **Batch replay**: `FROM BUFFER 'eventos' AT '<dir>'` in a plain batch script
  reads every retained segment — the queue is just another source.
- One owner per buffer: `serve` holds a lock on `eventos.lock` in the WAL
  directory while the buffer is open, so a second `basalt serve` (or a second
  script) declaring the same buffer in the same directory is refused at
  startup, naming it, instead of loading its segments twice. Run one process
  per WAL directory; replicas each get their own.
- A reload keeps a buffer the new scripts still declare (same name and
  directory) open and hands it to them: its flusher finishes the segment in
  flight, the new script takes over between runs, and accepts keep landing in
  the same segment throughout, so every acked row is loaded once. A buffer
  the new scripts drop has its pending rows loaded, then is closed.
- Stopping: the first SIGTERM or SIGINT is a graceful stop. The server takes
  no new connections, requests already in hand finish (an ack still means the
  rows are synced to the WAL), and the flusher finishes and marks the segment
  it is loading, then starts no other. A second signal is a hard stop: the
  load in flight is aborted, unmarked.
- Rows still pending at a stop stay in the WAL and are loaded once the next
  start's flusher runs, picking up from the manifest; a stop does not drain
  the whole queue, as a long drain only widens the window for a kill mid-load.
  A hard stop, a kill or a crash between loading a segment and marking it
  loads that segment again; only a label-deduplicating sink such as StarRocks
  absorbs that replay, so a sink without one (a CSV `APPEND`, say) is
  at-least-once there.
- Honest cost: `serve` becomes stateful (the WAL directory needs a persistent
  volume) and durability is the node's disk, not replicated.

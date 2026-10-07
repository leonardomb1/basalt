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
  way.
- **`FROM BODY (schema)`** declares the request contract. The body (JSON array
  or single object) is validated row by row: a missing/null `NOT NULL` column
  or an unreadable value rejects the request with a message naming the row —
  served as **422**. Extra keys are dropped. `JSON` columns ride as text.
- **`FROM HEADER('X-Tenant')`** on a `PARAM` binds it from that request header
  (case-insensitive); bare `FROM HEADER` matches the param's own name.
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
- Honest cost: `serve` becomes stateful (the WAL directory needs a persistent
  volume) and durability is the node's disk, not replicated.

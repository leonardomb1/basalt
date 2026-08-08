#!/usr/bin/env bash
# Integration suite: seed CSV -> write to each backend (auto-created table or
# blob) -> read back -> compare against it/expected.csv. Needs docker compose.
#
#   ./it/run.sh                 all suites
#   ./it/run.sh azure           only that suite (boots only the containers it needs)
#   ./it/run.sh mysql postgres  several
#   KEEP=1 ./it/run.sh azure    leave the stack up afterwards
#
# Suite names: mysql postgres sqlserver starrocks azure
# Scripts are Basalt SQL (the BSL parser was removed in v0.2.0); connection
# attrs are passed as `OPTIONS(...)` bodies.
set -euo pipefail
cd "$(dirname "$0")/.."

ALL_SUITES="mysql postgres sqlserver starrocks azure parquet"
SUITES="${*:-$ALL_SUITES}"

for s in $SUITES; do
  case " $ALL_SUITES " in
    *" $s "*) ;;
    *) echo "unknown suite '$s'; known: $ALL_SUITES" >&2; exit 2 ;;
  esac
done

# Only start what the selected suites actually need — StarRocks alone takes
# minutes, so booting it to test one CSV path makes the suite unusable to iterate on.
services=""
for s in $SUITES; do
  case $s in
    mysql)     services="$services mysql" ;;
    postgres)  services="$services postgres" ;;
    sqlserver) services="$services mssql" ;;
    starrocks) services="$services starrocks" ;;
    azure)     services="$services azurite" ;;
    parquet)   services="$services static static-norange" ;;  # local fixtures, plus HTTP
  esac
done

zig build
B=./zig-out/bin/basalt
COMPOSE="docker compose -f it/compose.yaml"

echo "==> suites: $SUITES"
echo "==> starting:$services"
# shellcheck disable=SC2086
$COMPOSE up -d --wait $services
trap '[ "${KEEP:-}" ] || '"$COMPOSE"' down -v' EXIT

out=$(mktemp -d)
pass=0
fail=0

# Runs basalt with its output captured. The run summary prints even under -q, so
# letting it through would bury the PASS/FAIL lines; on failure the captured
# output is shown, which is when it is actually wanted.
brun() {
  if $B "$@" -q >"$out/last.log" 2>&1; then return 0; fi
  echo "--- basalt output ---"
  tail -20 "$out/last.log"
  return 1
}

runs() { # is suite $1 selected?
  case " $SUITES " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

report() { # $1 name, $2 ok|bad
  if [ "$2" = ok ]; then
    echo "PASS $1"; pass=$((pass + 1))
  else
    echo "FAIL $1"; fail=$((fail + 1))
  fi
}

check() { # $1 name, $2 actual csv, $3 expected csv
  if diff -u "$3" "$2" >"$out/$1.diff" 2>&1; then
    report "$1" ok
  else
    report "$1" bad
    head -20 "$out/$1.diff"
  fi
}

sqlrt() { # $1 connector, $2 OPTIONS(...) body -- round trip via replace + table read
  local decl="CREATE CONNECTION db TYPE $1 OPTIONS ($2);"
  if brun run -c "$decl
LOAD INTO db.basalt_it REPLACE AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "$decl
LOAD INTO '$out/$1.csv' AS SELECT * FROM db.basalt_it ORDER BY id;"; then
    check "$1" "$out/$1.csv" it/expected.csv
  else
    report "$1 (run error)" bad
  fi
}

volrt() { # $1 connector, $2 OPTIONS(...) body
  local decl="CREATE CONNECTION db TYPE $1 OPTIONS ($2);"
  if brun run -c "$decl
LOAD INTO db.basalt_vol REPLACE AS SELECT * FROM '$volcsv';" &&
     brun run -c "$decl
LOAD INTO '$out/vol_$1.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM db.basalt_vol;"; then
    check "$1-volume" "$out/vol_$1.csv" "$out/vol_expected.csv"
  else
    report "$1-volume (run error)" bad
  fi
}

MYSQL_OPTS="host = '127.0.0.1', port = 33306, user = 'root', password = 'it', database = 'it'"
PG_OPTS="host = '127.0.0.1', port = 35432, user = 'postgres', password = 'it', database = 'it'"
MSSQL_OPTS="host = '127.0.0.1', port = 31433, user = 'sa', password = 'It_Passw0rd1', database = 'master', tls = 'insecure'"

runs mysql     && sqlrt mysql     "$MYSQL_OPTS"
runs postgres  && sqlrt postgres  "$PG_OPTS"
runs sqlserver && sqlrt sqlserver "$MSSQL_OPTS"

# Split-parallel probe over a shared join index: the key-range lanes must produce
# exactly what the serial driver does. The split is forced by hint — basalt_it is
# three rows, far under the auto-split threshold. Lanes interleave, so both sides
# are sorted before the compare.
pgjoin() { # $1 threads, $2 output csv
  brun run -j "$1" -c "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);
LOAD INTO '$2' AS
WITH labels AS (SELECT id, name FROM 'it/seed.csv')
SELECT t.id, l.name FROM db.basalt_it t WITH (split = id, splits = 4) JOIN labels l ON t.id = l.id;"
}

if runs postgres; then
  if pgjoin 4 "$out/pg_join_j4.csv" && pgjoin 1 "$out/pg_join_j1.csv"; then
    sort "$out/pg_join_j4.csv" >"$out/pg_join_j4.sorted"
    sort "$out/pg_join_j1.csv" >"$out/pg_join_j1.sorted"
    check postgres-split-join "$out/pg_join_j4.sorted" "$out/pg_join_j1.sorted"
  else
    report "postgres-split-join (run error)" bad
  fi
fi

# Decimal aggregates against the source's own answer. A bare postgres `numeric`
# has no typmod, so the column is typed decimal(38,6) while each value arrives at
# its own dscale — summing raw unscaled integers scaled the total by 10^4, and
# hashing (unscaled, scale) counted `1.5` and `1.50` as two distinct values.
# psql computes the expected row, so this stays honest if the engine changes.
if runs postgres; then
  pgq() { docker compose -f it/compose.yaml exec -T postgres psql -U postgres -d it -t -A -F, "$@"; }
  pgq -c "DROP TABLE IF EXISTS it_dec;
          CREATE TABLE it_dec (k int, n numeric, m numeric(12,2));
          INSERT INTO it_dec VALUES (1,1.5,1.50),(1,1.50,1.50),(1,0.001,0.10),(2,2.25,2.25);" >/dev/null 2>&1
  { echo "s,d,mn,mx,fixed";
    pgq -c "SELECT CAST(SUM(n) AS numeric(20,3)), COUNT(DISTINCT n),
                   CAST(MIN(n) AS numeric(20,3)), CAST(MAX(n) AS numeric(20,3)),
                   CAST(SUM(m) AS numeric(20,2)) FROM it_dec;"; } \
    | sed 's/[[:space:]]*$//' >"$out/pg_dec_expected.csv"
  if brun run -c "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);
LOAD INTO '$out/pg_dec.csv' AS
SELECT CAST(SUM(n) AS DECIMAL(20,3)) AS s, COUNT(DISTINCT n) AS d,
       CAST(MIN(n) AS DECIMAL(20,3)) AS mn, CAST(MAX(n) AS DECIMAL(20,3)) AS mx,
       CAST(SUM(m) AS DECIMAL(20,2)) AS fixed
FROM db.it_dec;"; then
    check postgres-decimal-aggregates "$out/pg_dec.csv" "$out/pg_dec_expected.csv"
  else
    report "postgres-decimal-aggregates (run error)" bad
  fi
fi

# SQL Server money and time. `money` is a scaled integer of ten-thousandths sent
# high word first, not a float — decoding it as one turned -0.0001 into NaN — and
# the fixed-length forms (money NOT NULL) were missing from the type switch
# entirely. `time(n)` was read as CP1252 text, so it came back as mojibake.
if runs sqlserver; then
  docker compose -f it/compose.yaml exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P It_Passw0rd1 -C -Q \
    "USE master; DROP TABLE IF EXISTS dbo.it_money;
     CREATE TABLE dbo.it_money (k int, a money NULL, b smallmoney NULL,
                                c money NOT NULL, f smallmoney NOT NULL,
                                t0 time(0) NULL, t7 time(7) NULL);
GO
     INSERT INTO dbo.it_money VALUES
       (1, -0.0001, -0.0001, 1.0000, 2.5000, '12:00:00', '23:59:59.9999999'),
       (2, 922337203685477.5807, 214748.3647, -214748.3648, -2.5000, '00:00:00', '00:00:00.0000001'),
       (3, NULL, NULL, 0.0000, 0.0000, NULL, NULL);" >/dev/null 2>&1
  { echo "k,a,b,c,f,t0,t7";
    echo "1,-0.0001,-0.0001,1.0000,2.5000,12:00:00.000000,23:59:59.999999";
    echo "2,922337203685477.5807,214748.3647,-214748.3648,-2.5000,00:00:00.000000,00:00:00.000000";
    echo "3,,,0.0000,0.0000,,"; } >"$out/mssql_money_expected.csv"
  if brun run -c "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);
LOAD INTO '$out/mssql_money.csv' AS SELECT * FROM db.it_money ORDER BY k;"; then
    check sqlserver-money-time "$out/mssql_money.csv" "$out/mssql_money_expected.csv"
  else
    report "sqlserver-money-time (run error)" bad
  fi
fi

# Volume: ~7MB encoded (300k rows, nulls every 10th val) — crosses the 4MB
# segment boundary, so each bulk sink commits and count-verifies 2+ segments,
# and the Azure writer stages more than one block.
volcsv="$out/vol.csv"
{ echo "id,name,val"; awk 'BEGIN{for(i=1;i<=300000;i++) printf "%d,name_%d,%s\n", i, i, (i%10==0 ? "" : i*3)}'; } > "$volcsv"
brun run -c "LOAD INTO '$out/vol_expected.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM '$volcsv';"

runs mysql     && volrt mysql     "$MYSQL_OPTS"
runs postgres  && volrt postgres  "$PG_OPTS"
runs sqlserver && volrt sqlserver "$MSSQL_OPTS"

# StarRocks: write via stream load, read back through its MySQL-protocol FE.
if runs starrocks; then
  if brun run -c "CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');
LOAD INTO sr.basalt_it USING stream_load REPLACE AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "CREATE CONNECTION fe TYPE mysql OPTIONS (host = '127.0.0.1', port = 39030, user = 'root', password = '', database = 'it');
LOAD INTO '$out/starrocks.csv' AS SELECT * FROM fe.basalt_it ORDER BY id;"; then
    check starrocks "$out/starrocks.csv" it/expected.csv
  else
    report "starrocks (run error)" bad
  fi
fi

# Azure Blob (Azurite). ADLS Gen2 data is reached through the Blob endpoint —
# Azurite implements no DFS endpoint and no hierarchical namespace. Credentials
# are Azurite's published well-known devstore pair, not a secret.
if runs azure; then
  export AZURE_BLOB_ENDPOINT="http://127.0.0.1:31000"
  export AZURE_STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

  # Single blob: out through the block-staging writer, back through the signed
  # reader — a green run exercises both halves of the Shared Key path.
  if brun run -c "LOAD INTO 'az://devstoreaccount1/basalt-it/seed.csv' AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "LOAD INTO '$out/azure.csv' AS SELECT * FROM 'az://devstoreaccount1/basalt-it/seed.csv' ORDER BY id;"; then
    check azure "$out/azure.csv" it/expected.csv
  else
    report "azure (run error)" bad
  fi

  # Prefix read: two blobs under one prefix must read back as a single table, in
  # listing order. Guards the multi-blob rollover, which one blob cannot.
  if brun run -c "LOAD INTO 'az://devstoreaccount1/basalt-it/parts/a.csv' AS SELECT * FROM 'it/seed.csv' WHERE id <= 1;" &&
     brun run -c "LOAD INTO 'az://devstoreaccount1/basalt-it/parts/b.csv' AS SELECT * FROM 'it/seed.csv' WHERE id > 1;" &&
     brun run -c "LOAD INTO '$out/azure_prefix.csv' AS SELECT * FROM 'az://devstoreaccount1/basalt-it/parts/' ORDER BY id;"; then
    check azure-prefix "$out/azure_prefix.csv" it/expected.csv
  else
    report "azure-prefix (run error)" bad
  fi

  # Volume: ~7MB is several 4MiB blocks, so this is the only test that commits a
  # multi-entry block list. A single-block write never exercises that path.
  if brun run -c "LOAD INTO 'az://devstoreaccount1/basalt-it/vol.csv' AS SELECT * FROM '$volcsv';" &&
     brun run -c "LOAD INTO '$out/vol_azure.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM 'az://devstoreaccount1/basalt-it/vol.csv';"; then
    check azure-volume "$out/vol_azure.csv" "$out/vol_expected.csv"
  else
    report "azure-volume (run error)" bad
  fi

  # Parquet out to a blob. The parquet writer used to open its target with
  # `std.fs.cwd().createFile` unconditionally, so an `az://` path failed with
  # FileNotFound from a local directory named `az:` — the README's own opening
  # example. This is the routing guard; the read half already worked.
  if brun run -c "LOAD INTO 'az://devstoreaccount1/basalt-it/bronze/seed.parquet' AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "LOAD INTO '$out/azure_parquet.csv' AS
SELECT * FROM 'az://devstoreaccount1/basalt-it/bronze/seed.parquet' ORDER BY id;"; then
    check azure-parquet "$out/azure_parquet.csv" it/expected.csv
  else
    report "azure-parquet (run error)" bad
  fi

  # The same volume rows land at ~4.1MB of Parquet — just past one 4MiB block, so
  # the footer is written into a second block and the commit has a list to order.
  # A one-block write would pass even if block sequencing were wrong.
  if brun run -c "LOAD INTO 'az://devstoreaccount1/basalt-it/bronze/vol.parquet' AS SELECT * FROM '$volcsv';" &&
     brun run -c "LOAD INTO '$out/vol_azure_parquet.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM 'az://devstoreaccount1/basalt-it/bronze/vol.parquet';"; then
    check azure-parquet-volume "$out/vol_azure_parquet.csv" "$out/vol_expected.csv"
  else
    report "azure-parquet-volume (run error)" bad
  fi

  # Reading that blob back is now a HEAD plus ranged GETs rather than one
  # whole-object fetch, and Shared Key signs the Range header — so a signing
  # mistake shows up here as a 403, not as wrong data. The volume blob is the
  # one worth reading: at ~4.1MB it spans several ranges.
  if brun run -c "LOAD INTO '$out/azure_parquet_ranged.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM 'az://devstoreaccount1/basalt-it/bronze/vol.parquet';"; then
    check azure-parquet-ranged "$out/azure_parquet_ranged.csv" "$out/vol_expected.csv"
  else
    report "azure-parquet-ranged (run error)" bad
  fi

  # A prefix that matches nothing must say so. It used to surface as `EmptyCsv`,
  # which sent the reader to debug a file rather than the prefix they mistyped.
  if $B run -c "SELECT * FROM 'az://devstoreaccount1/basalt-it/nothing-here/';" >"$out/empty.log" 2>&1; then
    report "azure-empty-prefix (expected failure, got success)" bad
  elif grep -q "no blobs under prefix" "$out/empty.log"; then
    report azure-empty-prefix ok
  else
    report "azure-empty-prefix (wrong message)" bad
    tail -3 "$out/empty.log"
  fi
fi

# Quoted column names, end to end. Headers with spaces are the rule in
# corporate CSV ("Data Emissao", "Valor Total"), and `"..."` used to be a second
# string syntax — so the query below wrote the constant "Valor Total" down the
# column instead of the values, with no error anywhere.
{ echo "Data Emissao,Valor Total"; echo "2025-01-01,100"; echo "2025-01-02,250"; } > "$out/spaced.csv"
printf 'd,total\n2025-01-01,100\n2025-01-02,250\n' > "$out/spaced_expected.csv"
if brun run -c "LOAD INTO '$out/spaced_out.csv' AS
SELECT \"Data Emissao\" AS d, SUM(\"Valor Total\") AS total FROM '$out/spaced.csv'
GROUP BY \"Data Emissao\" ORDER BY d;"; then
  check quoted-idents "$out/spaced_out.csv" "$out/spaced_expected.csv"
else
  report "quoted-idents (run error)" bad
fi

# An empty string is not a NULL. The writer used to emit it bare, which is
# exactly how the reader spells NULL — so `""` degraded to NULL on every hop and
# the degradation was invisible until something counted nulls. Two hops, because
# one would pass on a writer that merely echoed its input.
printf 'id,s\n1,""\n2,\n3,x\n' > "$out/es.csv"
printf 'id,s,n\n1,"",false\n2,,true\n3,x,false\n' > "$out/es_expected.csv"
if brun run -c "LOAD INTO '$out/es1.csv' AS SELECT * FROM '$out/es.csv' ORDER BY id;" &&
   brun run -c "LOAD INTO '$out/es2.csv' AS
SELECT id, s, s IS NULL AS n FROM '$out/es1.csv' ORDER BY id;"; then
  check csv-empty-string-vs-null "$out/es2.csv" "$out/es_expected.csv"
else
  report "csv-empty-string-vs-null (run error)" bad
fi

# THROW guards the script's own invariants, which basalt cannot infer. It must
# fire at plan time — so `check` rejects it, not just `run` — carry the author's
# message verbatim, and be permanent (exit 1), never transient: a scheduler must
# not retry a script that can never succeed.
tp_ok=1
TP="PARAM tbl STRING DEFAULT ''; THROW 'tbl is required' WHEN \$tbl IS EMPTY;
LOAD INTO '$out/tp.csv' AS SELECT * FROM 'it/seed.csv';"
if $B check -c "$TP" >"$out/tp.log" 2>&1; then tp_ok=0; echo "  check accepted a firing guard"; fi
if ! grep -q "tbl is required" "$out/tp.log"; then tp_ok=0; echo "  message not verbatim"; fi
tp_rc=0
$B run -q -c "$TP" >"$out/tp2.log" 2>&1 || tp_rc=$?
if [ "$tp_rc" != 1 ]; then tp_ok=0; echo "  run exit was $tp_rc, want 1 (75 would mean retryable)"; fi
if ! $B check -c "$TP" -p tbl=SC5 >"$out/tp3.log" 2>&1; then tp_ok=0; echo "  -p did not satisfy the guard"; fi
if [ "$tp_ok" = 1 ]; then report throw-guard ok; else report "throw-guard" bad; fi

# PRINT is the script's own output, not a diagnostic: visible by default (no
# --log-level needed), silenced by -q, and on stderr so --format json's stdout
# contract stays parseable.
pr_ok=1
PR="PRINT 'hello from the script'; SELECT id FROM 'it/seed.csv';"
$B run -c "$PR" 2>"$out/pr_err.log" >"$out/pr_out.log" || true
if ! grep -q "hello from the script" "$out/pr_err.log"; then pr_ok=0; echo "  not visible by default"; fi
if grep -q "hello from the script" "$out/pr_out.log"; then pr_ok=0; echo "  leaked onto stdout"; fi
$B run -q -c "$PR" 2>"$out/pr_q.log" >/dev/null || true
if grep -q "hello from the script" "$out/pr_q.log"; then pr_ok=0; echo "  -q did not silence it"; fi
if [ "$pr_ok" = 1 ]; then report print-stmt ok; else report "print-stmt" bad; fi

# EXPLAIN ANALYZE must print a plan whatever the pipeline shape, and must move
# no data. At the default -j it used to print nothing at all for an aggregate or
# a LOAD — ten parallel paths, only three of which reported — so whether you got
# output depended on the query. It also used to perform the load.
printf 'g,v\na,10\na,20\nb,30\n' > "$out/ea.csv"
rm -f "$out/ea_out.csv"
ea_ok=1
for q in "SELECT g, v FROM '$out/ea.csv' WHERE v > 5" "SELECT g, COUNT(*) AS n FROM '$out/ea.csv' GROUP BY g"; do
  $B run -c "EXPLAIN ANALYZE $q;" >"$out/ea.log" 2>&1
  grep -q "plan (actuals" "$out/ea.log" || { ea_ok=0; echo "  no plan for: $q"; }
  grep -qE "^(a|b)  *[0-9]" "$out/ea.log" && { ea_ok=0; echo "  printed rows for: $q"; }
done
$B run -c "EXPLAIN ANALYZE LOAD INTO '$out/ea_out.csv' AS SELECT g, v FROM '$out/ea.csv';" >>"$out/ea.log" 2>&1
[ -e "$out/ea_out.csv" ] && { ea_ok=0; echo "  EXPLAIN ANALYZE wrote the sink"; }
if [ "$ea_ok" = 1 ]; then report explain-analyze-plan-only ok; else report "explain-analyze-plan-only" bad; fi

# EXPLAIN is a statement, not just a script prefix: it has to be accepted after
# other statements, explain the query it precedes against the declarations above
# it, and leave those statements running normally. It used to be a parse error,
# which made it useless for any query built on a connection or a CTE.
rm -f "$out/es_ran.csv" "$out/es_never.csv"
es_ok=1
$B run -q -c "LOAD INTO '$out/es_ran.csv' AS SELECT g, v FROM '$out/ea.csv';
              EXPLAIN LOAD INTO '$out/es_never.csv' AS
              WITH big AS (SELECT g, v FROM '$out/ea.csv' WHERE v > 15)
              SELECT g FROM big;" >"$out/es.log" 2>&1 || { es_ok=0; echo "  run failed"; cat "$out/es.log"; }
grep -q "^plan" "$out/es.log" || { es_ok=0; echo "  no plan printed"; }
[ -e "$out/es_ran.csv" ] || { es_ok=0; echo "  the statement before EXPLAIN did not run"; }
[ -e "$out/es_never.csv" ] && { es_ok=0; echo "  EXPLAIN executed the query it explained"; }
$B run -q -c "SELECT g FROM '$out/ea.csv'; EXPLAIN ANALYZE SELECT g, COUNT(*) AS n FROM '$out/ea.csv' GROUP BY g;" >"$out/es2.log" 2>&1
grep -q "plan (actuals" "$out/es2.log" || { es_ok=0; echo "  no actuals for a mid-script EXPLAIN ANALYZE"; }
if [ "$es_ok" = 1 ]; then report explain-statement ok; else report "explain-statement" bad; fi

# NTLM config surface. The handshake itself needs a domain-joined server, which
# the mssql container is not — but the plan-time refusals need no server at all,
# and they are what stops a misconfigured script from ever reaching the wire.
NTLM_CONN="CREATE CONNECTION c TYPE sqlserver OPTIONS (host='h', database='d', auth='ntlm'"
if C_USER=u C_PASS=p $B run -q -c "${NTLM_CONN}); SELECT 1 AS x FROM c.QUERY(\$\$SELECT 1\$\$);" >"$out/ntlm_tls.log" 2>&1; then
  report "ntlm-requires-encryption (accepted plaintext)" bad
elif grep -q "requires an encrypted channel" "$out/ntlm_tls.log"; then
  report ntlm-requires-encryption ok
else
  report "ntlm-requires-encryption (wrong error)" bad; head -2 "$out/ntlm_tls.log"
fi

# A mistyped auth mode used to fall back to a SQL login, so the user got an
# opaque rejection instead of being told the keyword was wrong.
if $B run -q -c "CREATE CONNECTION c TYPE sqlserver OPTIONS (host='h', database='d', auth='ntlmv2', tls='require'); SELECT 1 AS x FROM c.QUERY(\$\$SELECT 1\$\$);" >"$out/ntlm_typo.log" 2>&1; then
  report "ntlm-auth-typo (accepted unknown mode)" bad
elif grep -q 'must be "sql", "aad" or "ntlm"' "$out/ntlm_typo.log"; then
  report ntlm-auth-typo ok
else
  report "ntlm-auth-typo (wrong error)" bad; head -2 "$out/ntlm_typo.log"
fi

# Write dispositions on a file target. `APPEND` used to truncate like every
# other disposition — two runs left only the second one's rows, silently, on the
# keyword documented as the default. A bare `LOAD INTO` and `REPLACE` must still
# truncate; only an explicit `APPEND` accumulates, and only one header.
printf 'id\n1\n2\n3\n' > "$out/app_expected.csv"
printf 'id\n3\n' > "$out/trunc_expected.csv"
rm -f "$out/app.csv" "$out/bare.csv" "$out/rep.csv"
for i in 1 2 3; do brun run -c "LOAD INTO '$out/app.csv' APPEND AS SELECT $i AS id;" || break; done
check append-accumulates "$out/app.csv" "$out/app_expected.csv"

for i in 1 2 3; do brun run -c "LOAD INTO '$out/bare.csv' AS SELECT $i AS id;" || break; done
check append-bare-truncates "$out/bare.csv" "$out/trunc_expected.csv"

for i in 1 2 3; do brun run -c "LOAD INTO '$out/rep.csv' REPLACE AS SELECT $i AS id;" || break; done
check append-replace-truncates "$out/rep.csv" "$out/trunc_expected.csv"

# A parquet footer is written last, so the file cannot be extended in place.
# Refusing at plan time beats silently discarding the previous run — and `check`
# must catch it without touching the filesystem.
rm -f "$out/never.parquet"
if $B check -c "LOAD INTO '$out/never.parquet' APPEND AS SELECT 1 AS id;" >"$out/app_pq.log" 2>&1; then
  report "append-parquet-refused (check accepted it)" bad
elif grep -q "is not supported" "$out/app_pq.log" && [ ! -e "$out/never.parquet" ]; then
  report append-parquet-refused ok
else
  report "append-parquet-refused (wrong message or file created)" bad
  head -2 "$out/app_pq.log"
fi

# Parquet: read the committed fixtures through the CLI. The unit tests decode
# pages directly; this is the only check that the .parquet dispatch, planning and
# sink path all line up. Reference output comes from DuckDB, so a green run means
# basalt agrees with another implementation rather than with itself.
if runs parquet; then
  if brun run -c "LOAD INTO '$out/parquet.csv' AS
SELECT id, name, amt, flag FROM 'src/connect/testdata/zstd.parquet' ORDER BY id;"; then
    check parquet "$out/parquet.csv" it/parquet_expected.csv
  else
    report "parquet (run error)" bad
  fi

  # Same rows, every codec: proves the codec dispatch survives the full pipeline,
  # not just the decoder unit tests.
  for c in uncompressed snappy gzip lz4; do
    if brun run -c "LOAD INTO '$out/parquet_$c.csv' AS
SELECT id, name, amt, flag FROM 'src/connect/testdata/$c.parquet' ORDER BY id;"; then
      check "parquet-$c" "$out/parquet_$c.csv" it/parquet_expected.csv
    else
      report "parquet-$c (run error)" bad
    fi
  done

  # Write path: CSV -> parquet -> read back. Reading our own output only proves
  # self-consistency, so the seed round-trip is checked against it/expected.csv,
  # which every other backend is held to as well.
  if brun run -c "LOAD INTO '$out/w.parquet' AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "LOAD INTO '$out/parquet_rt.csv' AS SELECT * FROM '$out/w.parquet' ORDER BY id;"; then
    check parquet-write "$out/parquet_rt.csv" it/expected.csv
  else
    report "parquet-write (run error)" bad
  fi

  # Decimals wider than the INT64 storage must fail, not saturate. `12.5` in a
  # DECIMAL(38,18) column restates to 1.25e19, and clamping wrote
  # 9.223372036854775807 into the file; DECIMAL(30,20) clamped its precision to
  # 18 and emitted scale > precision, which no other reader accepts.
  printf 'v\n12.5\n' >"$out/dec_in.csv"
  rm -f "$out/dec_wide.parquet" "$out/dec_badscale.parquet"
  if $B run -q -c "LOAD INTO '$out/dec_wide.parquet' AS SELECT CAST(v AS DECIMAL(38,18)) AS d FROM '$out/dec_in.csv';" >"$out/dec_wide.log" 2>&1 ||
     $B run -q -c "LOAD INTO '$out/dec_badscale.parquet' AS SELECT CAST(v AS DECIMAL(30,20)) AS d FROM '$out/dec_in.csv';" >>"$out/dec_wide.log" 2>&1; then
    report "parquet-decimal-overflow (a lossy write was accepted)" bad
  elif grep -q "UnsupportedParquetDecimal" "$out/dec_wide.log"; then
    report parquet-decimal-overflow ok
  else
    report "parquet-decimal-overflow (wrong message)" bad
    head -3 "$out/dec_wide.log"
  fi

  # …while a decimal that does fit still round-trips unchanged.
  if brun run -c "LOAD INTO '$out/dec_ok.parquet' AS SELECT CAST(v AS DECIMAL(18,4)) AS d FROM '$out/dec_in.csv';" &&
     brun run -c "LOAD INTO '$out/dec_ok.csv' AS SELECT * FROM '$out/dec_ok.parquet';"; then
    { echo "d"; echo "12.5000"; } >"$out/dec_ok_expected.csv"
    check parquet-decimal-inrange "$out/dec_ok.csv" "$out/dec_ok_expected.csv"
  else
    report "parquet-decimal-inrange (run error)" bad
  fi

  # Parallel aggregate over a multi-row-group file. An ungrouped one (no GROUP
  # BY) returned 0 instead of the row count: lanes fold into a table keyed by
  # the grouping columns, and with none they folded into nothing at all, so the
  # merge emitted the identity. Only wrong above -j 1, and nothing else here
  # combines a local parquet, several row groups and more than one lane.
  if brun run -c "LOAD INTO '$out/vol.parquet' AS SELECT * FROM '$volcsv';" &&
     brun run -j 4 -c "LOAD INTO '$out/pq_par_agg.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM '$out/vol.parquet';"; then
    check parquet-parallel-agg "$out/pq_par_agg.csv" "$out/vol_expected.csv"
  else
    report "parquet-parallel-agg (run error)" bad
  fi

  # The grouped form runs through the same lanes and radix merge. Serial is the
  # oracle: one lane cannot disagree with itself about which group a row joins,
  # so any difference is a merge bug. ~9973 distinct keys spread across every
  # lane and partition.
  if brun run -j 1 -c "LOAD INTO '$out/grp_serial.csv' AS
SELECT name, COUNT(*) AS n, SUM(val) AS s FROM '$out/vol.parquet' GROUP BY name ORDER BY name;" &&
     brun run -j 4 -c "LOAD INTO '$out/grp_par.csv' AS
SELECT name, COUNT(*) AS n, SUM(val) AS s FROM '$out/vol.parquet' GROUP BY name ORDER BY name;"; then
    check parquet-parallel-agg-grouped "$out/grp_par.csv" "$out/grp_serial.csv"
  else
    report "parquet-parallel-agg-grouped (run error)" bad
  fi

  # A file written by basalt must also satisfy a different implementation. Skipped
  # rather than failed when duckdb is absent, so the suite stays runnable anywhere.
  if command -v duckdb >/dev/null 2>&1 || [ -x "$HOME/.duckdb/cli/latest/duckdb" ]; then
    DUCK=$(command -v duckdb || echo "$HOME/.duckdb/cli/latest/duckdb")
    # COPY with NULLSTR '' so duckdb renders nulls the way basalt's CSV sink does
    if "$DUCK" -c "COPY (SELECT id, name, val FROM '$out/w.parquet' ORDER BY id) TO '$out/parquet_duck.csv' (FORMAT CSV, HEADER, NULLSTR '');" >/dev/null 2>&1; then
      check parquet-interop "$out/parquet_duck.csv" it/expected.csv
    else
      report "parquet-interop (duckdb could not read basalt output)" bad
    fi

    # Reading it back is not enough: duckdb walks page headers and so tolerates
    # chunk sizes that Arrow-based readers (StarRocks, pyarrow) reject. Compare
    # each chunk's declared total_compressed_size against where the next chunk
    # actually starts. They were once 29 bytes apart — one uncounted page header
    # — which surfaced only as "Page was smaller than expected", elsewhere.
    if brun run -c "LOAD INTO '$out/sizes.parquet' AS SELECT * FROM '$volcsv';"; then
      mismatch=$("$DUCK" -noheader -list -c "
        SELECT COUNT(*) FROM (
          SELECT total_compressed_size AS declared,
                 LEAD(COALESCE(dictionary_page_offset, data_page_offset))
                   OVER (ORDER BY COALESCE(dictionary_page_offset, data_page_offset))
                 - COALESCE(dictionary_page_offset, data_page_offset) AS actual
          FROM parquet_metadata('$out/sizes.parquet')
        ) WHERE actual IS NOT NULL AND declared <> actual;" 2>/dev/null)
      if [ "$mismatch" = "0" ]; then
        report parquet-chunk-sizes ok
      else
        report "parquet-chunk-sizes ($mismatch chunk(s) misdeclared)" bad
      fi
    else
      report "parquet-chunk-sizes (run error)" bad
    fi
  else
    echo "SKIP parquet-interop (no duckdb)"
    echo "SKIP parquet-chunk-sizes (no duckdb)"
  fi

  # Parquet over plain HTTP. This used to hit `std.fs.cwd().openFile("http://…")`
  # and fail with FileNotFound — documented as supported, never implemented. The
  # same fixture read locally is the oracle: identical rows, or the range
  # arithmetic is wrong.
  if brun run -c "LOAD INTO '$out/parquet_http.csv' AS
SELECT id, name, amt, flag FROM 'http://127.0.0.1:38080/snappy.parquet' ORDER BY id;"; then
    check parquet-http "$out/parquet_http.csv" it/parquet_expected.csv
  else
    report "parquet-http (run error)" bad
  fi

  # Projection over HTTP: two columns of four. Exercises the point of ranged
  # reads — chunks the query does not touch are never fetched — and would still
  # pass if the reader silently fell back to pulling the whole object, so it is
  # a correctness check, not a transfer-volume one.
  if brun run -c "LOAD INTO '$out/parquet_http_proj.csv' AS
SELECT id, name FROM 'http://127.0.0.1:38080/snappy.parquet' ORDER BY id;"; then
    if cut -d, -f1,2 it/parquet_expected.csv > "$out/proj_expected.csv"; then
      check parquet-http-projection "$out/parquet_http_proj.csv" "$out/proj_expected.csv"
    fi
  else
    report "parquet-http-projection (run error)" bad
  fi

  # An origin that ignores Range answers 200 with the whole body. The reader has
  # to keep that body and serve every chunk from it — the rows must come out
  # identical to the ranged read above. Multiple row groups matter here: the
  # kept buffer is read again on the next batch, after the batch arena that
  # carried it has been recycled.
  if brun run -c "LOAD INTO '$out/parquet_norange.csv' AS
SELECT id, name, amt, flag FROM 'http://127.0.0.1:38081/snappy.parquet' ORDER BY id;"; then
    check parquet-http-norange "$out/parquet_norange.csv" it/parquet_expected.csv
  else
    report "parquet-http-norange (run error)" bad
  fi

  # A URL that is not there must report the HTTP fact, not a filesystem one.
  if $B run -q -c "SELECT * FROM 'http://127.0.0.1:38080/absent.parquet';" >"$out/missing.log" 2>&1; then
    report "parquet-http-missing (expected failure, got success)" bad
  elif grep -q "FileNotFound" "$out/missing.log"; then
    report "parquet-http-missing (reported a filesystem error for a URL)" bad
    tail -3 "$out/missing.log"
  else
    report parquet-http-missing ok
  fi
fi

echo "==> $pass passed, $fail failed"
[ "$fail" -eq 0 ]

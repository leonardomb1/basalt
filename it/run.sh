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
    parquet)   ;;  # reads committed fixtures; needs no container
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
  else
    echo "SKIP parquet-interop (no duckdb)"
  fi
fi

echo "==> $pass passed, $fail failed"
[ "$fail" -eq 0 ]

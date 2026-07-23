#!/usr/bin/env bash
# Integration suite: seed CSV -> write to each DB (auto-created table) -> read
# back -> compare with it/expected.csv. Needs docker compose. KEEP=1 leaves the
# stack up for debugging. Scripts are Basalt SQL (the BSL parser was removed in
# v0.2.0); connection attrs are passed as `OPTIONS(...)` bodies.
set -euo pipefail
cd "$(dirname "$0")/.."

zig build
B=./zig-out/bin/basalt
COMPOSE="docker compose -f it/compose.yaml"

$COMPOSE up -d --wait
trap '[ "${KEEP:-}" ] || '"$COMPOSE"' down -v' EXIT

out=$(mktemp -d)
fail=0

check() { # $1 driver name, $2 read-back csv path
  if diff -u it/expected.csv "$2" >"$out/$1.diff" 2>&1; then
    echo "PASS $1"
  else
    echo "FAIL $1"
    cat "$out/$1.diff"
    fail=1
  fi
}

sqlrt() { # $1 connector, $2 OPTIONS(...) body -- round trip via replace + table read
  local decl="CREATE CONNECTION db TYPE $1 OPTIONS ($2);"
  $B run -c "$decl
LOAD INTO db.basalt_it REPLACE AS SELECT * FROM 'it/seed.csv';" -q &&
  $B run -c "$decl
LOAD INTO '$out/$1.csv' AS SELECT * FROM db.basalt_it ORDER BY id;" -q &&
  check "$1" "$out/$1.csv" || { echo "FAIL $1 (run error)"; fail=1; }
}

sqlrt mysql     "host = '127.0.0.1', port = 33306, user = 'root', password = 'it', database = 'it'"
sqlrt postgres  "host = '127.0.0.1', port = 35432, user = 'postgres', password = 'it', database = 'it'"
sqlrt sqlserver "host = '127.0.0.1', port = 31433, user = 'sa', password = 'It_Passw0rd1', database = 'master', tls = 'insecure'"

# Volume: ~7MB encoded (300k rows, nulls every 10th val) — crosses the 4MB
# segment boundary, so each bulk sink commits and count-verifies 2+ segments.
volcsv="$out/vol.csv"
{ echo "id,name,val"; awk 'BEGIN{for(i=1;i<=300000;i++) printf "%d,name_%d,%s\n", i, i, (i%10==0 ? "" : i*3)}'; } > "$volcsv"
$B run -c "LOAD INTO '$out/vol_expected.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM '$volcsv';" -q

volcheck() { # like check(), but against vol_expected.csv
  if diff -u "$out/vol_expected.csv" "$2" >"$out/$1.diff" 2>&1; then
    echo "PASS $1"
  else
    echo "FAIL $1"
    cat "$out/$1.diff"
    fail=1
  fi
}

volrt() { # $1 connector, $2 OPTIONS(...) body
  local decl="CREATE CONNECTION db TYPE $1 OPTIONS ($2);"
  $B run -c "$decl
LOAD INTO db.basalt_vol REPLACE AS SELECT * FROM '$volcsv';" -q &&
  $B run -c "$decl
LOAD INTO '$out/vol_$1.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM db.basalt_vol;" -q &&
  volcheck "$1-volume" "$out/vol_$1.csv" || { echo "FAIL $1-volume (run error)"; fail=1; }
}

volrt mysql     "host = '127.0.0.1', port = 33306, user = 'root', password = 'it', database = 'it'"
volrt postgres  "host = '127.0.0.1', port = 35432, user = 'postgres', password = 'it', database = 'it'"
volrt sqlserver "host = '127.0.0.1', port = 31433, user = 'sa', password = 'It_Passw0rd1', database = 'master', tls = 'insecure'"

# StarRocks: write via stream load, read back through its MySQL-protocol FE.
$B run -c "CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');
LOAD INTO sr.basalt_it USING stream_load REPLACE AS SELECT * FROM 'it/seed.csv';" -q &&
$B run -c "CREATE CONNECTION fe TYPE mysql OPTIONS (host = '127.0.0.1', port = 39030, user = 'root', password = '', database = 'it');
LOAD INTO '$out/starrocks.csv' AS SELECT * FROM fe.basalt_it ORDER BY id;" -q &&
check starrocks "$out/starrocks.csv" || { echo "FAIL starrocks (run error)"; fail=1; }

exit $fail

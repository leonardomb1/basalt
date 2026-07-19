#!/usr/bin/env bash
# Integration suite: seed CSV -> write to each DB (auto-created table) -> read
# back -> compare with it/expected.csv. Needs docker compose. KEEP=1 leaves the
# stack up for debugging.
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

sqlrt() { # $1 connector, $2 conn attrs -- round trip via overwrite + table read
  local decl="connection db = $1 $2"
  $B run -c "@batch
$decl
read csv \"it/seed.csv\" | write db basalt_it overwrite" -q &&
  $B run -c "@batch
$decl
read db table basalt_it | sort id | write csv \"$out/$1.csv\"" -q &&
  check "$1" "$out/$1.csv" || { echo "FAIL $1 (run error)"; fail=1; }
}

sqlrt mysql     'host = "127.0.0.1" port = 33306 user = "root" password = "it" database = "it"'
sqlrt postgres  'host = "127.0.0.1" port = 35432 user = "postgres" password = "it" database = "it"'
sqlrt sqlserver 'host = "127.0.0.1" port = 31433 user = "sa" password = "It_Passw0rd1" database = "master" tls = "insecure"'

# Volume: ~7MB encoded (300k rows, nulls every 10th val) — crosses the 4MB
# segment boundary, so each bulk sink commits and count-verifies 2+ segments.
volcsv="$out/vol.csv"
{ echo "id,name,val"; awk 'BEGIN{for(i=1;i<=300000;i++) printf "%d,name_%d,%s\n", i, i, (i%10==0 ? "" : i*3)}'; } > "$volcsv"
$B run -c "@batch
read csv \"$volcsv\" | aggregate rows = count(), ids = sum(id), vals = sum(val) | write csv \"$out/vol_expected.csv\"" -q

volcheck() { # like check(), but against vol_expected.csv

  if diff -u "$out/vol_expected.csv" "$2" >"$out/$1.diff" 2>&1; then
    echo "PASS $1"
  else
    echo "FAIL $1"
    cat "$out/$1.diff"
    fail=1
  fi
}

volrt() { # $1 connector, $2 conn attrs
  local decl="connection db = $1 $2"
  $B run -c "@batch
$decl
read csv \"$volcsv\" | write db basalt_vol overwrite" -q &&
  $B run -c "@batch
$decl
read db table basalt_vol | aggregate rows = count(), ids = sum(id), vals = sum(val) | write csv \"$out/vol_$1.csv\"" -q &&
  volcheck "$1-volume" "$out/vol_$1.csv" || { echo "FAIL $1-volume (run error)"; fail=1; }
}

volrt mysql     'host = "127.0.0.1" port = 33306 user = "root" password = "it" database = "it"'
volrt postgres  'host = "127.0.0.1" port = 35432 user = "postgres" password = "it" database = "it"'
volrt sqlserver 'host = "127.0.0.1" port = 31433 user = "sa" password = "It_Passw0rd1" database = "master" tls = "insecure"'

# StarRocks: write via stream load, read back through its MySQL-protocol FE.
$B run -c '@batch
connection sr = starrocks fe_host = "127.0.0.1" fe_port = 39030 be_url = "http://127.0.0.1:38040" database = "it" user = "root"
read csv "it/seed.csv" | write sr stream_load basalt_it overwrite' -q &&
$B run -c "@batch
connection fe = mysql host = \"127.0.0.1\" port = 39030 user = \"root\" database = \"it\"
read fe table basalt_it | sort id | write csv \"$out/starrocks.csv\"" -q &&
check starrocks "$out/starrocks.csv" || { echo "FAIL starrocks (run error)"; fail=1; }

exit $fail

#!/usr/bin/env bash
# Integration suite: seed CSV -> write to each backend (auto-created table or
# blob) -> read back -> compare against it/expected.csv. Needs docker compose.
#
#   ./it/run.sh                 all suites
#   ./it/run.sh azure           only that suite (boots only the containers it needs)
#   ./it/run.sh mysql postgres  several
#   KEEP=1 ./it/run.sh azure    leave the stack up afterwards
#
# Suite names: mysql postgres sqlserver starrocks azure parquet s3 arrow stdout kernel
# (arrow needs `uv`: it reads the stream back with pyarrow; stdout needs nothing;
# kernel needs python3)
# Scripts are Basalt SQL (the BSL parser was removed in v0.2.0); connection
# attrs are passed as `OPTIONS(...)` bodies.
set -euo pipefail
cd "$(dirname "$0")/.."

ALL_SUITES="mysql postgres sqlserver starrocks azure parquet s3 arrow stdout kernel"
DEFAULT_SUITES="$ALL_SUITES"
SUITES="${*:-$DEFAULT_SUITES}"

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
    s3)        services="$services s3" ;;
    parquet)   services="$services static static-norange" ;;  # local fixtures, plus HTTP
  esac
done

zig build
B=./zig-out/bin/basalt
COMPOSE="docker compose -f it/compose.yaml"

echo "==> suites: $SUITES"
# A suite with no service (arrow) must not start the whole stack: a bare
# `compose up` brings every container up, StarRocks included.
if [ -n "$services" ]; then
  echo "==> starting:$services"
  # shellcheck disable=SC2086
  $COMPOSE up -d --wait $services
  trap '[ "${KEEP:-}" ] || '"$COMPOSE"' down -v' EXIT
fi

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

# ORDER BY ... LIMIT and LIMIT descend into the source (pushdown.planTopN). Each
# pushed query must answer exactly what the engine alone does — the reference
# adds `to_hex(id) IS NOT NULL`, always true and never pushed, which keeps the
# whole sort here — and the debug log must show whether it descended. The keys
# carry nulls, so a source ordering them its own way (postgres DESC puts them
# first) shows up as a diff.
topnrt() { # $1 label, $2 CREATE CONNECTION db ..., $3 LOAD target
  local decl=$2
  if ! brun run -c "$decl
LOAD INTO $3 REPLACE AS SELECT CAST(id AS INT) AS id, CAST(v AS INT) AS v, CAST(ts AS TIMESTAMP) AS ts, CAST(amt AS DECIMAL(10,2)) AS amt, name FROM 'it/topn.csv';"; then
    report "$1-topn (load error)" bad
    return
  fi
  tq() { # label, cols, where, order/limit, pushed|refused, test name
    local w=$3 ref
    if [ -n "$w" ]; then ref="WHERE ($w) AND to_hex(id) IS NOT NULL"; w="WHERE $w"; else ref="WHERE to_hex(id) IS NOT NULL"; fi
    local g="$out/topn_$1_$2"
    $B run --format csv --log-level debug -c "$decl SELECT $2 FROM db.it_topn $w $4;" >"$g.csv" 2>"$g.log" &&
      $B run --format csv -q -c "$decl SELECT $2 FROM db.it_topn $ref $4;" >"$g.ref" 2>&1
    local how=refused
    grep -q "top-N pushdown" "$g.log" && how=pushed
    if cmp -s "$g.ref" "$g.csv" && [ -s "$g.csv" ] && [ "$how" = "$5" ]; then
      report "$1-topn-$6" ok
    else
      report "$1-topn-$6 (got $how, want $5)" bad
      diff "$g.ref" "$g.csv" | head -8 || true
    fi
  }
  tq "$1" "id, v"      ""      "ORDER BY v DESC, id LIMIT 4"            pushed int-desc-nulls-last
  tq "$1" "id, v"      ""      "ORDER BY v DESC, id LIMIT 8"            pushed int-desc-every-row
  tq "$1" "id, ts"     ""      "ORDER BY ts, id LIMIT 8"                pushed timestamp-asc-nulls-last
  tq "$1" "id, amt"    ""      "ORDER BY amt DESC, id LIMIT 3 OFFSET 2" pushed decimal-with-offset
  tq "$1" "id, v"      "id > 2" "ORDER BY v, id LIMIT 3"                pushed filter-and-topn
  tq "$1" "id AS k, v" ""      "ORDER BY k DESC LIMIT 2"                pushed renamed-key
  tq "$1" "id, name"   ""      "ORDER BY name, id LIMIT 3"              refused string-key
  tq "$1" "id, v"      "to_hex(v) = '5'" "ORDER BY v, id LIMIT 3"       refused untranslatable-filter
  # a plain LIMIT's rows are the source's pick: count them, and see it descended
  if $B run --format csv --log-level debug -c "$decl SELECT id FROM db.it_topn LIMIT 3 OFFSET 1;" >"$out/topn_plain.csv" 2>"$out/topn_plain.log" &&
     [ "$(tail -n +2 "$out/topn_plain.csv" | wc -l)" = 3 ] && grep -q "top-N pushdown: limit 4" "$out/topn_plain.log"; then
    report "$1-topn-plain-limit-offset" ok
  else
    report "$1-topn-plain-limit-offset" bad
  fi
}

# The source's catalog, as basalt asks it: SHOW TABLES lists a table, and Tab
# offers the columns of a table named at the very end of the script, with their
# type. Runs after topnrt, whose it_topn table it looks for.
catalogrt() { # $1 label, $2 CREATE CONNECTION db ..., $3 schema holding it_topn
  if $B run --format csv -c "$2 SHOW TABLES FROM db.$3 LIKE 'it_topn';" >"$out/cat_$1.csv" 2>&1 &&
     grep -q "^$3,it_topn," "$out/cat_$1.csv"; then
    report "$1-show-tables" ok
  else
    report "$1-show-tables" bad; head -5 "$out/cat_$1.csv"
  fi
  local s="$2 SELECT amt FROM db.$3.it_topn"
  local at=$(( ${#2} + 11 ))
  if $B complete --connect --pos "$at" -c "$s" >"$out/cmp_$1.json" 2>/dev/null &&
     python3 -c 'import json,sys; o=json.load(open(sys.argv[1])); sys.exit(0 if any(i["text"]=="amt" and i["kind"]=="column" and i.get("detail") for i in o["items"]) else 1)' "$out/cmp_$1.json"; then
    report "$1-complete-columns" ok
  else
    report "$1-complete-columns" bad; cat "$out/cmp_$1.json"
  fi
}

MYSQL_OPTS="host = '127.0.0.1', port = 33306, user = 'root', password = 'it', database = 'it'"
PG_OPTS="host = '127.0.0.1', port = 35432, user = 'postgres', password = 'it', database = 'it'"
MSSQL_OPTS="host = '127.0.0.1', port = 31433, user = 'sa', password = 'It_Passw0rd1', database = 'master', tls = 'insecure'"

runs mysql     && sqlrt mysql     "$MYSQL_OPTS"
runs postgres  && sqlrt postgres  "$PG_OPTS"
runs sqlserver && sqlrt sqlserver "$MSSQL_OPTS"

runs mysql     && topnrt mysql     "CREATE CONNECTION db TYPE mysql OPTIONS ($MYSQL_OPTS);"         db.it_topn
runs postgres  && topnrt postgres  "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);"         db.it_topn
runs sqlserver && topnrt sqlserver "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);"    db.it_topn

# A `$param` / LET in a WHERE descends as its value. It used to reach the source
# as a column of that name — `[since]` — and fail there (serial) or silently not
# descend (split). The SQL sent must carry the values and no such column.
paramrt() { # $1 label, $2 CREATE CONNECTION db ...
  if $B run --format csv --log-level debug -c "$2 PARAM lo INT DEFAULT 5; LET hi = 7;
SELECT id FROM db.it_topn WHERE id >= \$lo AND id <= \$hi ORDER BY id;" >"$out/param_$1.csv" 2>"$out/param_$1.log" &&
     [ "$(tr '\n' ' ' <"$out/param_$1.csv")" = "id 5 6 7 " ] &&
     grep "sql read" "$out/param_$1.log" | grep -q ">= 5" &&
     ! grep "sql read" "$out/param_$1.log" | grep -qE '[]"`](lo|hi)[]"`]'; then
    report "$1-param-pushdown" ok
  else
    report "$1-param-pushdown" bad; cat "$out/param_$1.csv"; grep "sql read" "$out/param_$1.log" | head -3
  fi
}

# Text comparisons against collations that disagree with basalt's byte order:
# case-insensitive, space-padding, `[` as a LIKE class, character-counting
# length(). Every query must answer what the engine alone answers from a parquet
# copy of the same rows — pushed down as far as that stays true, and no further.
collrt() { # $1 label, $2 CREATE CONNECTION db ..., $3 LOAD target
  local decl=$2 pq="$out/coll_$1.parquet"
  printf 'n,code\n1,a\n2,B\n3,b\n4,000123\n5,000123   \n6,Z\n7,x[1]\n8,\n9,a\303\251\n10,  \n' >"$out/coll.csv"
  if ! brun run -c "$decl
LOAD INTO $3 REPLACE AS SELECT CAST(n AS INT) AS n, CAST(code AS STRING) AS code FROM '$out/coll.csv' WITH (null = '');" ||
     ! brun run -c "$decl
LOAD INTO '$pq' AS SELECT n, code FROM db.it_coll;"; then
    report "$1-collation (setup error)" bad
    return
  fi
  local bad=0 q
  for q in "code >= 'B'" "code > '000123'" "code < 'b'" "code <> 'b'" "NOT (code = 'b')" "code = 'b'" \
           "contains(code, '[1]')" "code IS NOT EMPTY" "length(code) = 2" "code >= 'B' AND n > 1"; do
    $B run --format csv -q -c "$decl SELECT n FROM db.it_coll WHERE $q ORDER BY n;" >"$out/coll_got.csv" 2>&1
    $B run --format csv -q -c "SELECT n FROM '$pq' WHERE $q ORDER BY n;" >"$out/coll_want.csv" 2>&1
    if ! cmp -s "$out/coll_got.csv" "$out/coll_want.csv"; then
      bad=1; echo "  $1: WHERE $q"; diff "$out/coll_want.csv" "$out/coll_got.csv" | head -4 || true
    fi
  done
  # descended whole: an aggregate and a top-N take the source's rows as they are
  $B run --format csv -q -c "$decl SELECT count(*) AS c FROM db.it_coll WHERE code = 'b';" >"$out/coll_got.csv" 2>&1
  $B run --format csv -q -c "SELECT count(*) AS c FROM '$pq' WHERE code = 'b';" >"$out/coll_want.csv" 2>&1
  cmp -s "$out/coll_got.csv" "$out/coll_want.csv" || { bad=1; echo "  $1: count where code = 'b'"; diff "$out/coll_want.csv" "$out/coll_got.csv" | head -4 || true; }
  $B run --format csv -q -c "$decl SELECT n FROM db.it_coll WHERE code >= 'B' ORDER BY n DESC LIMIT 2;" >"$out/coll_got.csv" 2>&1
  $B run --format csv -q -c "SELECT n FROM '$pq' WHERE code >= 'B' ORDER BY n DESC LIMIT 2;" >"$out/coll_want.csv" 2>&1
  cmp -s "$out/coll_got.csv" "$out/coll_want.csv" || { bad=1; echo "  $1: top-N over code >= 'B'"; diff "$out/coll_want.csv" "$out/coll_got.csv" | head -4 || true; }
  # where the catalog proves byte order, the text range reaches the source
  if [ -n "${4:-}" ]; then
    $B run --format csv --log-level debug -c "$decl SELECT n FROM db.it_coll WHERE code >= 'B';" >"$out/coll_log.txt" 2>&1 || true
    grep "sql read" "$out/coll_log.txt" | grep -q ">= 'B'" || { bad=1; echo "  $1: the text range did not descend"; }
  fi
  if [ $bad = 0 ]; then report "$1-collation" ok; else report "$1-collation" bad; fi
}

runs mysql     && collrt mysql     "CREATE CONNECTION db TYPE mysql OPTIONS ($MYSQL_OPTS);"      db.it_coll
runs postgres  && collrt postgres  "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);"      db.it_coll descends  # 16-alpine: musl, byte order
runs sqlserver && collrt sqlserver "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);" db.it_coll

# A QUERY batch runs to its end: statements before the SELECT and after it both
# run, a second result set is refused rather than dropped or appended, and an
# error after the rows fails the read with the server's message.
batchrt() { # $1 label, $2 CREATE CONNECTION db ..., $3 table name, $4 its create statement
  local decl=$2 t=$3 bad=0 got
  brun run -c "$decl SELECT * FROM db.QUERY(\$\$$4\$\$);" >/dev/null 2>&1 || true
  got=$($B run --format csv -q -c "$decl SELECT * FROM db.QUERY(\$\$DELETE FROM $t; INSERT INTO $t (n) VALUES (1), (2); SELECT count(*) AS c FROM $t\$\$);" 2>&1 | tail -1)
  [ "$got" = 2 ] || { bad=1; echo "  $1: DELETE; INSERT; SELECT gave '$got'"; }
  $B run --format csv -q -c "$decl SELECT * FROM db.QUERY(\$\$SELECT 1 AS a; INSERT INTO $t (n) VALUES (3)\$\$);" >/dev/null 2>&1 || true
  got=$($B run --format csv -q -c "$decl SELECT * FROM db.QUERY(\$\$SELECT count(*) AS c FROM $t\$\$);" 2>&1 | tail -1)
  [ "$got" = 3 ] || { bad=1; echo "  $1: the INSERT after the SELECT did not run (count '$got')"; }
  if $B run --format csv -q -c "$decl SELECT * FROM db.QUERY(\$\$SELECT 1 AS a; SELECT 2 AS a\$\$);" >"$out/b_two.txt" 2>&1 ||
     ! grep -q "more than one result set" "$out/b_two.txt"; then bad=1; echo "  $1: two result sets were not refused"; cat "$out/b_two.txt"; fi
  if [ $bad = 0 ]; then report "$1-query-batch" ok; else report "$1-query-batch" bad; fi
}

runs postgres  && batchrt postgres  "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);"      it_multi "CREATE TABLE IF NOT EXISTS it_multi (n int)"
runs sqlserver && batchrt sqlserver "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);" dbo.it_multi "IF OBJECT_ID('dbo.it_multi') IS NULL CREATE TABLE dbo.it_multi (n int, s varchar(3))"

# The SQL Server session is the one SSMS and the ODBC/JDBC/.NET drivers open:
# a value too long for its column is refused, not truncated; a varchar keeps its
# trailing spaces; a column declared without NULL allows nulls; and an error
# after a SELECT's rows fails the read with SQL Server's words.
if runs sqlserver; then
  MX="CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);"
  sbad=0
  got=$($B run --format csv -q -c "$MX SELECT * FROM db.QUERY(\$\$SELECT CAST(SESSIONPROPERTY('ANSI_WARNINGS') AS int) + CAST(SESSIONPROPERTY('ANSI_NULLS') AS int) + CAST(SESSIONPROPERTY('ANSI_PADDING') AS int) + CAST(SESSIONPROPERTY('QUOTED_IDENTIFIER') AS int) + CAST(SESSIONPROPERTY('CONCAT_NULL_YIELDS_NULL') AS int) + CAST(SESSIONPROPERTY('ARITHABORT') AS int) + SIGN(@@OPTIONS & 1024) AS k\$\$);" 2>&1 | tail -1)
  [ "$got" = 7 ] || { sbad=1; echo "  session options on: $got of 7"; }
  if $B run -q -c "$MX SELECT * FROM db.QUERY(\$\$INSERT INTO dbo.it_multi (n, s) VALUES (9, 'abcdef')\$\$);" >"$out/s_trunc.txt" 2>&1 ||
     ! grep -q "would be truncated" "$out/s_trunc.txt"; then sbad=1; echo "  a too-long value was not refused"; fi
  if $B run -q -c "$MX SELECT * FROM db.QUERY(\$\$SELECT 1 AS a; INSERT INTO dbo.it_multi (n, s) VALUES (9, 'abcdef')\$\$);" >"$out/s_after.txt" 2>&1 ||
     ! grep -q "read failed (QueryFailed): String or binary data would be truncated" "$out/s_after.txt"; then sbad=1; echo "  an error after the rows did not fail the read with its message"; cat "$out/s_after.txt"; fi
  got=$($B run --format csv -q -c "$MX SELECT * FROM db.QUERY(\$\$IF OBJECT_ID('dbo.it_sess') IS NOT NULL DROP TABLE dbo.it_sess; CREATE TABLE dbo.it_sess (n int, v varchar(10)); INSERT INTO dbo.it_sess VALUES (NULL, 'ab  '); SELECT DATALENGTH(v) AS dl FROM dbo.it_sess\$\$);" 2>&1 | tail -1)
  [ "$got" = 4 ] || { sbad=1; echo "  a varchar's trailing spaces, or a null into an undeclared column: '$got'"; }
  if [ $sbad = 0 ]; then report "sqlserver-session-options" ok; else report "sqlserver-session-options" bad; fi
fi

runs mysql     && paramrt mysql     "CREATE CONNECTION db TYPE mysql OPTIONS ($MYSQL_OPTS);"
runs postgres  && paramrt postgres  "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);"
runs sqlserver && paramrt sqlserver "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);"

runs mysql     && catalogrt mysql     "CREATE CONNECTION db TYPE mysql OPTIONS ($MYSQL_OPTS);"      it
runs postgres  && catalogrt postgres  "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);"      public
runs sqlserver && catalogrt sqlserver "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);" dbo

# A SQL Server database with a binary collation, as Protheus runs on: there only
# `INFORMATION_SCHEMA.TABLES` and `TABLE_NAME` resolve, not their lower case.
if runs sqlserver; then
  MSSQL_BIN=$(printf '%s' "$MSSQL_OPTS" | sed "s/database = 'master'/database = 'it_bin'/")
  case $MSSQL_BIN in *"'it_bin'"*) ;; *) echo "MSSQL_BIN did not take the it_bin database" >&2; exit 2 ;; esac
  if brun run -c "CREATE CONNECTION m TYPE sqlserver OPTIONS ($MSSQL_OPTS);
SELECT * FROM m.QUERY(\$\$IF DB_ID('it_bin') IS NULL EXEC('CREATE DATABASE it_bin COLLATE Latin1_General_BIN'); SELECT 1 AS ok\$\$);" &&
     brun run -c "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_BIN);
LOAD INTO db.dbo.it_topn REPLACE AS SELECT CAST(id AS INT) AS id, CAST(amt AS DECIMAL(10,2)) AS amt FROM 'it/topn.csv';"; then
    catalogrt sqlserver-binary-collation "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_BIN);" dbo
    collrt sqlserver-binary "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_BIN);" db.it_coll

    # Protheus-shaped: char/varchar under a binary collation, where a char(n) comes
    # back space-padded and SQL Server compares padded — `c > 'ab'` is false there
    # for 'ab      ' and true here. The pushed forms (widened where re-filtered,
    # exact through DATALENGTH where not) must still answer as the engine does.
    PX="CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_BIN);"
    # one batch, run to its end
    if [ "$($B run --format csv -q -c "$PX SELECT * FROM db.QUERY(\$\$IF OBJECT_ID('dbo.it_prot') IS NOT NULL DROP TABLE dbo.it_prot; CREATE TABLE dbo.it_prot (n int, c char(8), v varchar(10), p char(8) NOT NULL); INSERT INTO dbo.it_prot VALUES (1, 'ab', 'ab', 'ab'), (2, 'ab' + CHAR(9), 'aB', 'ab'), (3, 'abc', 'b', 'abc'), (4, 'aa', '000123', 'aa'), (5, NULL, NULL, 'zz'), (6, 'B', 'Z', 'B'); SELECT count(*) AS k FROM dbo.it_prot\$\$);" | tail -1)" = 6 ] &&
       brun run -c "$PX LOAD INTO '$out/prot.parquet' AS SELECT n, c, v, p FROM db.dbo.it_prot;"; then
      pbad=0
      # p is a NOT NULL char(8): it comes back padded, 'ab      '
      for q in "c > 'ab'" "c >= 'ab'" "c < 'ab'" "c <= 'ab'" "c = 'ab'" "c <> 'ab'" "v >= 'b'" "v > '000123'" "c IS NOT EMPTY" \
               "p > 'ab'" "p >= 'ab'" "p < 'ab'" "p = 'ab'" "p <> 'ab'" "NOT (p = 'ab')"; do
        $B run --format csv -q -c "$PX SELECT n FROM db.dbo.it_prot WHERE $q ORDER BY n;" >"$out/p_got.csv" 2>&1
        $B run --format csv -q -c "SELECT n FROM '$out/prot.parquet' WHERE $q ORDER BY n;" >"$out/p_want.csv" 2>&1
        cmp -s "$out/p_got.csv" "$out/p_want.csv" || { pbad=1; echo "  protheus: WHERE $q"; diff "$out/p_want.csv" "$out/p_got.csv" | head -4 || true; }
      done
      for q in "count(*) AS k FROM %T WHERE c = 'ab'" "count(*) AS k FROM %T WHERE c > 'ab'" "n FROM %T WHERE c >= 'ab' ORDER BY n DESC LIMIT 2" \
               "count(*) AS k FROM %T WHERE p = 'ab'" "count(*) AS k FROM %T WHERE p > 'ab'" "n FROM %T WHERE p > 'ab' ORDER BY n LIMIT 3"; do
        $B run --format csv -q -c "$PX SELECT ${q//%T/db.dbo.it_prot};" >"$out/p_got.csv" 2>&1
        $B run --format csv -q -c "SELECT ${q//%T/\'$out/prot.parquet\'};" >"$out/p_want.csv" 2>&1
        cmp -s "$out/p_got.csv" "$out/p_want.csv" || { pbad=1; echo "  protheus: SELECT $q"; diff "$out/p_want.csv" "$out/p_got.csv" | head -4 || true; }
      done
      # and it still descends: the range reaches SQL Server
      $B run --format csv --log-level debug -c "$PX SELECT n FROM db.dbo.it_prot WHERE c >= 'ab';" >"$out/p_log.txt" 2>&1 || true
      grep -q "LIKE 'ab%'" "$out/p_log.txt" || { pbad=1; echo "  protheus: the range did not descend"; }
      if [ $pbad = 0 ]; then report "sqlserver-protheus-padding" ok; else report "sqlserver-protheus-padding" bad; fi
    else
      report "sqlserver-protheus-padding (setup error)" bad
    fi

  else
    report "sqlserver-binary-collation (setup error)" bad
  fi
fi

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

  # A column pushed to the table must be the table's: `l.label` is the joined
  # side's, and so is a bare `label` the table does not have. Asking the table
  # for either — or for a column named after the qualifier `l` — is a query
  # the database refuses.
  if brun run -c "CREATE CONNECTION db TYPE postgres OPTIONS ($PG_OPTS);
LOAD INTO '$out/pg_join_cols.csv' AS
WITH labels AS (SELECT id AS lid, name AS label FROM 'it/seed.csv')
SELECT id, l.label AS a, upper(label) AS b FROM db.basalt_it t JOIN labels l ON t.id = l.lid ORDER BY id;"; then
    printf 'id,a,b\n1,alpha,ALPHA\n2,"beta, gamma","BETA, GAMMA"\n3,delta,DELTA\n' >"$out/pg_join_cols_want.csv"
    check postgres-join-projects-own-columns "$out/pg_join_cols.csv" "$out/pg_join_cols_want.csv"
  else
    report "postgres-join-projects-own-columns (run error)" bad
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
    echo "1,-0.0001,-0.0001,1.0000,2.5000,12:00:00,23:59:59.999999";
    echo "2,922337203685477.5807,214748.3647,-214748.3648,-2.5000,00:00:00,00:00:00";
    echo "3,,,0.0000,0.0000,,"; } >"$out/mssql_money_expected.csv"
  if brun run -c "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);
LOAD INTO '$out/mssql_money.csv' AS SELECT * FROM db.it_money ORDER BY k;"; then
    check sqlserver-money-time "$out/mssql_money.csv" "$out/mssql_money_expected.csv"
  else
    report "sqlserver-money-time (run error)" bad
  fi

  # Non-BMP text. UCS-2 sends it as a surrogate PAIR; the reader encoded each
  # half on its own, which UTF-8 rejects, so every emoji came back as `??` even
  # though the writer had always paired them — an asymmetric, silent round-trip.
  # Written by basalt and read back by basalt, so both halves are on trial.
  printf 'k,s\n1,a\xf0\x9f\x98\x80b\n2,h\xc3\xa9llo\n3,\xf0\x9d\x84\x9e\xf0\x9d\x95\x8f\n' >"$out/mssql_uni.csv"
  if brun run -c "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);
LOAD INTO db.it_uni REPLACE AS SELECT * FROM '$out/mssql_uni.csv';" &&
     brun run -c "CREATE CONNECTION db TYPE sqlserver OPTIONS ($MSSQL_OPTS);
LOAD INTO '$out/mssql_uni_out.csv' AS SELECT k, s FROM db.it_uni ORDER BY k;"; then
    check sqlserver-unicode "$out/mssql_uni_out.csv" "$out/mssql_uni.csv"
  else
    report "sqlserver-unicode (run error)" bad
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

# A value over 16 MB does not fit one MySQL packet; the server splits it and the
# client must glue the run back together. `readPacket` took only the first
# packet, so the query failed *and* left the continuations queued — every later
# read on that connection was one packet out of step. The second statement runs
# on the same connection and is the desync half of the check.
if runs mysql; then
  docker exec it-mysql-1 mysql -uroot -pit it -e "
    DROP TABLE IF EXISTS basalt_big;
    CREATE TABLE basalt_big (id INT, v LONGTEXT);
    INSERT INTO basalt_big VALUES (1, CONCAT('A', REPEAT('x', 19999998), 'Z'));" >/dev/null 2>&1
  if brun run -c "CREATE CONNECTION db TYPE mysql OPTIONS ($MYSQL_OPTS);
LOAD INTO '$out/mysql_big.csv' AS
SELECT id, LENGTH(v) AS n, SUBSTR(v, 1, 1) AS a, SUBSTR(v, 20000000, 1) AS z FROM db.basalt_big;
LOAD INTO '$out/mysql_after.csv' AS SELECT * FROM db.basalt_it ORDER BY id;"; then
    { echo "id,n,a,z"; echo "1,20000000,A,Z"; } >"$out/mysql_big_expected.csv"
    check mysql-large-packet "$out/mysql_big.csv" "$out/mysql_big_expected.csv"
    check mysql-large-packet-no-desync "$out/mysql_after.csv" it/expected.csv
  else
    report "mysql-large-packet (run error)" bad
  fi
fi

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

  # `\N` is the Stream Load null marker and StarRocks CSV has no escape for it —
  # `enclose` does not exempt the field and backslashes are never unescaped — so
  # a value of exactly those two bytes used to land as NULL. It must fail the
  # load instead. `\N` inside a longer value is ordinary data and must survive.
  SR_CONN="CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');"
  printf 'id,s\n1,\\N\n' > "$out/sr_marker.csv"
  printf 'id,s\n2,a\\Nb\n' > "$out/sr_embedded.csv"
  printf 'id,s\n2,a\\Nb\n' > "$out/sr_embedded_expected.csv"
  if $B run -q -c "$SR_CONN
LOAD INTO sr.it_nullmark USING stream_load REPLACE AS SELECT * FROM '$out/sr_marker.csv';" >"$out/sr_marker.log" 2>&1; then
    report "starrocks-null-marker (a literal \\N was accepted)" bad
  elif ! grep -q "StreamLoadNullMarkerInData" "$out/sr_marker.log"; then
    report "starrocks-null-marker (wrong message)" bad
    head -3 "$out/sr_marker.log"
  elif brun run -c "$SR_CONN
LOAD INTO sr.it_nullmark2 USING stream_load REPLACE AS SELECT * FROM '$out/sr_embedded.csv';" &&
       brun run -c "CREATE CONNECTION fe TYPE mysql OPTIONS (host = '127.0.0.1', port = 39030, user = 'root', password = '', database = 'it');
LOAD INTO '$out/sr_embedded_out.csv' AS SELECT id, s FROM fe.it_nullmark2 ORDER BY id;"; then
    check starrocks-null-marker "$out/sr_embedded_out.csv" "$out/sr_embedded_expected.csv"
  else
    report "starrocks-null-marker (run error)" bad
  fi

  # read back through a starrocks connection, so the starrocks dialect renders it
  topnrt starrocks "CREATE CONNECTION db TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');" "db.it_topn USING stream_load"
  paramrt starrocks "CREATE CONNECTION db TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');"
  collrt starrocks "CREATE CONNECTION db TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');" "db.it_coll USING stream_load"
  catalogrt starrocks "CREATE CONNECTION db TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030, be_url = 'http://127.0.0.1:38040', database = 'it', user = 'root', password = '');" it
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

# S3 (SeaweedFS). Real SigV4 signing, ListObjectsV2 and ranged GETs against an S3
# implementation that is not ours. Credentials are the stock dev pair, not a
# secret. The store starts with no buckets and its image ships `weed shell`, so
# the suite creates the bucket here rather than via a one-shot init container.
if runs s3; then
  export AWS_ENDPOINT_URL="http://127.0.0.1:39000"
  export AWS_ACCESS_KEY_ID="minioadmin"
  export AWS_SECRET_ACCESS_KEY="minioadmin"
  export AWS_REGION="us-east-1"

  docker compose -f it/compose.yaml exec -T s3 sh -c \
    "echo 's3.bucket.create -name basalt-it' | weed shell" >/dev/null 2>&1

  # Single object: out through the writer, back through the signed reader — a
  # green run exercises both halves of the SigV4 path.
  if brun run -c "LOAD INTO 's3://basalt-it/seed.csv' AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "LOAD INTO '$out/s3.csv' AS SELECT * FROM 's3://basalt-it/seed.csv' ORDER BY id;"; then
    check s3 "$out/s3.csv" it/expected.csv
  else
    report "s3 (run error)" bad
  fi

  # Prefix read: two objects under one prefix must read back as a single table,
  # in listing order. ListObjectsV2 paging or ordering bugs show up here and
  # nowhere else.
  if brun run -c "LOAD INTO 's3://basalt-it/parts/a.csv' AS SELECT * FROM 'it/seed.csv' WHERE id <= 1;" &&
     brun run -c "LOAD INTO 's3://basalt-it/parts/b.csv' AS SELECT * FROM 'it/seed.csv' WHERE id > 1;" &&
     brun run -c "LOAD INTO '$out/s3_prefix.csv' AS SELECT * FROM 's3://basalt-it/parts/' ORDER BY id;"; then
    check s3-prefix "$out/s3_prefix.csv" it/expected.csv
  else
    report "s3-prefix (run error)" bad
  fi

  # Parquet out to an object and back — the routing guard, same as azure-parquet:
  # an s3:// target must not be opened as a local file named `s3:`.
  if brun run -c "LOAD INTO 's3://basalt-it/bronze/seed.parquet' AS SELECT * FROM 'it/seed.csv';" &&
     brun run -c "LOAD INTO '$out/s3_parquet.csv' AS
SELECT * FROM 's3://basalt-it/bronze/seed.parquet' ORDER BY id;"; then
    check s3-parquet "$out/s3_parquet.csv" it/expected.csv
  else
    report "s3-parquet (run error)" bad
  fi

  # Ranged read: at ~4.1MB the volume parquet spans several ranged GETs, and
  # SigV4 signs the Range header — a signing mistake surfaces here as a 403,
  # not as wrong data.
  if brun run -c "LOAD INTO 's3://basalt-it/bronze/vol.parquet' AS SELECT * FROM '$volcsv';" &&
     brun run -c "LOAD INTO '$out/s3_parquet_ranged.csv' AS
SELECT COUNT(*) AS rows, SUM(id) AS ids, SUM(val) AS vals FROM 's3://basalt-it/bronze/vol.parquet';"; then
    check s3-parquet-ranged "$out/s3_parquet_ranged.csv" "$out/vol_expected.csv"
  else
    report "s3-parquet-ranged (run error)" bad
  fi

  # Projection: two columns of three, so the val chunks need never be fetched —
  # the point of ranged reads. Correctness check, not a transfer-volume one: it
  # would still pass on a whole-object fallback.
  printf 'id,name\n1,alpha\n2,"beta, gamma"\n3,delta\n' >"$out/s3_proj_expected.csv"
  if brun run -c "LOAD INTO '$out/s3_parquet_proj.csv' AS
SELECT id, name FROM 's3://basalt-it/bronze/seed.parquet' ORDER BY id;"; then
    check s3-parquet-projection "$out/s3_parquet_proj.csv" "$out/s3_proj_expected.csv"
  else
    report "s3-parquet-projection (run error)" bad
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

  # Decimals past 18 digits need FIXED_LEN_BYTE_ARRAY: `12.5` in a DECIMAL(38,18)
  # column restates to 1.25e19, which INT64 cannot hold — it was first clamped to
  # 9.223372036854775807 and then rejected outright. DECIMAL(30,20) additionally
  # used to emit scale > precision, a schema no reader accepts.
  printf 'v\n12.5\n' >"$out/dec_in.csv"
  if brun run -c "LOAD INTO '$out/dec_wide.parquet' AS SELECT CAST(v AS DECIMAL(38,18)) AS d FROM '$out/dec_in.csv';" &&
     brun run -c "LOAD INTO '$out/dec_wide.csv' AS SELECT * FROM '$out/dec_wide.parquet';" &&
     brun run -c "LOAD INTO '$out/dec_badscale.parquet' AS SELECT CAST(v AS DECIMAL(30,20)) AS d FROM '$out/dec_in.csv';" &&
     brun run -c "LOAD INTO '$out/dec_badscale.csv' AS SELECT * FROM '$out/dec_badscale.parquet';"; then
    { echo "d"; echo "12.500000000000000000"; } >"$out/dec_wide_expected.csv"
    check parquet-decimal-wide "$out/dec_wide.csv" "$out/dec_wide_expected.csv"
    { echo "d"; echo "12.50000000000000000000"; } >"$out/dec_badscale_expected.csv"
    check parquet-decimal-scale-over-18 "$out/dec_badscale.csv" "$out/dec_badscale_expected.csv"
  else
    report "parquet-decimal-wide (run error)" bad
  fi

  # scale > precision is still no DECIMAL at all, and must say so rather than
  # writing a schema Spark and Arrow reject.
  rm -f "$out/dec_impossible.parquet"
  if $B run -q -c "LOAD INTO '$out/dec_impossible.parquet' AS SELECT CAST(v AS DECIMAL(10,12)) AS d FROM '$out/dec_in.csv';" >"$out/dec_bad.log" 2>&1; then
    report "parquet-decimal-impossible (scale > precision was accepted)" bad
  elif grep -q "UnsupportedParquetDecimal" "$out/dec_bad.log"; then
    report parquet-decimal-impossible ok
  else
    report "parquet-decimal-impossible (wrong message)" bad
    head -3 "$out/dec_bad.log"
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

  # `DISTINCT ON (k)` keeps the *first* row per key. Under -j>1 each lane
  # deduped its own chunk and whichever one reached the merge mutex first
  # supplied the non-key columns, so `v` changed between two -j 8 runs and
  # disagreed with -j 1. Row counts were always right, which is why it hid.
  # Each format is its own oracle: a parallel write interleaves row groups, so
  # dup.parquet is not in dup.csv's row order and "first row per key" is a
  # different row in each. -j 1 vs -j 8 over the *same* file is the comparison
  # that means anything.
  dupcsv="$out/dup.csv"
  { echo "k,v"; awk 'BEGIN{for(i=0;i<400000;i++) printf "%d,%d\n", i%500, i}'; } > "$dupcsv"
  if brun run -c "LOAD INTO '$out/dup.parquet' AS SELECT * FROM '$dupcsv';" &&
     brun run -j 1 -c "LOAD INTO '$out/dist_csv_ser.csv' AS SELECT DISTINCT ON (k) k, v FROM '$dupcsv';" &&
     brun run -j 8 -c "LOAD INTO '$out/dist_csv_par.csv' AS SELECT DISTINCT ON (k) k, v FROM '$dupcsv';" &&
     brun run -j 1 -c "LOAD INTO '$out/dist_pq_ser.csv' AS SELECT DISTINCT ON (k) k, v FROM '$out/dup.parquet';" &&
     brun run -j 8 -c "LOAD INTO '$out/dist_pq_par.csv' AS SELECT DISTINCT ON (k) k, v FROM '$out/dup.parquet';"; then
    check distinct-on-parallel-csv "$out/dist_csv_par.csv" "$out/dist_csv_ser.csv"
    check distinct-on-parallel-parquet "$out/dist_pq_par.csv" "$out/dist_pq_ser.csv"
  else
    report "distinct-on-parallel (run error)" bad
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

    # Our own reader agreeing with our own writer proves little about a
    # FIXED_LEN_BYTE_ARRAY decimal — the byte order and width are exactly what a
    # second implementation has to confirm.
    if "$DUCK" -c "COPY (SELECT d FROM '$out/dec_wide.parquet') TO '$out/dec_duck.csv' (FORMAT CSV, HEADER);" >/dev/null 2>&1; then
      check parquet-decimal-wide-interop "$out/dec_duck.csv" "$out/dec_wide_expected.csv"
    else
      report "parquet-decimal-wide-interop (duckdb could not read the decimal)" bad
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

# Arrow: the stream on stdout must be what an independent reader decodes, so the
# seed round-trip goes through pyarrow and is held to the same it/expected.csv as
# every backend. Rows are re-serialised by hand so the compare is byte-exact.
if runs arrow; then
  if ! command -v uv >/dev/null; then
    report "arrow (uv not installed, skipped)" bad
  elif $B run -q --format arrow -c "SELECT * FROM 'it/seed.csv' ORDER BY id;" >"$out/seed.arrows" 2>"$out/arrow.log" &&
       uv run --quiet --with pyarrow python - "$out/seed.arrows" "$out/arrow_rt.csv" <<'PY' 2>>"$out/arrow.log"
import sys, pyarrow.ipc as ipc
t = ipc.open_stream(sys.argv[1]).read_all()
t.validate(full=True)
def cell(v):
    if v is None: return ""
    s = str(v)
    return '"' + s.replace('"', '""') + '"' if any(c in s for c in ',"\n') else s
with open(sys.argv[2], "w") as f:
    f.write(",".join(t.column_names) + "\n")
    for row in t.to_pylist():
        f.write(",".join(cell(row[c]) for c in t.column_names) + "\n")
PY
  then
    check arrow "$out/arrow_rt.csv" it/expected.csv
  else
    report "arrow (run error)" bad
    tail -5 "$out/arrow.log"
  fi

  # Several results back to back: each stream names its statement, kind and
  # position, and ends with a zero-row trailer batch carrying its totals. An
  # EXPLAIN is a result of its own under arrow, a `plan` column.
  if command -v uv >/dev/null &&
     $B run -q --format arrow -c "SELECT id FROM 'it/seed.csv' ORDER BY id;
  SELECT 'x' AS b FROM RANGE(3);
EXPLAIN SELECT range FROM RANGE(5) WHERE range > 2;
DESCRIBE SELECT 1 AS z;" >"$out/multi.arrows" 2>>"$out/arrow.log" &&
     uv run --quiet --with pyarrow python - "$out/multi.arrows" <<'PY' >"$out/arrow_multi.txt" 2>>"$out/arrow.log"
import sys, pyarrow as pa, pyarrow.ipc as ipc
f = pa.OSFile(sys.argv[1])
while f.tell() < f.size():
    r = ipc.open_stream(f)
    md = {k.decode(): v.decode() for k, v in r.schema.metadata.items()}
    batches, trailer = [], {}
    while True:
        try:
            b, cm = r.read_next_batch_with_custom_metadata()
        except StopIteration:
            break
        if cm is not None:
            trailer = {k.decode(): v.decode() for k, v in cm.items()}
        batches.append(b)
    t = pa.Table.from_batches(batches, r.schema)
    t.validate(full=True)
    assert trailer["basalt.rows"] == str(t.num_rows), (trailer, t.num_rows)
    assert int(trailer["basalt.elapsed_ms"]) >= 0
    print(md["basalt.statement"], md["basalt.kind"], md["basalt.line"], md["basalt.col"], t.column_names[0], t.num_rows > 0)
PY
  then
    printf '0 select 1 1 id True\n1 select 2 3 b True\n2 explain 3 1 plan True\n3 describe 4 1 column True\n' >"$out/arrow_multi_want.txt"
    check "arrow: several results describe themselves" "$out/arrow_multi.txt" "$out/arrow_multi_want.txt"
  else
    report "arrow: several results describe themselves (run error)" bad
    tail -5 "$out/arrow.log"
  fi

  # Arrow IPC as a source and a sink. pyarrow writes the seed as an LZ4-compressed
  # Feather file, basalt reads it back; basalt writes `.arrow`, pyarrow reads it.
  # Both must come out as the seed itself.
  if command -v uv >/dev/null &&
     uv run --quiet --with pyarrow python - "$out/seed.feather" <<'PY' 2>>"$out/arrow.log" &&
import sys, pyarrow.csv as pcsv, pyarrow.feather as feather
feather.write_feather(pcsv.read_csv("it/seed.csv"), sys.argv[1], compression="lz4")
PY
     $B run -q -j 1 --format csv -c "SELECT * FROM '$out/seed.feather' ORDER BY id;" >"$out/feather_rt.csv" 2>>"$out/arrow.log"; then
    check "arrow: a pyarrow Feather file reads back as the seed" "$out/feather_rt.csv" it/expected.csv
  else
    report "arrow: a pyarrow Feather file reads back as the seed (run error)" bad
    tail -5 "$out/arrow.log"
  fi
  # Nested parquet columns — lists of structs, maps, any nesting — rebuilt as
  # JSON, cell by cell against pyarrow's reading, in three page layouts.
  if command -v uv >/dev/null && uv run --quiet --with pyarrow python it/parquet_nested.py "$B" "$out" >"$out/nested.log" 2>&1; then
    report "parquet: nested columns match pyarrow cell for cell" ok
  else
    report "parquet: nested columns match pyarrow cell for cell" bad
    tail -8 "$out/nested.log"
  fi
  if command -v uv >/dev/null &&
     brun run -c "LOAD INTO '$out/seed.arrow' AS SELECT * FROM 'it/seed.csv' ORDER BY id;" &&
     uv run --quiet --with pyarrow python - "$out/seed.arrow" "$out/arrow_sink_rt.csv" <<'PY' 2>>"$out/arrow.log"
import sys, pyarrow.ipc as ipc
t = ipc.open_file(sys.argv[1]).read_all()
t.validate(full=True)
def cell(v):
    if v is None: return ""
    s = str(v)
    return '"' + s.replace('"', '""') + '"' if any(c in s for c in ',"\n') else s
with open(sys.argv[2], "w") as f:
    f.write(",".join(t.column_names) + "\n")
    for row in t.to_pylist():
        f.write(",".join(cell(row[c]) for c in t.column_names) + "\n")
PY
  then
    check "arrow: LOAD INTO .arrow is a file pyarrow reads" "$out/arrow_sink_rt.csv" it/expected.csv
  else
    report "arrow: LOAD INTO .arrow is a file pyarrow reads (run error)" bad
    tail -5 "$out/arrow.log"
  fi
fi

# What `basalt run` puts on stdout, per --format. No container: the contract under
# test is the CLI's own — rows and nothing else for a program, and a result that is
# the same bytes whether stdout is a pipe or a file.
if runs stdout; then
  sel="SELECT * FROM 'it/seed.csv' ORDER BY id;"

  # csv is the seed back again: same quoting as a .csv sink, no rule, no footer.
  if $B run -q -j 1 --format csv -c "$sel" >"$out/stdout.csv" 2>"$out/stdout.log"; then
    check "stdout csv" "$out/stdout.csv" it/expected.csv
  else
    report "stdout csv (run error)" bad
    tail -5 "$out/stdout.log"
  fi

  # tsv carries the same cells; a field holding a comma needs no quotes under a tab.
  if $B run -q -j 1 --format tsv -c "$sel" >"$out/stdout.tsv" 2>>"$out/stdout.log" &&
     $B run -q -j 1 --format csv -c "SELECT * FROM '$out/stdout.tsv' WITH (format = 'csv', delimiter = '\t') ORDER BY id;" >"$out/stdout_tsv_rt.csv" 2>>"$out/stdout.log"; then
    check "stdout tsv" "$out/stdout_tsv_rt.csv" it/expected.csv
    if grep -q '"beta, gamma"' "$out/stdout.tsv"; then report "stdout tsv quotes only what a tab demands" bad; else report "stdout tsv quotes only what a tab demands" ok; fi
  else
    report "stdout tsv (run error)" bad
    tail -5 "$out/stdout.log"
  fi

  # Nothing but data on stdout: the summary and the logs stay on stderr.
  rows=$(($(wc -l <it/expected.csv)))
  got=$($B run --format csv -c "LOAD INTO '$out/stdout_side.csv' AS SELECT * FROM 'it/seed.csv'; $sel" 2>/dev/null | wc -l)
  if [ "$got" -eq "$rows" ]; then report "stdout csv carries rows only beside a LOAD" ok; else report "stdout csv carries rows only beside a LOAD ($got lines, want $rows)" bad; fi

  # A second SELECT appends. Redirected to a file it used to start again at offset
  # 0 and overwrite the first, while the same run through a pipe looked fine.
  for fmt in table csv json; do
    two="SELECT 1 AS first_q; SELECT 2 AS second_q;"
    $B run -q --format "$fmt" -c "$two" >"$out/two_file.$fmt" 2>/dev/null
    $B run -q --format "$fmt" -c "$two" 2>/dev/null | cat >"$out/two_pipe.$fmt"
    if grep -q first_q "$out/two_file.$fmt" && grep -q second_q "$out/two_file.$fmt" &&
       cmp -s "$out/two_file.$fmt" "$out/two_pipe.$fmt"; then
      report "stdout $fmt: two SELECTs to a file match the pipe" ok
    else
      report "stdout $fmt: two SELECTs to a file match the pipe" bad
      head -8 "$out/two_file.$fmt"
    fi
  done

  # `run` prints the whole table however narrow the terminal claims to be; only
  # the REPL fits one. `script` supplies the terminal, so skip where it is missing.
  if command -v script >/dev/null; then
    wide="SELECT range AS id, 'a fairly long value for row ' || range AS a, 'another long value for row ' || range AS b, 'tail-' || range AS last_col FROM RANGE(60);"
    script -qfc "stty cols 40; $B run -q -c \"$wide\"" /dev/null 2>/dev/null | tr -d '\r' >"$out/tty.txt"
    $B run -q -c "$wide" >"$out/pipe.txt" 2>/dev/null
    if cmp -s "$out/tty.txt" "$out/pipe.txt" && grep -q "last_col" "$out/tty.txt" && grep -q "^(60 rows)" "$out/tty.txt"; then
      report "stdout table: a terminal gets the same full table as a pipe" ok
    else
      report "stdout table: a terminal gets the same full table as a pipe" bad
      diff "$out/pipe.txt" "$out/tty.txt" | head -6 || true
    fi
  fi

  # Flags may come before the script, and `-` reads it from stdin wherever it sits.
  printf 'SELECT 1 AS a;\n' >"$out/flags_first.sql"
  got=$($B run -q --format csv "$out/flags_first.sql" 2>&1 | tr '\n' ' ')
  got2=$(printf 'SELECT 2 AS b;\n' | $B run -q --format csv - 2>&1 | tr '\n' ' ')
  if [ "$got" = "a 1 " ] && [ "$got2" = "b 2 " ]; then report "stdout: flags before the script path, and - after flags" ok
  else report "stdout: flags before the script path, and - after flags (got '$got' / '$got2')" bad; fi

  # Under --log-format json a SELECT reports its run too, on stderr, with what the
  # parquet reader was spared; stdout keeps only the rows.
  $B run -q --log-format json --format csv -c "SELECT id FROM '$out/stdout_side.csv';" >/dev/null 2>"$out/sel_summary.log" || true
  if grep -q '"event":"run_complete"' "$out/sel_summary.log"; then report "stdout: a SELECT's run_complete under --log-format json" ok
  else report "stdout: a SELECT's run_complete under --log-format json" bad; tail -3 "$out/sel_summary.log"; fi

  # A long LOAD under --log-format json says how it is going, once a second.
  timeout 3 $B run --log-format json -c "LOAD INTO '$out/endless.csv' AS SELECT range FROM RANGE(100000000000);" >/dev/null 2>"$out/progress.log" || true
  if grep -q '"event":"progress".*"rows":' "$out/progress.log"; then report "stdout: progress events under --log-format json" ok
  else report "stdout: progress events under --log-format json" bad; head -3 "$out/progress.log"; fi

  # Editor support: `check --format json` is an array of placed diagnostics, and
  # `complete` says what Tab would offer, as JSON.
  if $B check --format json -c "SELECT range FROM RANGE(2);" >"$out/chk_ok.json" 2>/dev/null &&
     ! $B check --format json -c "SELECT nope FROM RANGE(2);" >"$out/chk_bad.json" 2>/dev/null &&
     $B complete -c "SELECT range FROM RANGE(2) ORDER BY ran" >"$out/cmp.json" 2>/dev/null &&
     python3 - "$out" <<'PY' 2>>"$out/jerr.log"
import json, sys
d = sys.argv[1]
assert json.load(open(d + "/chk_ok.json")) == []
bad = json.load(open(d + "/chk_bad.json"))
assert len(bad) == 1 and bad[0]["col"] == 8 and bad[0]["end_col"] == 12, bad
cmp = json.load(open(d + "/cmp.json"))
assert {"text": "range", "kind": "keyword"} in cmp["items"] or any(i["text"].lower() == "range" for i in cmp["items"]), cmp
PY
  then report "stdout: check --format json and complete speak JSON" ok
  else report "stdout: check --format json and complete speak JSON" bad; cat "$out/chk_bad.json" "$out/cmp.json" 2>/dev/null | head -5; fi

  # `check` goes past its first error, parse and analysis alike, and `--known`
  # names tables the script reads without declaring.
  if ! $B check --format json --known enrich -c "SELECT nope FROM RANGE(2);
SELECT x FROM enrich WHERE y > 1;
SELECT 1 +;
SELECT alsonope FROM RANGE(2);" >"$out/chk_many.json" 2>/dev/null &&
     python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
assert [(e["line"], e["msg"].split("`")[1] if "`" in e["msg"] else "syntax") for e in d] == [(1, "nope"), (3, "syntax"), (4, "alsonope")], d
' "$out/chk_many.json"
  then report "stdout: check reports every problem, known tables included" ok
  else report "stdout: check reports every problem, known tables included" bad; cat "$out/chk_many.json"; fi

  # --max-rows: the first N rows of an endless source, at once, then a clean end.
  got=$(timeout 10 $B run -q --max-rows 3 --format csv -c "SELECT range FROM RANGE(100000000000);" 2>/dev/null | tr '\n' ' ')
  if [ "$got" = "range 0 1 2 " ]; then report "stdout: --max-rows previews an endless source" ok; else report "stdout: --max-rows previews an endless source (got '$got')" bad; fi

  # Under --log-format json an error is one NDJSON object an editor can place:
  # the span of the offending name, on its own line of a multi-line statement.
  $B run --log-format json -c "SELECT id,
       upper(nope) AS u
FROM 'it/seed.csv';" >/dev/null 2>"$out/jerr.log" || true
  if python3 -c '
import json, sys
e = json.loads(open(sys.argv[1]).read().strip().splitlines()[-1])
assert e["level"] == "error" and e["class"] == "permanent", e
assert (e["line"], e["col"], e["end_line"], e["end_col"]) == (2, 14, 2, 18), e
' "$out/jerr.log" 2>>"$out/jerr.log"; then
    report "stdout: a json-log error carries the offending span" ok
  else
    report "stdout: a json-log error carries the offending span" bad
    tail -3 "$out/jerr.log"
  fi

  if $B run --format xml -c "SELECT 1;" >/dev/null 2>"$out/badfmt.log"; then
    report "stdout: an unknown --format is refused" bad
  elif grep -q "table|json|csv|tsv|arrow" "$out/badfmt.log"; then
    report "stdout: an unknown --format is refused" ok
  else
    report "stdout: an unknown --format is refused" bad
  fi

  # The split-parallel union re-reads each branch's table by key range; a branch that
  # is a query has none, and at -j > 1 it opened an empty path instead.
  printf 'k\n1\n2\n' >"$out/ua.csv"; printf 'k\n3\n' >"$out/ub.csv"
  if $B run -q -j 4 --format csv -c "SELECT k FROM '$out/ua.csv' WHERE k > 1 UNION ALL BY NAME SELECT k FROM '$out/ub.csv';" >"$out/uq.csv" 2>"$out/uq.log" &&
     [ "$(tr -d '\r' <"$out/uq.csv" | paste -sd' ')" = "k 2 3" ]; then
    report "stdout: a union of queries runs at -j 4" ok
  else
    report "stdout: a union of queries runs at -j 4" bad
    cat "$out/uq.log" "$out/uq.csv"
  fi
fi

# `basalt kernel`: the session protocol a notebook drives. The driver speaks it
# over pipes to the real binary and prints its own PASS/FAIL lines.
if runs kernel; then
  if ! command -v python3 >/dev/null; then
    report "kernel (python3 not installed, skipped)" bad
  else
    python3 it/kernel.py "$B" >"$out/kernel.log" 2>&1 || true
    grep -E '^(PASS|FAIL) ' "$out/kernel.log" || tail -20 "$out/kernel.log"
    pass=$((pass + $(grep -c '^PASS ' "$out/kernel.log" || true)))
    fail=$((fail + $(grep -c '^FAIL ' "$out/kernel.log" || true)))
    grep -q '^==> kernel:' "$out/kernel.log" || { report "kernel (driver crashed)" bad; tail -20 "$out/kernel.log"; }
  fi
fi

echo "==> $pass passed, $fail failed"
[ "$fail" -eq 0 ]

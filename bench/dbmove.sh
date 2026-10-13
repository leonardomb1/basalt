#!/usr/bin/env bash
# SQL Server -> StarRocks movement benchmark: basalt's main real-world shape, on the
# integration stack's `mssql` and `starrocks` containers (tests/integration/compose.yaml).
# It times bench/dbmove.sql and splits the time between reading SQL Server, encoding
# the stream-load body and waiting on StarRocks, from basalt's debug log.
#
#   bench/dbmove.sh up              start mssql + starrocks (StarRocks takes minutes)
#   bench/dbmove.sh gen [ROWS]      create database `bench`: ORD010/ORD020/ORD030 (150
#                                   columns, ROWS each, default 1000000) and SGL001..SGL050
#   bench/dbmove.sh run [SHAPE...]  full-j4 incr-j1 incr-j4 singles-j1 singles-j4
#                                   (default: all, in this order). `full` recreates the bronze
#                                   database (its JOBS 4 makes -j moot); `incr` upserts into
#                                   the table `full` made, after touching 1% of UPDATED_AT
#   bench/dbmove.sh down            stop the stack, dropping its volumes
#
# BASALT=path picks the binary (default zig-out/bin/basalt; build it ReleaseFast), SCRIPT=path
# a variant of bench/dbmove.sql for an A/B run.
# OUT=dir keeps the logs (default a temp dir). Each run prints wall, CPU, RSS, rows/s,
# MB/s of stream-load body, stream-load transactions, the lane-time split and the
# connections opened (counted by strace on connect(2), per port).
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f tests/integration/compose.yaml"
B=${BASALT:-zig-out/bin/basalt}
SCRIPT=${SCRIPT:-bench/dbmove.sql}
OUT=${OUT:-$(mktemp -d)}
SQLCMD=(docker exec -i it-mssql-1 /opt/mssql-tools18/bin/sqlcmd -C -U sa -P It_Passw0rd1 -b -h -1 -W)

tsql() { "${SQLCMD[@]}" "$@"; }

company_ddl() { # $1 table name
  local cols="" i n
  local lens=(2 3 4 6 8 10 15 20 30 40)
  for i in $(seq -w 1 100); do
    n=${lens[$((10#$i % 10))]}
    cols+="ORD_C$i char($n) NOT NULL,"
  done
  for i in $(seq -w 1 20); do cols+="ORD_V$i varchar($((10 + 10#$i * 3))) NOT NULL,"; done
  for i in $(seq -w 1 18); do cols+="ORD_N$i numeric(14,2) NOT NULL,"; done
  cols+="ORD_Q01 numeric(12,4) NOT NULL,ORD_Q02 numeric(12,4) NOT NULL,"
  cols+="ORD_F01 float NOT NULL,ORD_F02 float NOT NULL,"
  for i in 1 2 3 4 5; do cols+="ORD_D0$i char(8) NOT NULL,"; done
  echo "CREATE TABLE dbo.$1 ($cols IS_DELETED char(1) NOT NULL, ROW_ID int IDENTITY PRIMARY KEY CLUSTERED, UPDATED_AT datetime NOT NULL);"
}

company_select() { # select list over n (row number) from the numbers CTE
  local sel="" i n k
  local lens=(2 3 4 6 8 10 15 20 30 40)
  for i in $(seq -w 1 100); do
    n=${lens[$((10#$i % 10))]}
    k=$((10#$i))
    if ((k % 5 == 0)); then sel+="'',"
    else sel+="RIGHT('000000000' + CAST((n * $k) % 9973 AS varchar(10)), $((n < 9 ? n : 9))),"
    fi
  done
  for i in $(seq -w 1 20); do
    n=$((10 + 10#$i * 3))
    sel+="LEFT('ITEM ' + CAST(n % 50021 AS varchar(10)) + ' DESC $i' + SPACE($n), $n),"
  done
  for i in $(seq 1 18); do sel+="CAST((n % 100003) * $i / 7.0 AS numeric(14,2)),"; done
  sel+="CAST((n % 997) / 3.0 AS numeric(12,4)),CAST((n % 991) / 7.0 AS numeric(12,4)),"
  sel+="(n % 10007) / 13.0,(n % 1009) * 1.5,"
  for i in 1 2 3 4 5; do sel+="CONVERT(char(8), DATEADD(DAY, (n * $i) % 3000, '20180101'), 112),"; done
  echo "${sel}CASE WHEN n % 100 = 0 THEN '*' ELSE '' END, DATEADD(SECOND, -(n % 86400) - 7200, GETDATE())"
}

company_cols() {
  local c="" i
  for i in $(seq -w 1 100); do c+="ORD_C$i,"; done
  for i in $(seq -w 1 20); do c+="ORD_V$i,"; done
  for i in $(seq -w 1 18); do c+="ORD_N$i,"; done
  echo "${c}ORD_Q01,ORD_Q02,ORD_F01,ORD_F02,ORD_D01,ORD_D02,ORD_D03,ORD_D04,ORD_D05,IS_DELETED,UPDATED_AT"
}

numbers() { # CTE `nums(n)` with $1 rows
  echo "WITH e1(x) AS (SELECT 1 FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) v(x)),
e6(x) AS (SELECT 1 FROM e1 a, e1 b, e1 c, e1 d, e1 e, e1 f),
e7(x) AS (SELECT 1 FROM e6 a, e1 b),
nums(n) AS (SELECT TOP ($1) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM e7)"
}

gen() {
  local rows=${1:-1000000} cols t i r
  cols=$(company_cols)
  tsql -Q "IF DB_ID('bench') IS NOT NULL BEGIN ALTER DATABASE bench SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE bench; END;
CREATE DATABASE bench; ALTER DATABASE bench SET RECOVERY SIMPLE;"
  for t in ORD010 ORD020 ORD030; do tsql -d bench -Q "$(company_ddl $t)"; done
  echo "==> ORD010: $rows rows"
  time tsql -d bench -Q "$(numbers "$rows")
INSERT INTO dbo.ORD010 WITH (TABLOCK) ($cols) SELECT $(company_select) FROM nums;"
  for t in ORD020 ORD030; do
    echo "==> $t: copy of ORD010"
    tsql -d bench -Q "INSERT INTO dbo.$t WITH (TABLOCK) ($cols) SELECT $cols FROM dbo.ORD010 ORDER BY ROW_ID;"
  done
  echo "==> SGL001..SGL050"
  local sql=""
  for i in $(seq -w 1 50); do
    r=$((100 + (10#$i * 997) % 4900))
    sql+="SELECT TOP ($r) IDENTITY(int, 1, 1) AS ROW_ID, ORD_C001 AS SGL_C01, ORD_C002 AS SGL_C02, ORD_C003 AS SGL_C03,
ORD_C004 AS SGL_C04, ORD_C006 AS SGL_C06, ORD_C007 AS SGL_C07, ORD_C008 AS SGL_C08, ORD_C009 AS SGL_C09, ORD_C011 AS SGL_C11,
ORD_C012 AS SGL_C12, ORD_C013 AS SGL_C13, ORD_C014 AS SGL_C14, ORD_V01 AS SGL_V01, ORD_V02 AS SGL_V02, ORD_V03 AS SGL_V03,
ORD_N01 AS SGL_N01, ORD_N02 AS SGL_N02, ORD_D01 AS SGL_D01, IS_DELETED, UPDATED_AT INTO dbo.SGL0$i FROM dbo.ORD010 ORDER BY ROW_ID;
"
  done
  tsql -d bench -Q "$sql"
  tsql -d bench -Q "SELECT COUNT(*) AS tables FROM sys.tables; EXEC sp_spaceused 'dbo.ORD010';"
}

touch_window() { # move UPDATED_AT of 1% of each company table into the 30-minute window
  local t
  for t in ORD010 ORD020 ORD030; do
    tsql -d bench -Q "SET NOCOUNT ON; UPDATE dbo.$t SET UPDATED_AT = GETDATE() WHERE ROW_ID % 100 = 7;"
  done
}

conns() { # strace log -> "mssql=N fe=N be=N"
  awk '/connect\(/ && /sin_port=htons\(/ {
         match($0, /sin_port=htons\([0-9]+\)/); p = substr($0, RSTART + 15, RLENGTH - 16)
         if (p == 31433) m++; else if (p == 39030) f++; else if (p == 38040) b++ }
       END { printf "mssql=%d fe_mysql=%d be_http=%d", m, f, b }' "$1"
}

report() { # $1 shape, $2 log prefix
  local log=$2.log tv=$2.time st=$2.strace
  local wall cpu_u cpu_s rss
  wall=$(awk -F': ' '/Elapsed \(wall clock\)/ { n = split($2, a, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + a[i]; print s }' "$tv")
  cpu_u=$(awk -F': ' '/User time/ { print $2 }' "$tv")
  cpu_s=$(awk -F': ' '/System time/ { print $2 }' "$tv")
  rss=$(awk -F': ' '/Maximum resident/ { printf "%.0f", $2 / 1024 }' "$tv")
  echo "== $1: wall ${wall}s, cpu user ${cpu_u}s sys ${cpu_s}s, max rss ${rss} MB, conns: $(conns "$st")"
  awk '{ printf "   server cpu: mssql %.1fs, starrocks %.1fs\n", $1 / 1e3, $2 / 1e3 }' "$2.servers"
  awk -v wall="$wall" '
    /stream load .* #[0-9]+:/ {
      for (i = 1; i <= NF; i++) {
        if ($(i+1) == "rows,") rows += $i
        if ($(i+1) == "bytes,") bytes += $i
        if ($i == "total") tot += $(i+1)
        if ($i == "begin") beg += $(i+1)
        if ($i == "plan") pl += $(i+1)
        if ($i == "read") rd += $(i+1)
        if ($i == "write") wr += $(i+1)
        if ($i == "commit+publish") cp += $(i+1)
      }
      loads++
    }
    / closed: / {
      for (i = 1; i <= NF; i++) {
        if ($i == "life") life += $(i+1)
        if ($i == "setup") su += $(i+1)
        if ($i == "encode") enc += $(i+1)
        if ($i == "spool") sp += $(i+1)
        if ($i == "send") snd += $(i+1)
        if ($i == "wait") wt += $(i+1)
      }
      sinks++
    }
    END {
      if (loads == 0) { print "   no stream loads logged"; exit }
      printf "   %d rows, %.1f MB body: %.0f rows/s, %.1f MB/s\n", rows, bytes / 1e6, rows / wall, bytes / 1e6 / wall
      printf "   %d stream loads over %d sinks, avg %.2f MB / %d rows each\n", loads, sinks, bytes / 1e6 / loads, rows / loads
      printf "   lane time %.1fs: sink setup %.0f%%, read+decode %.0f%%, encode %.0f%%, spool %.0f%%, blocked sending %.0f%%, waiting on commit %.0f%%; setup avg %.0f ms/sink\n", \
        life / 1e3, 100 * su / life, 100 * (life - su - enc - sp - snd - wt) / life, 100 * enc / life, 100 * sp / life, 100 * snd / life, 100 * wt / life, su / sinks
      printf "   StarRocks per load (avg ms): total %.0f = begin %.0f + plan %.0f + read %.0f + write %.0f + commit/publish %.0f; client-side commit wait avg %.0f\n", \
        tot / loads, beg / loads, pl / loads, rd / loads, wr / loads, cp / loads, wt / loads
    }' "$log"
}

cpu_us() { # $1 container: CPU microseconds used so far
  docker exec "$1" awk '/^usage_usec/ { print $2 }' /sys/fs/cgroup/cpu.stat
}

run_one() { # $1 shape name, $2 basalt shape param, $3 -j
  local p="$OUT/$1" m0 s0
  m0=$(cpu_us it-mssql-1) s0=$(cpu_us it-starrocks-1)
  strace -f --seccomp-bpf -qq -e trace=connect -o "$p.strace" \
    /usr/bin/time -v -o "$p.time" \
    "$B" run "$SCRIPT" -p shape="$2" -j "$3" --log-level debug --log-format json >"$p.out" 2>"$p.log" || {
      echo "== $1 failed"; tail -20 "$p.log"; return 1; }
  echo "$(( ($(cpu_us it-mssql-1) - m0) / 1000 )) $(( ($(cpu_us it-starrocks-1) - s0) / 1000 ))" >"$p.servers"
  report "$1" "$p"
}

drop_targets() {
  docker exec it-starrocks-1 mysql -h127.0.0.1 -P9030 -uroot -e "DROP DATABASE IF EXISTS bronze FORCE; CREATE DATABASE bronze"
}

run() {
  local shapes=${*:-full-j4 incr-j1 incr-j4 singles-j1 singles-j4} s
  mkdir -p "$OUT"
  echo "logs: $OUT"
  for s in $shapes; do
    case $s in
      full-*) drop_targets ;;
      incr-*) touch_window ;;
    esac
    run_one "$s" "${s%-j*}" "${s##*-j}"
  done
}

case ${1:-} in
  up) $COMPOSE up -d --wait mssql starrocks ;;
  gen) shift; gen "$@" ;;
  run) shift; run "$@" ;;
  down) $COMPOSE down -v ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac

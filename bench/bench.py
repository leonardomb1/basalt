"""basalt against DuckDB, ClickHouse Local, Polars and DataFusion: startup, query
time and memory, on TPC-H data generated here.

Every engine runs as a process, one per query and repetition, so all are measured
the same way: the process's wall time, its peak resident memory (from the kernel
when it exits) and its average resident memory (sampled from /proc every
millisecond while it runs). An engine's startup is the same measurement of a
`SELECT 1`; a query's time is its wall time less that. DuckDB and ClickHouse run
as their CLIs, as basalt does; Polars and DataFusion, which have none, as a small
Python process that imports the library, registers the tables and runs the query.
Engines take turns on each repetition, so warm caches and heat favour none.

    uv run --with duckdb --with polars --with datafusion --with pyarrow \\
        python bench/bench.py gen --sf 1
    uv run ... python bench/bench.py check
    uv run ... python bench/bench.py run -n 3

`gen` writes the data outside the repository (`--data`, default
~/.local/share/basalt-bench/data/sf<N>, as `parquet/`, `csv/` and `ndjson/`); `check` compares every engine's answers
with DuckDB's at a small scale; `run` times them and writes bench/results/.
"""

import argparse
import csv
import datetime
import io
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
HOME = os.path.expanduser("~/.local/share/basalt-bench")
TABLES = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]
ENGINES = ["basalt", "duckdb", "clickhouse", "polars", "datafusion"]
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024


def tasks():
    """The analytical queries (bench/analytics/*.sql, with per-engine overrides
    `<name>.<engine>.sql`) and the movement tasks (bench/movement.json)."""
    out = []
    folder = os.path.join(ROOT, "analytics")
    for f in sorted(os.listdir(folder)):
        m = re.fullmatch(r"([a-z0-9_]+)\.sql", f)
        if not m:
            continue
        name = m.group(1)
        base = open(os.path.join(folder, f)).read()
        per = {}
        for e in ENGINES:
            o = os.path.join(folder, f"{name}.{e}.sql")
            per[e] = open(o).read() if os.path.exists(o) else base
        out.append({"suite": "analytics", "name": name, "per": per})
    for name, per in json.load(open(os.path.join(ROOT, "movement.json"))).items():
        out.append({"suite": "movement", "name": name, "per": per})
    return out


def table_ref(engine, table, data):
    """How engine `engine` names table `table` of the data in `data`."""
    path = os.path.join(data, "parquet", f"{table}.parquet")
    return {
        "basalt": f"'{path}'",
        "duckdb": f"read_parquet('{path}')",
        "clickhouse": f"file('{path}', Parquet)",
    }.get(engine, table)


def render(engine, text, data, out):
    """`text` with its `{table}`, `{data}` and `{out}` placeholders filled in."""
    def sub(m):
        key = m.group(1)
        if key == "data":
            return data
        if key == "out":
            return out
        if key in TABLES:
            return table_ref(engine, key, data)
        return m.group(0)
    return re.sub(r"\{([a-z]+)\}", sub, text)


def command(engine, text, data, out, threads, tools, check):
    """The process that runs `text` on `engine`, with its environment; under
    `check` it prints the result as CSV with a header."""
    env = dict(os.environ)
    if engine == "basalt":
        cmd = [tools["basalt"], "run", "-j", str(threads)]
        if check:
            cmd += ["--format", "csv"]
        return cmd + ["-c", text], env
    if engine == "duckdb":
        cmd = [tools["duckdb"], "-batch"] + (["-csv"] if check else [])
        return cmd + ["-c", f"SET threads = {threads};", "-c", text], env
    if engine == "clickhouse":
        q = text.rstrip().rstrip(";")
        if check and not q.upper().startswith("INSERT"):
            q += " FORMAT CSVWithNames"
        return [tools["clickhouse"], "local", "--max_threads", str(threads), "--query", q], env
    env["POLARS_MAX_THREADS"] = str(threads)
    return [sys.executable, os.path.abspath(__file__), "child", engine, data, str(threads), "1" if check else "0", text], env


def child(engine, data, threads, check, text):
    """Runs `text` on Polars or DataFusion in this process: a query is collected
    whole; a statement starting with `py:` is Python, run with `data`, `out` and
    `pl` (Polars) or `ctx` (a DataFusion session) bound."""
    if engine == "polars":
        import polars as pl
        if text.startswith("py:"):
            exec(text[3:], {"pl": pl, "data": data, "out": os.environ.get("BENCH_OUT", "")})
            return
        ctx = pl.SQLContext({t: pl.scan_parquet(os.path.join(data, "parquet", f"{t}.parquet")) for t in TABLES if os.path.exists(os.path.join(data, "parquet", f"{t}.parquet"))})
        df = ctx.execute(text.rstrip().rstrip(";")).collect()
        if check:
            df.write_csv(sys.stdout)
        return
    from datafusion import SessionConfig, SessionContext
    ctx = SessionContext(SessionConfig().with_target_partitions(threads))
    if text.startswith("py:"):
        exec(text[3:], {"ctx": ctx, "data": data, "out": os.environ.get("BENCH_OUT", "")})
        return
    for t in TABLES:
        p = os.path.join(data, "parquet", f"{t}.parquet")
        if os.path.exists(p):
            ctx.register_parquet(t, p)
    batches = ctx.sql(text.rstrip().rstrip(";")).collect()
    if check:
        import pyarrow as pa
        import pyarrow.csv as pcsv
        table = pa.Table.from_batches(batches)
        cols = [c.cast(pa.string()) if str(c.type) == "string_view" else c for c in table.columns]
        pcsv.write_csv(pa.table(cols, names=table.column_names), sys.stdout.buffer)


class Run:
    """One process's wall time (s), peak and average resident memory (MB), the
    number of memory samples, its exit code and what it printed."""

    def __init__(self, wall, peak, avg, samples, code, out, err):
        self.wall, self.peak, self.avg, self.samples = wall, peak, avg, samples
        self.code, self.out, self.err = code, out, err


def measure(cmd, env, capture):
    """Runs `cmd`, sampling its resident memory every millisecond until it exits."""
    so = tempfile.TemporaryFile() if capture else subprocess.DEVNULL
    se = tempfile.TemporaryFile()
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=so, stderr=se, env=env)
    statm = f"/proc/{p.pid}/statm"
    total = 0
    n = 0
    while True:
        pid, status, ru = os.wait4(p.pid, os.WNOHANG)
        if pid != 0:
            break
        try:
            with open(statm) as f:
                total += int(f.read().split()[1]) * PAGE_KB
                n += 1
        except (OSError, ValueError, IndexError):
            pass
        time.sleep(0.001)
    wall = time.perf_counter() - t0
    p.returncode = os.waitstatus_to_exitcode(status)
    out = b""
    if capture:
        so.seek(0)
        out = so.read()
    se.seek(0)
    err = se.read().decode(errors="replace")
    return Run(wall, ru.ru_maxrss / 1024, (total / n / 1024) if n else ru.ru_maxrss / 1024, n, p.returncode, out, err)


def version(engine, tools):
    """The engine's version, as it reports it."""
    try:
        if engine == "basalt":
            return subprocess.run([tools["basalt"], "version"], capture_output=True, text=True).stdout.split()[-1]
        if engine == "duckdb":
            return subprocess.run([tools["duckdb"], "--version"], capture_output=True, text=True).stdout.split()[0]
        if engine == "clickhouse":
            return subprocess.run([tools["clickhouse"], "local", "--version"], capture_output=True, text=True).stdout.split()[3]
        mod = __import__(engine)
        return mod.__version__
    except Exception:
        return "?"


def find_tools(args):
    """The engines' programs: flags first, then PATH, then the bench's own folder."""
    def pick(flag, name, fallback):
        if flag:
            return flag
        return shutil.which(name) or fallback
    return {
        "basalt": pick(args.basalt, "basalt", os.path.join(os.path.dirname(ROOT), "zig-out", "bin", "basalt")),
        "duckdb": pick(args.duckdb, "duckdb", "duckdb"),
        "clickhouse": pick(args.clickhouse, "clickhouse", os.path.join(HOME, "bin", "clickhouse")),
    }


def data_dir(args, sf):
    return args.data or os.path.join(HOME, "data", f"sf{sf:g}")


def gen(args):
    """TPC-H at `--sf` from DuckDB's generator, laid out as `parquet/` (every
    table), `csv/` (`lineitem` as CSV and gzipped CSV) and `ndjson/` (`lineitem`),
    the movement tasks' sources. What is already there is kept."""
    if args.sf >= 10 and not args.big:
        sys.exit(f"SF{args.sf:g} writes about {args.sf:g} GB per copy of lineitem; pass --big to go ahead")
    import duckdb
    d = data_dir(args, args.sf)
    for sub in ["parquet", "csv", "ndjson"]:
        os.makedirs(os.path.join(d, sub), exist_ok=True)
    con = duckdb.connect()
    pq = os.path.join(d, "parquet")
    if not all(os.path.exists(os.path.join(pq, f"{t}.parquet")) for t in TABLES):
        con.execute("INSTALL tpch; LOAD tpch;")
        con.execute(f"CALL dbgen(sf = {args.sf})")
        for t in TABLES:
            con.execute(f"COPY {t} TO '{pq}/{t}.parquet' (FORMAT parquet)")
    src = f"read_parquet('{pq}/lineitem.parquet')"
    wants = [
        ("csv/lineitem.csv", "(HEADER)"),
        ("csv/lineitem.csv.gz", "(HEADER, COMPRESSION gzip)"),
        ("ndjson/lineitem.ndjson", "(FORMAT json)"),
    ]
    for rel, opts in wants:
        path = os.path.join(d, rel)
        if not os.path.exists(path):
            con.execute(f"COPY (SELECT * FROM {src}) TO '{path}' {opts}")
    print(f"ready: {d}")


def canon(text):
    """CSV text as rows of cells, the header row dropped: a number as a float,
    the rest trimmed text."""
    out = []
    for r in list(csv.reader(io.StringIO(text)))[1:]:
        cells = []
        for c in r:
            c = c.strip()
            try:
                cells.append(float(c))
            except ValueError:
                cells.append(c)
        out.append(cells)
    return out


def same(a, b):
    """Whether two answers agree: numbers within a relative 1e-4 (an average of
    decimals keeps six places in one engine and is a float in another), the
    rest exactly."""
    if not isinstance(a, list) or not isinstance(b, list):
        return a == b
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb):
            if isinstance(x, float) and isinstance(y, float):
                if abs(x - y) > 1e-4 * max(abs(x), abs(y), 1e-9):
                    return False
            elif x != y:
                return False
    return True


def check(args):
    """Every engine's answer to every analytical query, at `--sf` (default 0.01),
    against DuckDB's, and every movement task's output row count against its
    source's."""
    sf = args.sf if args.sf != 1 else 0.01
    d = data_dir(args, sf)
    if not os.path.exists(os.path.join(d, "parquet", "lineitem.parquet")):
        sys.exit(f"no data in {d}: run `gen --sf {sf:g}` first")
    tools = find_tools(args)
    engines = [e for e in ENGINES if not args.engine or e in args.engine]
    bad = 0
    out = tempfile.mkdtemp(prefix="basalt-bench-")
    import duckdb
    for t in tasks():
        if args.filter and args.filter not in t["name"]:
            continue
        want = None
        for e in ["duckdb"] + [x for x in engines if x != "duckdb"]:
            shutil.rmtree(out, ignore_errors=True)
            os.makedirs(out)
            text = render(e, t["per"].get(e, ""), d, out)
            if not text:
                continue
            cmd, env = command(e, text, d, out, 4, tools, t["suite"] == "analytics")
            env["BENCH_OUT"] = out
            r = measure(cmd, env, True)
            if r.code != 0:
                print(f"{t['name']} {e}: failed: {r.err.strip().splitlines()[-1] if r.err.strip() else r.code}")
                bad += 1
                continue
            if t["suite"] == "movement":
                files = [os.path.join(out, f) for f in os.listdir(out)]
                got = duckdb.connect().execute(f"SELECT count(*) FROM '{files[0]}'").fetchall()[0][0] if files else -1
            else:
                got = canon(r.out.decode(errors="replace"))
            if e == "duckdb":
                want = got
                continue
            ok = same(got, want)
            bad += 0 if ok else 1
            print(f"{t['name']} {e}: {'ok' if ok else 'DIFFERS'}")
            if not ok and args.verbose:
                print("  want", want[:5] if isinstance(want, list) else want)
                print("  got ", got[:5] if isinstance(got, list) else got)
    shutil.rmtree(out, ignore_errors=True)
    sys.exit(1 if bad else 0)


def run(args):
    """Startup, then every task on every engine, `-n` times each, engines taking
    turns; the best wall time, its memory, and a table in bench/results/."""
    d = data_dir(args, args.sf)
    if not os.path.exists(os.path.join(d, "parquet", "lineitem.parquet")):
        sys.exit(f"no data in {d}: run `gen --sf {args.sf:g}` first")
    tools = find_tools(args)
    engines = [e for e in ENGINES if not args.engine or e in args.engine]
    threads = args.threads or os.cpu_count()
    out = tempfile.mkdtemp(prefix="basalt-bench-")
    startup = {}
    for e in engines:
        best = None
        for _ in range(max(args.n, 3)):
            cmd, env = command(e, "SELECT 1", d, out, threads, tools, False)
            r = measure(cmd, env, False)
            if best is None or r.wall < best.wall:
                best = r
        startup[e] = best
        print(f"startup {e}: {best.wall * 1000:.0f} ms, {best.peak:.0f} MB peak", flush=True)
    results = []
    for t in tasks():
        if args.filter and args.filter not in t["name"]:
            continue
        best = {}
        for _ in range(args.n):
            for e in engines:
                text = t["per"].get(e, "")
                if not text:
                    continue
                shutil.rmtree(out, ignore_errors=True)
                os.makedirs(out)
                cmd, env = command(e, render(e, text, d, out), d, out, threads, tools, False)
                env["BENCH_OUT"] = out
                r = measure(cmd, env, False)
                if r.code != 0:
                    best[e] = r
                    continue
                if e not in best or best[e].code != 0 or r.wall < best[e].wall:
                    best[e] = r
        results.append((t, best))
        line = "  ".join(f"{e} {fmt_time(best[e], startup[e])}" for e in engines if e in best)
        print(f"{t['suite']}/{t['name']}: {line}", flush=True)
    shutil.rmtree(out, ignore_errors=True)
    write_results(args, d, threads, tools, engines, startup, results)


def fmt_time(r, s):
    if r.code != 0:
        return "failed"
    q = max(r.wall - s.wall, 0)
    return f"{q * 1000:.0f} ms" if q < 10 else f"{q:.1f} s"


def fmt_mem(r):
    if r.code != 0:
        return "—"
    return f"{r.peak:.0f} / {r.avg:.0f}"


def write_results(args, d, threads, tools, engines, startup, results):
    """bench/results/<date>-<cpu>.md: the machine, the versions, startup, and a
    time table and a memory table per suite."""
    cpu = "?"
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    now = datetime.date.today().isoformat()
    lines = [
        f"# {now}: SF{args.sf:g}, {threads} threads, best of {args.n}",
        "",
        f"Machine: {cpu}, {platform.system()} {platform.release()}. Data: TPC-H SF{args.sf:g} as Parquet (`bench.py gen`).",
        "",
        "Versions: " + ", ".join(f"{e} {version(e, tools)}" for e in engines) + ".",
        "",
        "Every engine runs as a process; a query's time is its wall time less the engine's",
        "startup (a `SELECT 1`). Memory is the process's resident memory in MB, peak / average",
        "(sampled every millisecond); Polars and DataFusion include Python and the library.",
        "",
        "## Startup",
        "",
        "| engine | wall | peak memory (MB) |",
        "|---|---|---|",
    ]
    for e in engines:
        lines.append(f"| {e} | {startup[e].wall * 1000:.0f} ms | {startup[e].peak:.0f} |")
    for suite in ["analytics", "movement"]:
        rows = [(t, b) for t, b in results if t["suite"] == suite]
        if not rows:
            continue
        lines += ["", f"## {suite.capitalize()}: time", "", "| task | " + " | ".join(engines) + " |", "|---|" + "---|" * len(engines)]
        for t, b in rows:
            lines.append(f"| {t['name']} | " + " | ".join(fmt_time(b[e], startup[e]) if e in b else "n/a" for e in engines) + " |")
        lines += ["", f"## {suite.capitalize()}: memory, peak / average MB", "", "| task | " + " | ".join(engines) + " |", "|---|" + "---|" * len(engines)]
        for t, b in rows:
            lines.append(f"| {t['name']} | " + " | ".join(fmt_mem(b[e]) if e in b else "n/a" for e in engines) + " |")
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "-", cpu.lower()).strip("-")[:40]
    path = os.path.join(ROOT, "results", f"{now}-{slug}.md")
    open(path, "w").write("\n".join(lines) + "\n")
    print(f"wrote {path}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "child":
        child(sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5] == "1", sys.argv[6])
        return
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("cmd", choices=["gen", "check", "run"])
    ap.add_argument("--sf", type=float, default=1)
    ap.add_argument("--big", action="store_true", help="allow SF10 and above")
    ap.add_argument("--data", default="", help="the data folder (default ~/.local/share/basalt-bench/data/sf<N>)")
    ap.add_argument("-n", type=int, default=3, help="repetitions; the best counts")
    ap.add_argument("--threads", type=int, default=0, help="threads for every engine (default: all cores)")
    ap.add_argument("--engine", action="append", default=[], choices=ENGINES)
    ap.add_argument("--filter", default="", help="only tasks whose name holds this")
    ap.add_argument("--basalt", default="")
    ap.add_argument("--duckdb", default="")
    ap.add_argument("--clickhouse", default="")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    {"gen": gen, "check": check, "run": run}[args.cmd](args)


if __name__ == "__main__":
    main()

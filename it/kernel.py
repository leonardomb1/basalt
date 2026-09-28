"""Drives `basalt kernel` over its stdin/stdout protocol, as a notebook would.

Usage: python3 it/kernel.py <basalt binary>
Prints PASS/FAIL lines; exits non-zero on any failure. Standard library only.
"""

import json
import os
import signal
import tempfile
import subprocess
import sys
import time


class Kernel:
    def __init__(self, binary, *args):
        self.p = subprocess.Popen(
            [binary, "kernel", *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def send(self, **req):
        self.p.stdin.write((json.dumps(req) + "\n").encode())
        self.p.stdin.flush()

    def reply(self):
        """Reads frames until a status: returns (data bytes, status dict).

        The bytes of each finished result, split at its `result` frame, are kept
        on `self.results` as (frame, bytes), and `load` frames on `self.loads`."""
        data = b""
        self.results = []
        self.loads = []
        pending = b""
        while True:
            line = self.p.stdout.readline()
            if not line:
                raise EOFError("kernel closed stdout")
            h = json.loads(line)
            if h["type"] == "data":
                chunk = self.p.stdout.read(h["len"])
                data += chunk
                pending += chunk
            elif h["type"] == "result":
                self.results.append((h, pending))
                pending = b""
            elif h["type"] == "load":
                self.loads.append(h)
            elif h["type"] == "status":
                return data, h

    def run(self, script, **kw):
        self.send(op="run", script=script, **kw)
        return self.reply()

    def close(self):
        self.send(op="close")
        self.reply()
        self.p.stdin.close()
        return self.p.wait(timeout=10)


passed = failed = 0


def report(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"PASS kernel: {name}")
    else:
        failed += 1
        print(f"FAIL kernel: {name} {detail}")


def rows(data):
    return [json.loads(l) for l in data.decode().splitlines() if l]


def main(binary):
    k = Kernel(binary, "--format", "json", "-j", "1")

    # Declarations persist: the connection, function and param of one script are
    # in scope for the next.
    _, st = k.run(
        "CREATE CONNECTION files TYPE http OPTIONS (base_url = 'http://127.0.0.1:1');\n"
        "CREATE FUNCTION dbl(x) AS x * 2;\n"
        "PARAM n INT DEFAULT 3;\n"
        "LET k = 10;",
        id="decl",
    )
    names = sorted(d["name"] for d in st.get("declared", []))
    report("a declaration-only script reports what it declared", st["ok"] and names == ["dbl", "files", "k", "n"], st)

    data, st = k.run("SELECT dbl($n) + $k AS v;", id="use")
    report("a later script sees earlier declarations", st["ok"] and rows(data) == [{"v": 16}], (data, st))

    data, st = k.run("SELECT dbl($n) AS v;", id="p", params={"n": 21})
    report("params bind per script", st["ok"] and rows(data) == [{"v": 42}], (data, st))
    data, _ = k.run("SELECT $n AS v;")
    report("a bound param does not leak into the next script", rows(data) == [{"v": 3}], data)

    # Several results in one script arrive in order, before the status.
    data, st = k.run("SELECT 1 AS a;\nSELECT 2 AS b;\nSELECT 3 AS c;")
    report("several results in one script", st["ok"] and rows(data) == [{"a": 1}, {"b": 2}, {"c": 3}], data)

    # Each result is closed by a `result` frame, so NDJSON rows of different
    # results never run together; its line counts in the script as sent, even
    # with declarations replayed ahead of it.
    k.run("SELECT 1 AS a;\n\n   SELECT 2 AS b;")
    got = [(h["statement"], h["kind"], h["line"], h["col"], h["rows"], rows(b)) for h, b in k.results]
    report(
        "each result is closed by a result frame with its own position",
        got == [(0, "select", 1, 1, 1, [{"a": 1}]), (1, "select", 3, 4, 1, [{"b": 2}])],
        got,
    )
    _, st = k.run("DESCRIBE SELECT 1 AS z;")
    report("the status lists the script's results", [r["kind"] for r in st.get("results", [])] == ["describe"], st)

    # A row cap keeps the first rows, stops reading, and says it cut; an
    # aggregate under it still counts every input row.
    data, st = k.run("SELECT range FROM RANGE(100000000000);", max_rows=3)
    report(
        "max_rows previews an endless source",
        st["ok"] and st.get("truncated") is True and rows(data) == [{"range": 0}, {"range": 1}, {"range": 2}]
        and k.results[0][0]["truncated"] is True,
        st,
    )
    data, st = k.run("SELECT range % 3 AS g, COUNT(*) AS n FROM RANGE(30000) GROUP BY g ORDER BY g;", max_rows=2)
    report("a capped aggregate still counts every row", rows(data) == [{"g": 0, "n": 10000}, {"g": 1, "n": 10000}], data)
    data, st = k.run("SELECT range FROM RANGE(3); SELECT range AS r FROM RANGE(2);", max_rows=3)
    report(
        "a result that fits is not truncated, and a cut does not leak into the next",
        st["ok"] and st.get("truncated") is False and len(rows(data)) == 5,
        (data, st),
    )

    # The same CTE name in two scripts: each sees its own.
    k.run("WITH t AS (SELECT 1 AS x) SELECT x FROM t;")
    data, st = k.run("WITH t AS (SELECT 2 AS x) SELECT x FROM t;")
    report("a CTE name can be reused by a later script", st["ok"] and rows(data) == [{"x": 2}], (data, st))

    # Errors are located in the script as sent, not in the replayed declarations.
    data, st = k.run("SELECT 1 AS a;\n\nSELECT nope FROM RANGE(2);", id="err")
    e = st.get("error") or {}
    report(
        "a runtime error is located in the script's own lines",
        not st["ok"] and e.get("line") == 3 and e.get("file") == "script" and "nope" in e.get("msg", ""),
        st,
    )
    report("results before a failing statement are still delivered", rows(data) == [{"a": 1}], data)
    _, st = k.run("SELECT range,\n       upper(nope) AS u\nFROM RANGE(3);")
    e = st.get("error") or {}
    report(
        "an unknown column is underlined exactly, on its own line",
        (e.get("line"), e.get("col"), e.get("end_line"), e.get("end_col")) == (2, 14, 2, 18),
        e,
    )
    _, st = k.run("SELECT frobnicate(1) AS x;")
    e = st.get("error") or {}
    report("an unknown function underlines its name", (e.get("col"), e.get("end_col")) == (8, 18), e)
    _, st = k.run("SELECT FROM WHERE;")
    report("a parse error fails the script, not the session", not st["ok"] and st["error"].get("line") == 1, st)
    _, st = k.run("SELECT dbl(1) AS v;")
    report("the session survives errors", st["ok"], st)

    # A redeclaration replaces the old one.
    k.run("CREATE OR REPLACE FUNCTION dbl(x) AS x * 3;")
    data, _ = k.run("SELECT dbl(2) AS v;")
    report("a redeclaration replaces the stored one", rows(data) == [{"v": 6}], data)

    # A script-level format override.
    data, st = k.run("SELECT 1 AS a, 'x' AS b;", format="csv")
    report("format overrides per script", st["ok"] and data == b"a,b\n1,x\n", data)

    # Cancel stops a running script (an aggregate that drains its input inside
    # one pull) and the session carries on.
    k.send(op="run", id="long", script="SELECT COUNT(*) AS n FROM RANGE(100000000000);")
    time.sleep(0.5)
    k.send(op="cancel", id="long")
    t0 = time.time()
    _, st = k.reply()
    report("cancel stops a running aggregate", st.get("cancelled") is True and time.time() - t0 < 5, st)
    data, st = k.run("SELECT dbl(1) AS v;")
    report("the session survives a cancel", st["ok"] and rows(data) == [{"v": 3}], (data, st))

    # A cancel naming another id leaves the running script alone.
    k.send(op="run", id="short", script="SELECT COUNT(*) AS n FROM RANGE(3000000);")
    k.send(op="cancel", id="someone-else")
    data, st = k.reply()
    report("a cancel for another id is ignored", st["ok"] and rows(data) == [{"n": 3000000}], st)

    # A long statement sends progress frames while it runs.
    k.send(op="run", id="prog", script="SELECT COUNT(*) AS n FROM RANGE(100000000000);")
    seen = None
    t0 = time.time()
    while time.time() - t0 < 10:
        h = json.loads(k.p.stdout.readline())
        if h["type"] == "progress":
            seen = h
            break
    k.send(op="cancel", id="prog")
    _, st = k.reply()
    report(
        "a long statement sends progress frames",
        seen is not None and seen["id"] == "prog" and seen["rows"] > 0 and "range" in seen["target"],
        seen,
    )

    # SIGINT cancels, never kills — even twice.
    k.send(op="run", id="long2", script="SELECT COUNT(*) AS n FROM RANGE(100000000000);")
    time.sleep(0.5)
    k.p.send_signal(signal.SIGINT)
    time.sleep(0.1)
    k.p.send_signal(signal.SIGINT)
    _, st = k.reply()
    report("SIGINT cancels the running script", st.get("cancelled") is True, st)
    data, st = k.run("SELECT 1 AS v;")
    report("the process survives SIGINT", st["ok"] and rows(data) == [{"v": 1}], st)

    # Completion against the session's declarations and the cell's own.
    k.send(op="complete", id="cmp", script="CREATE FUNCTION tri(x) AS x * 3;\nSELECT tr", pos=41)
    _, st = k.reply()
    names = [(i["text"], i["kind"]) for i in (st.get("complete") or {}).get("items", [])]
    report("complete offers the cell's own function", ("tri", "function") in names, st)
    k.send(op="complete", id="cmp2", script="SELECT db")
    _, st = k.reply()
    names = [(i["text"], i["kind"]) for i in (st.get("complete") or {}).get("items", [])]
    report("complete offers a function an earlier script declared", ("dbl", "function") in names, st)

    # A LET keeps the value the declaring cell decided: later cells never
    # recompute now() nor re-run the query, even after its source changes.
    lf = os.path.join(tempfile.mkdtemp(), "let.csv")
    with open(lf, "w") as f:
        f.write("v\n1\n2\n")
    k.run("LET born = now();\nLET cnt = (SELECT COUNT(*) FROM '%s');" % lf)
    time.sleep(1.1)
    with open(lf, "w") as f:
        f.write("v\n1\n2\n3\n4\n")
    data, st = k.run("SELECT $cnt AS cnt, date_diff('second', CAST($born AS TIMESTAMP), now()) AS age;")
    os.remove(lf)
    r = rows(data)
    report("a LET's value is decided once, in the cell that declares it", st["ok"] and r and r[0]["cnt"] == 2 and r[0]["age"] >= 1, (data, st))

    # UTF-16 offsets for a JavaScript editor: an emoji is two units, four bytes.
    k.send(op="complete", id="u16", script="SELECT '😀' AS x, dbl(1) AS y ORDER BY d", pos=40, utf16=True)
    _, st = k.reply()
    c = st.get("complete") or {}
    report("complete speaks UTF-16 offsets when asked", c.get("start") == 39 and c.get("end") == 40 and any(i["text"] == "dbl" for i in c.get("items", [])), st)

    # Each LOAD reports as it finishes, and the status carries them with the totals.
    ld = tempfile.mkdtemp()
    with open(os.path.join(ld, "names.csv"), "w") as f:
        f.write("n\na\nb\nc\n")
    os.mkdir(os.path.join(ld, "a"))
    os.mkdir(os.path.join(ld, "c"))
    _, st = k.run(f"LOAD INTO '{ld}/one.csv' AS SELECT range AS v FROM RANGE(7);\n"
                  f"LOAD INTO '{ld}/none/x.csv' AS SELECT 1 AS v;")
    fr = k.loads
    report("a load frame per LOAD, a failed one before writing too",
           [f["ok"] for f in fr] == [True, False] and fr[0]["rows_written"] == 7 and fr[0]["rows_read"] == 7
           and fr[0]["target"] == f"{ld}/one.csv" and fr[1]["line"] == 2 and "FileNotFound" in fr[1]["reason"]
           and fr[1]["transient"] is False, fr)
    report("the status lists the loads with the run's totals, failed run included",
           not st["ok"] and st.get("loads") == [{k2: v for k2, v in f.items() if k2 not in ("type", "id")} for f in fr]
           and st.get("loads_ok") == 1 and st.get("loads_failed") == 1 and st.get("rows_loaded") == 7, st)
    _, st = k.run(f"FOR EACH ROW OF ('{ld}/names.csv') AS (n) PARALLEL ON ERROR CONTINUE\n"
                  f"  LOAD INTO IDENTIFIER('{ld}/' || $n || '/x.csv') AS SELECT range AS v FROM RANGE(4);\nEND FOR;")
    fr = sorted(k.loads, key=lambda f: f["loop_row"])
    report("a for-each row's load says which row, and a failed row does not stop the rest",
           [f["ok"] for f in fr] == [True, False, True] and all(f["loop_rows"] == 3 and f["loop_total"] == 3 for f in fr)
           and fr[1]["target"] == f"{ld}/b/x.csv" and st.get("loads_ok") == 2 and st.get("loads_failed") == 1, (k.loads, st))
    report("a load run one at a time counts its own rows read", all(f["rows_read"] == (4 if f["ok"] else 0) for f in fr), fr)
    kj = Kernel(binary, "-j", "3")
    _, st = kj.run(f"FOR EACH ROW OF ('{ld}/names.csv') AS (n) PARALLEL ON ERROR CONTINUE\n"
                   f"  LOAD INTO IDENTIFIER('{ld}/' || $n || '/x.csv') AS SELECT range AS v FROM RANGE(4);\nEND FOR;")
    report("loads side by side each count their own rows read",
           len(kj.loads) == 3 and all(f["rows_read"] == (4 if f["ok"] else 0) for f in kj.loads) and st.get("rows_read") == 8, (kj.loads, st))
    kj.close()
    _, st = k.run("SELECT 1 AS x;")
    report("a run without loads says so", st.get("loads") == [] and st.get("loads_ok") == 0 and not k.loads, st)

    # `check` reads a cell against the session and the tables other cells hold,
    # lists every problem, and changes nothing.
    k.run("PARAM since DATE DEFAULT '2024-01-01';\nCREATE FUNCTION dbl(v) AS v * 2;")
    k.send(op="check", id="ck", script="SELECT id, dbl(amt) AS a FROM enrich WHERE day >= $since;\n"
           "SELECT nope FROM enrich;\nSELECT 1 +;\nPARAM fresh INT DEFAULT 1;",
           tables=[{"name": "enrich", "columns": [{"name": "id", "type": "int"},
                                                   {"name": "amt", "type": "decimal(10,2)"},
                                                   {"name": "day", "type": "date"}]}])
    _, st = k.reply()
    diags = st.get("diagnostics")
    report("check lists every problem in a cell, placed in the cell",
           st["ok"] and [(d["file"], d["line"]) for d in diags or []] == [("script", 2), ("script", 3)]
           and "`nope`" in diags[0]["msg"], st)
    k.send(op="check", id="ck2", script="SELECT anything FROM enrich;", tables=["enrich"])
    _, st = k.reply()
    report("check takes a table by name alone, its columns unresolved", st.get("diagnostics") == [], st)
    _, st = k.run("SELECT $fresh AS f;")
    report("check leaves the session as it was", not st["ok"] and "fresh" in st["error"]["msg"], st)
    k.send(op="check", id="ck3", script="SELECT 1 AS x;", tables=[{"name": "t", "columns": [{"name": "x", "type": "nope"}]}])
    _, st = k.reply()
    report("check refuses a column type it does not know", not st["ok"] and "type" in st["error"]["msg"], st)

    # Reset forgets every declaration.
    k.send(op="reset", id="r")
    _, st = k.reply()
    _, st2 = k.run("SELECT dbl(1) AS v;")
    report("reset forgets declarations", st["ok"] and not st2["ok"], st2)

    # A malformed request is answered, not fatal.
    k.p.stdin.write(b"this is not json\n")
    k.p.stdin.flush()
    _, st = k.reply()
    report("a malformed request gets an error status", not st["ok"], st)

    # A script larger than the read buffer arrives whole.
    big = "SELECT " + ", ".join(f"{i} AS c{i}" for i in range(12000)) + ";"
    data, st = k.run(big)
    report("a script larger than 64 KiB arrives whole", st["ok"] and len(rows(data)[0]) == 12000, st)

    code = k.close()
    report("close exits 0", code == 0, code)

    print(f"==> kernel: {passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else os.path.join("zig-out", "bin", "basalt")))

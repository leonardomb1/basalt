"""Drives `basalt kernel` over its stdin/stdout protocol, as a notebook would.

Usage: python3 it/kernel.py <basalt binary>
Prints PASS/FAIL lines; exits non-zero on any failure. Standard library only.
"""

import json
import os
import signal
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
        """Reads frames until a status: returns (data bytes, status dict)."""
        data = b""
        while True:
            line = self.p.stdout.readline()
            if not line:
                raise EOFError("kernel closed stdout")
            h = json.loads(line)
            if h["type"] == "data":
                data += self.p.stdout.read(h["len"])
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

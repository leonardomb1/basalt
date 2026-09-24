//! End-to-end runtime tests: fixtures in a tmp dir, a script through `run`,
//! assertions on the written output.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const column = @import("../exec/column.zig");
const csv = @import("../connect/csv.zig");
const driver = @import("../connect/driver.zig");
const Wal = @import("../connect/wal.zig").Wal;
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const obs = @import("obs.zig");
const Value = @import("../exec/value.zig").Value;

const parser = @import("../lang/sql_parser.zig");

const Diag = @import("env.zig").Diag;
const isTransient = @import("env.zig").isTransient;
const LogConfig = @import("env.zig").LogConfig;
const LoopRow = @import("env.zig").LoopRow;
const no_loop_vars = @import("env.zig").no_loop_vars;
const OutcomeSink = @import("env.zig").OutcomeSink;
const ParamArg = @import("env.zig").ParamArg;

const sqlWithWhere = @import("connect.zig").sqlWithWhere;

const agg_combine_parallel_min = @import("lanes.zig").agg_combine_parallel_min;
const classifyAggPipeline = @import("lanes.zig").classifyAggPipeline;
const classifyWholeAgg = @import("lanes.zig").classifyWholeAgg;
const joinKindLaneSafe = @import("lanes.zig").joinKindLaneSafe;

const interpAll = @import("script.zig").interpAll;
const printText = @import("script.zig").printText;

const run = @import("run.zig").run;
const describeRows = @import("run.zig").describeRows;

/// Run `LOAD INTO out.csv AS <query>` over `input`. `$IN` in the query is
/// replaced with the input CSV's path.
fn runToString(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8) ![]u8 {
    return runToStringP(alloc, tmp, input, query, &[_]ParamArg{});
}

fn runToStringP(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8, cli_params: []const ParamArg) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .params = cli_params }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "EXPLAIN mid-script explains that query without running it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n2,pending\n" });

    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const ran_path = try std.fs.path.join(alloc, &.{ base, "ran.csv" });
    defer alloc.free(ran_path);
    const never_path = try std.fs.path.join(alloc, &.{ base, "never.csv" });
    defer alloc.free(never_path);

    // The EXPLAIN is not the first statement — the whole point — and its query is
    // built on a CTE, so it only plans if the binding above it is in scope.
    const script = try std.fmt.allocPrint(alloc,
        \\LOAD INTO '{s}' AS SELECT id FROM '{s}';
        \\EXPLAIN LOAD INTO '{s}' AS
        \\WITH paid AS (SELECT id FROM '{s}' WHERE status = 'paid')
        \\SELECT id FROM paid;
    , .{ ran_path, in_path, never_path, in_path });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    try std.testing.expectEqual(ast.ExplainMode.none, prog.explain);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .log = .{ .quiet = true, .summary = .none } }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };

    // The plan itself goes to stdout (analyze.render), which a test cannot capture;
    // what is checkable is the half that matters — the statement before it ran, and
    // the explained load wrote nothing.
    const ran = try tmp.dir.readFileAlloc(alloc, "ran.csv", 1 << 20);
    defer alloc.free(ran);
    try std.testing.expectEqualStrings("id\n1\n2\n", ran);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("never.csv", .{}));
}

test "union all by name: a branch may be any query, not just a table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "k,v\na,1\nb,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "k,v,extra\nc,3,x\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const pa = try std.fs.path.join(alloc, &.{ base, "a.csv" });
    defer alloc.free(pa);
    const pb = try std.fs.path.join(alloc, &.{ base, "b.csv" });
    defer alloc.free(pb);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    // A branch used to have to be `SELECT ['tag' AS c,] t.* FROM <conn>.<table>` — the
    // reconciliation case the feature was built for. A file, a filter or an aggregate
    // in a branch was refused, which is most of what a UNION is actually written for.
    // Reconciliation still applies: `extra` is dropped against the first branch's canon.
    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT k, v FROM '{s}' WHERE v = 1" ++
            " UNION ALL BY NAME SELECT k, v, extra FROM '{s}';",
        .{ out_path, pa, pb },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,v\na,1\nc,3\n", out);
}

test "aggregate: two aggregates over different expressions stay separate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // `exprKey` rendered every expression kind it did not handle as a single `?`, so
    // `SUM(v*2)` and `SUM(v+100)` both keyed as `sum(?)` and the second bound to the
    // first's accumulator: the answer came back as 2 * SUM(v*2), silently. Reported
    // against 0.6.1. Verified against DuckDB: 120 + 360 = 480.
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n30\n",
        "SELECT SUM(v * 2) + SUM(v + 100) AS chk, MIN(v * 2) + MIN(v + 100) AS m FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("chk,m\n480,130\n", out);
}

test "aggregate: the same expression twice still shares one accumulator" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The dedup is the point of the key, so an identical argument must still collapse to
    // one accumulator rather than being computed twice.
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n30\n",
        "SELECT SUM(v * 2) + SUM(v * 2) AS d, SUM(v) + COUNT(*) AS c FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("d,c\n240,63\n", out);
}

test "window: a column only named inside OVER survives the projection" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    // Reported against 0.6.2. The projection was built from the ordinary SELECT items,
    // so `categoria`, `id` and `valor` were pruned before the window operator ran and
    // resolution failed — the error even migrated as you projected them by hand. They
    // are now carried through hidden and dropped afterwards, the way an outer ORDER BY
    // already treats its own keys. Output is `ant` alone.
    const out = try runToString(
        alloc,
        &t1,
        "id,categoria,valor\n1,a,10\n2,a,20\n3,b,5\n",
        "SELECT LAG(valor) OVER (PARTITION BY categoria ORDER BY id) AS ant FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("ant\n\n10\n\n", out);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    // The shape the reporter's checksums use: the outer query names none of the inner
    // columns at all.
    const sub = try runToString(
        alloc,
        &t2,
        "id,categoria,valor\n1,a,10\n2,a,20\n3,b,5\n",
        "SELECT SUM(ant) AS s FROM (SELECT LAG(valor) OVER (PARTITION BY categoria ORDER BY id) AS ant FROM '$IN') x",
    );
    defer alloc.free(sub);
    try std.testing.expectEqualStrings("s\n10\n", sub);
}

test "window: row_number, rank and dense_rank number within a partition" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,10\na,20\na,20\nb,5\nb,7\n",
        "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,v,rn\na,10,1\na,20,2\na,20,3\nb,5,1\nb,7,2\n", out);
}

test "window: a tie holds rank and skips, dense_rank leaves no gap" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    // The case that separates the two: RANK is 1 + the rows strictly before, so after a
    // two-row tie at 20 the next value jumps to 4 and the last to 6. DENSE_RANK counts
    // distinct values instead: 1,2,2,3,3,4. Both verified against DuckDB.
    const rk = try runToString(
        alloc,
        &t1,
        "v\n10\n20\n20\n30\n30\n40\n",
        "SELECT v, RANK() OVER (ORDER BY v) AS rk FROM '$IN'",
    );
    defer alloc.free(rk);
    try std.testing.expectEqualStrings("v,rk\n10,1\n20,2\n20,2\n30,4\n30,4\n40,6\n", rk);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const dr = try runToString(
        alloc,
        &t2,
        "v\n10\n20\n20\n30\n30\n40\n",
        "SELECT v, DENSE_RANK() OVER (ORDER BY v) AS dr FROM '$IN'",
    );
    defer alloc.free(dr);
    try std.testing.expectEqualStrings("v,dr\n10,1\n20,2\n20,2\n30,3\n30,3\n40,4\n", dr);
}

test "window: lag and lead stop at the partition edge" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    // The first row of a partition has nothing behind it and the last nothing ahead, so
    // both yield null rather than reaching into the neighbouring partition. Verified
    // against DuckDB.
    const lag = try runToString(
        alloc,
        &t1,
        "k,v\na,10\na,20\na,30\nb,5\nb,7\n",
        "SELECT k, v, LAG(v) OVER (PARTITION BY k ORDER BY v) AS prev FROM '$IN'",
    );
    defer alloc.free(lag);
    try std.testing.expectEqualStrings("k,v,prev\na,10,\na,20,10\na,30,20\nb,5,\nb,7,5\n", lag);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const lead = try runToString(
        alloc,
        &t2,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, v, LEAD(v) OVER (PARTITION BY k ORDER BY v) AS nxt FROM '$IN'",
    );
    defer alloc.free(lead);
    try std.testing.expectEqualStrings("k,v,nxt\na,10,20\na,20,\nb,5,\n", lead);
}

test "window: an explicit lag offset skips that many rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n30\n40\n",
        "SELECT v, LAG(v, 2) OVER (ORDER BY v) AS prev2 FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("v,prev2\n10,\n20,\n30,10\n40,20\n", out);
}

test "window: a lag result feeds an expression through a derived table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // A window function cannot sit inside an expression, so change detection composes
    // by wrapping it — which is what derived tables are for.
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,10\na,25\nb,5\nb,7\n",
        "SELECT k, v - prev AS delta FROM (SELECT k, v, LAG(v) OVER (PARTITION BY k ORDER BY v) AS prev FROM '$IN') x WHERE prev IS NOT NULL",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,delta\na,15\nb,2\n", out);
}

test "window: an aggregate frame is the partition, or the peers so far" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    // No ORDER BY: every row of the partition is a peer, so the frame is the whole
    // partition — share-of-total.
    const tot = try runToString(
        alloc,
        &t1,
        "k,v\na,10\na,20\na,20\nb,5\nb,7\n",
        "SELECT k, v, SUM(v) OVER (PARTITION BY k) AS tot FROM '$IN'",
    );
    defer alloc.free(tot);
    try std.testing.expectEqualStrings("k,v,tot\na,10,50\na,20,50\na,20,50\nb,5,12\nb,7,12\n", tot);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    // With ORDER BY it accumulates peer group by peer group, so the two tied 20s BOTH
    // read 50 rather than 30 and 50. That is standard RANGE framing, and it matches
    // DuckDB — getting it row-by-row instead would be a subtle wrong answer.
    const running = try runToString(
        alloc,
        &t2,
        "k,v\na,10\na,20\na,20\nb,5\nb,7\n",
        "SELECT k, v, SUM(v) OVER (PARTITION BY k ORDER BY v) AS run FROM '$IN'",
    );
    defer alloc.free(running);
    try std.testing.expectEqualStrings("k,v,run\na,10,10\na,20,50\na,20,50\nb,5,5\nb,7,12\n", running);
}

test "window: COUNT(*) over a partition, and a plain SUM still aggregates" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const n = try runToString(
        alloc,
        &t1,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, COUNT(*) OVER (PARTITION BY k) AS n FROM '$IN'",
    );
    defer alloc.free(n);
    try std.testing.expectEqualStrings("k,n\na,2\na,2\nb,1\n", n);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    // Without OVER, `SUM` is the ordinary aggregate: the item parser rewinds unless it
    // sees the whole `name ( .. ) OVER` prefix, so these names stay unreserved.
    const agg = try runToString(
        alloc,
        &t2,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, SUM(v) AS s FROM '$IN' GROUP BY k ORDER BY k",
    );
    defer alloc.free(agg);
    try std.testing.expectEqualStrings("k,s\na,30\nb,5\n", agg);
}

test "window: min, max and avg share one window in a single SELECT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // `MIN(v) OVER (w), MAX(v) OVER (w)` is the natural pairing, so an identical OVER
    // clause has to be accepted — the first attempt rejected any repeat of the clause.
    // MIN/MAX use the same `lessV` ordering the grouped aggregate does, so a window
    // extreme and a grouped one cannot disagree.
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, v, MIN(v) OVER (PARTITION BY k) AS lo, MAX(v) OVER (PARTITION BY k) AS hi, AVG(v) OVER (PARTITION BY k) AS mean FROM '$IN'",
    );
    defer alloc.free(out);
    // `v` has to be projected: the window reads it, and the stage appends to the
    // projection rather than reaching behind it.
    try std.testing.expectEqualStrings("k,v,lo,hi,mean\na,10,10,20,15\na,20,10,20,15\nb,5,5,5,5\n", out);
}

test "window: two different windows in one SELECT are refused" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "k,v\na,1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    // Each distinct window would need its own stage; say so instead of silently using
    // one of them for both.
    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT k, MIN(v) OVER (PARTITION BY k) AS lo, MAX(v) OVER (ORDER BY v) AS hi FROM '{s}';",
        .{ out_path, in_path },
    );
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try std.testing.expectError(error.ParseFailed, parser.parseSource(parena.allocator(), script, &pdiag));
}

test "window: a ROWS frame counts rows where the default counts peers" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    // The whole reason ROWS exists. Same query but for the frame: under ROWS the two
    // tied 20s read 30 and 50, under the RANGE default they both read 50. Both match
    // DuckDB; picking one behaviour for both spellings would be a silent wrong answer.
    const rows = try runToString(
        alloc,
        &t1,
        "v\n10\n20\n20\n30\n",
        "SELECT v, SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM '$IN'",
    );
    defer alloc.free(rows);
    try std.testing.expectEqualStrings("v,run\n10,10\n20,30\n20,50\n30,80\n", rows);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const range = try runToString(
        alloc,
        &t2,
        "v\n10\n20\n20\n30\n",
        "SELECT v, SUM(v) OVER (ORDER BY v) AS run FROM '$IN'",
    );
    defer alloc.free(range);
    try std.testing.expectEqualStrings("v,run\n10,10\n20,50\n20,50\n30,80\n", range);
}

test "window: a bounded ROWS frame is a moving window" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // A moving average over two rows, clipped at the partition's first row.
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n20\n30\n",
        "SELECT v, AVG(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ma FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("v,ma\n10,10\n20,15\n20,20\n30,25\n", out);
}

test "window: a column named `rank` is still a column" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The item parser only commits once it has seen `name ( ) OVER`, and rewinds
    // otherwise — so the function names are not reserved words.
    const out = try runToString(
        alloc,
        &tmp,
        "rank,v\n7,1\n",
        "SELECT rank, v FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("rank,v\n7,1\n", out);
}

test "derived table: a subquery in FROM is an anonymous CTE" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // `FROM (SELECT ...) x` lowers to the binding `WITH x AS (...)` would produce, so
    // it needs no execution machinery — only a name. Every TPC-DS query that reads a
    // derived table needed hand-rewriting into a CTE before this.
    const out = try runToString(
        alloc,
        &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT id, amount FROM (SELECT id, amount FROM '$IN' WHERE status = 'paid') x",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "derived table: nested, and beside a WITH binding" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The inner query's own bindings used to be drained into the very list the outer
    // parse was collecting into, which cleared it: this failed with "unknown binding
    // `a`". A `WITH` inside a derived table hits the same path.
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n",
        "SELECT id FROM (WITH w AS (SELECT id FROM '$IN') SELECT id FROM (SELECT id FROM w) a) b ORDER BY id",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "derived table: a join right side may be a subquery" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // A join's right side is named by binding anyway, so an inline subquery there is
    // the same lowering. Both positions in one query, to prove they do not clobber
    // each other's bindings.
    const out = try runToString(
        alloc,
        &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT x.id, x.amount FROM (SELECT id, amount FROM '$IN') x " ++
            "JOIN (SELECT id AS pid FROM '$IN' WHERE status = 'paid') p ON x.id = p.pid ORDER BY x.id",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "CSV -> filter/select -> CSV round-trips" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT id, amount FROM '$IN' WHERE status = 'paid'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "aggregate: count and sum by group (nulls skipped)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\npaid,\n",
        "SELECT status, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total FROM '$IN' GROUP BY status ORDER BY status ASC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,n,total\npaid,3,300\npending,1,50\n", out);
}

test "aggregate: an interleaved SELECT list keeps its column order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The aggregate stage emits grouping keys first and aggregates after, so this
    // used to come out `status,region,n` — the values right, the columns moved.
    // TPC-H Q3 has exactly this shape, and a positional CSV consumer downstream
    // would have loaded the wrong columns without a word.
    const out = try runToString(
        alloc,
        &tmp,
        "status,region,amount\npaid,west,100\npaid,west,50\n",
        "SELECT status, COUNT(*) AS n, region FROM '$IN' GROUP BY status, region",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,n,region\npaid,2,west\n", out);
}

test "aggregate: keys-then-aggregates adds no projection" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The common order must stay on the shorter plan — the reorder projection is
    // only for lists that actually interleave.
    const out = try runToString(
        alloc,
        &tmp,
        "status,region,amount\npaid,west,100\npaid,west,50\n",
        "SELECT status, region, COUNT(*) AS n FROM '$IN' GROUP BY status, region",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,region,n\npaid,west,2\n", out);
}

test "sort: numeric desc, nulls last" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,\n3,200\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n1,100\n2,\n", out);
}

test "aggregate: group by a numeric (int) key (value-keyed hashing)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,n\n1,5\n2,5\n3,7\n4,5\n",
        "SELECT CAST(n AS INT) AS g, COUNT(*) AS c FROM '$IN' GROUP BY g ORDER BY g ASC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,c\n5,3\n7,1\n", out);
}

/// Run `read csv | <body> | write csv` with an explicit thread count, returning
/// out.csv. Used to exercise the parallel CSV-aggregate path (`threads > 1`), which
/// the default in-process harness (`threads = 1`) never reaches.
fn runCsvThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8, threads: usize) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

/// A parquet file with three row groups — enough for the parallel scan, which needs
/// at least `pq_min_lanes` lanes and more than one row group. 5000 rows of
/// `id = 1..5000`, `f = id * 0.5` and `g = id % 4` (a low-cardinality group key), so
/// `SUM(id)` and `SUM(f)` are both exactly representable and the assertion holds
/// whatever order the lanes add in.
const fx_rg2 = @embedFile("testdata/rg2.parquet");

/// Run a query over `fx_rg2` at an explicit thread count, returning out.csv.
fn runParquetThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, query: []const u8, threads: usize) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.parquet", .data = fx_rg2 });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.parquet" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "parallel parquet aggregate: an ungrouped agg (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    // This shape used to be refused by the parallel path and sent to the serial
    // driver, because folding into zero groups made the radix merge emit the
    // identity and COUNT(*) came back 0. It is the most common analytical query
    // there is, and it ran on one core.
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, "SELECT COUNT(*) AS n, SUM(id) AS sid, SUM(f) AS sf FROM '$IN'", 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, "SELECT COUNT(*) AS n, SUM(id) AS sid, SUM(f) AS sf FROM '$IN'", 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sid,sf\n5000,12502500,6251250\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: an ungrouped agg under a filter (threads>1)" {
    const alloc = std.testing.allocator;
    // A prefix filter is the q06 shape: the lanes must each apply it and still
    // combine into the single implicit group.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runParquetThreaded(alloc, &tmp, "SELECT COUNT(*) AS n, SUM(id) AS sid FROM '$IN' WHERE id <= 100", 4);
    defer alloc.free(par);
    try std.testing.expectEqualStrings("n,sid\n100,5050\n", par);
}

test "parallel parquet aggregate: a join then an ungrouped agg (threads>1)" {
    const alloc = std.testing.allocator;
    // Neither classifier used to admit this: `classifyAggPipeline` rejects a join and
    // `classifyMapJoinPipeline` rejects the aggregate, so join-then-aggregate had no
    // parallel path and TPC-H q12/q14 ran the same at -j 1 and -j 16.
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE id <= 50) " ++
        "SELECT COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sf\n50,637.5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a chain of two joins then an agg (threads>1)" {
    const alloc = std.testing.allocator;
    // One join was the original limit, which left TPC-H q03 — two dimensions — serial
    // while q12 and q14 fanned out. Each join's output schema is the next one's probe
    // schema, so the chain has to resolve left to right.
    const q = "WITH d1 AS (SELECT id AS did FROM '$IN' WHERE id <= 100), " ++
        "d2 AS (SELECT id AS eid FROM '$IN' WHERE id <= 50) " ++
        "SELECT COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d1 ON id = did JOIN d2 ON id = eid";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sf\n50,637.5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a right join under an aggregate is not fanned out" {
    const alloc = std.testing.allocator;
    // A right or full join must emit the build rows nothing matched. Each lane tracks
    // matches against its own `op.Join`, so every lane emitted them again: this
    // returned 160 at -j 4 where the answer is 10. The shape has to decline these
    // kinds and let the serial driver have them, which is what the map+join paths do.
    //
    // `d` is 4996..5005 against ids 1..5000, so five keys match and five do not.
    const q = "WITH d AS (SELECT (id + 4995) AS did FROM '$IN' WHERE id <= 10) " ++
        "SELECT COUNT(*) AS n, SUM(id) AS sum_left FROM '$IN' RIGHT JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sum_left\n10,24990\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a full join under an aggregate is not fanned out" {
    const alloc = std.testing.allocator;
    const q = "WITH d AS (SELECT (id + 4995) AS did FROM '$IN' WHERE id <= 10) " ++
        "SELECT COUNT(*) AS n FROM '$IN' FULL JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    // 5000 left rows, of which 5 match, plus the 5 build rows with no left partner.
    try std.testing.expectEqualStrings("n\n5005\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: the lane-safe kinds still fan out under an aggregate" {
    const alloc = std.testing.allocator;
    // The guard must not be so broad that it takes the ordinary kinds with it.
    inline for (.{ "INNER", "LEFT", "SEMI", "ANTI" }) |kind| {
        const q = "WITH d AS (SELECT (id + 4995) AS did FROM '$IN' WHERE id <= 10) " ++
            "SELECT COUNT(*) AS n FROM '$IN' " ++ kind ++ " JOIN d ON id = did";
        var t1 = std.testing.tmpDir(.{});
        defer t1.cleanup();
        const serial = try runParquetThreaded(alloc, &t1, q, 1);
        defer alloc.free(serial);
        var t4 = std.testing.tmpDir(.{});
        defer t4.cleanup();
        const par = try runParquetThreaded(alloc, &t4, q, 4);
        defer alloc.free(par);
        try std.testing.expectEqualStrings(serial, par);
    }
}

test "parallel parquet aggregate: an interleaved SELECT list still fans out" {
    const alloc = std.testing.allocator;
    // The reordering projection that keeps an interleaved SELECT list in its declared
    // order lands in the tail, and rejecting a tail projection sent the whole query to
    // the serial driver: 379ms against 88ms on q01's shape. Both facts are asserted
    // here — the column order AND that -j changes nothing about the answer.
    const q = "SELECT g, COUNT(*) AS n, id FROM '$IN' WHERE id <= 4 GROUP BY g, id ORDER BY id";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("g,n,id\n1,1,1\n2,1,2\n3,1,3\n0,1,4\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a join then a grouped agg (threads>1)" {
    const alloc = std.testing.allocator;
    // The grouped case goes through the radix merge rather than the single-partition
    // fold, so it exercises the other half of the merge with a join underneath.
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE id <= 8) " ++
        "SELECT g, COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d ON id = did GROUP BY g ORDER BY g";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("g,n,sf\n0,2,6\n1,2,3\n2,2,4\n3,2,5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel CSV aggregate: global agg (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runCsvThreaded(alloc, &tmp, input, "SELECT COUNT(*) AS n, SUM(CAST(v AS INT)) AS s FROM '$IN'", 4);
    defer alloc.free(par);
    try std.testing.expectEqualStrings("n,s\n5,150\n", par);
}

test "parallel CSV aggregate: a join then an agg (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    // The CSV twin of the parquet join+aggregate tests above. Both exist on purpose:
    // the ungrouped aggregate was parallel for CSV and serial for parquet for exactly
    // this reason — one hand-rolled path grew a capability its twin never did.
    const input = "id,v\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE CAST(id AS INT) <= 3) " ++
        "SELECT COUNT(*) AS n, SUM(CAST(v AS INT)) AS s FROM '$IN' JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runCsvThreaded(alloc, &t1, input, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runCsvThreaded(alloc, &t4, input, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,s\n3,60\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel CSV aggregate: filter/select prefix + sort/limit tail (threads>1)" {
    const alloc = std.testing.allocator;
    const input = "id,g,v\n1,a,10\n2,b,20\n3,a,30\n4,b,5\n5,a,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input, "SELECT g, SUM(CAST(v AS INT)) AS s FROM '$IN' WHERE CAST(v AS INT) > 6 GROUP BY g ORDER BY s DESC LIMIT 1", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,s\na,90\n", out);
}

test "parallel CSV distinct (threads>1): dedups across chunks" {
    const alloc = std.testing.allocator;
    const input = "id,g\n1,a\n2,b\n3,a\n4,c\n5,b\n6,a\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input, "SELECT DISTINCT g FROM '$IN' ORDER BY g ASC", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g\na\nb\nc\n", out);
}

test "parallel CSV Top-N: sort | limit (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,40\n3,20\n4,50\n5,30\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input, "SELECT id, CAST(v AS INT) AS v FROM '$IN' ORDER BY v DESC, id ASC LIMIT 3", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,v\n4,50\n2,40\n5,30\n", out);
}

test "parallel CSV aggregate: grouped agg (threads>1) merges partials by key" {
    const alloc = std.testing.allocator;
    const input = "id,g,v\n1,a,10\n2,b,20\n3,a,30\n4,b,40\n5,a,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runCsvThreaded(alloc, &tmp, input, "SELECT g, SUM(CAST(v AS INT)) AS s FROM '$IN' GROUP BY g", 4);
    defer alloc.free(par);
    try std.testing.expect(std.mem.startsWith(u8, par, "g,s\n"));
    try std.testing.expect(std.mem.indexOf(u8, par, "a,90\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, par, "b,60\n") != null);
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, par, "\n"));
}

// A float SUM whose value depends on the order the chunk partials are added:
// 1e16 has a spacing of 2, so `1e16 + 1` is 1e16 exactly, and adding the small
// values first is the only way the ones survive. The big value sits in the last
// row, hence the last chunk, so combining the chunks in index order sums the
// eight 1.0s before it and lands on 1e16 + 8 (even, so exact). Combining in
// completion order would drop some of them.
const float_order_csv =
    "g,v\n" ++
    "a,1\n" ++ "a,1\n" ++ "a,1\n" ++ "a,1\n" ++
    "a,1\n" ++ "a,1\n" ++ "a,1\n" ++ "a,1\n" ++
    "a,10000000000000000\n";

test "parallel CSV aggregate: float SUM combines partials in chunk order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, float_order_csv, "SELECT g, SUM(CAST(v AS FLOAT)) AS s FROM '$IN' GROUP BY g", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,s\na,10000000000000008\n", out);
}

test "parallel CSV aggregate: float SUM is identical across runs at one -j" {
    const alloc = std.testing.allocator;
    // Same input every round; only the thread scheduling can differ. Any run that
    // disagrees with the first means partials are being combined in arrival order.
    var first: ?[]u8 = null;
    defer if (first) |f| alloc.free(f);
    for (0..8) |_| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const out = try runCsvThreaded(alloc, &tmp, float_order_csv, "SELECT g, SUM(CAST(v AS FLOAT)) AS s FROM '$IN' GROUP BY g", 8);
        if (first) |f| {
            defer alloc.free(out);
            try std.testing.expectEqualStrings(f, out);
        } else {
            first = out;
        }
    }
}

test "parallel CSV aggregate: high-cardinality combine is partitioned and exact" {
    const alloc = std.testing.allocator;
    // Above `agg_combine_parallel_min`, so this takes the radix-partitioned
    // combine rather than the single pass.
    const ngroups = (agg_combine_parallel_min * 3) / 2;
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("k,v\n");
    for (0..ngroups) |k| {
        // Each key twice, so every group must come back with count 2 and sum 3.
        try input.writer().print("{d},1\n{d},2\n", .{ k, k });
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input.items, "SELECT k, COUNT(*) AS c, SUM(CAST(v AS INT)) AS s FROM '$IN' GROUP BY k", 4);
    defer alloc.free(out);

    var seen: usize = 0;
    var it = std.mem.tokenizeScalar(u8, out, '\n');
    _ = it.next(); // header
    while (it.next()) |line| {
        var f = std.mem.tokenizeScalar(u8, line, ',');
        const k = f.next().?;
        try std.testing.expectEqualStrings("2", f.next().?);
        try std.testing.expectEqualStrings("3", f.next().?);
        // Every key must be one of the ones written, and appear once.
        const kv = try std.fmt.parseInt(usize, k, 10);
        try std.testing.expect(kv < ngroups);
        seen += 1;
    }
    try std.testing.expectEqual(ngroups, seen);
}

test "parallel CSV aggregate: the partitioned combine is reproducible" {
    const alloc = std.testing.allocator;
    const ngroups = (agg_combine_parallel_min * 3) / 2;
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("k,v\n");
    // Values that only add up the same way if the partials are combined in a
    // fixed order, as in the low-cardinality test above.
    for (0..ngroups) |k| {
        try input.writer().print("{d},1\n{d},1\n{d},10000000000000000\n", .{ k, k, k });
    }

    var first: ?[]u8 = null;
    defer if (first) |f| alloc.free(f);
    for (0..4) |_| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const out = try runCsvThreaded(alloc, &tmp, input.items, "SELECT k, SUM(CAST(v AS FLOAT)) AS s FROM '$IN' GROUP BY k ORDER BY k", 8);
        if (first) |f| {
            defer alloc.free(out);
            try std.testing.expectEqualStrings(f, out);
        } else {
            first = out;
        }
    }
}

test "distinct: multi-column key (value-keyed)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "a,b\nx,1\nx,1\nx,2\ny,1\n",
        "SELECT DISTINCT ON (a, b) * FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("a,b\nx,1\nx,2\ny,1\n", out);
}

test "top-N: sort | limit fuses to the K largest, in order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 2",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n5,150\n", out);
}

test "top-N: offset skips before taking" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 2 OFFSET 1",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n5,150\n1,100\n", out);
}

test "top-N: nulls sort last, matching a full sort | limit-all" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 99",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n5,150\n1,100\n2,50\n4,\n", out);
}

test "distinct keeps first row per key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\n",
        "SELECT DISTINCT ON (status) * FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,amount\npaid,100\npending,50\n", out);
}

test "for-each over a JSON array param iterates and binds fields by name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n2,pending\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const body = try std.fmt.allocPrint(alloc, "{{\"tables\":[{{\"name\":\"{s}\"}}]}}", .{in_path});
    defer alloc.free(body);
    const script = try std.fmt.allocPrint(alloc, "PARAM job JSON FROM BODY;\n" ++
        "FOR EACH ROW OF ($job.tables) AS (name) SEQUENTIAL\n" ++
        "  LOAD INTO '{s}' AS SELECT id FROM '${{name}}';\n" ++
        "END FOR;", .{out_path});
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .request_body = body }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "for-each loop var interpolates into a select column value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n2,pending\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const body = try std.fmt.allocPrint(alloc, "{{\"tables\":[{{\"name\":\"{s}\",\"emp\":\"01\"}}]}}", .{in_path});
    defer alloc.free(body);
    const script = try std.fmt.allocPrint(alloc, "PARAM job JSON FROM BODY;\n" ++
        "FOR EACH ROW OF ($job.tables) AS (name, emp) SEQUENTIAL\n" ++
        "  LOAD INTO '{s}' AS SELECT id, '${{emp}}' AS EMPRESA FROM '${{name}}';\n" ++
        "END FOR;", .{out_path});
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .request_body = body }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,EMPRESA\n1,01\n2,01\n", out);
}

test "for-each loop var used as an expression value binds per row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
        "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, $name AS empresa FROM '{s}/${{name}}.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,empresa\n1,alpha\n", a);
    try std.testing.expectEqualStrings("id,empresa\n2,beta\n", b);
}

test "for-each: a typed loop var used as a value binds as its declared type" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "nums.csv", .data = "n\n5\n" });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    // `$n * 2` only type-checks (and yields 10) if `n` bound as an int, not text.
    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/nums.csv') AS (n:INT)\n" ++
        "  LOAD INTO '{s}/out.csv' AS SELECT id, $n * 2 AS twice FROM '{s}/in.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,twice\n7,10\n", out);
}

test "for-each: a loop var shadows a same-named source column" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\n" });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,name\n1,from_file\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
        "  LOAD INTO '{s}/out.csv' AS SELECT id, $name AS who FROM '{s}/in.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,who\n1,alpha\n", out);
}

test "interpAll: bare-var fast path and expression bodies" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const u8{ "name", "pk" };

    const empty = [_][]const u8{ "Account", "" };
    const empty_row = LoopRow{ .names = &names, .cells = &empty };
    try std.testing.expectEqualStrings("Account", try interpAll(a, "${name}", empty_row));
    try std.testing.expectEqualStrings("${nope}", try interpAll(a, "${nope}", empty_row));

    try std.testing.expectEqualStrings("crm_account", try interpAll(a, "crm_${lower(name)}", empty_row));
    try std.testing.expectEqualStrings("ACCOUNT", try interpAll(a, "${upper(name)}", empty_row));

    const key = "${if(pk == '', concat(lower(name), 'id'), pk)}";
    try std.testing.expectEqualStrings("accountid", try interpAll(a, key, empty_row));

    const given = [_][]const u8{ "ListMember", "lm_custom_id" };
    try std.testing.expectEqualStrings("lm_custom_id", try interpAll(a, key, .{ .names = &names, .cells = &given }));

    try std.testing.expectEqualStrings("}", try interpAll(a, "${if(pk == '', '}', pk)}", empty_row));
}

test "interpAll: malformed bodies error" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const u8{ "name", "pk" };
    const vals = [_][]const u8{ "Account", "" };
    const row = LoopRow{ .names = &names, .cells = &vals };
    try std.testing.expectError(error.InterpFailed, interpAll(a, "${if(pk ==)}", row));
    try std.testing.expectError(error.InterpFailed, interpAll(a, "${name:lower}", row));
}

test "interpAll: a typed loop var binds as its type in an expression body" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const u8{"port"};
    const expr = "${if(port >= 1000, 'big', 'small')}";

    const typed = [_]?types.Type{types.Type.init(.int)};
    try std.testing.expectEqualStrings("big", try interpAll(a, expr, .{ .names = &names, .types = &typed, .cells = &[_][]const u8{"9030"} }));
    try std.testing.expectEqualStrings("small", try interpAll(a, expr, .{ .names = &names, .types = &typed, .cells = &[_][]const u8{"80"} }));

    const untyped = [_]?types.Type{null};
    try std.testing.expectError(error.InterpFailed, interpAll(a, expr, .{ .names = &names, .types = &untyped, .cells = &[_][]const u8{"9030"} }));
}

/// Parse and run a fully-assembled `script`, returning the contents of out.csv.
fn runScript(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, script: []const u8, cli_params: []const ParamArg) ![]u8 {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .params = cli_params }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "param substitution filters by a CLI-bound value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n2,200\n3,50\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM min INT DEFAULT 0;\nLOAD INTO '{s}' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= $min;",
        .{ out_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "min", .val = "100" }});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "IDENTIFIER path: a PARAM resolves outside any FOR EACH" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    // The parser lowers IDENTIFIER(...) to a `${...}` template, which only the
    // for-each renderer used to fill in — a plain script opened the literal
    // path `<dir>/${name}.csv` and failed with FileNotFound.
    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM dir STRING DEFAULT '{s}';\nPARAM name STRING DEFAULT 'in';\n" ++
            "LOAD INTO '{s}' AS SELECT id FROM IDENTIFIER($dir || '/' || $name || '.csv');",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "IDENTIFIER path: a PARAM resolves inside a CTE body" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n3\n4\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    // CTE bodies are registered by a pre-pass that runs before script scope is
    // built, so this used to open the literal path `<dir>/${name}.csv` even though
    // the identical read in a top-level FROM resolved (the test above). Found by
    // the TPC-H harness, where three of five queries put a dimension in a CTE.
    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM dir STRING DEFAULT '{s}';\nPARAM name STRING DEFAULT 'in';\n" ++
            "LOAD INTO '{s}' AS WITH src AS (SELECT id FROM IDENTIFIER($dir || '/' || $name || '.csv'))\n" ++
            "SELECT id FROM src;",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n3\n4\n", out);
}

test "IDENTIFIER path: a LET resolves, and an expression hole sees script scope" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LET dir = '{s}';\nPARAM name STRING DEFAULT 'IN';\n" ++
            "LOAD INTO '{s}' AS SELECT id FROM IDENTIFIER($dir || '/' || lower($name) || '.csv');",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n7\n", out);
}

test "IDENTIFIER path: a loop variable shadows a same-named PARAM" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n5\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    // `name` is bound in both scopes; §9's rule is that the innermost wins, so
    // the read must resolve to in.csv and not to the param's `absent`.
    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM dir STRING DEFAULT '{s}';\nPARAM name STRING DEFAULT 'absent';\n" ++
            "FOR EACH ROW OF (SELECT 'in' AS name) AS (name)\n" ++
            "  LOAD INTO '{s}' AS SELECT id FROM IDENTIFIER($dir || '/' || $name || '.csv');\n" ++
            "END FOR;",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n5\n", out);
}

test "LOAD INTO IDENTIFIER: one output file per for-each row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "cat.csv", .data = "r\nnorth\nsouth\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LET dir = '{s}';\n" ++
            "FOR EACH ROW OF (SELECT r FROM '{s}/cat.csv') AS (r)\n" ++
            "  LOAD INTO IDENTIFIER($dir || '/out_' || $r || '.csv') AS SELECT $r AS region;\n" ++
            "END FOR;",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };

    const north = try tmp.dir.readFileAlloc(alloc, "out_north.csv", 1 << 16);
    defer alloc.free(north);
    try std.testing.expectEqualStrings("region\nnorth\n", north);
    const south = try tmp.dir.readFileAlloc(alloc, "out_south.csv", 1 << 16);
    defer alloc.free(south);
    try std.testing.expectEqualStrings("region\nsouth\n", south);
}

test "aggregate: a literal tag sits beside an aggregate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,g\n1,a\n2,b\n3,a\n",
        "SELECT 'nightly' AS run, COUNT(*) AS c FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("run,c\nnightly,3\n", out);
}

test "aggregate: a PARAM tag sits beside a grouped aggregate, in select order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,g\n1,a\n2,b\n3,a\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM tag STRING DEFAULT 'x';\nLOAD INTO '{s}' AS " ++
            "SELECT $tag AS run, g, COUNT(*) AS c FROM '{s}' GROUP BY g ORDER BY g;",
        .{ out_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "tag", .val = "nightly" }});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("run,g,c\nnightly,a,2\nnightly,b,1\n", out);
}

test "aggregate: a loop variable and a function parameter count as constants" {
    const alloc = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    // A loop variable is script scope per §9's rule, exactly as a PARAM is, so it
    // has one value per row and may sit beside an aggregate. This was refused while
    // the same shape with a PARAM was allowed — `SELECT $tabela, COUNT(*)` over a
    // discovered catalog is the whole point of a for-each.
    _ = try parser.parseSource(ar.allocator(), "FOR EACH ROW OF (SELECT 'z' AS x) AS (x)\n" ++
        "  LOAD INTO '/tmp/o.csv' AS SELECT $x AS a, COUNT(*) AS n FROM 'in.csv';\n" ++
        "END FOR;", &pdiag);

    // Same for a statement function's parameters, which bind the same way.
    _ = try parser.parseSource(ar.allocator(), "CREATE FUNCTION f(t) AS\n" ++
        "  LOAD INTO '/tmp/o.csv' AS SELECT $t AS a, COUNT(*) AS n FROM 'in.csv';\n" ++
        "END;\nCALL f('x');", &pdiag);

    // Scoped to the body: outside it the name is an ordinary column again, and a
    // column beside an aggregate is still refused.
    var d2: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parser.parseSource(ar.allocator(), "FOR EACH ROW OF (SELECT 'z' AS x) AS (x)\n" ++
        "  LOAD INTO '/tmp/o.csv' AS SELECT $x AS a FROM 'in.csv';\n" ++
        "END FOR;\n" ++
        "LOAD INTO '/tmp/p.csv' AS SELECT x, COUNT(*) AS n FROM 'in.csv';", &d2);
    try std.testing.expectError(error.ParseFailed, r);
    try std.testing.expect(std.mem.indexOf(u8, d2.msg, "neither an aggregate") != null);
}

test "aggregate: a bare column beside an aggregate is still refused" {
    const alloc = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    // `g` has no single value per group; only constants get the new pass.
    const r = parser.parseSource(ar.allocator(), "LOAD INTO '/tmp/o.csv' AS SELECT g, COUNT(*) AS c FROM 'in.csv';", &pdiag);
    try std.testing.expectError(error.ParseFailed, r);
    try std.testing.expect(std.mem.indexOf(u8, pdiag.msg, "neither an aggregate nor a grouping key") != null);
}

test "read a semicolon latin-1 file end to end" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The shape the CVM fund registry ships in.
    const out = try runToString(
        alloc,
        &tmp,
        "SIT;N\r\nLIQUIDA\xC7\xC3O;2\r\nCANCELADA;1\r\n",
        "SELECT SIT, N FROM '$IN' WITH (delimiter = ';', encoding = 'latin1') ORDER BY N DESC",
    );
    defer alloc.free(out);
    // Out comes UTF-8, comma-separated, with the columns actually separated.
    try std.testing.expectEqualStrings("SIT,N\nLIQUIDAÇÃO,2\nCANCELADA,1\n", out);
}

test "an unreadable extension is refused by run, not just check" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.json", .data = "{\"a\":1}\n{\"a\":2}\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.json" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT COUNT(*) AS c FROM '{s}';", .{ out_path, in_path });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    // `run` does not analyze the pipeline first, so this is its own guard.
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "cannot read") != null);
}

test "WITH (format = 'csv') reads a file whose extension says nothing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.dat", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.dat" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT id FROM '{s}' WITH (format = 'csv');", .{ out_path, in_path });
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

const fx_zip = @embedFile("../connect/testdata/two_members.zip");

test "read a zip member end to end with the :: reference" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = fx_zip });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT id, v FROM '{s}/t.zip :: a.csv' ORDER BY id;",
        .{ out_path, base },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,v\n1,x\n2,y\n", out);
}

test "a zip holding several files refuses to guess which one" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = fx_zip });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT * FROM '{s}/t.zip';", .{ out_path, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    // The message has to name the candidates, or the user has to go find them.
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "a.csv") != null);
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "b.csv") != null);
}

test "explode splits a delimited column into rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,tags\n1,\"a,b,c\"\n2,x\n3,\n",
        "SELECT * FROM '$IN' CROSS JOIN UNNEST(tags) AS tag",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,b\n1,c\n2,x\n", out);
}

test "JSON_EACH explodes a JSON array; json_get walks keys and indexes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const in =
        \\id,doc
        \\1,"{""name"":{""common"":""Brazil""},""capital"":[""Brasília""],""tags"":[""a"",1,null,{""k"":true}]}"
        \\2,"{""name"":{},""tags"":[]}"
        \\3,"{""tags"":null}"
        \\
    ;
    const got = try runToString(alloc, &tmp, in,
        \\SELECT id, json_get(doc, 'name.common') AS name, json_get(doc, 'capital[0]') AS cap, json_get(doc, '$.capital.0') AS cap2
        \\FROM '$IN'
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("id,name,cap,cap2\n1,Brazil,Brasília,Brasília\n2,,,\n3,,,\n", got);

    var tmp2 = std.testing.tmpDir(.{});
    defer tmp2.cleanup();
    const tags = try runToString(alloc, &tmp2, in,
        \\SELECT id, tag FROM (SELECT id, json_get(doc, 'tags') AS tags FROM '$IN')
        \\CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag
    );
    defer alloc.free(tags);
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,1\n1,\n1,\"{\"\"k\"\":true}\"\n", tags);
}

test "parallel driver matches serial output across many batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,amount\n");
    var k: usize = 0;
    while (k < 5000) : (k += 1) try in.writer().print("{d},{d}\n", .{ k, (k * 7) % 1000 });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });

    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);

    var outputs: [2][]u8 = undefined;
    for ([_]usize{ 1, 4 }, 0..) |nthreads, idx| {
        const out_path = try std.fs.path.join(alloc, &.{ base, if (idx == 0) "s.csv" else "p.csv" });
        defer alloc.free(out_path);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT id, CAST(amount AS INT) * 2 AS doubled FROM '{s}' WHERE CAST(amount AS INT) >= 500;", .{ out_path, in_path });
        defer alloc.free(script);

        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

        var rdiag: Diag = .{};
        _ = try run(alloc, prog, .{ .threads = nthreads }, &rdiag);
        outputs[idx] = try tmp.dir.readFileAlloc(alloc, if (idx == 0) "s.csv" else "p.csv", 1 << 20);
    }
    defer alloc.free(outputs[0]);
    defer alloc.free(outputs[1]);

    const s = try sortedLines(alloc, outputs[0]);
    defer alloc.free(s);
    const p = try sortedLines(alloc, outputs[1]);
    defer alloc.free(p);
    try std.testing.expectEqualStrings(s, p);
    try std.testing.expect(std.mem.indexOf(u8, outputs[1], "id,doubled\n") != null);
}

/// Sort the newline-separated lines of `text` (for order-insensitive comparison of
/// parallel vs serial output). Caller frees.
fn sortedLines(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    var lines = std.array_list.Managed([]const u8).init(alloc);
    defer lines.deinit();
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |l| try lines.append(l);
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    var out = std.array_list.Managed(u8).init(alloc);
    errdefer out.deinit();
    for (lines.items) |l| {
        try out.appendSlice(l);
        try out.append('\n');
    }
    return out.toOwnedSlice();
}

test "let binding + inner join" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,B\n3,Z\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\nSELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,label\n1,Apple\n2,Banana\n", out);
}

test "aggregate folds groups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("code,amount,name\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{c},{d},n{d:0>4}\n", .{ "XY"[i % 2], i, i });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT code, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total, MIN(name) AS first_name FROM '{s}/in.csv' GROUP BY code ORDER BY code;",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("code,n,total,first_name\nX,1500,2248500,n0000\nY,1500,2250000,n0001\n", out);
}

test "global aggregate streams vectorized partials across batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("amount\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{d}\n", .{i});
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total, MIN(CAST(amount AS INT)) AS lo, MAX(CAST(amount AS INT)) AS hi FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("n,total,lo,hi\n3000,4498500,0,2999\n", out);
}

test "distinct dedups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("code\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{c}\n", .{"XYZ"[i % 3]});
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT DISTINCT * FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("code\nX\nY\nZ\n", out);
}

test "join probe side spanning multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("id,code\n");
    var i: usize = 0;
    while (i < 2500) : (i += 1) {
        try in_buf.writer().print("{d},{s}\n", .{ i, if (i % 2 == 0) "A" else "B" });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\nSELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqual(@as(usize, 2501), std.mem.count(u8, out, "\n"));
    try std.testing.expect(std.mem.startsWith(u8, out, "id,label\n0,Apple\n1,Banana\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "\n2499,Banana\n") != null);
}

test "union reconciles branches to a canon schema (tag, null-fill, drop-extra)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,w\n3,99\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS\nSELECT '01' AS src, t.* FROM '{s}/a.csv' t\nUNION ALL BY NAME\nSELECT '02' AS src, t.* FROM '{s}/b.csv' t\nANCHOR SCHEMA first;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expect(std.mem.startsWith(u8, out, "src,id,v\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "w") == null);
    try std.testing.expect(std.mem.indexOf(u8, out, "01,1,10") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "01,2,20") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "02,3,") != null);
}

test "for-each fans out over a discovered list with interpolation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
}

test "log defaults to warn: a healthy run says nothing" {
    try std.testing.expectEqual(obs.Level.warn, (LogConfig{}).level);
}

test "for-each discovers its rows from an in-engine SELECT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "catalog.csv", .data = "name,active\nALPHA,1\nBETA,1\nGAMMA,0\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    try tmp.dir.writeFile(.{ .sub_path = "gamma.csv", .data = "id,v\n9,90\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF (SELECT name, lower(name) AS slug FROM '{s}/catalog.csv' WHERE active = 1) AS (name, slug)\n  LOAD INTO '{s}/out_${{slug}}.csv' AS SELECT id, v FROM '{s}/${{slug}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
    // `active = 0` never reached the body: the discovery filter ran in-engine.
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out_gamma.csv", .{}));
}

test "for-each SELECT discovery with fewer columns than loop variables fails to plan" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "catalog.csv", .data = "name\nalpha\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF (SELECT name FROM '{s}/catalog.csv') AS (name, slug)\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "fewer columns than loop variables") != null);
}

test "sqlWithWhere: table appends WHERE, query wraps, empty is a no-op" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings(
        "SELECT * FROM SC1010 WHERE S_T_A_M_P_ >= '2026-05-09'",
        try sqlWithWhere(a, "SELECT * FROM SC1010", false, "S_T_A_M_P_ >= '2026-05-09'"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT id FROM t WHERE x = 1) _w WHERE id > 5",
        try sqlWithWhere(a, "SELECT id FROM t WHERE x = 1", true, "id > 5"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM SC1010",
        try sqlWithWhere(a, "SELECT * FROM SC1010", false, ""),
    );
}

test "for-each parallel + on_error=continue isolates a failing table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nghost\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name) PARALLEL ON ERROR CONTINUE\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .threads = 2 }, &rdiag));
    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id\n7\n", a);
}

test "FROM BUFFER replays WAL segments as a source (batch mode)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const wal_dir = try std.fs.path.join(alloc, &.{ base, "wal" });
    defer alloc.free(wal_dir);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    {
        var w = try Wal.open(alloc, wal_dir, "ev", 1 << 20);
        defer w.close();
        try w.append("{\"device_id\":\"a\",\"v\":1}");
        try w.append("{\"device_id\":\"b\",\"v\":2}");
        try w.rotate();
        try w.append("{\"device_id\":\"c\",\"v\":3}");
        try w.sync();
    }

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT device_id, CAST(v AS INT) AS v FROM BUFFER 'ev' AT '{s}';",
        .{ out_path, wal_dir },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("device_id,v\na,1\nb,2\nc,3\n", out);
}

test "empty source and an all-dropping filter still write just the header" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const empty = try runToString(alloc, &tmp, "id,v\n", "SELECT id FROM '$IN'");
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("id\n", empty);
    const dropped = try runToString(alloc, &tmp, "id,v\n1,10\n2,20\n", "SELECT * FROM '$IN' WHERE v = 999");
    defer alloc.free(dropped);
    try std.testing.expectEqualStrings("id,v\n", dropped);
}

test "csv aggregate: min/max on inferred numeric columns compare numerically, not lexically" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "id\n9\n10\n2\n", "SELECT MIN(id) AS mn, MAX(id) AS mx, SUM(id) AS s FROM '$IN'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("mn,mx,s\n2,10,21\n", got);
}

test "limit: plain (unfused) limit takes the first N; offset past the end empties" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first2 = try runToString(alloc, &tmp, "id\n1\n2\n3\n", "SELECT * FROM '$IN' LIMIT 2");
    defer alloc.free(first2);
    try std.testing.expectEqualStrings("id\n1\n2\n", first2);
    const none = try runToString(alloc, &tmp, "id\n1\n2\n3\n", "SELECT * FROM '$IN' LIMIT 5 OFFSET 100");
    defer alloc.free(none);
    try std.testing.expectEqualStrings("id\n", none);
}

test "join: an empty build side drops all rows (inner) and null-fills (left)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,B\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/inner.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT t.id, l.label FROM '{s}/in.csv' t JOIN labels l ON t.code = l.code;\n" ++
        "LOAD INTO '{s}/left.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT t.id, l.label FROM '{s}/in.csv' t LEFT JOIN labels l ON t.code = l.code;", .{ base, base, base, base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const inner = try tmp.dir.readFileAlloc(alloc, "inner.csv", 1 << 20);
    defer alloc.free(inner);
    try std.testing.expectEqualStrings("id,label\n", inner);
    const left = try tmp.dir.readFileAlloc(alloc, "left.csv", 1 << 20);
    defer alloc.free(left);
    try std.testing.expectEqualStrings("id,label\n1,\n2,\n", left);
}

test "join: two-key ON matches on both columns" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,day,code\n1,mon,A\n2,tue,A\n3,mon,B\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "day,code,label\nmon,A,first\ntue,A,second\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\n" ++
            "SELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code AND l.day = t.day;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    // Row 3 (mon,B) has no lookup row; row 2 matches only on the (tue,A) pair.
    try std.testing.expectEqualStrings("id,label\n1,first\n2,second\n", out);
}

test "join: duplicate build keys fan out (inner); semi/anti reduce to existence" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,Z\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,x1\nA,x2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/inner.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT t.id, l.label FROM '{s}/in.csv' t JOIN labels l ON t.code = l.code;\n" ++
        "LOAD INTO '{s}/semi.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT * FROM '{s}/in.csv' t SEMI JOIN labels l ON t.code = l.code;\n" ++
        "LOAD INTO '{s}/anti.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT * FROM '{s}/in.csv' t ANTI JOIN labels l ON t.code = l.code;", .{ base, base, base, base, base, base, base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const inner = try tmp.dir.readFileAlloc(alloc, "inner.csv", 1 << 20);
    defer alloc.free(inner);
    try std.testing.expectEqualStrings("id,label\n1,x1\n1,x2\n", inner);
    const semi = try tmp.dir.readFileAlloc(alloc, "semi.csv", 1 << 20);
    defer alloc.free(semi);
    try std.testing.expectEqualStrings("id,code\n1,A\n", semi);
    const anti = try tmp.dir.readFileAlloc(alloc, "anti.csv", 1 << 20);
    defer alloc.free(anti);
    try std.testing.expectEqualStrings("id,code\n2,Z\n", anti);
}

test "statement-level CASE dispatches on a resolved param (default arm otherwise)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,v\n1,10\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "PARAM mode STRING DEFAULT 'small';\nCASE $mode\n" ++
        "  WHEN 'big' THEN LOAD INTO '{s}/out.csv' AS SELECT id, v FROM '{s}/in.csv';\n" ++
        "  ELSE LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';\nEND CASE;", .{ base, base, base, base });
    defer alloc.free(script);

    const dflt = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(dflt);
    try std.testing.expectEqualStrings("id\n1\n", dflt);
    const big = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "mode", .val = "big" }});
    defer alloc.free(big);
    try std.testing.expectEqualStrings("id,v\n1,10\n", big);
}

test "for-each on_error=continue with an OutcomeSink: run succeeds, failure recorded" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nghost\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name) SEQUENTIAL ON ERROR CONTINUE\n" ++
        "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var oc_arena = std.heap.ArenaAllocator.init(alloc);
    defer oc_arena.deinit();
    var outcomes = OutcomeSink.init(oc_arena.allocator());
    defer outcomes.deinit();

    var rdiag: Diag = .{};
    const stats = try run(alloc, prog, .{ .outcomes = &outcomes }, &rdiag);
    try std.testing.expectEqual(@as(usize, 1), stats.rows_out);
    try std.testing.expectEqual(@as(usize, 2), outcomes.list.items.len);
    try std.testing.expectEqual(@as(usize, 1), outcomes.failures());
    for (outcomes.list.items) |o| {
        if (o.ok) {
            try std.testing.expectEqualStrings("alpha", o.item);
        } else {
            try std.testing.expectEqualStrings("ghost", o.item);
            try std.testing.expect(o.err.len > 0);
            try std.testing.expect(!o.retryable);
        }
    }
}

test "for-each with an empty discovery list is a no-op" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
        "  LOAD INTO '{s}/out.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    const stats = try run(alloc, prog, .{}, &rdiag);
    try std.testing.expectEqual(@as(usize, 0), stats.rows_out);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20));
}

test "generated sources: SELECT with no FROM and RANGE(lo, hi)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/one.csv' AS SELECT 1 AS x, 'a' AS s;\n" ++
        "LOAD INTO '{s}/r.csv' AS SELECT range AS i FROM RANGE(2, 5) WHERE range != 3;\n", .{ base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{}, &rdiag);

    const one = try tmp.dir.readFileAlloc(alloc, "one.csv", 1 << 20);
    defer alloc.free(one);
    try std.testing.expectEqualStrings("x,s\n1,a\n", one);
    const r = try tmp.dir.readFileAlloc(alloc, "r.csv", 1 << 20);
    defer alloc.free(r);
    try std.testing.expectEqualStrings("i\n2\n4\n", r);
}

/// Build the LET fixture script: a param, a LET over it, a LET over that LET, and
/// two pipelines that both reference them.
fn letScript(alloc: std.mem.Allocator, base: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc,
        \\PARAM days INT DEFAULT 3;
        \\LET span = $days * 10;
        \\LET label = concat('n', $span);
        \\LOAD INTO '{s}/a.csv' AS SELECT id, $span AS s, $label AS l FROM '{s}/in.csv';
        \\LOAD INTO '{s}/b.csv' AS SELECT $label AS l FROM '{s}/in.csv';
    , .{ base, base, base, base });
}

fn runLetScript(alloc: std.mem.Allocator, script: []const u8, cli: []const ParamArg, rdiag: *Diag) !void {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    _ = try run(alloc, prog, .{ .params = cli, .log = .{ .summary = .none, .quiet = true } }, rdiag);
}

test "LET folds once at plan time and hands every pipeline the same value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try runLetScript(alloc, script, &[_]ParamArg{}, &rdiag);

    const a = try tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,s,l\n1,30,n30\n2,30,n30\n", a);
    const b = try tmp.dir.readFileAlloc(alloc, "b.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("l\nn30\nn30\n", b);
}

test "LET reads a param, so `-p` reaches it indirectly" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try runLetScript(alloc, script, &[_]ParamArg{.{ .key = "days", .val = "7" }}, &rdiag);

    const a = try tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,s,l\n1,70,n70\n", a);
}

test "a LET is sealed: `-p` naming one is a plan error, not a silent override" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, runLetScript(alloc, script, &[_]ParamArg{.{ .key = "span", .val = "99" }}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "`span` is a LET, not a PARAM") != null);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20));
}

test "a LET and a PARAM may not share a name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        \\PARAM span INT DEFAULT 1;
        \\LET span = 5;
        \\LOAD INTO '{s}/a.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, runLetScript(alloc, script, &[_]ParamArg{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "declared twice") != null);
}

test "CALL renders a statement function per call (defaults fill the omitted arg)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "CREATE FUNCTION sync(name, tag DEFAULT 'Z') AS\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v, '${{tag}}' AS tag FROM '{s}/${{name}}.csv';\n" ++
            "END;\n" ++
            "CALL sync('alpha', 'A');\n" ++
            "CALL sync('beta');\n",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v,tag\n1,10,A\n2,20,A\n", a);
    try std.testing.expectEqualStrings("id,v,tag\n3,30,Z\n", b);
}

test "CALL nesting is depth-guarded (mutual recursion)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "CREATE FUNCTION ping(n) AS\n  CALL pong($n);\nEND;\n" ++
            "CREATE FUNCTION pong(n) AS\n  LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';\n  CALL ping($n);\nEND;\n" ++
            "CALL ping('x');\n",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "too deep") != null);
}

test "THROW: a fired guard is the verbatim, permanent error; a false WHEN is a no-op" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const guarded = try std.fmt.allocPrint(alloc,
        \\PARAM tbl STRING DEFAULT '';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(guarded);
    const gprog = try parser.parseSource(parena.allocator(), guarded, &pdiag);

    var gdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, gprog, .{}, &gdiag));
    try std.testing.expectEqualStrings("tbl is required (e.g. -p tbl=SC5)", gdiag.msg);
    try std.testing.expect(!gdiag.retryable);
    try std.testing.expect(!isTransient(error.PlanFailed));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20));

    var okdiag: Diag = .{};
    _ = try run(alloc, gprog, .{ .params = &[_]ParamArg{.{ .key = "tbl", .val = "SC5" }} }, &okdiag);
    const wrote = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(wrote);
    try std.testing.expect(std.mem.indexOf(u8, wrote, "id") != null);

    const bare = try std.fmt.allocPrint(alloc,
        \\LET tag = 'zz';
        \\THROW 'unreachable branch: ' || $tag;
        \\LOAD INTO '{s}/never.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(bare);
    const bprog = try parser.parseSource(parena.allocator(), bare, &pdiag);

    var bdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, bprog, .{}, &bdiag));
    try std.testing.expectEqualStrings("unreachable branch: zz", bdiag.msg);
    try std.testing.expect(!bdiag.retryable);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "never.csv", 1 << 20));
}

test "PRINT renders literals, `||` over params, and a loop variable" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var params = std.StringHashMap(Value).init(a);
    try params.put("since", .{ .string = "2024-01-01" });
    try params.put("n", .{ .int = 7 });

    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const lit = try parser.parseExprStr(a, "'loading'", &pdiag);
    try std.testing.expectEqualStrings("loading", try printText(a, lit, no_loop_vars, &params));

    const cat = try parser.parseExprStr(a, "'since ' || $since", &pdiag);
    try std.testing.expectEqualStrings("since 2024-01-01", try printText(a, cat, no_loop_vars, &params));

    const num = try parser.parseExprStr(a, "$n", &pdiag);
    try std.testing.expectEqualStrings("7", try printText(a, num, no_loop_vars, &params));

    const lr = LoopRow{ .names = &[_][]const u8{"name"}, .cells = &[_][]const u8{"acme"} };
    const row = try parser.parseExprStr(a, "'company ' || $name", &pdiag);
    try std.testing.expectEqualStrings("company acme", try printText(a, row, lr, &params));
}

test "PRINT runs at the top level and per row inside a FOR EACH body" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM tag STRING DEFAULT 'nightly';\n" ++
            "PRINT 'run ' || $tag;\n" ++
            "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
            "  PRINT 'company ' || $name;\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v FROM '{s}/${{name}}.csv';\n" ++
            "END FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
}

test "PRINT over an unbound name is a plan error, not a silent blank" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "PRINT 'x ' || $nope;\nLOAD INTO '{s}/a.csv' AS SELECT id FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "PRINT:") != null);
}

/// Run a join script over `in.csv` (probe) + `lookup.csv` (build) at an explicit
/// thread count. `$IN`/`$LOOKUP` in `body` are replaced with the two paths; the
/// result comes back as sorted lines, since lanes interleave the output order.
fn runJoinThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, body: []const u8, threads: usize) ![]u8 {
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_name = try std.fmt.allocPrint(alloc, "out{d}.csv", .{threads});
    defer alloc.free(out_name);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, out_name });
    defer alloc.free(out_path);

    const q1 = try std.mem.replaceOwned(u8, alloc, body, "$IN", in_path);
    defer alloc.free(q1);
    const q2 = try std.mem.replaceOwned(u8, alloc, q1, "$LOOKUP", lookup_path);
    defer alloc.free(q2);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q2 });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const raw = try tmp.dir.readFileAlloc(alloc, out_name, 1 << 20);
    defer alloc.free(raw);
    return sortedLines(alloc, raw);
}

/// `in.csv` = 2000 probe rows cycling five codes; `lookup.csv` = labels for three
/// of them, so two fifths of the probe side stays unmatched.
fn writeJoinFixtures(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) !void {
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,code\n");
    var i: usize = 0;
    while (i < 2000) : (i += 1) try in.writer().print("{d},{c}\n", .{ i, "ABCDE"[i % 5] });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\nC,Cherry\n" });
}

test "parallel join: inner join over CSV chunks matches the serial driver" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.label FROM '$IN' t JOIN labels l ON t.code = l.code";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    // header + 3 matching codes x 400 rows + the trailing empty line
    try std.testing.expectEqual(@as(usize, 1202), std.mem.count(u8, par, "\n"));
}

test "parallel join: left join keeps unmatched probe rows on every lane" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.label FROM '$IN' t LEFT JOIN labels l ON t.code = l.code";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expectEqual(@as(usize, 2002), std.mem.count(u8, par, "\n"));
}

test "parallel join: a suffix filter on a right-side column runs after the join" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.label FROM '$IN' t JOIN labels l ON t.code = l.code WHERE l.label = 'Cherry'";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expectEqual(@as(usize, 402), std.mem.count(u8, par, "\n"));
    try std.testing.expect(std.mem.indexOf(u8, par, "Apple") == null);
}

test "parallel join: a breaker after the join falls back to the serial driver" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    // An aggregate past the join fails every classifier, so this runs serially —
    // the fallback must still produce the same answer at threads > 1. (RIGHT/FULL,
    // the other serial-only join case, is rejected by the SQL parser today, so it
    // has no runnable script to compare.)
    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT l.label, COUNT(*) AS n FROM '$IN' t JOIN labels l ON t.code = l.code GROUP BY l.label";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expect(std.mem.indexOf(u8, par, "Cherry,400") != null);
}

test "join kinds allowed on the parallel probe path" {
    try std.testing.expect(joinKindLaneSafe(.inner));
    try std.testing.expect(joinKindLaneSafe(.left));
    try std.testing.expect(joinKindLaneSafe(.semi));
    try std.testing.expect(joinKindLaneSafe(.anti));
}

test "classifyWholeAgg: filters-only prefix, unrestricted tail, no hints" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const p = ast.Pos{ .line = 0, .col = 0 };
    const pred = try a.create(ast.Expr);
    pred.* = .{ .bool_lit = true };
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;

    const rd = ast.Stage{ .node = .{ .read = .{ .connector = "db", .form = .{ .table = .{ .parts = &.{"t"} } } } }, .hints = &.{}, .pos = p };
    const flt = ast.Stage{ .node = .{ .filter = pred }, .hints = &.{}, .pos = p };
    const sel = ast.Stage{ .node = .{ .select = items }, .hints = &.{}, .pos = p };
    const agg = ast.Stage{ .node = .{ .aggregate = .{ .aggs = &.{}, .by = &.{} } }, .hints = &.{}, .pos = p };
    const wrt = ast.Stage{ .node = .{ .write = .{ .connector = "csv", .form = null, .target = "o.csv", .mode = .default } }, .hints = &.{}, .pos = p };

    const simple = [_]ast.Stage{ rd, flt, agg, wrt };
    const s1 = classifyWholeAgg(&simple).?;
    try std.testing.expectEqual(@as(usize, 1), s1.prefix.len);
    try std.testing.expectEqual(@as(usize, 0), s1.tail.len);

    // A post-aggregate filter is HAVING: it stays engine-side, so it must NOT
    // disqualify the descent — nor the lane shape, whose tail runs it over
    // the merged groups.
    const having = [_]ast.Stage{ rd, agg, flt, flt, wrt };
    const s2 = classifyWholeAgg(&having).?;
    try std.testing.expectEqual(@as(usize, 0), s2.prefix.len);
    try std.testing.expectEqual(@as(usize, 2), s2.tail.len);
    const lane = classifyAggPipeline(&having).?;
    try std.testing.expectEqual(@as(usize, 0), lane.prefix.len);
    try std.testing.expectEqual(@as(usize, 2), lane.tail.len);

    // A `select` in the prefix renames columns out from under the group keys.
    const selected = [_]ast.Stage{ rd, sel, agg, wrt };
    try std.testing.expect(classifyWholeAgg(&selected) == null);

    // No aggregate at all, and an aggregate behind a hinted read.
    const no_agg = [_]ast.Stage{ rd, flt, wrt };
    try std.testing.expect(classifyWholeAgg(&no_agg) == null);

    const hints = try a.alloc(ast.Hint, 1);
    hints[0] = .{ .key = "split", .value = .{ .ident = "id" }, .pos = p };
    var hinted_rd = rd;
    hinted_rd.hints = hints;
    const hinted = [_]ast.Stage{ hinted_rd, flt, agg, wrt };
    try std.testing.expect(classifyWholeAgg(&hinted) == null);
}

test "a plan failure carries the failing stage's position" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS\nSELECT id\nFROM '{s}'\nWHERE nosuch > 1;", .{ base, in_path });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expectEqualStrings("unknown field `nosuch`", rdiag.msg);
    try std.testing.expectEqual(@as(u32, 4), rdiag.pos.?.line);
    try std.testing.expectEqual(@as(u32, 7), rdiag.pos.?.col);
}

test "decimal arithmetic is exact: + - * stay DECIMAL, / is a float" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "a,b\n1.1,0.3\n28875360.09,21199414.08\n",
        \\SELECT CAST(a AS DECIMAL(18,2)) + CAST(b AS DECIMAL(18,2)) AS s,
        \\       CAST(a AS DECIMAL(18,2)) - CAST(b AS DECIMAL(18,2)) AS d,
        \\       CAST(a AS DECIMAL(18,2)) * CAST(b AS DECIMAL(18,2)) AS p,
        \\       CAST(a AS DECIMAL(18,2)) * 3 AS p3,
        \\       CAST(a AS DECIMAL(18,2)) / 2 AS q
        \\FROM '$IN'
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("s,d,p,p3,q\n1.40,0.80,0.3300,3.30,0.55\n50074774.17,7675946.01,612140715257016.0672,86626080.27,14437680.045\n", got);
}

test "MEDIAN: odd count takes the middle, even count the mean of the two, nulls ignored, empty is null" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,v\na,3\na,1\na,\na,2\nb,20\nb,10\nc,\n", "SELECT k, MEDIAN(v) AS m FROM '$IN' GROUP BY k ORDER BY k");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("k,m\na,2\nb,15\nc,\n", got);
}

test "csv: a column of ISO dates is inferred as DATE (empty cells are null)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "d\n2026-01-31\n\n2026-02-01\n", "SELECT date_add('day', 1, d) AS n FROM '$IN' WHERE d >= '2026-01-01'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("n\n2026-02-01\n2026-02-02\n", got);
}

test "window ROWS frame slides: bounded sum/min/max/count per partition, nulls skipped" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,i,v\n0,0,0\n0,2,4\n0,4,8\n0,6,2\n0,8,6\n1,1,7\n1,3,1\n1,5,\n1,7,9\n1,9,3\n",
        \\SELECT k, i, v,
        \\       SUM(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s3,
        \\       MIN(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mn3,
        \\       MAX(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mx3,
        \\       COUNT(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS c3
        \\FROM '$IN' ORDER BY k, i
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("k,i,v,s3,mn3,mx3,c3\n0,0,0,0,0,0,1\n0,2,4,4,0,4,2\n0,4,8,12,0,8,3\n0,6,2,14,2,8,3\n0,8,6,16,2,8,3\n1,1,7,7,7,7,1\n1,3,1,8,1,7,2\n1,5,,8,1,7,2\n1,7,9,10,1,9,2\n1,9,3,12,3,9,2\n", got);
}

test "sort: the radix fast path keeps input order on ties, honors DESC and multi-key; nulls still sort last" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const desc = try runToString(alloc, &tmp, "i,k\n0,1\n1,0\n2,1\n4,0\n", "SELECT i FROM '$IN' ORDER BY k DESC");
    defer alloc.free(desc);
    try std.testing.expectEqualStrings("i\n0\n2\n1\n4\n", desc);
    const multi = try runToString(alloc, &tmp, "i,k\n0,1\n1,0\n2,1\n4,0\n", "SELECT i FROM '$IN' ORDER BY k, i DESC");
    defer alloc.free(multi);
    try std.testing.expectEqualStrings("i\n4\n1\n2\n0\n", multi);
    const nulls = try runToString(alloc, &tmp, "i,k\n0,1\n1,0\n2,1\n3,\n4,0\n", "SELECT i FROM '$IN' ORDER BY k DESC");
    defer alloc.free(nulls);
    try std.testing.expectEqualStrings("i\n0\n2\n1\n4\n3\n", nulls);
}

test "distinct: the single int-key path keeps first occurrences, null included once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,v\n1,2\n2,\n3,1\n4,2\n5,\n6,1\n", "SELECT DISTINCT v FROM '$IN'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("v\n2\n\n1\n", got);
}

test "parallel parquet: an aggregate after DISTINCT and a HAVING both fan out and the sink gets the tail's columns" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const dist = try runParquetThreaded(alloc, &t1, "SELECT COUNT(*) AS n, MIN(g) AS mg FROM (SELECT DISTINCT g FROM '$IN') t", 4);
    defer alloc.free(dist);
    try std.testing.expectEqualStrings("n,mg\n4,0\n", dist);
    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    // g = id % 4 over id = 1..5000: SUM(id) is 3127500, 3123750, 3125000, 3126250 for g = 0..3.
    const having = try runParquetThreaded(alloc, &t2, "SELECT g, COUNT(*) AS c FROM '$IN' GROUP BY g HAVING SUM(id) > 3125000 ORDER BY g", 4);
    defer alloc.free(having);
    try std.testing.expectEqualStrings("g,c\n0,1250\n3,1250\n", having);
}

/// Check, then run, a script in which `$B` stands for the tmp dir — so every test
/// below also asserts that `check` accepts what `run` executes. Fixtures: `a.csv`
/// and `b.csv` (id, grp, amt), `outer.csv` (name: a, b), `inner.csv` (suffix: one, two).
fn checkAndRun(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, tmpl: []const u8, threads: usize, cli_params: []const ParamArg) !void {
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,grp,amt\n7,x,1\n8,x,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "outer.csv", .data = "name\na\nb\n" });
    try tmp.dir.writeFile(.{ .sub_path = "inner.csv", .data = "suffix\none\ntwo\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc, tmpl, "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSource(parena.allocator(), script, &pdiag) catch |e| {
        std.debug.print("parse error: {s}\n", .{pdiag.msg});
        return e;
    };
    const overrides = try parena.allocator().alloc(analyze.ParamOverride, cli_params.len);
    for (cli_params, overrides) |kv, *o| o.* = .{ .name = kv.key, .value = kv.val };
    var adiag = analyze.Diag{};
    _ = analyze.analyzeWith(parena.allocator(), prog, overrides, &adiag) catch |e| {
        std.debug.print("check error: {s}\n", .{adiag.msg});
        return e;
    };
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads, .params = cli_params, .log = .{ .quiet = true } }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
}

fn expectFile(tmp: *std.testing.TmpDir, name: []const u8, want: []const u8) !void {
    const alloc = std.testing.allocator;
    const got = try tmp.dir.readFileAlloc(alloc, name, 1 << 16);
    defer alloc.free(got);
    try std.testing.expectEqualStrings(want, got);
}

const nested_loop_script =
    \\FOR EACH ROW OF ('$B/outer.csv') AS (name) MODE
    \\  FOR EACH ROW OF ('$B/inner.csv') AS (suffix)
    \\    THROW 'never ' || $name WHEN $name = 'zzz';
    \\    CASE $name WHEN 'a' THEN
    \\      LOAD INTO IDENTIFIER('$B/out_' || $name || '_' || $suffix || '.csv') AS
    \\      WITH src AS (SELECT grp, amt FROM IDENTIFIER('$B/' || $name || '.csv'))
    \\      SELECT grp, SUM(amt) AS total, $name AS src_name, $suffix AS sfx FROM src GROUP BY grp ORDER BY grp;
    \\    END CASE;
    \\  END FOR;
    \\END FOR;
;

test "nested for-each: an outer loop variable is a name, a value, a THROW operand and a CASE subject" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "SEQUENTIAL", "PARALLEL" }) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const script = try std.mem.replaceOwned(u8, alloc, nested_loop_script, "MODE", mode);
        defer alloc.free(script);
        try checkAndRun(alloc, &tmp, script, 4, &.{});
        try expectFile(&tmp, "out_a_one.csv", "grp,total,src_name,sfx\na,30,a,one\nb,5,a,one\n");
        try expectFile(&tmp, "out_a_two.csv", "grp,total,src_name,sfx\na,30,a,two\nb,5,a,two\n");
        try std.testing.expectError(error.FileNotFound, tmp.dir.access("out_b_one.csv", .{}));
    }
}

test "for-each body WITH: rendered per row, and a same-named top-level WITH is left alone" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "SEQUENTIAL", "PARALLEL" }) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const tmpl =
            \\LOAD INTO '$B/out_first.csv' AS WITH src AS (SELECT id FROM '$B/a.csv') SELECT COUNT(*) AS n FROM src;
            \\FOR EACH ROW OF ('$B/outer.csv') AS (name) MODE
            \\  LOAD INTO IDENTIFIER('$B/out_' || $name || '.csv') AS
            \\  WITH src AS (SELECT id FROM IDENTIFIER('$B/' || $name || '.csv') WHERE id <> 1)
            \\  SELECT COUNT(*) AS n FROM src;
            \\END FOR;
            \\LOAD INTO '$B/out_last.csv' AS SELECT COUNT(*) AS n FROM src;
        ;
        const script = try std.mem.replaceOwned(u8, alloc, tmpl, "MODE", mode);
        defer alloc.free(script);
        try checkAndRun(alloc, &tmp, script, 4, &.{});
        try expectFile(&tmp, "out_first.csv", "n\n3\n");
        try expectFile(&tmp, "out_a.csv", "n\n2\n");
        try expectFile(&tmp, "out_b.csv", "n\n2\n");
        try expectFile(&tmp, "out_last.csv", "n\n3\n");
    }
}

test "top-level WITH: two statements reusing a CTE name each read their own" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/out_1.csv' AS WITH src AS (SELECT id FROM '$B/a.csv') SELECT COUNT(*) AS n FROM src;
        \\LOAD INTO '$B/out_2.csv' AS WITH src AS (SELECT id FROM '$B/b.csv') SELECT COUNT(*) AS n FROM src;
    , 1, &.{});
    try expectFile(&tmp, "out_1.csv", "n\n3\n");
    try expectFile(&tmp, "out_2.csv", "n\n2\n");
}

test "a WITH declared in a for-each body is not visible after it, to check and run alike" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id\n1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "outer.csv", .data = "name\na\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc,
        \\FOR EACH ROW OF ('$B/outer.csv') AS (name)
        \\  LOAD INTO '$B/out.csv' AS WITH src AS (SELECT id FROM '$B/a.csv') SELECT id FROM src;
        \\END FOR;
        \\LOAD INTO '$B/out_2.csv' AS SELECT id FROM src;
    , "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
    try std.testing.expect(std.mem.indexOf(u8, adiag.msg, "src") != null);
    var rdiag: Diag = .{};
    try std.testing.expect(std.meta.isError(run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "src") != null);
}

test "CALL: a nested FOR EACH in a statement function sees its argument and the script PARAMs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\PARAM tag STRING;
        \\CREATE FUNCTION fan(src) AS
        \\  FOR EACH ROW OF ('$B/inner.csv') AS (suffix)
        \\    LOAD INTO IDENTIFIER('$B/' || $tag || '_' || $src || '_' || $suffix || '.csv') AS
        \\    SELECT COUNT(*) AS n, $src AS s, $suffix AS sfx FROM IDENTIFIER('$B/' || $src || '.csv');
        \\  END FOR;
        \\END;
        \\FOR EACH ROW OF ('$B/outer.csv') AS (name)
        \\  CALL fan($name);
        \\END FOR;
    , 1, &[_]ParamArg{.{ .key = "tag", .val = "out" }});
    try expectFile(&tmp, "out_a_one.csv", "n,s,sfx\n3,a,one\n");
    try expectFile(&tmp, "out_b_two.csv", "n,s,sfx\n2,b,two\n");
}

test "LET inside a for-each body is refused by check and run with the same message" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "outer.csv", .data = "name\na\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc,
        \\FOR EACH ROW OF ('$B/outer.csv') AS (name)
        \\  LET x = 1;
        \\  LOAD INTO '$B/out.csv' AS SELECT $name AS n;
        \\END FOR;
    , "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    const want = "LET `x` must be declared at the top level of the script";
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
    try std.testing.expectEqualStrings(want, adiag.msg);
    try std.testing.expectEqual(@as(u32, 2), adiag.pos.?.line);
    var rdiag: Diag = .{};
    try std.testing.expect(std.meta.isError(run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, want) != null);
}

test "IDENTIFIER names a column per row: SELECT list, WHERE, GROUP BY, ORDER BY, DISTINCT ON, an expression" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "cols.csv", .data = "c\ngrp\nid\n" });
    try checkAndRun(alloc, &tmp,
        \\FOR EACH ROW OF ('$B/cols.csv') AS (c)
        \\  LOAD INTO IDENTIFIER('$B/agg_' || $c || '.csv') AS
        \\  SELECT IDENTIFIER($c), COUNT(*) AS n FROM '$B/a.csv'
        \\  WHERE IDENTIFIER($c) IS NOT NULL GROUP BY IDENTIFIER($c) ORDER BY IDENTIFIER($c) DESC;
        \\  LOAD INTO IDENTIFIER('$B/dist_' || $c || '.csv') AS
        \\  SELECT DISTINCT ON (IDENTIFIER($c)) IDENTIFIER($c), upper(CAST(IDENTIFIER($c) AS STRING)) AS u, $c AS col FROM '$B/a.csv';
        \\END FOR;
    , 1, &.{});
    try expectFile(&tmp, "agg_grp.csv", "grp,n\nb,1\na,2\n");
    try expectFile(&tmp, "agg_id.csv", "id,n\n3,1\n2,1\n1,1\n");
    try expectFile(&tmp, "dist_grp.csv", "grp,u,col\na,A,grp\nb,B,grp\n");
}

test "IDENTIFIER names a column from a PARAM outside any loop" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\PARAM col STRING;
        \\LOAD INTO '$B/out.csv' AS
        \\SELECT IDENTIFIER($col) AS k, SUM(amt) AS s FROM '$B/a.csv' GROUP BY IDENTIFIER($col) ORDER BY k;
    , 1, &[_]ParamArg{.{ .key = "col", .val = "grp" }});
    try expectFile(&tmp, "out.csv", "k,s\na,30\nb,5\n");
}

test "join: `b.col` names the right side's column even when the join renamed it `col_r`" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 1, 4 }) |threads| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "id,grp,amt\n1,p,100\n2,q,200\n9,z,900\n" });
        try checkAndRun(alloc, &tmp,
            \\LOAD INTO '$B/both.csv' AS
            \\WITH r AS (SELECT id, grp, amt FROM '$B/r.csv')
            \\SELECT a.id, a.amt, b.amt FROM '$B/a.csv' a JOIN r b ON a.id = b.id ORDER BY a.id;
            \\LOAD INTO '$B/filt.csv' AS
            \\WITH r AS (SELECT id, grp, amt FROM '$B/r.csv')
            \\SELECT a.id, b.amt AS bamt, a.amt + b.amt AS tot FROM '$B/a.csv' a LEFT JOIN r b ON a.id = b.id
            \\WHERE b.amt > 150 OR b.amt IS NULL ORDER BY b.amt DESC;
            \\LOAD INTO '$B/agg.csv' AS
            \\WITH r AS (SELECT id, grp, amt FROM '$B/r.csv')
            \\SELECT b.grp, SUM(b.amt) AS sb, SUM(a.amt) AS sa FROM '$B/a.csv' a JOIN r b ON a.id = b.id GROUP BY b.grp ORDER BY b.grp;
            \\LOAD INTO '$B/chain.csv' AS
            \\WITH r AS (SELECT id, amt FROM '$B/r.csv'), s AS (SELECT id, amt FROM '$B/r.csv' WHERE amt >= 200)
            \\SELECT a.id, b.amt, c.amt AS camt FROM '$B/a.csv' a JOIN r b ON a.id = b.id JOIN s c ON b.id = c.id;
            \\LOAD INTO '$B/unaliased.csv' AS
            \\WITH r AS (SELECT id, amt FROM '$B/r.csv')
            \\SELECT a.id, r.amt FROM '$B/a.csv' a JOIN r ON a.id = r.id ORDER BY a.id;
        , threads, &.{});
        try expectFile(&tmp, "both.csv", "id,amt,amt_r\n1,10,100\n2,20,200\n");
        try expectFile(&tmp, "filt.csv", "id,bamt,tot\n2,200,220\n3,,\n");
        try expectFile(&tmp, "agg.csv", "grp,sb,sa\np,100,10\nq,200,20\n");
        try expectFile(&tmp, "chain.csv", "id,amt,camt\n2,200,200\n");
        try expectFile(&tmp, "unaliased.csv", "id,amt\n1,100\n2,200\n");
    }
}

test "DISTINCT ON keys are input columns: renamed, unprojected, and beside an ORDER BY on another hidden column" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "d.csv", .data = "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n4,b,50\n" });
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/renamed.csv' AS SELECT DISTINCT ON (grp) grp AS k, upper(grp) AS u FROM '$B/d.csv' ORDER BY grp DESC;
        \\LOAD INTO '$B/hidden.csv' AS SELECT DISTINCT ON (grp) id, amt FROM '$B/d.csv';
        \\LOAD INTO '$B/sorted.csv' AS SELECT DISTINCT ON (grp) id FROM '$B/d.csv' ORDER BY amt DESC;
        \\LOAD INTO '$B/output.csv' AS SELECT DISTINCT ON (k) grp AS k, id FROM '$B/d.csv';
        \\LOAD INTO '$B/pair.csv' AS SELECT DISTINCT ON (grp, amt) id FROM '$B/d.csv';
    , 1, &.{});
    try expectFile(&tmp, "renamed.csv", "k,u\nb,B\na,A\n");
    try expectFile(&tmp, "hidden.csv", "id,amt\n1,10\n3,5\n");
    try expectFile(&tmp, "sorted.csv", "id\n1\n3\n");
    try expectFile(&tmp, "output.csv", "k,id\na,1\nb,3\n");
    try expectFile(&tmp, "pair.csv", "id\n1\n2\n3\n4\n");
}

test "DESCRIBE rows: name, engine type (decimal with its precision), nullable" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const fields = [_]types.Schema.Field{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2).asNullable() },
        .{ .name = "a,b", .ty = types.Type.init(.string).asNullable() },
    };
    const rows = try describeRows(ar.allocator(), .{ .fields = &fields });
    try std.testing.expectEqualStrings("id,int,no\namt,decimal(10,2),yes\n\"a,b\",string,yes\n", rows);
}

test "EXCEPT (IDENTIFIER($cols)): a comma list excludes each name, an empty one excludes nothing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\CREATE FUNCTION slim(ex) AS
        \\  LOAD INTO IDENTIFIER('$B/out_' || length($ex) || '.csv') AS SELECT * EXCEPT (IDENTIFIER($ex)) FROM '$B/a.csv' WHERE id = 1;
        \\END;
        \\CALL slim('grp, amt');
        \\CALL slim('');
        \\CALL slim('nope');
    , 1, &.{});
    try expectFile(&tmp, "out_8.csv", "id\n1\n");
    try expectFile(&tmp, "out_0.csv", "id,grp,amt\n1,a,10\n");
    try expectFile(&tmp, "out_4.csv", "id,grp,amt\n1,a,10\n");
}

test "union: SELECT * EXCEPT drops a column before the branches are reconciled, so its type never has to agree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "ua.csv", .data = "id,x\n1,2024-01-01\n" });
    try tmp.dir.writeFile(.{ .sub_path = "ub.csv", .data = "id,x\n2,7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const with_u =
        \\WITH u AS (SELECT id, CAST(x AS DATE) AS x FROM '$B/ua.csv'
        \\           UNION ALL BY NAME SELECT id, CAST(x AS INT) AS x FROM '$B/ub.csv' ANCHOR SCHEMA first)
    ;
    // A date `x` in one branch and an int `x` in the other: no common type.
    const bad = try std.mem.replaceOwned(u8, alloc, "LOAD INTO '$B/bad.csv' AS " ++ with_u ++ "\nSELECT * FROM u;", "$B", base);
    defer alloc.free(bad);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const bad_prog = try parser.parseSource(parena.allocator(), bad, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expect(std.meta.isError(run(alloc, bad_prog, .{ .log = .{ .quiet = true } }, &rdiag)));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "no common type") != null);

    // Excepted, the column is dropped from the canon: neither branch casts it.
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/ok.csv' AS " ++ with_u ++ "\nSELECT * EXCEPT (x) FROM u ORDER BY id;", 1, &.{});
    try expectFile(&tmp, "ok.csv", "id\n1\n2\n");
}

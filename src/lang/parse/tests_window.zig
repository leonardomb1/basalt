//! Parser tests for window functions: frames, the function list, IGNORE NULLS,
//! FILTER, windows inside expressions, several OVER clauses, expression keys and
//! named windows, and the errors each refuses with.

const Diagnostic = @import("../sql_parser.zig").Diagnostic;
const ast = @import("../ast.zig");
const firstPipelineStages = @import("testing_util.zig").firstPipelineStages;
const parseSource = @import("../sql_parser.zig").parseSource;
const parseTest = @import("testing_util.zig").parseTest;
const std = @import("std");
const testing = std.testing;

fn windows(stages: []const ast.Stage, out: *std.array_list.Managed(ast.Window)) !void {
    for (stages) |st| if (st.node == .window) try out.append(st.node.window);
}

fn onlyWindow(a: std.mem.Allocator, src: []const u8) !ast.Window {
    var ws = std.array_list.Managed(ast.Window).init(a);
    try windows(firstPipelineStages(try parseTest(a, src)), &ws);
    try testing.expectEqual(@as(usize, 1), ws.items.len);
    return ws.items[0];
}

fn expectRefused(a: std.mem.Allocator, src: []const u8, want: []const u8) !void {
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    if (parseSource(a, src, &diag)) |_| {
        std.debug.print("accepted: {s}\n", .{src});
        return error.TestUnexpectedResult;
    } else |e| try testing.expectEqual(error.ParseFailed, e);
    if (std.mem.indexOf(u8, diag.msg, want) == null) {
        std.debug.print("want `{s}` in `{s}`\n", .{ want, diag.msg });
        return error.TestUnexpectedResult;
    }
}

test "window frames: ROWS and RANGE with every bound, the short form ending at CURRENT ROW" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const w = try onlyWindow(a,
        \\SELECT SUM(v) OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS a,
        \\       SUM(v) OVER (ORDER BY t ROWS 3 PRECEDING) AS b,
        \\       SUM(v) OVER (ORDER BY t RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS c,
        \\       SUM(v) OVER (ORDER BY t RANGE BETWEEN INTERVAL '2' DAY PRECEDING AND 1.5 FOLLOWING) AS d,
        \\       SUM(v) OVER (ORDER BY t) AS e
        \\FROM 'in.csv';
    );
    try testing.expectEqual(@as(usize, 5), w.funcs.len);
    const f0 = w.funcs[0].frame.?;
    try testing.expectEqual(ast.FrameUnit.rows, f0.unit);
    try testing.expectEqualStrings("2", f0.start.preceding.text);
    try testing.expectEqualStrings("1", f0.end.following.text);
    const f1 = w.funcs[1].frame.?;
    try testing.expectEqualStrings("3", f1.start.preceding.text);
    try testing.expect(f1.end == .current_row);
    const f2 = w.funcs[2].frame.?;
    try testing.expectEqual(ast.FrameUnit.range, f2.unit);
    try testing.expect(f2.start == .current_row and f2.end == .unbounded_following);
    const f3 = w.funcs[3].frame.?;
    try testing.expectEqual(ast.IntervalUnit.day, f3.start.preceding.unit.?);
    try testing.expectEqualStrings("1.5", f3.end.following.text);
    try testing.expect(w.funcs[4].frame == null);
    try testing.expectEqualStrings("RANGE BETWEEN INTERVAL '2' day PRECEDING AND 1.5 FOLLOWING", try f3.render(a));
}

test "window frames: a start after its end, impossible bounds and bad offsets are refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_][2][]const u8{
        .{ "ROWS BETWEEN CURRENT ROW AND 1 PRECEDING", "this frame starts after it ends — CURRENT ROW comes after PRECEDING" },
        .{ "ROWS 2 FOLLOWING", "this frame starts after it ends" },
        .{ "ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING", "1 PRECEDING comes after 3 PRECEDING" },
        .{ "ROWS BETWEEN 3 FOLLOWING AND 1 FOLLOWING", "3 FOLLOWING comes after 1 FOLLOWING" },
        .{ "ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING", "cannot start at UNBOUNDED FOLLOWING" },
        .{ "ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING", "cannot end at UNBOUNDED PRECEDING" },
        .{ "ROWS 1.5 PRECEDING", "a ROWS offset is a whole number of rows" },
        .{ "ROWS INTERVAL '1' DAY PRECEDING", "a ROWS offset is a whole number of rows" },
        .{ "ROWS -1 PRECEDING", "must not be negative" },
        .{ "RANGE INTERVAL '1' WEEK PRECEDING", "unknown INTERVAL unit `WEEK`" },
        .{ "GROUPS 1 PRECEDING", "GROUPS frames are not supported" },
        .{ "ROWS BETWEEN 1 PRECEDING", "expected `and`" },
        .{ "ROWS 1", "expected PRECEDING or FOLLOWING" },
    };
    for (cases) |c| {
        const src = try std.fmt.allocPrint(a, "SELECT SUM(v) OVER (ORDER BY t {s}) AS s FROM 'in.csv';", .{c[0]});
        try expectRefused(a, src, c[1]);
    }
    try expectRefused(a, "SELECT SUM(v) OVER (ORDER BY k, t RANGE 1 PRECEDING) AS s FROM 'in.csv';", "RANGE with an offset needs exactly one ORDER BY key");
    try expectRefused(a, "SELECT SUM(v) OVER (RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS s FROM 'in.csv';", "RANGE with an offset needs exactly one ORDER BY key");
    _ = try parseTest(a, "SELECT SUM(v) OVER (ORDER BY k, t RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM 'in.csv';");
    _ = try parseTest(a, "SELECT SUM(v) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM 'in.csv';");
}

test "window functions: what each takes, and what a frame, ORDER BY or IGNORE NULLS may go with" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const w = try onlyWindow(a,
        \\SELECT NTILE(4) OVER (ORDER BY t) AS q, NTH_VALUE(v, 3) IGNORE NULLS OVER (ORDER BY t) AS n,
        \\       LAG(v IGNORE NULLS) OVER (ORDER BY t) AS l, LEAD(v, 2, 0) RESPECT NULLS OVER (ORDER BY t) AS r,
        \\       COUNT(DISTINCT v) OVER (ORDER BY t) AS c, COUNT(*) OVER (ORDER BY t) AS s
        \\FROM 'in.csv';
    );
    try testing.expectEqual(ast.WinKind.ntile, w.funcs[0].func.win);
    try testing.expectEqual(@as(i64, 4), w.funcs[0].offset);
    try testing.expectEqual(@as(i64, 3), w.funcs[1].offset);
    try testing.expect(w.funcs[1].ignore_nulls);
    try testing.expect(w.funcs[2].ignore_nulls);
    try testing.expect(!w.funcs[3].ignore_nulls);
    try testing.expectEqual(@as(i64, 2), w.funcs[3].offset);
    try testing.expect(w.funcs[4].distinct);
    try testing.expectEqual(ast.AggFunc.count, w.funcs[5].func.agg);
    try testing.expect(w.funcs[5].arg == null);

    try expectRefused(a, "SELECT NTILE(0) OVER (ORDER BY t) AS q FROM 'in.csv';", "NTILE takes a positive whole number of buckets");
    try expectRefused(a, "SELECT NTILE(v) OVER (ORDER BY t) AS q FROM 'in.csv';", "NTILE takes a positive whole number of buckets");
    try expectRefused(a, "SELECT NTH_VALUE(v, 0) OVER (ORDER BY t) AS q FROM 'in.csv';", "NTH_VALUE's position must be a positive whole number");
    try expectRefused(a, "SELECT RANK(v) OVER (ORDER BY t) AS q FROM 'in.csv';", "`rank()` takes no arguments");
    try expectRefused(a, "SELECT LAG(v, -1) OVER (ORDER BY t) AS q FROM 'in.csv';", "must not be negative");
    try expectRefused(a, "SELECT PERCENT_RANK() OVER (PARTITION BY k) AS q FROM 'in.csv';", "needs ORDER BY inside OVER");
    try expectRefused(a, "SELECT LEAD(v) OVER (ORDER BY t ROWS 1 PRECEDING) AS q FROM 'in.csv';", "`lead` does not take one");
    try expectRefused(a, "SELECT SUM(v) IGNORE NULLS OVER (ORDER BY t) AS q FROM 'in.csv';", "IGNORE NULLS and RESPECT NULLS apply to LAG, LEAD");
    try expectRefused(a, "SELECT LAG(v) IGNORE NULLS AS q FROM 'in.csv';", "needs OVER (...)");
    try expectRefused(a, "SELECT SUM(DISTINCT v) OVER () AS q FROM 'in.csv';", "only supported inside COUNT");
    try expectRefused(a, "SELECT UPPER(v) OVER () AS q FROM 'in.csv';", "`upper` is not a window function");
    try expectRefused(a, "SELECT SUM(LAG(v) OVER (ORDER BY t)) OVER () AS q FROM 'in.csv';", "window functions cannot be nested");
    try expectRefused(a, "SELECT SUM(ROW_NUMBER() OVER (ORDER BY t)) AS q FROM 'in.csv';", "an aggregate cannot take a window function's result");
}

test "window placement: in an expression it is lifted under a hidden name; WHERE, GROUP BY and HAVING refuse it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const stages = firstPipelineStages(try parseTest(a,
        \\SELECT k, v - LAG(v) OVER (PARTITION BY k ORDER BY t) AS d, ROW_NUMBER() OVER (PARTITION BY k ORDER BY t) AS rn
        \\FROM 'in.csv';
    ));
    var ws = std.array_list.Managed(ast.Window).init(a);
    try windows(stages, &ws);
    try testing.expectEqual(@as(usize, 1), ws.items.len);
    try testing.expectEqual(@as(usize, 2), ws.items[0].funcs.len);
    try testing.expect(std.mem.startsWith(u8, ws.items[0].funcs[0].out, "__w"));
    try testing.expectEqualStrings("rn", ws.items[0].funcs[1].out);
    const last = stages[stages.len - 2].node.select;
    try testing.expectEqual(@as(usize, 3), last.len);
    try testing.expectEqualStrings("d", last[1].computed.name);
    try testing.expect(last[1].computed.expr.* == .binary);
    try testing.expectEqualStrings("rn", last[2].field.last());

    const where = "a window function is only allowed in the SELECT list or ORDER BY";
    try expectRefused(a, "SELECT k FROM 'in.csv' WHERE ROW_NUMBER() OVER (ORDER BY t) = 1;", where);
    try expectRefused(a, "SELECT k, COUNT(*) AS n FROM 'in.csv' GROUP BY k HAVING RANK() OVER (ORDER BY k) = 1;", where);
    try expectRefused(a, "WITH c AS (SELECT 1 AS k) SELECT t FROM 'in.csv' JOIN c ON c.k = ROW_NUMBER() OVER (ORDER BY t);", where);
    try expectRefused(a, "SELECT k FROM 'in.csv' ORDER BY ROW_NUMBER() OVER (ORDER BY t);", "a window function in ORDER BY must repeat one the SELECT list computes");
    _ = try parseTest(a, "SELECT k, ROW_NUMBER() OVER (ORDER BY t) AS rn FROM 'in.csv' ORDER BY ROW_NUMBER() OVER (ORDER BY t) DESC;");
}

test "window stages: one per distinct PARTITION BY / ORDER BY; expression keys and arguments become hidden columns" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const stages = firstPipelineStages(try parseTest(a,
        \\SELECT SUM(x * y) OVER (PARTITION BY substr(k, 1, 3)) AS a,
        \\       MAX(x) OVER (ORDER BY x + y) AS b,
        \\       MIN(x) OVER (PARTITION BY substr(k, 1, 3)) AS c
        \\FROM 'in.csv';
    ));
    var ws = std.array_list.Managed(ast.Window).init(a);
    try windows(stages, &ws);
    try testing.expectEqual(@as(usize, 2), ws.items.len);
    try testing.expectEqual(@as(usize, 2), ws.items[0].funcs.len);
    try testing.expectEqual(@as(usize, 1), ws.items[1].funcs.len);
    try testing.expect(std.mem.startsWith(u8, ws.items[0].partition_by[0].last(), "__wa"));
    try testing.expect(std.mem.startsWith(u8, ws.items[0].funcs[0].arg.?.last(), "__wa"));
    try testing.expectEqualStrings("x", ws.items[0].funcs[1].arg.?.last());
    try testing.expect(std.mem.startsWith(u8, ws.items[1].order_by[0].field.last(), "__wa"));
    var pre: ?[]const ast.SelectItem = null;
    for (stages) |st| {
        if (st.node == .window) break;
        if (st.node == .select) pre = st.node.select;
    }
    var hidden: usize = 0;
    for (pre.?) |it| {
        if (it == .computed and std.mem.startsWith(u8, it.computed.name, "__wa")) hidden += 1;
    }
    try testing.expectEqual(@as(usize, 3), hidden);
}

test "named windows: OVER w takes it whole, OVER (w ...) may add ORDER BY and a frame" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const w = try onlyWindow(a,
        \\SELECT SUM(v) OVER w AS s, AVG(v) OVER (w ROWS 2 PRECEDING) AS m, RANK() OVER w AS r
        \\FROM 'in.csv'
        \\WINDOW w AS (PARTITION BY k ORDER BY t);
    );
    try testing.expectEqual(@as(usize, 3), w.funcs.len);
    try testing.expectEqualStrings("k", w.partition_by[0].last());
    try testing.expectEqualStrings("t", w.order_by[0].field.last());
    try testing.expect(w.funcs[0].frame == null);
    try testing.expectEqualStrings("2", w.funcs[1].frame.?.start.preceding.text);

    const w2 = try onlyWindow(a, "SELECT SUM(v) OVER (p ORDER BY t) AS s FROM 'in.csv' WINDOW p AS (PARTITION BY k), q AS (p);");
    try testing.expectEqualStrings("t", w2.order_by[0].field.last());

    try expectRefused(a, "SELECT SUM(v) OVER w AS s FROM 'in.csv';", "unknown window `w`");
    try expectRefused(a, "SELECT SUM(v) OVER (w PARTITION BY k) AS s FROM 'in.csv' WINDOW w AS (ORDER BY t);", "cannot add a PARTITION BY");
    try expectRefused(a, "SELECT SUM(v) OVER (w ORDER BY k) AS s FROM 'in.csv' WINDOW w AS (ORDER BY t);", "cannot replace the ORDER BY");
    try expectRefused(a, "SELECT SUM(v) OVER (w ORDER BY k) AS s FROM 'in.csv' WINDOW w AS (ROWS 1 PRECEDING);", "has a frame");
    try expectRefused(a, "SELECT SUM(v) OVER w AS s FROM 'in.csv' WINDOW w AS (ORDER BY t), w AS (ORDER BY k);", "defined twice");
}

test "FILTER: an aggregate's FILTER folds into its argument, under GROUP BY and over a window alike" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const stages = firstPipelineStages(try parseTest(a, "SELECT k, SUM(v) FILTER (WHERE v > 0) AS pos, COUNT(*) FILTER (WHERE v < 0) AS neg FROM 'in.csv' GROUP BY k;"));
    var ag: ?ast.Aggregate = null;
    for (stages) |st| {
        if (st.node == .aggregate) ag = st.node.aggregate;
    }
    try testing.expectEqual(ast.AggFunc.sum, ag.?.aggs[0].func);
    try testing.expect(ag.?.aggs[0].arg.?.* == .cond);
    try testing.expectEqual(ast.AggFunc.count_if, ag.?.aggs[1].func);
    try testing.expect(ag.?.aggs[1].arg.?.* == .binary);

    const w = try onlyWindow(a, "SELECT SUM(v) FILTER (WHERE v > 0) OVER (PARTITION BY k) AS s FROM 'in.csv';");
    try testing.expect(std.mem.startsWith(u8, w.funcs[0].arg.?.last(), "__wa"));

    try expectRefused(a, "SELECT UPPER(k) FILTER (WHERE v > 0) AS s FROM 'in.csv';", "FILTER (WHERE …) applies to an aggregate; `upper` is not one");
    try expectRefused(a, "SELECT SUM(v) FILTER (v > 0) AS s FROM 'in.csv';", "expected `where`");
    try expectRefused(a, "SELECT SUM() FILTER (WHERE v > 0) AS s FROM 'in.csv';", "`sum` needs an argument");
}

test "EXPLAIN's window detail: the keys, then each function with its frame, the default one spelled out" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const w = try onlyWindow(a,
        \\SELECT SUM(v) OVER (PARTITION BY k ORDER BY t DESC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s,
        \\       AVG(v) OVER (PARTITION BY k ORDER BY t DESC) AS m,
        \\       LAG(v) IGNORE NULLS OVER (PARTITION BY k ORDER BY t DESC) AS l
        \\FROM 'in.csv';
    );
    try testing.expectEqualStrings(
        "PARTITION BY k ORDER BY t DESC: sum ROWS BETWEEN 2 PRECEDING AND CURRENT ROW; avg RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW; lag IGNORE NULLS",
        try ast.windowDetail(a, w),
    );
    const whole = try onlyWindow(a, "SELECT COUNT(*) OVER () AS n FROM 'in.csv';");
    try testing.expectEqualStrings("whole input: count whole partition", try ast.windowDetail(a, whole));
}

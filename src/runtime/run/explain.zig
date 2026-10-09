//! EXPLAIN and EXPLAIN ANALYZE: the plan printed, or run for its actuals and printed
//! with them, writing nothing.

const Env = @import("../env.zig").Env;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const analyze = @import("../analyze.zig");
const arrow = @import("../../format/arrow.zig");
const ast = @import("../../lang/ast.zig");
const column = @import("../../exec/column.zig");
const op = @import("../../exec/op.zig");
const planErr = @import("../env.zig").planErr;
const runDescribe = @import("../run.zig").runDescribe;
const runOutput = @import("output.zig").runOutput;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// `EXPLAIN [ANALYZE] <query>;` against what the statements above put in scope.
/// ANALYZE runs the pipeline serially with the sink discarded; the plain form renders
/// the static plan through `analyze.render`. Both are scoped to this one statement.
pub fn runExplain(env: *Env, e: ast.ExplainStmt, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    if (e.mode == .describe) return runDescribe(env, e.pipeline, stats);
    if (e.mode == .analyze) {
        const outer = env.explain;
        env.explain = true;
        defer env.explain = outer;
        var o = opts;
        o.explain = true;
        o.threads = 1;
        return runOutput(env, e.pipeline, o, stats, lanes_used, batch_arena);
    }

    var adiag = analyze.Diag{};
    const plan = analyze.analyzeOne(env.arena, env.kind_name, e.pipeline, env.bindings, env.connections, env.params_expr, &adiag) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.AnalyzeFailed => return planErr(env.diag, try env.arena.dupe(u8, adiag.msg)),
    };

    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    try analyze.render(plan, &aw.writer);
    if (env.stdout_format == .arrow) {
        const info = env.takeResult();
        const n = try printPlanArrow(env.gpa, aw.writer.buffered(), info);
        stats.rows_out += n;
        if (env.on_result) |h| h.call(.{ .info = info, .rows = n });
        return;
    }
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
}

pub fn printPlanArrow(gpa: std.mem.Allocator, text: []const u8, info: arrow.ResultInfo) !usize {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const fields = [_]types.Schema.Field{.{ .name = "plan", .ty = .{ .kind = .string } }};
    const schema = types.Schema{ .fields = &fields };
    var b = column.Builder.init(a, fields[0].ty);
    var it = std.mem.splitScalar(u8, std.mem.trimRight(u8, text, "\n"), '\n');
    var n: usize = 0;
    while (it.next()) |ln| : (n += 1) try b.append(.{ .string = ln });
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try b.finish();
    const w = try arrow.ArrowWriter.open(gpa, schema, info);
    errdefer w.abort();
    try w.writeBatch(a, .{ .schema = &schema, .columns = cols, .len = n });
    try w.close();
    return n;
}

/// Time is exclusive: an operator's own cost with its inputs' subtracted, since a
/// pull pipeline nests children inside the parent's `next`.
pub fn explainTree(arena: std.mem.Allocator, root: op.Op) !void {
    var buf = std.array_list.Managed(u8).init(arena);
    try buf.appendSlice("plan (actuals, exclusive time)\n");
    try explainNode(arena, root, &buf, 1);
    std.debug.print("{s}", .{buf.items});
}

fn explainNode(arena: std.mem.Allocator, node: op.Op, buf: *std.array_list.Managed(u8), depth: usize) !void {
    var kids = std.array_list.Managed(op.Op).init(arena);
    try node.inputs(&kids);
    var child_ns: u64 = 0;
    for (kids.items) |k| child_ns += k.stats().ns;
    const st = node.stats();
    const excl: u64 = if (st.ns > child_ns) st.ns - child_ns else 0;
    try buf.appendNTimes(' ', depth * 2);
    try buf.writer().print("{s:<10} {d:>9.1}ms {d:>12} rows {d:>8} batches\n", .{
        @tagName(node),
        @as(f64, @floatFromInt(excl)) / 1e6,
        st.rows,
        st.calls,
    });
    if (node == .join and node.join.note.len > 0) {
        try buf.appendNTimes(' ', depth * 2 + 2);
        try buf.writer().print("{s}\n", .{node.join.note});
    }
    for (kids.items) |k| try explainNode(arena, k, buf, depth + 1);
}

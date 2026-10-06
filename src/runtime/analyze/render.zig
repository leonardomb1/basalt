//! `EXPLAIN`'s text: the plan tree, each stage's schema, what a source is sent, and
//! the physical shape a pipeline runs in.

const Plan = @import("../analyze.zig").Plan;
const arrowread = @import("../../format/arrowread.zig");
const azure = @import("../../store/azure.zig");
const pqwrite = @import("../../format/parquet/write.zig");
const s3 = @import("../../store/s3.zig");
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const xlsx = @import("../../format/xlsx.zig");
const Diag = @import("../analyze.zig").Diag;
const Stage = @import("../analyze.zig").Stage;
const analyze = @import("../analyze.zig").analyze;
const parse = @import("../analyze.zig").parse;

/// The plan as a tree, root first and source deepest, as `EXPLAIN ANALYZE` prints it.
/// A split or morsel fan-out prints as a candidate: only the source or file can settle it.
pub fn render(plan: Plan, w: anytype) !void {
    for (plan.outputs) |o| {
        if (std.mem.eql(u8, plan.kind, "batch")) {
            try w.writeAll("plan\n");
        } else {
            try w.print("plan ({s})\n", .{plan.kind});
        }

        var depth: usize = 1;
        try indent(w, depth);
        if (o.sink.target.len > 0) {
            try w.print("write  {s}  {s} ({s})\n", .{ sinkKind(o.sink), o.sink.target, o.sink.mode });
        } else {
            try w.print("write  {s}  ({s})\n", .{ sinkKind(o.sink), o.sink.mode });
        }

        var i = o.stages.len;
        while (i > 0) {
            i -= 1;
            depth += 1;
            const st = o.stages[i];
            try indent(w, depth);
            if (st.detail.len > 0) {
                try w.print("{s}  {s}\n", .{ st.kind, st.detail });
            } else {
                try w.print("{s}\n", .{st.kind});
            }
            try printSchema(w, depth + 1, st.out_schema);
            if (st.right_scan) |rs| {
                try indent(w, depth + 1);
                try w.print("right  scan  {s}\n", .{rs});
                if (st.right_pushdown.len > 0) {
                    try indent(w, depth + 2);
                    try w.print("pushdown: {s}\n", .{st.right_pushdown});
                }
            }
        }

        depth += 1;
        try indent(w, depth);
        try w.print("scan  {s}  {s}\n", .{ sinkKind(o.source), o.source.detail });
        if (o.source.pushdown.len > 0) {
            try indent(w, depth + 1);
            try w.print("pushdown: {s}\n", .{o.source.pushdown});
        }
        if (o.source.schema == null) {
            try indent(w, depth + 1);
            try w.writeAll("schema: unresolved\n");
        }
        try printSchema(w, depth + 1, o.source.schema);

        try w.writeAll("  physical: ");
        if (o.physical.splittable) {
            try w.writeAll("split-parallel candidate");
            if (o.physical.sink_parallel) try w.writeAll(", per-lane sink");
        } else if (o.physical.morsel_parallel) {
            try w.writeAll("morsel-parallel candidate");
            if (o.physical.has_breaker) try w.writeAll(" (per-lane partials, combined)");
        } else {
            try w.writeAll("serial");
            if (o.physical.top_n) |n| {
                if (o.physical.has_breaker) try w.print(" (top-N pushed, sorts at most {d} rows)", .{n}) else try w.print(" (limit pushed, at most {d} rows arrive)", .{n});
            } else if (o.physical.has_breaker) try w.writeAll(" (has breaker, materializes)");
        }
        try w.writeAll("\n");
    }
}

/// Why `APPEND` cannot be honoured for a file target, or null: a parquet footer is
/// written last, a block blob is committed whole, and a server file is renamed into
/// place. Shared with the runtime planner.
pub fn appendUnsupported(target: []const u8) ?[]const u8 {
    if (azure.isUrl(target) or s3.isUrl(target)) return "an object-store blob is replaced on write, never extended";
    if (sftp.isUrl(target) or smb.isUrl(target)) return "a file on a server is written to `name.part` and renamed over the target once complete, never extended";
    if (pqwrite.Writer.isPath(target)) return "a parquet file's footer indexes every row group and is written last, so appending means rewriting the file";
    if (arrowread.isPath(target)) return "an Arrow IPC file's footer indexes every batch and is written last, so appending means rewriting the file";
    return null;
}

/// Every file sink runs on the `csv` connector, so the format is named from the target.
pub fn sinkKind(node: anytype) []const u8 {
    const path = if (@hasField(@TypeOf(node), "target")) node.target else node.detail;
    if (std.mem.eql(u8, node.connector, "csv")) {
        if (pqwrite.Writer.isPath(path)) return "parquet";
        if (arrowread.isPath(path)) return "arrow";
        if (xlsx.isPath(path)) return "xlsx";
    }
    return node.connector;
}

fn indent(w: anytype, depth: usize) !void {
    var n: usize = 0;
    while (n < depth) : (n += 1) try w.writeAll("  ");
}

/// Printed once, at the scan that could not resolve it, and labelled, since a bare
/// column list at a child's depth reads like another operator.
fn printSchema(w: anytype, depth: usize, schema: ?types.Schema) !void {
    const s = schema orelse return;
    try indent(w, depth);
    try w.writeAll("schema: ");
    for (s.fields, 0..) |f, i| {
        if (i > 0) try w.writeAll("  ");
        try w.print("{s}:{s}{s}", .{ f.name, @tagName(f.ty.kind), if (f.ty.nullable) "?" else "" });
    }
    try w.writeAll("\n");
}

test "analyze: EXPLAIN shows the WHERE a join's right side sends, under the join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = Diag{};
    const plan = try analyze(a, try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');
        \\CREATE FUNCTION active(flag INT) RETURNS TABLE AS SELECT cid, name AS nm FROM pg.customers WHERE active = $flag;
        \\LOAD INTO '/tmp/x.csv' AS SELECT o.id, c.nm FROM pg.orders o JOIN active(1) c ON o.cid = c.cid;
    ), &diag);
    var join: ?Stage = null;
    for (plan.outputs[0].stages) |st| {
        if (std.mem.eql(u8, st.kind, "join")) join = st;
    }
    try std.testing.expectEqualStrings("(\"active\" = 1)", join.?.right_pushdown);
    var out = std.Io.Writer.Allocating.init(a);
    try render(plan, &out.writer);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "right  scan  postgres  table customers (via binding __tvf") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "pushdown: (\"active\" = 1)") != null);
}

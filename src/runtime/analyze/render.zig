//! `EXPLAIN`'s text: the plan tree, each stage's schema, what a source is sent, and
//! the physical shape a pipeline runs in.

const Plan = @import("../analyze.zig").Plan;
const azure = @import("../../store/azure.zig");
const s3 = @import("../../store/s3.zig");
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const targets = @import("targets.zig");
const formats = @import("../../format/formats.zig");
const FileFormat = @import("targets.zig").FileFormat;
const Diag = @import("../analyze.zig").Diag;
const Stage = @import("../analyze.zig").Stage;
const Output = @import("../analyze.zig").Output;
const Source = @import("../analyze.zig").Source;
const Physical = @import("../analyze.zig").Physical;
const analyze = @import("../analyze.zig").analyze;
const parse = @import("../analyze.zig").parse;

/// The plan as `EXPLAIN ANALYZE` lays it out, without the figures: a summary naming
/// the sink and the path, then the operators as a tree, root first and source
/// deepest, numbered in the order they print, each with its SQL and the schema it
/// yields. A split or morsel fan-out prints as a candidate: only the source or file
/// can settle it.
pub fn render(plan: Plan, w: anytype) !void {
    for (plan.outputs) |o| {
        try w.writeAll("Summary\n  Sink: ");
        try w.writeAll(sinkKind(o.sink));
        if (o.sink.target.len > 0) try w.print(" {s}", .{o.sink.target});
        try w.print(" ({s})\n  Path: ", .{o.sink.mode});
        try physical(w, o.physical);
        try w.writeAll("\nplan\n");
        var num: usize = 0;
        try stageNode(w, o, o.stages.len, .input, "", true, &num);
    }
}

const Role = enum { input, probe, build };

/// The stage at `i` (counting from 1; 0 is the source) and everything under it.
fn stageNode(w: anytype, o: Output, i: usize, role: Role, prefix: []const u8, last: bool, num: *usize) !void {
    if (i == 0) return sourceNode(w, o.source, role, prefix, last, num);
    const st = o.stages[i - 1];
    const name = if (std.mem.eql(u8, st.kind, "select")) "project" else if (st.how.len > 0) "nested-loop join" else st.kind;
    try header(w, prefix, last, role, name, st.sql, num);

    var kid_buf: [512]u8 = undefined;
    const kid_prefix = childPrefix(&kid_buf, prefix, last);
    var line_buf: [520]u8 = undefined;
    const line = std.fmt.bufPrint(&line_buf, "{s}│  ", .{kid_prefix}) catch kid_prefix;
    if (st.how.len > 0) try w.print("{s}strategy: {s}\n", .{ line, st.how });
    if (st.key_pushdown.len > 0) try w.print("{s}key pushdown: {s}\n", .{ line, st.key_pushdown });
    if (st.join_order.len > 0) try w.print("{s}join order: {s}\n", .{ line, st.join_order });
    try printSchema(w, line, st.out_schema);

    const join = std.mem.eql(u8, st.kind, "join");
    try stageNode(w, o, i - 1, if (join) .probe else .input, kid_prefix, st.right_scan == null, num);
    if (st.right_scan) |rs| {
        try header(w, kid_prefix, true, .build, "scan", rs, num);
        if (st.right_pushdown.len > 0) try w.print("{s}      pushdown: {s}\n", .{ kid_prefix, st.right_pushdown });
    }
}

fn sourceNode(w: anytype, src: Source, role: Role, prefix: []const u8, last: bool, num: *usize) !void {
    var text_buf: [1024]u8 = undefined;
    const text = std.fmt.bufPrint(&text_buf, "{s} {s}", .{ sinkKind(src), src.detail }) catch src.detail;
    try header(w, prefix, last, role, "scan", text, num);
    var kid_buf: [512]u8 = undefined;
    const kid_prefix = childPrefix(&kid_buf, prefix, last);
    if (src.pushdown.len > 0) try w.print("{s}   pushdown: {s}\n", .{ kid_prefix, src.pushdown });
    if (src.schema == null) try w.print("{s}   schema: unresolved\n", .{kid_prefix});
    var line_buf: [520]u8 = undefined;
    try printSchema(w, std.fmt.bufPrint(&line_buf, "{s}   ", .{kid_prefix}) catch kid_prefix, src.schema);
}

fn header(w: anytype, prefix: []const u8, last: bool, role: Role, name: []const u8, text: []const u8, num: *usize) !void {
    num.* += 1;
    try w.print("{s}{s}", .{ prefix, if (last) "└─ " else "├─ " });
    switch (role) {
        .input => {},
        .probe => try w.writeAll("<probe> "),
        .build => try w.writeAll("<build> "),
    }
    try w.print("{s} (#{d})", .{ name, num.* });
    if (text.len > 0) try w.print("  {s}", .{text});
    try w.writeAll("\n");
}

fn childPrefix(buf: []u8, prefix: []const u8, last: bool) []const u8 {
    return std.fmt.bufPrint(buf, "{s}{s}", .{ prefix, if (last) "   " else "│  " }) catch prefix;
}

fn physical(w: anytype, ph: Physical) !void {
    if (ph.splittable) {
        try w.writeAll("split-parallel candidate");
        if (ph.sink_parallel) try w.writeAll(", per-lane sink");
    } else if (ph.morsel_parallel) {
        try w.writeAll("morsel-parallel candidate");
        if (ph.has_breaker) try w.writeAll(" (per-lane partials, combined)");
    } else {
        try w.writeAll("serial");
        if (ph.top_n) |n| {
            if (ph.has_breaker) try w.print(" (top-N pushed, sorts at most {d} rows)", .{n}) else try w.print(" (limit pushed, at most {d} rows arrive)", .{n});
        } else if (ph.has_breaker) try w.writeAll(" (has breaker, materializes)");
    }
}

/// Why `APPEND` cannot be honoured for a file target, or null: a parquet footer is
/// written last, a block blob is committed whole, and a server file is renamed into
/// place. Shared with the runtime planner.
pub fn appendUnsupported(target: []const u8, explicit: ?FileFormat) ?[]const u8 {
    if (azure.isUrl(target) or s3.isUrl(target)) return "an object-store blob is replaced on write, never extended";
    if (sftp.isUrl(target) or smb.isUrl(target)) return "a file on a server is written to `name.part` and renamed over the target once complete, never extended";
    return formats.of(explicit orelse targets.formatOfPath(target) orelse .csv).write.append_refused;
}

/// Every file sink runs on the `csv` connector, so the format is named from the target.
pub fn sinkKind(node: anytype) []const u8 {
    const path = if (@hasField(@TypeOf(node), "target")) node.target else node.detail;
    if (@hasField(@TypeOf(node), "format")) if (node.format) |f| return f;
    if (std.mem.eql(u8, node.connector, "csv")) if (targets.formatOfPath(path)) |f| return @tagName(f);
    return node.connector;
}

/// The columns a node yields, on one line under it.
fn printSchema(w: anytype, line: []const u8, schema: ?types.Schema) !void {
    const sch = schema orelse return;
    try w.print("{s}schema: ", .{line});
    for (sch.fields, 0..) |f, i| {
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
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "<build> scan (#") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "postgres table customers (via binding __tvf") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "pushdown: (\"active\" = 1)") != null);
}

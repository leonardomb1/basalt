//! A Parquet file loaded whole into a Parquet file as its column chunks stand
//! (`format/parquet/copy.zig`), where `path.chunkCopy` names the shape. The SELECT
//! resolves as the planner resolves it (`selectCols`), each column traced to the file
//! column it passes on (`hoist.projectedFrom`), and the output names are checked as
//! every LOAD checks them. False leaves the load to the path `path.choose` names: a
//! column computed, nested or of a type the writer would write otherwise, or a file
//! the reader cannot open, which that path then reports in its own words.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const Diag = @import("../analyze.zig").Diag;
const Env = @import("../env.zig").Env;
const Output = @import("../../format/output.zig").Output;
const Stats = @import("../env.zig").Stats;
const localRead = @import("../connect/source.zig").localRead;
const msgfmt = @import("../../base/msgfmt.zig");
const planErr = @import("../env.zig").planErr;
const pqcopy = @import("../../format/parquet/copy.zig");
const pqread = @import("../../format/parquet/read.zig");
const pqwrite = @import("../../format/parquet/write.zig");
const projectedFrom = @import("../pushdown/hoist.zig").projectedFrom;
const schemaOfCols = @import("../analyze/schema.zig").schemaOfCols;
const selectCols = @import("../analyze/schema.zig").selectCols;
const uniqueTargetColumns = @import("../connect/sink.zig").uniqueTargetColumns;

pub fn run(env: *Env, stages: []const ast.Stage, w: ast.Write, stats: *Stats) !bool {
    const arena = env.arena;
    const path = (try localRead(env, stages[0].node.read)).form.path;
    const rd = pqread.Reader.open(arena, path) catch return false;
    defer rd.close();
    const items: []const ast.SelectItem = if (stages.len == 3) stages[1].node.select else &.{.star};
    const resolved = (try columns(env, rd.schema, items)) orelse return false;
    const p = (try pqcopy.plan(arena, rd, resolved.cols)) orelse return false;
    try uniqueTargetColumns(env, w, resolved.schema);

    const t0 = std.time.Instant.now() catch null;
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    const written = if (env.explain) blk: {
        var discard = std.Io.Writer.Discarding.init(&.{});
        break :blk try copied(env, &scratch, rd, p, &discard.writer, path);
    } else blk: {
        const out = try Output.open(arena, w.target, pqwrite.parquet_content_type, .truncate);
        errdefer out.abort();
        const n = copied(env, &scratch, rd, p, out.writer(), path) catch |e| return out.specific(e);
        try out.finish();
        break :blk n;
    };

    const rows: u64 = @intCast(@max(0, rd.md.num_rows));
    _ = env.rows_read.fetchAdd(rows, .monotonic);
    stats.rows_out += rows;
    if (env.profile) |prof| {
        prof.path = try msgfmt.allocPrint(arena, "parquet chunk copy, {d} row groups, {d} of {d} columns", .{ rd.md.row_groups.len, p.slot.len, rd.outputs.len });
        _ = prof.sink_bytes.fetchAdd(written, .monotonic);
        if (t0) |t| try prof.phase("copy the column chunks", (std.time.Instant.now() catch t).since(t));
    }
    env.log.log(.debug, "parquet chunk copy: {d} row groups, {d} columns, {d} bytes", .{ rd.md.row_groups.len, p.slot.len, written });
    return true;
}

fn copied(env: *Env, scratch: *std.heap.ArenaAllocator, rd: *pqread.Reader, p: pqcopy.Plan, w: *std.Io.Writer, path: []const u8) !u64 {
    return pqcopy.copy(env.arena, scratch, rd, p, w) catch |e| switch (e) {
        error.CorruptParquetChunk => planErr(env.diag, try msgfmt.allocPrint(env.arena, "parquet file `{s}` has a column chunk whose pages do not frame up to the value count its footer gives (truncated or corrupt)", .{path})),
        else => e,
    };
}

/// The SELECT's columns over the file's schema `in`, as the planner resolves them,
/// each with the file column it passes on; null when one is computed, or when the
/// SELECT does not resolve, which the regular path then reports.
fn columns(env: *Env, in: types.Schema, items: []const ast.SelectItem) !?struct { cols: []const pqcopy.Col, schema: types.Schema } {
    var diag = Diag{};
    const cols = selectCols(env.arena, in, items, env.params_expr, &diag) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    const out = try env.arena.alloc(pqcopy.Col, cols.len);
    for (cols, out) |c, *o| {
        const field = switch (c.source) {
            .passthrough => |i| i,
            .expr => blk: {
                const q = (try projectedFrom(env.arena, items, c.name)) orelse return null;
                break :blk in.resolve(q.parts) orelse return null;
            },
        };
        o.* = .{ .field = field, .to = c.name };
    }
    return .{ .cols = out, .schema = try schemaOfCols(env.arena, cols) };
}

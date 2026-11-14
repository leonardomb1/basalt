//! What a `USING changes` statement settles before it runs, the same whatever its
//! target, as the write is an ordinary UPSERT:
//!   - `$since` is the largest value of the `position` column in the target table,
//!     read through the target as any query reads it, NULL when the table or the
//!     column is not there yet;
//!   - a source that reads its change feed from `$since` (`changes_from = $since`)
//!     is asked whether its log still reaches back that far; when it does not,
//!     the changes since are gone, so `$since` is NULL and the table is read whole;
//!   - a read with `$since` NULL into a table that exists is a snapshot, so a key
//!     the table holds live and the snapshot lacks was deleted meanwhile: the
//!     table's live rows join the changes as deletes ranked below any change
//!     (`tombstones`), and a key with nothing newer comes out deleted.

const Env = @import("../env.zig").Env;
const RunOptions = @import("../env.zig").RunOptions;
const Value = @import("../../exec/value.zig").Value;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const changes = @import("../connect/changes.zig");
const catalogQuery = @import("../plan/estimate.zig").catalogQuery;
const delta = @import("../../format/lake/delta.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const lake = @import("../../format/lake/table.zig");
const mkLit = @import("../env.zig").mkLit;
const msgfmt = @import("../../format/msgfmt.zig");
const sql = @import("../../db/sql.zig");
const runToMemory = @import("output.zig").runToMemory;
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// Binds `$since` for `out`, a `USING changes` statement, and returns its stages,
/// with the target's live rows among the changes when it reads a snapshot into a
/// table that exists.
pub fn prepare(env: *Env, out: ast.Pipeline, opts: RunOptions, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) ![]const ast.Stage {
    const w = out.stages[out.stages.len - 1].node.write;
    const ch = w.changes.?;
    var since: Value = .null;
    const cols = try columnsOf(env, w);
    if (cols) |names| if (ast.nameIn(names, ch.position)) {
        since = try largest(env, w, out.pos, ch.position, opts, lanes_used, batch_arena);
    };
    if (since == .string) if (try cleanedPast(env, out.stages[0], since.string)) {
        env.log.log(.warn, "the change feed {s} reads no longer reaches back to {s}, the largest `{s}` in {s}: reading the table whole instead, and deleting the keys it no longer holds", .{ try readLabel(env, out.stages[0]), since.string, ch.position, w.target });
        since = .null;
    };
    try env.params.put(analyze.since_name, since);
    try env.params_expr.put(analyze.since_name, if (since == .null) try analyze.mk(env.arena, .null_lit) else try mkLit(env.arena, since));
    const names = cols orelse return out.stages;
    if (since != .null) return out.stages;
    return tombstones(env, out, w, names);
}

/// The target's column names; null when the table is not there yet.
fn columnsOf(env: *Env, w: ast.Write) !?[]const []const u8 {
    const a = env.arena;
    if (env.connections.get(w.connector)) |conn| {
        const info = sqlConnInfo(conn) orelse return null;
        var parts = std.mem.splitScalar(u8, w.target, '.');
        var list = std.array_list.Managed([]const u8).init(a);
        while (parts.next()) |p| try list.append(p);
        const table = list.items[list.items.len - 1];
        var q = std.Io.Writer.Allocating.init(a);
        try q.writer.writeAll("SELECT column_name FROM information_schema.columns WHERE table_name = ");
        try info.dialect.writeStrLit(&q.writer, table, .catalog_name);
        try q.writer.writeAll(" AND table_schema = ");
        if (list.items.len > 1) {
            try info.dialect.writeStrLit(&q.writer, list.items[list.items.len - 2], .catalog_name);
        } else try q.writer.writeAll(switch (info.dialect) {
            .postgres => "current_schema()",
            .sqlserver => "SCHEMA_NAME()",
            else => "DATABASE()",
        });
        var names = std.array_list.Managed([]const u8).init(a);
        const Ctx = struct { a: std.mem.Allocator, out: *std.array_list.Managed([]const u8) };
        try catalogQuery(env, conn, info, q.written(), Ctx{ .a = a, .out = &names }, struct {
            fn rows(cx: Ctx, b: anytype) !void {
                for (0..b.len) |r| try cx.out.append(try cx.a.dupe(u8, std.mem.trim(u8, try fmtnum.valueToString(cx.a, b.columns[0].getValue(r)), " ")));
            }
        }.rows);
        if (names.items.len == 0) return null;
        return names.items;
    }
    const found = (try lake.detect(a, w.target)) orelse return null;
    if (found.kind != .delta) return null;
    const st = try delta.snapshot(a, w.target, .{}, found.listed);
    const text = st.schema_string orelse return null;
    const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, text, .{}) catch return null;
    const fields = (if (parsed == .object) parsed.object.get("fields") else null) orelse return null;
    if (fields != .array) return null;
    var names = std.array_list.Managed([]const u8).init(a);
    for (fields.array.items) |f| if (f == .object) if (f.object.get("name")) |n| if (n == .string) try names.append(n.string);
    return names.items;
}

/// The target as a read: its connection's table, or its Delta folder.
fn targetRead(w: ast.Write, a: std.mem.Allocator) !ast.Read {
    if (std.mem.eql(u8, w.connector, "csv")) return .{ .connector = "csv", .form = .{ .path = w.target } };
    var parts = std.array_list.Managed([]const u8).init(a);
    var it = std.mem.splitScalar(u8, w.target, '.');
    while (it.next()) |p| try parts.append(p);
    return .{ .connector = w.connector, .form = .{ .table = .{ .parts = parts.items } } };
}

fn targetHints(a: std.mem.Allocator, w: ast.Write, pos: ast.Pos) ![]const ast.Hint {
    if (!std.mem.eql(u8, w.connector, "csv") or std.mem.endsWith(u8, w.target, "/")) return &.{};
    return a.dupe(ast.Hint, &.{.{ .key = "format", .value = .{ .str = "delta" }, .pos = pos }});
}

fn expr(a: std.mem.Allocator, e: ast.Expr) !*ast.Expr {
    const out = try ast.mk(a, e);
    return out;
}

fn field(a: std.mem.Allocator, name: []const u8) !*ast.Expr {
    return expr(a, .{ .field = .{ .parts = try a.dupe([]const u8, &.{name}) } });
}

/// The largest `col` in the target: a database computes it (`SELECT MAX`), a
/// Delta table is read for it.
fn largest(env: *Env, w: ast.Write, pos: ast.Pos, col: []const u8, opts: RunOptions, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !Value {
    const a = env.arena;
    var rd = try targetRead(w, a);
    const stages = if (rd.form == .table) blk: {
        const dialect = sqlConnInfo(env.connections.get(w.connector).?).?.dialect;
        const table = try sql.quoteParts(a, dialect, rd.form.table.parts);
        rd.form = .{ .query = try msgfmt.allocPrint(a, "SELECT MAX({s}) AS since FROM {s}", .{ try sql.quoteName(a, dialect, col), table }) };
        break :blk try a.dupe(ast.Stage, &.{.{ .node = .{ .read = rd }, .hints = &.{}, .pos = pos }});
    } else blk: {
        const aggs = try a.dupe(ast.AggItem, &.{.{ .name = "since", .func = .max, .arg = try field(a, col) }});
        break :blk try a.dupe(ast.Stage, &.{
            .{ .node = .{ .read = rd }, .hints = try targetHints(a, w, pos), .pos = pos },
            .{ .node = .{ .aggregate = .{ .aggs = aggs, .by = &.{} } }, .hints = &.{}, .pos = pos },
        });
    };
    const ms = try runToMemory(env, opts, stages, "$since", lanes_used, batch_arena);
    for (ms.batches.items) |b| if (b.len > 0) return b.columns[0].getValue(0).own(a);
    return .null;
}

/// Whether `src`, a SQL table read `WITH (changes_from = $since)`, reads a change
/// feed whose log no longer reaches back to `since`.
fn cleanedPast(env: *Env, src: ast.Stage, since: []const u8) !bool {
    if (src.node != .read) return false;
    const rd = src.node.read;
    for (src.hints) |h| if (std.mem.eql(u8, h.key, "changes_from")) {
        if (h.value != .ident or !std.mem.eql(u8, h.value.ident, "$" ++ analyze.since_name)) return false;
        const conn = env.connections.get(rd.connector) orelse return false;
        const info = sqlConnInfo(conn) orelse return false;
        return changes.cleanedPast(env, rd, src.hints, conn, info, since);
    };
    return false;
}

fn readLabel(env: *Env, src: ast.Stage) ![]const u8 {
    if (src.node != .read or src.node.read.form != .table) return "the source";
    return std.mem.join(env.arena, ".", src.node.read.form.table.parts);
}

/// `out` with the target's live rows among its changes, ahead of the window that
/// keeps each key's latest: each as a delete of its key at the lowest place in the
/// order (a NULL `sequence_by`), keeping its own position, so a key the snapshot
/// holds takes the snapshot's row and one it lacks comes out deleted.
fn tombstones(env: *Env, out: ast.Pipeline, w: ast.Write, cols: []const []const u8) ![]const ast.Stage {
    const a = env.arena;
    const stages = out.stages;
    var wi: usize = 0;
    while (wi < stages.len) : (wi += 1) {
        const st = stages[wi].node;
        if (st == .window and st.window.funcs.len > 0 and std.mem.eql(u8, st.window.funcs[0].out, ast.Changes.rank_col)) break;
    } else return stages;
    const ch = w.changes.?;
    var live = std.array_list.Managed(ast.Stage).init(a);
    try live.append(.{ .node = .{ .read = try targetRead(w, a) }, .hints = try targetHints(a, w, out.pos), .pos = out.pos });
    if (ast.nameIn(cols, ast.Changes.deleted_col)) {
        const flag = try expr(a, .{ .cast = .{ .e = try field(a, ast.Changes.deleted_col), .ty = types.Type.init(.bool) } });
        const known = try expr(a, .{ .call = .{ .name = "coalesce", .args = try a.dupe(*ast.Expr, &.{ flag, try expr(a, .{ .bool_lit = false }) }) } });
        const not = try expr(a, .{ .unary = .{ .op = .not, .e = known } });
        try live.append(.{ .node = .{ .filter = not }, .hints = &.{}, .pos = out.pos });
    }
    try live.append(.{ .node = .{ .select = try a.dupe(ast.SelectItem, &.{
        .star,
        .{ .computed = .{ .name = ast.Changes.seq_col, .expr = try expr(a, .null_lit) } },
        .{ .computed = .{ .name = ast.Changes.del_col, .expr = try expr(a, .{ .bool_lit = true }) } },
        .{ .computed = .{ .name = ast.Changes.pos_col, .expr = if (ast.nameIn(cols, ch.position)) try field(a, ch.position) else try expr(a, .null_lit) } },
    }) }, .hints = &.{}, .pos = out.pos });
    const feed_name = "__changes_feed";
    const live_name = "__changes_live";
    try env.bindings.put(feed_name, .{ .stages = stages[0..wi], .pos = out.pos });
    try env.bindings.put(live_name, .{ .stages = live.items, .pos = out.pos });
    const branch = struct {
        fn of(al: std.mem.Allocator, name: []const u8, pos: ast.Pos) !ast.UnionBranch {
            return .{ .read = .{ .connector = "csv", .form = .{ .path = "" } }, .tag = null, .pipeline = .{ .stages = try al.dupe(ast.Stage, &.{.{ .node = .{ .ref = name }, .hints = &.{}, .pos = pos }}), .pos = pos } };
        }
    }.of;
    const un = ast.Union{ .branches = try a.dupe(ast.UnionBranch, &.{ try branch(a, feed_name, out.pos), try branch(a, live_name, out.pos) }), .pos = out.pos };
    return std.mem.concat(a, ast.Stage, &.{ &.{.{ .node = .{ .union_ = un }, .hints = &.{}, .pos = out.pos }}, stages[wi..] });
}

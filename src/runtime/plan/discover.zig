//! FOR EACH discovery: the rows a loop iterates, from a source, a full query or a
//! JSON parameter, as text.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const Row = @import("../env.zig").Row;
const aborting = @import("../env.zig").aborting;
const ast = @import("../../lang/ast.zig");
const buildPipeline = @import("../plan.zig").buildPipeline;
const jsonToStr = @import("../plan.zig").jsonToStr;
const openSource = @import("../connect.zig").openSource;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const std = @import("std");

/// Append a discovery batch's first `ncols` columns as text (null → ""), shared by
/// every discovery form so all agree on coercion and the column-count error.
fn appendDiscoveryRows(env: *Env, rows: *std.array_list.Managed(Row), b: Batch, ncols: usize) !void {
    if (b.columns.len == 0) return;
    if (b.columns.len < ncols)
        return planErr(env.diag, "for-each: the discovery query returns fewer columns than loop variables");
    for (0..b.len) |r| {
        const row = try env.arena.alloc([]const u8, ncols);
        for (0..ncols) |j| {
            row[j] = switch (b.columns[j].getValue(r)) {
                .null => "",
                .string, .bytes => |s| try env.arena.dupe(u8, s),
                .int => |x| try std.fmt.allocPrint(env.arena, "{d}", .{x}),
                else => return planErr(env.diag, "for-each values must be string or int"),
            };
        }
        try rows.append(row);
    }
}

/// The discovery source's first `ncols` columns as text rows, fully materialized
/// (a table catalog is small). Keeps `openSource`'s own error message.
pub fn discoverRows(env: *Env, src_read: ast.Read, ncols: usize) ![]const Row {
    const src = openSource(env, src_read, &.{}) catch |e| {
        const why = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{why}));
    };
    defer src.close();
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        _ = da.reset(.retain_capacity);
        const b = (try src.next(da.allocator())) orelse break;
        try appendDiscoveryRows(env, &rows, b, ncols);
    }
    return rows.toOwnedSlice();
}

/// Discovery from a full query, planned through `buildPipeline` (pushdown free).
/// Its sources are closed here and per-pipeline scratch fields restored after.
pub fn discoverRowsPipeline(env: *Env, pipe: ast.Pipeline, ncols: usize) anyerror![]const Row {
    if (pipe.stages.len == 0) return planErr(env.diag, "for-each: empty discovery query");
    const src_base = env.sources.items.len;
    const saved_src_name = env.src_name;
    const saved_sql_desc = env.sql_desc;
    const saved_pq_readers = env.pq_readers;
    const saved_pq_reader = env.pq_reader;
    const saved_pq_folder = env.pq_folder;
    defer {
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.src_name = saved_src_name;
        env.sql_desc = saved_sql_desc;
        env.pq_readers = saved_pq_readers;
        env.pq_reader = saved_pq_reader;
        env.pq_folder = saved_pq_folder;
    }

    const res = buildPipeline(env, pipe.stages) catch |e| {
        const why = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{why}));
    };
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        if (aborting()) return error.Aborted;
        _ = da.reset(.retain_capacity);
        const b = (try res.op.next(da.allocator())) orelse break;
        try appendDiscoveryRows(env, &rows, b, ncols);
    }
    return rows.toOwnedSlice();
}

/// For-each rows from a JSON array param, each loop variable bound to the
/// like-named field of each element as text.
pub fn discoverRowsJson(env: *Env, path: ast.QualName, var_names: []const []const u8) ![]const Row {
    const head = path.parts[0];
    var cur = env.json_params.get(head) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: `{s}` is not a JSON param", .{head}));
    for (path.parts[1..], 0..) |key, j| {
        const seg_safe = j < path.safe.len and path.safe[j];
        cur = switch (cur) {
            .object => |o| o.get(key) orelse {
                if (seg_safe) return &.{};
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: json key `{s}` not found", .{key}));
            },
            else => {
                if (seg_safe) return &.{};
                return planErr(env.diag, "for-each: json path is not an object");
            },
        };
    }
    const arr = switch (cur) {
        .array => |a| a,
        else => return planErr(env.diag, "for-each: json source is not an array"),
    };
    var rows = std.array_list.Managed(Row).init(env.arena);
    for (arr.items) |elem| {
        const row = try env.arena.alloc([]const u8, var_names.len);
        for (var_names, 0..) |vn, i| {
            row[i] = switch (elem) {
                .object => |o| if (o.get(vn)) |fv| try jsonToStr(env.arena, fv) else "",
                else => "",
            };
        }
        try rows.append(row);
    }
    return rows.toOwnedSlice();
}

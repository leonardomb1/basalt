//! The sink each parallel lane writes through: a Stream Load of its own, dialed per
//! lane from one spec. A SQL database target has none: its load is one transaction,
//! which one connection holds, so lanes share the run's single sink (a setup sink's
//! committed DELETE and per-lane commits once left a failed REPLACE's table empty).

const Env = @import("../env.zig").Env;
const ast = @import("../../lang/ast.zig");
const driver = @import("../../connect/driver.zig");
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const parallel = @import("../parallel.zig");
const planErr = @import("../env.zig").planErr;
const resolveStreamLoadConfig = @import("sink.zig").resolveStreamLoadConfig;
const sql = @import("../../db/sql.zig");
const srOpenErr = @import("../env.zig").srOpenErr;
const std = @import("std");
const streamload = @import("../../db/streamload.zig");
const types = @import("../../lang/types.zig");

const StreamLoadSpec = struct {
    cfg: streamload.Config,
    target: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    logger: ?*obs.Logger = null,
    errctx: ?*op.ErrCtx = null,
};

fn openLaneStreamLoadSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    const spec: *StreamLoadSpec = @ptrCast(@alignCast(ctx_ptr));
    var cfg = spec.cfg;
    const lp = try std.fmt.allocPrint(gpa, "{s}_l{d}", .{ spec.cfg.label_prefix, lane_idx });
    defer gpa.free(lp);
    cfg.label_prefix = lp;
    const s = try streamload.StreamLoadSink.open(gpa, cfg, spec.target, spec.schema, spec.mode);
    s.logger = spec.logger;
    s.errctx = spec.errctx;
    return s.sink();
}

/// The bulk-vs-INSERT rule, shared by the serial and per-lane paths so they
/// cannot drift. On error the caller still owns and closes `conn`.
pub fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode, errctx: ?*op.ErrCtx) !driver.Sink {
    if (mode != .upsert) {
        const b = try BulkSink.open(gpa, conn, target, schema, mode);
        b.errctx = errctx;
        return b.sink();
    }
    const s = try sql.Sink.open(gpa, conn.sqlConn(), dialect, target, schema, mode);
    s.errctx = errctx;
    return s.sink();
}

/// A per-lane Stream Load sink for a split pipeline, or null for the
/// shared-mutex path.
pub fn buildParallelSink(env: *Env, w: ast.Write, schema: types.Schema) !?parallel.SinkMode {
    try @import("sink.zig").uniqueTargetColumns(env, w, schema);
    if (try buildStreamLoadSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneStreamLoadSink, .ctx = spec } };
    return null;
}

fn buildStreamLoadSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*StreamLoadSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const flavor = streamload.Flavor.of(conn.connector) orelse return null;

    var cfg = try resolveStreamLoadConfig(env, conn, flavor);
    cfg.run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp());

    const setup = streamload.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
        return srOpenErr(env, e, try std.fmt.allocPrint(env.arena, "{s} setup failed", .{flavor.name()}));
    setup.logger = env.log;
    setup.errctx = env.errctx;
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} setup close failed: {s}", .{ flavor.name(), @errorName(e) }));
    cfg.auto_create = false;

    const spec = try env.arena.create(StreamLoadSpec);
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode, .logger = env.log, .errctx = env.errctx };
    return spec;
}

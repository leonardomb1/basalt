//! Build executable operator trees from a parsed @batch program and drive them.
//! Handles full program structure: `param`s (bound from the CLI or defaults),
//! `let` bindings (recompiled per reference), `ref` sources, and multiple output
//! pipelines. Params are substituted into expressions as literals before planning.
//! (CSV is the only wired connector until the drivers land.)

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const eval = @import("../exec/eval.zig");
const op = @import("../exec/op.zig");
const csv = @import("../connect/csv.zig");
const driver = @import("../connect/driver.zig");
const starrocks = @import("../connect/starrocks.zig");
const tds = @import("../connect/tds.zig");
const mysql = @import("../connect/mysql.zig");
const postgres = @import("../connect/postgres.zig");
const sql = @import("../connect/sql.zig");
const request = @import("../connect/request.zig");
const valuemod = @import("../exec/value.zig");

const Value = valuemod.Value;

/// `msg` points into the inline `buf`, so it outlives the run's plan arena.
pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
};
pub const Stats = struct { rows_out: usize = 0 };
pub const ParamArg = struct { key: []const u8, val: []const u8 };

/// Inputs to a run: params (from CLI flags or an HTTP request's query string) and
/// an optional request body that `read request` consumes.
pub const RunOptions = struct {
    params: []const ParamArg = &.{},
    request_body: ?[]const u8 = null,
};

const Env = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    params: *std.StringHashMap(Value),
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    sources: *std.ArrayList(driver.Source),
    request_body: ?[]const u8,
    diag: *Diag,
};

const PipeRes = struct { op: op.Op, schema: types.Schema };

pub fn run(gpa: std.mem.Allocator, program: ast.Program, opts: RunOptions, diag: *Diag) !Stats {
    var plan_arena = std.heap.ArenaAllocator.init(gpa);
    defer plan_arena.deinit();
    const arena = plan_arena.allocator();

    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return planErr(diag, "script must begin with a @kind tag");
    // @batch runs once; @http reuses this for each request (with a request body).
    if (program.stmts[0].kind.kind == .stream)
        return planErr(diag, "@stream is not implemented yet");

    var params = std.StringHashMap(Value).init(arena);
    try resolveParams(arena, program, opts.params, &params, diag);

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    var outputs = std.ArrayList(ast.Pipeline).init(arena);
    for (program.stmts[1..]) |s| switch (s) {
        .binding => |b| try bindings.put(b.name, b.pipeline),
        .connection => |c| try connections.put(c.name, c),
        .output => |p| try outputs.append(p),
        .param, .kind => {},
    };
    if (outputs.items.len == 0)
        return planErr(diag, "no output pipeline (a pipeline ending in `write`)");

    var sources = std.ArrayList(driver.Source).init(arena);
    var env = Env{ .arena = arena, .gpa = gpa, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = opts.request_body, .diag = diag };

    var batch_arena = std.heap.ArenaAllocator.init(gpa);
    defer batch_arena.deinit();

    var stats = Stats{};
    for (outputs.items) |out| {
        const stages = out.stages;
        const last = stages[stages.len - 1].node;
        if (last != .write) return planErr(diag, "a top-level pipeline must end in `write`");

        const res = try buildPipeline(&env, stages[0 .. stages.len - 1]);
        const snk = try openSink(&env, last.write, res.schema);

        while (true) {
            _ = batch_arena.reset(.retain_capacity);
            const b = (try res.op.next(batch_arena.allocator())) orelse break;
            try snk.writeBatch(batch_arena.allocator(), b);
            stats.rows_out += b.len;
        }
        try snk.close();
    }
    for (sources.items) |sc| sc.close();
    return stats;
}

// --- pipeline construction ---

fn buildPipeline(env: *Env, stages: []const ast.Stage) anyerror!PipeRes {
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");

    var current: op.Op = undefined;
    var schema: types.Schema = undefined;

    switch (stages[0].node) {
        .read => |rd| {
            const src = try openSource(env, rd);
            try env.sources.append(src);
            const scan = try env.arena.create(op.Scan);
            scan.* = .{ .src = src };
            current = .{ .scan = scan };
            schema = src.schema();
        },
        .ref => |name| {
            const b = env.bindings.get(name) orelse
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown binding `{s}`", .{name}));
            const r = try buildPipeline(env, b.stages);
            current = r.op;
            schema = r.schema;
        },
        else => return planErr(env.diag, "a pipeline must start with `read` or a binding reference"),
    }

    for (stages[1..]) |stage| {
        const r = try buildStage(env, stage, current, schema);
        current = r.op;
        schema = r.schema;
    }
    return .{ .op = current, .schema = schema };
}

fn buildStage(env: *Env, stage: ast.Stage, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    switch (stage.node) {
        .filter => |pred0| {
            const pred = try substParams(env, pred0);
            var ctx = eval.TypeCtx{ .schema = schema, .arena = arena };
            const t = ctx.typeOf(pred) catch |e| return typeErr(e, env.diag, ctx.msg);
            if (!(t.kind == .bool or t.unknown)) return planErr(env.diag, "filter predicate must be bool");
            const f = try arena.create(op.Filter);
            f.* = .{ .child = child, .pred = pred };
            return .{ .op = .{ .filter = f }, .schema = schema };
        },
        .select => |items| return buildProject(env, items, schema, child),
        .limit => |lim| {
            const l = try arena.create(op.Limit);
            l.* = .{ .child = child, .remaining = lim.count, .to_skip = lim.offset };
            return .{ .op = .{ .limit = l }, .schema = schema };
        },
        .distinct => |d| {
            var keys: ?[]const usize = null;
            if (d.on) |fields| {
                const idxs = try arena.alloc(usize, fields.len);
                for (fields, 0..) |q, i| idxs[i] = schema.indexOf(lastp(q)) orelse
                    return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown field `{s}`", .{lastp(q)}));
                keys = idxs;
            }
            const o = try arena.create(op.Distinct);
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = keys };
            return .{ .op = .{ .distinct = o }, .schema = schema };
        },
        .sort => |s| {
            const ks = try arena.alloc(op.Sort.Key, s.keys.len);
            for (s.keys, 0..) |sk, i| {
                const idx = schema.indexOf(lastp(sk.field)) orelse
                    return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown field `{s}`", .{lastp(sk.field)}));
                ks[i] = .{ .idx = idx, .desc = sk.desc };
            }
            const o = try arena.create(op.Sort);
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks };
            return .{ .op = .{ .sort = o }, .schema = schema };
        },
        .aggregate => |ag| return buildAggregate(env, ag, schema, child),
        .join => |j| return buildJoin(env, j, schema, child),
        .explode => |ex| {
            const idx = schema.indexOf(ex.field) orelse
                return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown field `{s}`", .{ex.field}));
            const fty = schema.fields[idx].ty;
            if (!(fty.kind == .string or fty.kind == .bytes))
                return planErr(env.diag, "explode needs a string column (it splits a delimited value)");
            var fields = std.ArrayList(types.Schema.Field).init(arena);
            for (schema.fields, 0..) |f, i| {
                if (i == idx) {
                    try fields.append(.{ .name = ex.as_name orelse f.name, .ty = types.Type.init(.string) });
                } else {
                    try fields.append(f);
                }
            }
            const out = try arena.create(types.Schema);
            out.* = .{ .fields = try fields.toOwnedSlice() };
            const o = try arena.create(op.Explode);
            o.* = .{ .child = child, .field_idx = idx, .delim = ex.delim orelse ",", .out_schema = out };
            return .{ .op = .{ .explode = o }, .schema = out.* };
        },
        .read, .ref, .write => return planErr(env.diag, "unexpected operator in the middle of a pipeline"),
    }
}

fn buildProject(env: *Env, items: []const ast.SelectItem, in_schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    var cols = std.ArrayList(op.Project.Col).init(arena);
    var fields = std.ArrayList(types.Schema.Field).init(arena);

    for (items) |item| switch (item) {
        .star => for (in_schema.fields, 0..) |f, idx| {
            try cols.append(.{ .source = .{ .passthrough = idx }, .ty = f.ty });
            try fields.append(.{ .name = f.name, .ty = f.ty });
        },
        .star_except => |names| for (in_schema.fields, 0..) |f, idx| {
            if (containsName(names, f.name)) continue;
            try cols.append(.{ .source = .{ .passthrough = idx }, .ty = f.ty });
            try fields.append(.{ .name = f.name, .ty = f.ty });
        },
        .field => |q| {
            const nm = lastp(q);
            const idx = in_schema.indexOf(nm) orelse
                return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown field `{s}`", .{nm}));
            try cols.append(.{ .source = .{ .passthrough = idx }, .ty = in_schema.fields[idx].ty });
            try fields.append(.{ .name = nm, .ty = in_schema.fields[idx].ty });
        },
        .computed => |c| {
            const e = try substParams(env, c.expr);
            var ctx = eval.TypeCtx{ .schema = in_schema, .arena = arena };
            const t = ctx.typeOf(e) catch |err| return typeErr(err, env.diag, ctx.msg);
            try cols.append(.{ .source = .{ .expr = e }, .ty = t });
            try fields.append(.{ .name = c.name, .ty = t });
        },
    };

    const out = try arena.create(types.Schema);
    out.* = .{ .fields = try fields.toOwnedSlice() };
    const p = try arena.create(op.Project);
    p.* = .{ .child = child, .cols = try cols.toOwnedSlice(), .out_schema = out };
    return .{ .op = .{ .project = p }, .schema = out.* };
}

fn buildAggregate(env: *Env, ag: ast.Aggregate, schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    const by = try arena.alloc(usize, ag.by.len);
    var fields = std.ArrayList(types.Schema.Field).init(arena);
    for (ag.by, 0..) |q, i| {
        const idx = schema.indexOf(lastp(q)) orelse
            return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown group field `{s}`", .{lastp(q)}));
        by[i] = idx;
        try fields.append(.{ .name = lastp(q), .ty = schema.fields[idx].ty });
    }
    const aggs = try arena.alloc(op.Aggregate.Agg, ag.aggs.len);
    for (ag.aggs, 0..) |item, i| {
        const arg: ?*const ast.Expr = if (item.arg) |a| try substParams(env, a) else null;
        const aty = try aggResultType(arena, item.func, arg, schema, env.diag);
        aggs[i] = .{ .func = item.func, .arg = arg, .ty = aty };
        try fields.append(.{ .name = item.name, .ty = aty });
    }
    const out = try arena.create(types.Schema);
    out.* = .{ .fields = try fields.toOwnedSlice() };
    const o = try arena.create(op.Aggregate);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .by = by, .aggs = aggs, .out_schema = out };
    return .{ .op = .{ .aggregate = o }, .schema = out.* };
}

fn buildJoin(env: *Env, j: ast.Join, left_schema: types.Schema, probe: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    if (j.kind == .right or j.kind == .full or j.kind == .cross)
        return planErr(env.diag, "this join type is not implemented yet (inner/left/semi/anti supported)");
    const bnd = env.bindings.get(j.binding) orelse
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const build = try buildPipeline(env, bnd.stages);

    const lk = left_schema.indexOf(lastp(j.left_key)) orelse
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown left join key `{s}`", .{lastp(j.left_key)}));
    const rk = build.schema.indexOf(lastp(j.right_key)) orelse
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown right join key `{s}`", .{lastp(j.right_key)}));

    const emit_right = (j.kind == .inner or j.kind == .left);
    const right_nullable = (j.kind == .left);

    var fields = std.ArrayList(types.Schema.Field).init(arena);
    for (left_schema.fields) |f| try fields.append(f);
    if (emit_right) {
        for (build.schema.fields) |f| {
            var name = f.name;
            if (left_schema.indexOf(name) != null) name = try std.fmt.allocPrint(arena, "{s}_r", .{name});
            var ty = f.ty;
            if (right_nullable) ty = ty.asNullable();
            try fields.append(.{ .name = name, .ty = ty });
        }
    }
    const out = try arena.create(types.Schema);
    out.* = .{ .fields = try fields.toOwnedSlice() };

    const o = try arena.create(op.Join);
    o.* = .{
        .probe = probe,
        .build = build.op,
        .left_key = lk,
        .right_key = rk,
        .left_schema = try schemaPtr(arena, left_schema),
        .right_schema = try schemaPtr(arena, build.schema),
        .out_schema = out,
        .kind = j.kind,
    };
    return .{ .op = .{ .join = o }, .schema = out.* };
}

fn openSource(env: *Env, rd: ast.Read) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "request")) {
        const body = env.request_body orelse
            return planErr(env.diag, "`read request` is only available when serving HTTP (@http)");
        const s = request.RequestSource.open(env.gpa, body) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "could not parse request body as JSON: {s}", .{@errorName(e)}));
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "csv")) {
        if (rd.form != .path) return planErr(env.diag, "read csv needs a quoted path");
        const reader = csv.CsvReader.open(env.arena, rd.form.path) catch
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "could not open input CSV `{s}`", .{rd.form.path}));
        return reader.source();
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{rd.connector}));
    if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        const cfg = try resolveDbConfig(env, conn, 1433);
        const query = try readSql(env, rd);
        const c = tds.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver read failed: {s}", .{@errorName(e)}));
        return s.source();
    }
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        const cfg = try resolveDbConfig(env, conn, 3306);
        const query = try readSql(env, rd);
        const c = mysql.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql read failed: {s}", .{@errorName(e)}));
        return s.source();
    }
    if (std.mem.eql(u8, conn.connector, "postgres")) {
        const cfg = try resolveDbConfig(env, conn, 5432);
        const query = try readSql(env, rd);
        const c = postgres.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres read failed: {s}", .{@errorName(e)}));
        return s.source();
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported source connector `{s}`", .{conn.connector}));
}

const DbConfig = struct {
    host: []const u8 = "",
    port: u16,
    user: []const u8 = "",
    password: []const u8 = "",
    database: []const u8 = "",
};

fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
    var cfg = DbConfig{ .port = default_port };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "host")) {
            cfg.host = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "port")) {
            cfg.port = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = try evalCfgStr(env, attr.value);
        }
    }
    if (cfg.host.len == 0) return planErr(env.diag, "connection needs a `host`");
    return cfg;
}

fn readSql(env: *Env, rd: ast.Read) ![]const u8 {
    return switch (rd.form) {
        .query => |q| q,
        .table => |t| try std.fmt.allocPrint(env.arena, "SELECT * FROM {s}", .{try qualStr(env.arena, t)}),
        else => planErr(env.diag, "a DB read needs `table <name>` or `query \"...\"`"),
    };
}

fn qualStr(arena: std.mem.Allocator, q: ast.QualName) ![]const u8 {
    if (q.parts.len == 1) return q.parts[0];
    return std.mem.join(arena, ".", q.parts);
}

fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, "csv")) {
        const writer = csv.CsvWriter.open(env.arena, w.target, schema) catch
            return planErr(env.diag, "could not open output CSV");
        return writer.sink();
    }
    const conn = env.connections.get(w.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{w.connector}));
    if (std.mem.eql(u8, conn.connector, "starrocks")) {
        const cfg = try resolveStarrocksConfig(env, conn);
        const s = starrocks.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks sink open failed ({s}) — {s}", .{ @errorName(e), env.diag.msg }));
        return s.sink();
    }
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        const cfg = try resolveDbConfig(env, conn, 3306);
        const c = mysql.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql connect failed: {s}", .{@errorName(e)}));
        const s = sql.Sink.open(env.gpa, c.sqlConn(), .mysql, w.target, schema, w.mode) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql sink failed: {s}", .{@errorName(e)}));
        return s.sink();
    }
    if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        const cfg = try resolveDbConfig(env, conn, 1433);
        const c = tds.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver connect failed: {s}", .{@errorName(e)}));
        const s = sql.Sink.open(env.gpa, c.sqlConn(), .sqlserver, w.target, schema, w.mode) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver sink failed: {s}", .{@errorName(e)}));
        return s.sink();
    }
    if (std.mem.eql(u8, conn.connector, "postgres")) {
        const cfg = try resolveDbConfig(env, conn, 5432);
        const c = postgres.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres connect failed: {s}", .{@errorName(e)}));
        const s = sql.Sink.open(env.gpa, c.sqlConn(), .postgres, w.target, schema, w.mode) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres sink failed: {s}", .{@errorName(e)}));
        return s.sink();
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported sink connector `{s}`", .{conn.connector}));
}

fn resolveStarrocksConfig(env: *Env, conn: ast.Connection) !starrocks.Config {
    var cfg = starrocks.Config{ .database = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (eqlAny(k, &.{ "host", "fe_host" })) {
            cfg.fe_host = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "fe_port")) {
            cfg.fe_port = @intCast(try evalCfgInt(env, attr.value));
        } else if (eqlAny(k, &.{ "be_url", "load_url" })) {
            cfg.load_url = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "buckets")) {
            cfg.buckets = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "replication_num")) {
            cfg.replication_num = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "auto_create")) {
            cfg.auto_create = try evalCfgBool(env, attr.value);
        } else if (std.mem.eql(u8, k, "label_prefix")) {
            cfg.label_prefix = try evalCfgStr(env, attr.value);
        }
    }
    if (cfg.database.len == 0) return planErr(env.diag, "starrocks connection needs a `database`");
    return cfg;
}

fn evalCfgStr(env: *Env, expr: *const ast.Expr) ![]const u8 {
    switch (expr.*) {
        .str_lit => |s| return s,
        .int_lit => |i| return std.fmt.allocPrint(env.arena, "{d}", .{i}),
        .bool_lit => |b| return if (b) "true" else "false",
        .call => |c| {
            if ((std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and
                c.args.len == 1 and c.args[0].* == .str_lit)
            {
                const name = c.args[0].str_lit;
                return std.process.getEnvVarOwned(env.arena, name) catch
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "env var `{s}` is not set", .{name}));
            }
            return planErr(env.diag, "config value must be a literal or env()/secret()");
        },
        else => return planErr(env.diag, "config value must be a literal or env()/secret()"),
    }
}

fn evalCfgInt(env: *Env, expr: *const ast.Expr) !i64 {
    return switch (expr.*) {
        .int_lit => |i| i,
        .str_lit => |s| std.fmt.parseInt(i64, s, 10) catch return planErr(env.diag, "invalid integer config value"),
        else => planErr(env.diag, "config value must be an integer"),
    };
}

fn evalCfgBool(env: *Env, expr: *const ast.Expr) !bool {
    return switch (expr.*) {
        .bool_lit => |b| b,
        .str_lit => |s| std.mem.eql(u8, s, "true"),
        else => planErr(env.diag, "config value must be a bool"),
    };
}

fn eqlAny(k: []const u8, opts: []const []const u8) bool {
    for (opts) |o| {
        if (std.mem.eql(u8, k, o)) return true;
    }
    return false;
}

// --- params ---

fn resolveParams(arena: std.mem.Allocator, program: ast.Program, cli: []const ParamArg, params: *std.StringHashMap(Value), diag: *Diag) !void {
    for (program.stmts) |s| {
        if (s != .param) continue;
        const p = s.param;
        var v: ?Value = null;
        for (cli) |kv| {
            if (std.mem.eql(u8, kv.key, p.name)) {
                v = try parseParamValue(arena, p.ty, kv.val, diag);
                break;
            }
        }
        if (v == null) {
            if (p.default) |d| {
                v = try constEvalDefault(d, diag);
            } else {
                return planErr(diag, try std.fmt.allocPrint(arena, "missing required param `{s}`", .{p.name}));
            }
        }
        try params.put(p.name, v.?);
    }
}

fn parseParamValue(arena: std.mem.Allocator, ty: types.Type, str: []const u8, diag: *Diag) !Value {
    return switch (ty.kind) {
        .int => .{ .int = std.fmt.parseInt(i64, str, 10) catch return planErr(diag, "invalid integer param value") },
        .float => .{ .float = std.fmt.parseFloat(f64, str) catch return planErr(diag, "invalid float param value") },
        .string => .{ .string = try arena.dupe(u8, str) },
        .bool => if (std.mem.eql(u8, str, "true")) Value{ .bool = true } else if (std.mem.eql(u8, str, "false")) Value{ .bool = false } else planErr(diag, "invalid bool param value"),
        else => planErr(diag, "unsupported param type for CLI binding"),
    };
}

fn constEvalDefault(expr: *const ast.Expr, diag: *Diag) !Value {
    return switch (expr.*) {
        .int_lit => |i| .{ .int = i },
        .float_lit => |f| .{ .float = f },
        .str_lit => |s| .{ .string = s },
        .bool_lit => |b| .{ .bool = b },
        .null_lit => .null,
        else => planErr(diag, "param default must be a literal"),
    };
}

/// Deep-copy `expr`, replacing single-name field refs that match a param with a
/// literal of the param's value. No params => returns the original (no copy).
fn substParams(env: *Env, expr: *ast.Expr) anyerror!*ast.Expr {
    if (env.params.count() == 0) return expr;
    return substExpr(env, expr);
}

fn substExpr(env: *Env, expr: *ast.Expr) anyerror!*ast.Expr {
    const arena = env.arena;
    switch (expr.*) {
        .field => |q| {
            if (q.parts.len == 1) {
                if (env.params.get(q.parts[0])) |v| return try mkLit(arena, v);
            }
            return expr;
        },
        .unary => |u| return mk(arena, .{ .unary = .{ .op = u.op, .e = try substExpr(env, u.e) } }),
        .binary => |b| return mk(arena, .{ .binary = .{ .op = b.op, .l = try substExpr(env, b.l), .r = try substExpr(env, b.r) } }),
        .is_null => |n| return mk(arena, .{ .is_null = .{ .e = try substExpr(env, n.e), .negated = n.negated } }),
        .cast => |c| return mk(arena, .{ .cast = .{ .e = try substExpr(env, c.e), .ty = c.ty } }),
        .cond => |c| return mk(arena, .{ .cond = .{ .cond = try substExpr(env, c.cond), .then = try substExpr(env, c.then), .els = try substExpr(env, c.els) } }),
        .call => |c| {
            const args = try arena.alloc(*ast.Expr, c.args.len);
            for (c.args, 0..) |a, i| args[i] = try substExpr(env, a);
            return mk(arena, .{ .call = .{ .name = c.name, .args = args } });
        },
        else => return expr, // literals
    }
}

fn mk(arena: std.mem.Allocator, e: ast.Expr) anyerror!*ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

fn mkLit(arena: std.mem.Allocator, v: Value) anyerror!*ast.Expr {
    return switch (v) {
        .null => mk(arena, .null_lit),
        .bool => |b| mk(arena, .{ .bool_lit = b }),
        .int => |i| mk(arena, .{ .int_lit = i }),
        .float => |f| mk(arena, .{ .float_lit = f }),
        .string => |s| mk(arena, .{ .str_lit = s }),
        else => mk(arena, .null_lit),
    };
}

// --- small helpers ---

fn setMsg(diag: *Diag, msg: []const u8) void {
    const n = @min(msg.len, diag.buf.len);
    @memcpy(diag.buf[0..n], msg[0..n]);
    diag.msg = diag.buf[0..n];
}

fn planErr(diag: *Diag, msg: []const u8) error{PlanFailed} {
    setMsg(diag, msg);
    return error.PlanFailed;
}

fn typeErr(e: eval.TypeError, diag: *Diag, msg: []const u8) error{ PlanFailed, OutOfMemory } {
    if (e == error.OutOfMemory) return error.OutOfMemory;
    setMsg(diag, msg);
    return error.PlanFailed;
}

fn containsName(names: []const []const u8, n: []const u8) bool {
    for (names) |x| {
        if (std.mem.eql(u8, x, n)) return true;
    }
    return false;
}

fn lastp(q: ast.QualName) []const u8 {
    return q.parts[q.parts.len - 1];
}

fn schemaPtr(arena: std.mem.Allocator, schema: types.Schema) !*types.Schema {
    const p = try arena.create(types.Schema);
    p.* = schema;
    return p;
}

fn aggResultType(arena: std.mem.Allocator, func: ast.AggFunc, arg: ?*const ast.Expr, schema: types.Schema, diag: *Diag) !types.Type {
    switch (func) {
        .count => return types.Type.init(.int),
        else => {
            const a = arg orelse return planErr(diag, "this aggregate requires an argument");
            var ctx = eval.TypeCtx{ .schema = schema, .arena = arena };
            const at = ctx.typeOf(a) catch |e| return typeErr(e, diag, ctx.msg);
            return switch (func) {
                .sum => if (at.kind == .float) types.Type.init(.float).withNull(true) else types.Type.init(.int).withNull(true),
                .avg => types.Type.init(.float).withNull(true),
                .min, .max => at.withNull(true),
                .count => unreachable,
            };
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const parser = @import("../lang/parser.zig");

/// Run `@batch read csv | <body> | write csv` over `input`, returning the output.
fn runToString(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, body: []const u8) ![]u8 {
    return runToStringP(alloc, tmp, input, body, &[_]ParamArg{});
}

fn runToStringP(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, body: []const u8, cli_params: []const ParamArg) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc,
        "@batch\nread csv \"{s}\"\n{s}\n  | write csv \"{s}\"",
        .{ in_path, body, out_path },
    );
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

test "CSV -> filter/select -> CSV round-trips" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "  | filter status == \"paid\"\n  | select id, amount",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "aggregate: count and sum by group (nulls skipped)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\npaid,\n",
        "  | aggregate n = count(), total = sum(cast(amount as int)) by status",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,n,total\npaid,3,300\npending,1,50\n", out);
}

test "sort: numeric desc, nulls last" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,amount\n1,100\n2,\n3,200\n",
        "  | select id, amt = cast(amount as int)\n  | sort amt desc",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n1,100\n2,\n", out);
}

test "distinct keeps first row per key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\n",
        "  | distinct on status",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,amount\npaid,100\npending,50\n", out);
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

    const script = try std.fmt.allocPrint(alloc,
        "@batch\nparam min int = 0\nread csv \"{s}\"\n  | filter cast(amount as int) >= min\n  | select id\n  | write csv \"{s}\"",
        .{ in_path, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "min", .val = "100" }});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "explode splits a delimited column into rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,tags\n1,\"a,b,c\"\n2,x\n3,\n",
        "  | explode tags as tag",
    );
    defer alloc.free(out);
    // row 1 -> 3 rows; row 2 -> 1 row; row 3 (null) -> 0 rows
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,b\n1,c\n2,x\n", out);
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

    const script = try std.fmt.allocPrint(alloc,
        "@batch\nlet labels = read csv \"{s}\"\nread csv \"{s}\"\n  | join inner labels on code = code\n  | select id, label\n  | write csv \"{s}\"",
        .{ lookup_path, in_path, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    // id 3 (code Z) has no match -> dropped by inner join
    try std.testing.expectEqualStrings("id,label\n1,Apple\n2,Banana\n", out);
}

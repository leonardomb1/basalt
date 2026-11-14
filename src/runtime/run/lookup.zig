//! The runtime's side of a table function called per row (`exec/op/lookup.zig`): a
//! request is the body's own http read, sent as `FROM conn.GET(...)` sends one (the
//! connection's auth, retries and `items`), its `${p}` holes filled with the
//! arguments: in a JSON body each as JSON, an array of the request's rows in batch
//! mode; in the path or a form body percent-encoded; in a query arg (encoded as
//! the URL is built) or a header as its text, a header refusing a line break. The reply's items
//! become rows through the body's SELECT, each item a CAST; a field the reply lacks
//! reads as NULL.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const op = @import("../../exec/op.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const column = @import("../../exec/column.zig");
const eval = @import("../../exec/eval.zig");
const evalColumn = @import("../../exec/eval/vec.zig").evalColumn;
const valueToString = @import("../../exec/eval/format.zig").valueToString;
const jsontext = @import("../../exec/jsontext.zig");
const percent = @import("../../net/percent.zig");
const Value = @import("../../exec/value.zig").Value;
const http_client = @import("../../net/http_client.zig");
const msgfmt = @import("../../format/msgfmt.zig");
const analyze = @import("../analyze.zig");
const Env = @import("../env.zig").Env;
const LoopRow = @import("../env.zig").LoopRow;
const interpAll = @import("../script.zig").interpAll;
const planErr = @import("../env.zig").planErr;
const httpConn = @import("../connect/source.zig").httpConn;
const queryParams = @import("../connect.zig").queryParams;
const nullColumn = @import("../../exec/op/join.zig").nullColumn;

const default_concurrency = 4;
const max_concurrency = 64;
const max_batch = 4096;

const Service = struct {
    gpa: std.mem.Allocator,
    cc: http_client.ConnConfig,
    hints: []const ast.Hint,
    path: []const u8,
    params: []const []const u8,
    json_body: bool,
    exprs: []const *const ast.Expr,
    items: *const types.Schema,
    batched: bool,
    name: []const u8,
    errctx: ?*op.ErrCtx,
};

fn intHint(env: *Env, lk: ast.Lookup, key: []const u8, lo: i64, hi: i64) !?usize {
    for (lk.hints) |h| if (std.mem.eql(u8, h.key, key)) {
        const v = http_client.intOf(h.value) orelse lo - 1;
        if (v < lo or v > hi) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s}: `{s}` takes a whole number from {d} to {d}", .{ lk.name, key, lo, hi }));
        return @intCast(v);
    };
    return null;
}

/// The lookup operator for `lk` over `child`, its requests not yet sent.
pub fn build(env: *Env, lk: ast.Lookup, lp: analyze.LookupPlan, child: op.Op, out: *const types.Schema) !*op.Lookup {
    const conn = env.connections.get(lk.conn) orelse return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s}` is not a connection declared above", .{lk.conn}));
    const batch = (try intHint(env, lk, "batch", 1, max_batch)) orelse 0;
    const concurrency = (try intHint(env, lk, "concurrency", 1, max_concurrency)) orelse default_concurrency;
    var hints = std.array_list.Managed(ast.Hint).init(env.arena);
    var json_body = false;
    for (lk.hints) |h| {
        if (std.mem.eql(u8, h.key, "batch") or std.mem.eql(u8, h.key, "concurrency")) continue;
        if (std.mem.eql(u8, h.key, "body_type") and h.value == .str) json_body = std.ascii.eqlIgnoreCase(h.value.str, "json");
        try hints.append(h);
    }
    const hc = try httpConn(env, lk.conn, conn, hints.items);
    const svc = try env.arena.create(Service);
    svc.* = .{
        .gpa = env.gpa,
        .cc = hc.cc,
        .hints = hc.hints,
        .path = lk.path,
        .params = lk.params,
        .json_body = json_body,
        .exprs = lp.exprs,
        .items = lp.items,
        .batched = batch > 0,
        .name = lk.name,
        .errctx = env.errctx,
    };
    const o = try env.arena.create(op.Lookup);
    o.* = .{
        .child = child,
        .args = lp.args,
        .arg_types = lp.arg_types,
        .caller = .{ .ptr = svc, .call = call },
        .batch = batch,
        .concurrency = concurrency,
        .outer = lk.outer,
        .item_schema = lp.items,
        .item_fields = lp.item_fields,
        .list_ty = lp.list_ty,
        .out_schema = out,
        .errctx = env.errctx,
        .gpa = env.gpa,
    };
    return o;
}

const Form = enum { json, text, encoded };

/// Each parameter's `${p}` text for one request: in batch mode a JSON array of its
/// rows' values, else its one row's value as JSON, as text, or as text
/// percent-encoded for a path or a form body.
fn render(a: std.mem.Allocator, args: []const []const Value, batched: bool, form: Form) ![]const []const u8 {
    const out = try a.alloc([]const u8, args.len);
    for (out, args) |*o, vals| {
        if (form != .json) {
            const t = if (vals[0] == .null) "" else try valueToString(a, vals[0]);
            if (form == .text) {
                o.* = t;
                continue;
            }
            var buf = std.array_list.Managed(u8).init(a);
            try percent.encode(&buf, t);
            o.* = buf.items;
            continue;
        }
        var w = std.Io.Writer.Allocating.init(a);
        if (batched) try w.writer.writeByte('[');
        for (vals, 0..) |v, i| {
            if (i > 0) try w.writer.writeByte(',');
            try jsontext.scalar(&w.writer, v, jsontext.nested);
        }
        if (batched) try w.writer.writeByte(']');
        o.* = w.written();
    }
    return out;
}

fn call(ptr: *anyopaque, a: std.mem.Allocator, args: []const []const Value, rows: usize) anyerror!Batch {
    const svc: *Service = @ptrCast(@alignCast(ptr));
    const none = LoopRow{ .names = &.{}, .cells = &.{} };
    const as_json = LoopRow{ .names = svc.params, .cells = try render(a, args, svc.batched, .json) };
    const as_text = if (svc.batched) none else LoopRow{ .names = svc.params, .cells = try render(a, args, false, .text) };
    const as_url = if (svc.batched) none else LoopRow{ .names = svc.params, .cells = try render(a, args, false, .encoded) };
    const hints = try a.alloc(ast.Hint, svc.hints.len);
    for (hints, svc.hints) |*o, h| {
        const body = std.mem.eql(u8, h.key, "body");
        const row = if (body and svc.json_body) as_json else if (body) as_url else as_text;
        o.* = h;
        o.value = switch (h.value) {
            .str => |t| .{ .str = try interpAll(a, t, row) },
            .ident => |t| .{ .ident = try interpAll(a, t, row) },
            else => h.value,
        };
        if (std.mem.eql(u8, h.key, "header") and o.value == .str and std.mem.indexOfAny(u8, o.value.str, "\r\n") != null) {
            if (svc.errctx) |c| c.set("{s}: a row's value puts a line break in a header", .{svc.name});
            return error.HeaderLineBreak;
        }
    }
    const opts = try http_client.optsFromHints(a, hints);
    const path = try http_client.withQuery(a, try interpAll(a, svc.path, as_url), try queryParams(a, hints));
    const src = http_client.HttpSource.openConn(a, svc.gpa, svc.cc, path, opts) catch |e| return failed(svc, e, rows);
    defer src.close();
    var pages = std.array_list.Managed(Batch).init(a);
    while (src.next(a) catch |e| return failed(svc, e, rows)) |b| try pages.append(b);
    return itemsOf(a, svc, pages.items);
}

fn failed(svc: *Service, e: anyerror, rows: usize) anyerror {
    if (svc.errctx) |c| {
        const why = eval.takeFailure(e) orelse @errorName(e);
        c.set("{s}: a request for {d} row{s} failed: {s}", .{ svc.name, rows, if (rows == 1) "" else "s", why });
    }
    return e;
}

/// The reply's items as the body's SELECT makes them, each column its CAST over a
/// page, a field the page lacks read as NULL.
fn itemsOf(a: std.mem.Allocator, svc: *const Service, pages: []const Batch) !Batch {
    var n: usize = 0;
    for (pages) |p| n += p.len;
    const cols = try a.alloc(column.Column, svc.items.fields.len);
    for (svc.items.fields, svc.exprs, cols) |f, e, *c| {
        var b = column.Builder.init(a, f.ty);
        for (pages) |p| {
            const col = try evalColumn(a, e, try withMissing(a, p, e), f.ty);
            for (0..p.len) |r| try b.append(col.getValue(r));
        }
        c.* = try b.finish();
    }
    return .{ .schema = svc.items, .columns = cols, .len = n };
}

/// `p` with an all-NULL column for each field `e` reads that the page has not.
fn withMissing(a: std.mem.Allocator, p: Batch, e: *const ast.Expr) !Batch {
    var quals = std.array_list.Managed(ast.QualName).init(a);
    try ast.fieldsOf(e, &quals);
    var fields = std.array_list.Managed(types.Schema.Field).init(a);
    var cols = std.array_list.Managed(column.Column).init(a);
    try fields.appendSlice(p.schema.fields);
    try cols.appendSlice(p.columns);
    for (quals.items) |q| {
        const name = q.parts[0];
        if (q.dollar or p.schema.indexOf(name) != null) continue;
        for (fields.items) |f| {
            if (std.mem.eql(u8, f.name, name)) break;
        } else {
            const ty = types.Type{ .kind = .string, .nullable = true };
            try fields.append(.{ .name = name, .ty = ty });
            try cols.append(try nullColumn(a, ty, p.len));
        }
    }
    if (cols.items.len == p.columns.len) return p;
    const schema = try a.create(types.Schema);
    schema.* = .{ .fields = fields.items };
    return .{ .schema = schema, .columns = cols.items, .len = p.len };
}

//! Key pushdown: a join hands one side's key values to the other side's SQL read,
//! which then asks its database for only the rows that can match.
//!
//! Which side gives: a SQL right side takes the left side's keys (the left is read
//! ahead into memory, up to a cap, and replayed), else a SQL left side takes the
//! right side's keys once it is indexed. Dropping rows of the side that takes is
//! sound only where the join never emits its unmatched rows: the right side for
//! inner, left, semi and anti joins, the left side for inner, right and semi ones.
//! A null-aware anti join (`NOT IN`) is left alone, as a NULL it would filter out
//! changes its answer. `WITH (key_pushdown = false)` on the join turns it off.
//!
//! The taking side's SQL read is a `LateSql`: its schema comes from a `WHERE 1 = 0`
//! probe while planning, and the real query goes out on its first pull, with the
//! keys AND-ed into its `WHERE`. A join key is traced back through the side's
//! filters and selects to an expression over the read's own columns (`traceKey`);
//! a side with anything else between its read and the join, or a key that is no
//! function of the read's columns, takes nothing.
//!
//! The predicate may only ever keep more rows than match, never fewer, so a
//! value the dialect cannot spell drops that key's predicate rather than the
//! value: up to `op.Join.push_cap` distinct values go as `IN (…)`; past it a
//! number, decimal, date or timestamp key goes as its min/max range and a text
//! key as nothing, since a collation orders text otherwise than basalt. Text
//! compared under a case- or trailing-space-insensitive collation keeps more rows
//! than match, which the join then discards.
//!
//! A local Parquet file or folder takes keys the same way and on the same terms
//! (`ParquetKeys`), but nothing is re-read: its row groups whose min/max statistics
//! hold no key value (or, past the cap, no part of the keys' range) are skipped.
//! Only a key that is one of the file's columns as is counts; a computed one, a
//! text one (`groupMayHoldKeys`), or a group without statistics of the key's own
//! kind, keeps every group. A SQL side
//! takes before a Parquet one, so two sides never wait on each other's keys.

const Env = @import("env.zig").Env;
const Value = @import("../exec/value.zig").Value;
const ast = @import("../lang/ast.zig");
const connect = @import("connect.zig");
const driver = @import("../connect/driver.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const folder = @import("../connect/folder.zig");
const ftp = @import("../store/ftp.zig");
const op = @import("../exec/op.zig");
const pqdecode = @import("../format/parquet/read.zig");
const std = @import("std");
const translate = @import("pushdown/translate.zig");
const types = @import("../lang/types.zig");

pub const Dialect = translate.Dialect;

/// Whether `WITH (key_pushdown = false)` turned pushdown off for a join.
pub fn disabled(hints: []const ast.Hint) bool {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "key_pushdown")) continue;
        return switch (h.value) {
            .ident, .str => |s| std.ascii.eqlIgnoreCase(s, "false") or std.ascii.eqlIgnoreCase(s, "off"),
            .int => |i| i == 0,
            .flag => false,
        };
    }
    return false;
}

/// The join kinds whose right side may lose rows that match nothing on the left;
/// never a join with no key, which has none to send.
pub fn rightMayNarrow(j: ast.Join) bool {
    if (j.null_aware or j.keyless()) return false;
    return switch (j.kind) {
        .inner, .left, .semi, .anti => true,
        else => false,
    };
}

/// The join kinds whose left side may lose rows that match nothing on the right;
/// never a join with no key.
pub fn leftMayNarrow(j: ast.Join) bool {
    if (j.null_aware or j.keyless()) return false;
    return switch (j.kind) {
        .inner, .right, .semi => true,
        else => false,
    };
}

/// The dialect of a SQL read whose rows may be narrowed: a table or query of a SQL
/// connection, followed by nothing but filters and selects.
pub fn sqlDialect(env: *Env, stages: []const ast.Stage) ?Dialect {
    if (stages.len == 0 or stages[0].node != .read) return null;
    const rd = stages[0].node.read;
    if (rd.form != .table and rd.form != .query) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = connect.sqlConnInfo(conn) orelse return null;
    for (stages[1..]) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return info.dialect;
}

/// What a read at the head of `stages` takes keys as: its SQL dialect, or a local
/// Parquet file or folder, followed by nothing but filters and selects.
pub const Want = union(enum) { sql: Dialect, parquet };

pub fn takerFor(env: *Env, stages: []const ast.Stage) !?Want {
    if (sqlDialect(env, stages)) |d| return .{ .sql = d };
    if (try parquetSide(env, stages)) return .parquet;
    return null;
}

/// A local Parquet file, or a folder of them, read with nothing but filters and
/// selects after it. A folder that cannot be listed is left to fail on its read.
pub fn parquetSide(env: *Env, stages: []const ast.Stage) !bool {
    if (stages.len == 0 or stages[0].node != .read) return false;
    const rd = stages[0].node.read;
    if (!std.mem.eql(u8, rd.connector, "csv") or rd.form != .path) return false;
    for (stages[1..]) |st| switch (st.node) {
        .filter, .select => {},
        else => return false,
    };
    const path = rd.form.path;
    if (csv.CsvReader.isUrl(path) or ftp.isUrl(path)) return false;
    if (!folder.isFolder(path)) return pqdecode.Reader.isPath(path);
    const fr = (connect.resolveFolder(env, path, stages[0].hints) catch return false) orelse return false;
    return fr.kind == .parquet;
}

/// Column `name` after `stages` (what follows a read: filters and selects) as an
/// expression over the read's own columns, or null when a select drops it or a
/// stage of another kind sits in between.
pub fn traceKey(arena: std.mem.Allocator, stages: []const ast.Stage, name: []const u8) !?*ast.Expr {
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    var cur = try arena.create(ast.Expr);
    cur.* = .{ .field = .{ .parts = parts } };
    var i = stages.len;
    while (i > 0) {
        i -= 1;
        switch (stages[i].node) {
            .filter => {},
            .select => |items| cur = (try backThrough(arena, cur, items)) orelse return null,
            else => return null,
        }
    }
    return cur;
}

const Back = struct { arena: std.mem.Allocator, items: []const ast.SelectItem, ok: *bool };

/// `e` with each column swapped for what the select list made it from.
fn backThrough(arena: std.mem.Allocator, e: *ast.Expr, items: []const ast.SelectItem) !?*ast.Expr {
    var ok = true;
    const out = try backRecur(.{ .arena = arena, .items = items, .ok = &ok }, e);
    return if (ok) out else null;
}

fn backRecur(cx: Back, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field and !e.field.dollar) {
        const name = e.field.parts[e.field.parts.len - 1];
        if (try source(cx.arena, cx.items, name)) |src| return src;
        cx.ok.* = false;
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, backRecur);
}

/// What output column `name` of a select list is made from: a computed item's
/// expression, a listed column, or the same name through `*`.
fn source(arena: std.mem.Allocator, items: []const ast.SelectItem, name: []const u8) !?*ast.Expr {
    for (items) |it| switch (it) {
        .computed => |c| if (std.mem.eql(u8, c.name, name)) return c.expr,
        else => {},
    };
    for (items) |it| switch (it) {
        .field => |q| if (std.mem.eql(u8, q.parts[q.parts.len - 1], name)) return try fieldOf(arena, name),
        else => {},
    };
    for (items) |it| switch (it) {
        .star => return try fieldOf(arena, name),
        .star_except => |names| {
            for (names) |n| if (std.mem.eql(u8, n, name)) return null;
            return try fieldOf(arena, name);
        },
        .star_rename => |rs| {
            for (rs) |r| if (std.mem.eql(u8, r.to, name)) return try fieldOf(arena, r.from);
            for (rs) |r| if (std.mem.eql(u8, r.from, name)) return null;
            return try fieldOf(arena, name);
        },
        else => {},
    };
    return null;
}

fn fieldOf(arena: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    const e = try arena.create(ast.Expr);
    e.* = .{ .field = .{ .parts = parts } };
    return e;
}

/// The `WHERE` that keeps every row of a read whose key `exprs[i]` (null: not
/// traceable) could equal one of `keys[i]`, or null for no narrowing at all.
pub fn render(arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema, exprs: []const ?*ast.Expr, keys: []const op.KeyValues) !?[]const u8 {
    var parts = std.array_list.Managed([]const u8).init(arena);
    for (exprs, keys) |maybe, kv| {
        const e = maybe orelse continue;
        if (kv.values.len == 0 and !kv.overflow) return "1 = 0";
        const opts = translate.Opts{ .schema = schema, .check_fields = true, .need = .superset };
        const key_sql = (try translate.translatePred(arena, e, dialect, opts)) orelse continue;
        if (!kv.overflow) {
            var list = std.array_list.Managed(u8).init(arena);
            var ok = true;
            for (kv.values, 0..) |v, i| {
                const lit = (try literal(arena, dialect, v, false)) orelse {
                    ok = false;
                    break;
                };
                if (i > 0) try list.appendSlice(", ");
                try list.appendSlice(lit);
            }
            if (ok) {
                try parts.append(try std.fmt.allocPrint(arena, "{s} IN ({s})", .{ key_sql, list.items }));
                continue;
            }
        }
        const lo = (try literal(arena, dialect, kv.min orelse continue, true)) orelse continue;
        const hi = (try literal(arena, dialect, kv.max orelse continue, true)) orelse continue;
        try parts.append(try std.fmt.allocPrint(arena, "{s} >= {s} AND {s} <= {s}", .{ key_sql, lo, key_sql, hi }));
    }
    if (parts.items.len == 0) return null;
    return try std.mem.join(arena, " AND ", parts.items);
}

/// Per key, the column of the read it is unchanged, or null for a computed key.
pub fn plainColumns(arena: std.mem.Allocator, exprs: []const ?*ast.Expr) ![]const ?[]const u8 {
    const out = try arena.alloc(?[]const u8, exprs.len);
    for (exprs, out) |maybe, *c| {
        c.* = null;
        const e = maybe orelse continue;
        if (e.* == .field and !e.field.dollar and e.field.parts.len == 1) c.* = e.field.parts[0];
    }
    return out;
}

/// The row-group tests for a Parquet read whose key `cols[i]` (null: computed) must
/// equal one of `keys[i]`: the values while they are all listed, else their range.
/// A key the other side holds no value of rules out every group.
pub fn keyBounds(arena: std.mem.Allocator, cols: []const ?[]const u8, keys: []const op.KeyValues) ![]const pqdecode.KeyBound {
    var out = std.array_list.Managed(pqdecode.KeyBound).init(arena);
    for (cols, keys) |maybe, kv| {
        if (kv.values.len == 0 and !kv.overflow) {
            const none = try arena.alloc(pqdecode.KeyBound, 1);
            none[0] = .{ .column = "", .none = true };
            return none;
        }
        const c = maybe orelse continue;
        try out.append(.{ .column = c, .values = if (kv.overflow) &.{} else kv.values, .min = kv.min, .max = kv.max });
    }
    return out.toOwnedSlice();
}

/// A Parquet read handed a join's keys before its first row group: the reader (or
/// the folder, for every file it opens) then skips the groups `keyBounds` rules out.
pub const ParquetKeys = struct {
    arena: std.mem.Allocator,
    reader: ?*pqdecode.Reader = null,
    folder: ?*pqdecode.Folder = null,
    cols: []const ?[]const u8 = &.{},

    pub fn push(self: *ParquetKeys) op.KeyPush {
        return .{ .ctx = self, .apply = apply };
    }

    fn apply(ctx: *anyopaque, keys: []const op.KeyValues) anyerror!void {
        const self: *ParquetKeys = @ptrCast(@alignCast(ctx));
        if (self.cols.len != keys.len) return;
        const kb = try keyBounds(self.arena, self.cols, keys);
        if (self.reader) |r| r.keys = kb;
        if (self.folder) |f| f.keys = kb;
    }
};

/// The read a join hands its keys to, SQL or Parquet.
pub const Taker = union(enum) {
    sql: *LateSql,
    parquet: *ParquetKeys,

    /// Gives the taker each key's expression over its read's columns, and returns
    /// where the join sends the keys, or null when no key reaches the read.
    pub fn bind(self: Taker, exprs: []const ?*ast.Expr) !?op.KeyPush {
        switch (self) {
            .sql => |l| {
                l.exprs = exprs;
                return l.push();
            },
            .parquet => |p| {
                p.cols = try plainColumns(p.arena, exprs);
                for (p.cols) |c| if (c != null) return p.push();
                return null;
            },
        }
    }
};

/// `v` as a SQL literal, or null when it cannot be spelled exactly. Floats only
/// bound a range (`ranged`), as their text may not round-trip to the column's
/// precision; text never bounds one, as a collation may order it otherwise.
fn literal(arena: std.mem.Allocator, dialect: Dialect, v: Value, ranged: bool) !?[]const u8 {
    const opts = translate.Opts{ .need = .superset };
    return switch (v) {
        .int => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
        .decimal => try eval.valueToString(arena, v),
        .float => |f| if (ranged and std.math.isFinite(f)) try std.fmt.allocPrint(arena, "{e}", .{f}) else null,
        .string => |s| if (ranged) null else blk: {
            const e = try arena.create(ast.Expr);
            e.* = .{ .str_lit = s };
            break :blk try translate.translatePred(arena, e, dialect, opts);
        },
        .date, .timestamp => blk: {
            const text = try arena.create(ast.Expr);
            text.* = .{ .str_lit = try eval.valueToString(arena, v) };
            const e = try arena.create(ast.Expr);
            e.* = .{ .cast = .{ .e = text, .ty = types.Type.init(if (v == .date) .date else .timestamp) } };
            break :blk try translate.translatePred(arena, e, dialect, opts);
        },
        else => null,
    };
}

/// A SQL read opened twice: once with `WHERE 1 = 0` while planning, for its schema,
/// and for real on its first pull, with whatever `push` was handed AND-ed in. The
/// probe's query is not left as the read's description, and a failure of the late
/// open is reported in the read's own words rather than the join's label for it.
pub const LateSql = struct {
    env: *Env,
    rd: ast.Read,
    hints: []const ast.Hint,
    schema_: types.Schema,
    dialect: Dialect,
    /// Per join key, its expression over the read's columns; set by the join.
    exprs: []const ?*ast.Expr = &.{},
    extra: ?[]const u8 = null,
    inner: ?driver.Source = null,

    pub fn open(env: *Env, rd: ast.Read, hints: []const ast.Hint, dialect: Dialect) !*LateSql {
        var probe = rd;
        probe.where = "1 = 0";
        const src = try connect.openSourceProjected(env, probe, hints, null, &.{});
        const sch = try connect.dupeSchema(env.arena, src.schema());
        src.close();
        env.sql_desc = try connect.sqlDescForStage(env, .{ .node = .{ .read = rd }, .hints = hints, .pos = .{ .line = 0, .col = 0 } });
        const self = try env.arena.create(LateSql);
        self.* = .{ .env = env, .rd = rd, .hints = hints, .schema_ = sch, .dialect = dialect };
        return self;
    }

    pub fn source(self: *LateSql) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn push(self: *LateSql) op.KeyPush {
        return .{ .ctx = self, .apply = apply };
    }

    fn apply(ctx: *anyopaque, keys: []const op.KeyValues) anyerror!void {
        const self: *LateSql = @ptrCast(@alignCast(ctx));
        if (self.exprs.len != keys.len) return;
        self.extra = try render(self.env.arena, self.dialect, self.schema_, self.exprs, keys);
        if (self.extra) |x| self.env.log.log(.debug, "key pushdown into {s}: WHERE {s}", .{ self.rd.connector, clip(x) });
    }

    fn clip(s: []const u8) []const u8 {
        return if (s.len > 400) s[0..400] else s;
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *LateSql = @ptrCast(@alignCast(ptr));
        return self.schema_;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?@import("../exec/batch.zig").Batch {
        const self: *LateSql = @ptrCast(@alignCast(ptr));
        if (self.inner == null) {
            var rd = self.rd;
            if (self.extra) |x| rd.where = if (rd.where.len > 0)
                try std.fmt.allocPrint(self.env.arena, "({s}) AND ({s})", .{ rd.where, x })
            else
                x;
            self.inner = connect.openSourceProjected(self.env, rd, self.hints, null, &.{}) catch |e| {
                if (self.env.diag.msg.len > 0) self.env.errctx.set("{s}", .{self.env.diag.msg});
                return e;
            };
        }
        return self.inner.?.next(arena);
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *LateSql = @ptrCast(@alignCast(ptr));
        if (self.inner) |s| s.close();
        self.inner = null;
    }

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

test "traceKey: through filters, selects, renames and computed keys to the read's columns" {
    const testing = std.testing;
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../lang/sql_parser.zig");
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\WITH b AS (SELECT code AS cc, name FROM 'x.csv' WHERE name <> '')
        \\SELECT * FROM 'y.csv' JOIN b ON trim(b.cc) = y.k;
    , &pd);
    const bst = prog.stmts[1].binding.pipeline.stages[1..];
    const e = (try traceKey(a, bst, "cc")).?;
    try testing.expectEqualStrings("code", e.field.parts[0]);
    try testing.expect((try traceKey(a, bst, "nope")) == null);

    const pos = ast.Pos{ .line = 1, .col = 1 };
    const cc = try a.create(ast.Expr);
    cc.* = .{ .field = .{ .parts = &.{"cc"} } };
    const trimmed = try a.create(ast.Expr);
    trimmed.* = .{ .call = .{ .name = "trim", .args = &.{cc} } };
    const with_key = [_]ast.Stage{
        bst[0],                                                                                                                     bst[1],
        .{ .node = .{ .select = &.{ .star, .{ .computed = .{ .name = "__jk1", .expr = trimmed } } } }, .hints = &.{}, .pos = pos },
    };
    const k = (try traceKey(a, &with_key, "__jk1")).?;
    try testing.expectEqualStrings("trim", k.call.name);
    try testing.expectEqualStrings("code", k.call.args[0].field.parts[0]);

    const limited = [_]ast.Stage{ .{ .node = .{ .limit = .{ .count = 5 } }, .hints = &.{}, .pos = pos }, bst[1] };
    try testing.expect((try traceKey(a, &limited, "code")) == null);
}

test "render: IN lists per dialect, a range past the cap, nothing for what cannot be spelled" {
    const testing = std.testing;
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sch = types.Schema{ .fields = &.{
        .{ .name = "code", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };
    const code = try a.create(ast.Expr);
    code.* = .{ .field = .{ .parts = &.{"code"} } };
    const n = try a.create(ast.Expr);
    n.* = .{ .field = .{ .parts = &.{"n"} } };

    const strs = [_]Value{ .{ .string = "A1" }, .{ .string = "it's" } };
    const ints = [_]Value{ .{ .int = 3 }, .{ .int = 9 } };
    const both = try render(a, .postgres, sch, &.{ code, n }, &.{ .{ .values = &strs }, .{ .values = &ints } });
    try testing.expectEqualStrings("\"code\" IN ('A1', 'it''s') AND \"n\" IN (3, 9)", both.?);

    const over = try render(a, .mysql, sch, &.{ code, n }, &.{
        .{ .values = &strs, .overflow = true, .min = strs[0], .max = strs[1] },
        .{ .values = &ints, .overflow = true, .min = ints[0], .max = ints[1] },
    });
    try testing.expectEqualStrings("`n` >= 3 AND `n` <= 9", over.?);

    try testing.expectEqualStrings("1 = 0", (try render(a, .postgres, sch, &.{n}, &.{.{ .values = &.{} }})).?);
    try testing.expect((try render(a, .postgres, sch, &.{null}, &.{.{ .values = &ints }})) == null);
    const accented = [_]Value{.{ .string = "ação" }};
    try testing.expect((try render(a, .sqlserver, sch, &.{code}, &.{.{ .values = &accented }})) == null);
}

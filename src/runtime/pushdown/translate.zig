//! Expressions into a source's SQL, per dialect: what may be sent and in what form,
//! with the facts (collation, ASCII-only text) a text comparison depends on.

pub const Dialect = @import("../../db/sql.zig").Dialect;
const ast = @import("../../lang/ast.zig");
const inSchema = @import("../pushdown.zig").inSchema;
const split = @import("../../connect/split.zig");
const sqlStr = @import("../pushdown.zig").sqlStr;
const std = @import("std");
const types = @import("../../lang/types.zig");
pub const bin = @import("../pushdown.zig").bin;
const builtins = @import("../../exec/builtins.zig");
const callExpr = @import("testing_util.zig").callExpr;
pub const fld = @import("../pushdown.zig").fld;
const intLit = @import("testing_util.zig").intLit;
const strLit = @import("testing_util.zig").strLit;
const testSchema = @import("testing_util.zig").testSchema;
const testing = std.testing;

pub const Need = enum {
    superset,
    subset,
    exact,

    fn flip(n: Need) Need {
        return switch (n) {
            .superset => .subset,
            .subset => .superset,
            .exact => .exact,
        };
    }
};

pub const ColFacts = struct {
    text: bool,
    byte_order: bool = false,
    pads: bool = true,
    wide: bool = false,
};

pub const Facts = std.StringHashMap(ColFacts);

pub const Opts = struct {
    schema: types.Schema = .{ .fields = &.{} },
    check_fields: bool = false,
    facts: ?*const Facts = null,
    need: Need = .superset,
    wants_facts: ?*bool = null,
};

/// A predicate as SQL for `dialect` that keeps the rows `opts.need` asks for, or
/// null when no rendering is sure to.
pub fn translatePred(arena: std.mem.Allocator, e: *const ast.Expr, dialect: Dialect, opts: Opts) error{OutOfMemory}!?[]const u8 {
    const tx = Tx{ .arena = arena, .dialect = dialect, .o = opts };
    return tx.pred(e, opts.need);
}

/// Whether knowing the columns' collations could change how any filter in
/// `stages` descends at `need`, asked before paying a catalog round trip.
pub fn wantsFacts(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage, schema: types.Schema, check_fields: bool, need: Need) !bool {
    var w = false;
    for (stages) |st| {
        if (st.node != .filter) continue;
        _ = try translatePred(arena, st.node.filter, dialect, .{ .schema = schema, .check_fields = check_fields, .need = need, .wants_facts = &w });
        if (w) return true;
    }
    return false;
}

/// `translatePred` at `need = superset` with no catalog facts, as EXPLAIN and the
/// tests use.
pub fn translateExpr(arena: std.mem.Allocator, e: *const ast.Expr, dialect: Dialect, schema: types.Schema, check_fields: bool) error{OutOfMemory}!?[]const u8 {
    return translatePred(arena, e, dialect, .{ .schema = schema, .check_fields = check_fields });
}

fn numericKind(k: types.TypeKind) bool {
    return k == .int or k == .float or k == .decimal;
}

/// Whether the operand is certainly a number already: a literal, or a column the
/// schema at hand says is numeric. A nested numeric cast is checked on its own.
fn provablyNumeric(e: *const ast.Expr, schema: types.Schema) bool {
    return switch (e.*) {
        .int_lit, .float_lit => true,
        .str_lit => |s| plainNumber(s),
        .field => |q| blk: {
            if (schema.fields.len == 0) break :blk false;
            const idx = schema.indexOf(q.last()) orelse break :blk false;
            break :blk numericKind(schema.fields[idx].ty.kind);
        },
        else => false,
    };
}

fn plainNumber(s: []const u8) bool {
    var i: usize = 0;
    if (i < s.len and s[i] == '-') i += 1;
    var digits: usize = 0;
    var dot = false;
    while (i < s.len) : (i += 1) switch (s[i]) {
        '0'...'9' => digits += 1,
        '.' => {
            if (dot or digits == 0 or i + 1 >= s.len) return false;
            dot = true;
        },
        else => return false,
    };
    return digits > 0;
}

fn asciiPrintable(s: []const u8) bool {
    for (s) |c| if (c < 0x20 or c > 0x7e) return false;
    return true;
}

const text_fns = [_][]const u8{ "lower", "upper", "trim", "substr", "replace", "concat", "coalesce", "left", "right", "repeat", "reverse" };

fn isTextFn(name: []const u8) bool {
    for (text_fns) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

fn isLikeFn(name: []const u8) bool {
    return std.mem.eql(u8, name, "like") or std.mem.eql(u8, name, "starts_with") or
        std.mem.eql(u8, name, "ends_with") or std.mem.eql(u8, name, "contains");
}

const Tx = struct {
    arena: std.mem.Allocator,
    dialect: Dialect,
    o: Opts,

    const Err = error{OutOfMemory};

    const Kind = union(enum) {
        other,
        unknown,
        text: struct { facts: ?ColFacts, fn_: bool = false },
    };

    fn fmt(self: Tx, comptime f: []const u8, args: anytype) Err![]const u8 {
        return std.fmt.allocPrint(self.arena, f, args);
    }

    fn want(self: Tx) void {
        if (self.o.wants_facts) |w| w.* = true;
    }

    fn factsOf(self: Tx, name: []const u8) ?ColFacts {
        const f = self.o.facts orelse return null;
        if (f.get(name)) |c| return c;
        var it = f.iterator();
        while (it.next()) |kv| if (std.ascii.eqlIgnoreCase(kv.key_ptr.*, name)) return kv.value_ptr.*;
        return null;
    }

    fn kind(self: Tx, e: *const ast.Expr) Kind {
        return switch (e.*) {
            .str_lit => .{ .text = .{ .facts = null } },
            .int_lit, .float_lit, .bool_lit, .null_lit => .other,
            .field => |q| blk: {
                if (q.parts.len != 1) break :blk .unknown;
                if (self.factsOf(q.parts[0])) |f| break :blk if (f.text) Kind{ .text = .{ .facts = f } } else .other;
                if (self.o.schema.indexOf(q.parts[0])) |i| {
                    const k = self.o.schema.fields[i].ty.kind;
                    break :blk if (k == .string) Kind{ .text = .{ .facts = null } } else .other;
                }
                break :blk .unknown;
            },
            .cast => |c| if (c.ty.kind == .string) Kind{ .text = .{ .facts = null } } else .other,
            .call => |c| blk: {
                if (!isTextFn(c.name)) break :blk .other;
                break :blk .{ .text = .{ .facts = self.textFnFacts(c), .fn_ = true } };
            },
            else => .unknown,
        };
    }

    fn textFnFacts(self: Tx, c: ast.Expr.Call) ?ColFacts {
        for (c.args) |a| if (a.* == .field and a.field.parts.len == 1) return self.factsOf(a.field.parts[0]);
        return null;
    }

    fn pred(self: Tx, e: *const ast.Expr, need: Need) Err!?[]const u8 {
        switch (e.*) {
            .binary => |b| switch (b.op) {
                .@"and", .@"or" => {
                    const l = try self.pred(b.l, need);
                    const r = try self.pred(b.r, need);
                    if (b.op == .@"and" and need == .superset) {
                        if (l == null) return r;
                        if (r == null) return l;
                    }
                    return try self.fmt("({s} {s} {s})", .{ l orelse return null, if (b.op == .@"and") "AND" else "OR", r orelse return null });
                },
                .eq, .ne, .lt, .le, .gt, .ge => return self.compare(b.op, b.l, b.r, need),
                else => return null,
            },
            .unary => |u| {
                if (u.op != .not) return null;
                const inner = (try self.pred(u.e, need.flip())) orelse return null;
                return try self.fmt("(NOT ({s}))", .{inner});
            },
            .is_null => |n| {
                const v = (try self.value(n.e)) orelse return null;
                if (n.kind == .is_null)
                    return try self.fmt("({s} IS {s}NULL)", .{ v, if (n.negated) "NOT " else "" });
                const blank = (try self.textCompare(.eq, n.e, "", if (n.negated) need.flip() else need)) orelse return null;
                const t = try self.fmt("({s} IS NULL OR {s})", .{ v, blank });
                return if (n.negated) try self.fmt("(NOT {s})", .{t}) else t;
            },
            .call => |c| {
                if (isLikeFn(c.name)) return self.likePred(c, need);
                return self.value(e);
            },
            .cond => |c| {
                const cnd = (try self.pred(c.cond, .exact)) orelse return null;
                const t = (try self.value(c.then)) orelse return null;
                const f = (try self.value(c.els)) orelse return null;
                return try self.fmt("(CASE WHEN {s} THEN {s} ELSE {s} END)", .{ cnd, t, f });
            },
            .match => |m| return self.match(m),
            else => return self.value(e),
        }
    }

    /// TRY_CAST and text-to-number casts of unprovable input stay engine-side.
    fn value(self: Tx, e: *const ast.Expr) Err!?[]const u8 {
        switch (e.*) {
            .bool_lit => |b| return try self.arena.dupe(u8, if (b) "(1=1)" else "(1=0)"),
            .int_lit => |v| return try self.fmt("{d}", .{v}),
            .float_lit => |v| return try self.fmt("{d}", .{v}),
            .null_lit => return try self.arena.dupe(u8, "NULL"),
            .str_lit => |s| {
                if (self.dialect == .sqlserver and !asciiPrintable(s)) return null;
                if (self.dialect.mysqlWire() and std.mem.indexOfScalar(u8, s, '\\') != null) return null;
                return try sqlStr(self.arena, s);
            },
            .field => |q| {
                if (q.parts.len != 1) return null;
                if (self.o.check_fields and !inSchema(self.o.schema, q.parts[0])) return null;
                return try split.quoteIdent(self.arena, self.dialect, q.parts[0]);
            },
            .cast => |c| {
                if (c.safe) return null;
                if (numericKind(c.ty.kind) and !provablyNumeric(c.e, self.o.schema)) return null;
                const inner = (try self.value(c.e)) orelse return null;
                const ty = (try self.dialect.castType(self.arena, c.ty)) orelse return null;
                return try self.fmt("CAST({s} AS {s})", .{ inner, ty });
            },
            .call => |c| return self.call(c),
            .cond => |c| {
                const cnd = (try self.pred(c.cond, .exact)) orelse return null;
                const t = (try self.value(c.then)) orelse return null;
                const f = (try self.value(c.els)) orelse return null;
                return try self.fmt("(CASE WHEN {s} THEN {s} ELSE {s} END)", .{ cnd, t, f });
            },
            .match => |m| return self.match(m),
            .unary, .binary, .is_null => return self.pred(e, .exact),
            else => return null,
        }
    }

    /// `substr` descends only from position 1 (a UTF-8 SQL Server varchar counts bytes),
    /// `replace` only under a byte-order collation (others replace case-insensitively).
    fn call(self: Tx, c: ast.Expr.Call) Err!?[]const u8 {
        if (isLikeFn(c.name)) return self.likePred(c, .exact);
        const p = lookupPushable(c.name) orelse return null;
        if (c.args.len < p.min_args or c.args.len > p.max_args) return null;
        if (std.mem.eql(u8, c.name, "substr")) {
            if (c.args[1].* != .int_lit or c.args[1].int_lit != 1) return null;
        }
        if (std.mem.eql(u8, c.name, "replace")) {
            const f = self.textFnFacts(c) orelse {
                self.want();
                return null;
            };
            if (!f.byte_order) return null;
        }
        const args = try self.arena.alloc([]const u8, c.args.len);
        for (c.args, args) |a, *out| out.* = (try self.value(a)) orelse return null;
        return p.render(self.arena, p, c, args, self.dialect);
    }

    /// `CASE x WHEN p …` compares by equality, which is exact only off text; the guard
    /// form's conditions must hold exactly.
    fn match(self: Tx, m: ast.Match) Err!?[]const u8 {
        var out = std.array_list.Managed(u8).init(self.arena);
        const w = out.writer();
        if (m.subject) |subj| {
            const sk = self.kind(subj);
            if (sk == .text) return null;
            if (sk == .unknown) for (m.arms) |arm| for (arm.pats) |p| switch (p.*) {
                .int_lit, .float_lit => {},
                else => return null,
            };
            const s = (try self.value(subj)) orelse return null;
            try w.print("(CASE {s}", .{s});
        } else {
            try w.writeAll("(CASE");
        }
        for (m.arms) |arm| {
            const v = (try self.value(arm.value)) orelse return null;
            if (arm.is_default) {
                try w.print(" ELSE {s}", .{v});
            } else if (m.subject != null) {
                for (arm.pats) |p| {
                    if (self.kind(p) == .text) return null;
                    const ps = (try self.value(p)) orelse return null;
                    try w.print(" WHEN {s} THEN {s}", .{ ps, v });
                }
            } else {
                const g = (try self.pred(arm.guard orelse return null, .exact)) orelse return null;
                try w.print(" WHEN {s} THEN {s}", .{ g, v });
            }
        }
        try w.writeAll(" END)");
        return try out.toOwnedSlice();
    }

    fn mirror(op: ast.BinOp) ast.BinOp {
        return switch (op) {
            .lt => .gt,
            .le => .ge,
            .gt => .lt,
            .ge => .le,
            else => op,
        };
    }

    fn opSql(op: ast.BinOp) []const u8 {
        return switch (op) {
            .eq => "=",
            .ne => "<>",
            .lt => "<",
            .le => "<=",
            .gt => ">",
            .ge => ">=",
            else => unreachable,
        };
    }

    fn isLit(e: *const ast.Expr) bool {
        return switch (e.*) {
            .str_lit, .int_lit, .float_lit, .bool_lit, .null_lit => true,
            else => false,
        };
    }

    /// Numbers, dates and times compare alike everywhere. On text, equality only widens
    /// under a folding collation, and order is shared only by byte-ordered unpadded columns.
    fn compare(self: Tx, op_in: ast.BinOp, l_in: *const ast.Expr, r_in: *const ast.Expr, need: Need) Err!?[]const u8 {
        var l = l_in;
        var r = r_in;
        var op = op_in;
        if (isLit(l) and !isLit(r)) {
            l = r_in;
            r = l_in;
            op = mirror(op_in);
        }
        const lk = self.kind(l);
        const rk = self.kind(r);
        if (r.* == .str_lit and !isLit(l)) return self.textCompare(op, l, r.str_lit, need);
        const texty = lk == .text or rk == .text;
        const unknown = lk == .unknown or rk == .unknown;
        if (!texty and !(unknown and !isLit(r))) {
            const ls = (try self.value(l)) orelse return null;
            const rs = (try self.value(r)) orelse return null;
            return try self.fmt("({s} {s} {s})", .{ ls, opSql(op), rs });
        }
        const lf: ?ColFacts = if (lk == .text) lk.text.facts else null;
        const rf: ?ColFacts = if (rk == .text) rk.text.facts else null;
        const settled = (op == .eq and need == .superset) or (op == .ne and need == .subset);
        if ((lf == null or rf == null) and !settled) self.want();
        const bytes = if (lf) |a| (if (rf) |b| a.byte_order and b.byte_order and !a.pads and !b.pads and !(lk == .text and lk.text.fn_) and !(rk == .text and rk.text.fn_) else false) else false;
        const ok = bytes or switch (op) {
            .eq => need == .superset,
            .ne => need == .subset,
            else => false,
        };
        if (!ok) return null;
        const ls = (try self.value(l)) orelse return null;
        const rs = (try self.value(r)) orelse return null;
        return try self.fmt("({s} {s} {s})", .{ ls, opSql(op), rs });
    }

    /// `col op 'L'` over text. Under padding the shorter side compares as if
    /// space-filled, so ranges widen to a prefix test.
    fn textCompare(self: Tx, op: ast.BinOp, col: *const ast.Expr, lit: []const u8, need: Need) Err!?[]const u8 {
        const k = self.kind(col);
        if (k == .other) {
            const cs = (try self.value(col)) orelse return null;
            const ls = (try self.value(&.{ .str_lit = lit })) orelse return null;
            return try self.fmt("({s} {s} {s})", .{ cs, opSql(op), ls });
        }
        const known: ?ColFacts = if (k == .text) k.text.facts else null;
        const f = known orelse ColFacts{ .text = true };
        const is_fn = k == .text and k.text.fn_;
        const settled = (op == .eq and need == .superset) or (op == .ne and need == .subset);
        if (known == null and !is_fn and !settled) self.want();
        const cs = (try self.value(col)) orelse return null;
        const ls = (try self.value(&.{ .str_lit = lit })) orelse return null;
        const plain = try self.fmt("({s} {s} {s})", .{ cs, opSql(op), ls });
        const ascii = asciiPrintable(lit);

        if (is_fn) {
            if (!ascii) return null;
            return switch (op) {
                .eq => if (need == .superset) plain else null,
                .ne => if (need == .subset) plain else null,
                else => null,
            };
        }
        if (f.byte_order and !f.pads and ascii) return plain;
        switch (op) {
            .eq => {
                if (need == .superset) return plain;
                return self.padExact(.eq, cs, ls, lit, f);
            },
            .ne => {
                if (need == .subset) return plain;
                return self.padExact(.ne, cs, ls, lit, f);
            },
            .lt, .le, .gt, .ge => {
                if (!f.byte_order or !ascii) return null;
                if (!f.pads) return plain;
                if (need == .superset) {
                    if (op == .lt or op == .le) return try self.fmt("({s} <= {s})", .{ cs, ls });
                    const pre = (try self.prefixLike(lit)) orelse return null;
                    return try self.fmt("({s} >= {s} OR {s} LIKE {s})", .{ cs, ls, cs, pre });
                }
                return self.padExact(op, cs, ls, lit, f);
            },
            else => return null,
        }
    }

    /// Exact text comparison under a binary, padding collation, via byte length;
    /// printable ASCII literals with no trailing space only.
    fn padExact(self: Tx, op: ast.BinOp, cs: []const u8, ls: []const u8, lit: []const u8, f: ColFacts) Err!?[]const u8 {
        if (!f.byte_order or f.wide or !asciiPrintable(lit)) return null;
        if (lit.len > 0 and lit[lit.len - 1] == ' ') return null;
        const len_fn: []const u8 = switch (self.dialect) {
            .sqlserver => "DATALENGTH",
            .mysql, .starrocks, .doris => "LENGTH",
            .postgres => return null,
        };
        const n = lit.len;
        const eq = try self.fmt("({s} = {s} AND {s}({s}) = {d})", .{ cs, ls, len_fn, cs, n });
        if (op == .eq) return eq;
        if (op == .ne) return try self.fmt("(NOT {s})", .{eq});
        const pre = (try self.prefixLike(lit)) orelse return null;
        const ge = try self.fmt("({s} >= {s} OR {s} LIKE {s})", .{ cs, ls, cs, pre });
        const gt = try self.fmt("({s} > {s} OR ({s} LIKE {s} AND {s}({s}) > {d}))", .{ cs, ls, cs, pre, len_fn, cs, n });
        return switch (op) {
            .ge => ge,
            .gt => gt,
            .lt => try self.fmt("(NOT {s})", .{ge}),
            .le => try self.fmt("(NOT {s})", .{gt}),
            else => null,
        };
    }

    fn prefixLike(self: Tx, lit: []const u8) Err!?[]const u8 {
        var out = std.array_list.Managed(u8).init(self.arena);
        for (lit) |c| switch (self.dialect) {
            .sqlserver => switch (c) {
                '%', '_', '[' => try out.writer().print("[{c}]", .{c}),
                else => try out.append(c),
            },
            else => switch (c) {
                '%', '_', '\\' => return null,
                else => try out.append(c),
            },
        };
        try out.append('%');
        return try sqlStr(self.arena, out.items);
    }

    /// `like`/`starts_with`/`ends_with`/`contains` on a literal pattern. The source's `_`
    /// is a character, the engine's a byte; SQL Server reads `[` as a class, mysql `\` as an escape.
    fn likePred(self: Tx, c: ast.Expr.Call, need: Need) Err!?[]const u8 {
        if (c.args.len != 2 or c.args[1].* != .str_lit) return null;
        const raw = c.args[1].str_lit;
        if (!asciiPrintable(raw) and self.dialect == .sqlserver) return null;
        for (raw) |ch| {
            if (ch == '_' or ch == '\\') return null;
            if (!std.mem.eql(u8, c.name, "like") and ch == '%') return null;
        }
        var body = std.array_list.Managed(u8).init(self.arena);
        for (raw) |ch| {
            if (self.dialect == .sqlserver and ch == '[') try body.appendSlice("[[]") else try body.append(ch);
        }
        const pat = if (std.mem.eql(u8, c.name, "starts_with"))
            try self.fmt("{s}%", .{body.items})
        else if (std.mem.eql(u8, c.name, "ends_with"))
            try self.fmt("%{s}", .{body.items})
        else if (std.mem.eql(u8, c.name, "contains"))
            try self.fmt("%{s}%", .{body.items})
        else
            body.items;
        const k = self.kind(c.args[0]);
        if (k != .text and k != .unknown) return null;
        const known: ?ColFacts = if (k == .text) k.text.facts else null;
        if (need != .superset) {
            const f = known orelse {
                self.want();
                return null;
            };
            if (!f.byte_order or f.pads or (k == .text and k.text.fn_)) return null;
        }
        const target = (try self.value(c.args[0])) orelse return null;
        return try self.fmt("({s} LIKE {s})", .{ target, try sqlStr(self.arena, pat) });
    }
};

const Pushable = struct {
    name: []const u8,
    sql: []const u8 = "",
    min_args: usize,
    max_args: usize,
    render: *const fn (std.mem.Allocator, *const Pushable, ast.Expr.Call, []const []const u8, Dialect) error{OutOfMemory}!?[]const u8,
};

const variadic = std.math.maxInt(usize);

pub const pushable = [_]Pushable{
    .{ .name = "lower", .sql = "LOWER", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "upper", .sql = "UPPER", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "trim", .min_args = 1, .max_args = 1, .render = render.trim },
    .{ .name = "substr", .sql = "SUBSTRING", .min_args = 3, .max_args = 3, .render = render.plain },
    .{ .name = "replace", .sql = "REPLACE", .min_args = 3, .max_args = 3, .render = render.plain },
    .{ .name = "concat", .sql = "CONCAT", .min_args = 2, .max_args = variadic, .render = render.plain },
    .{ .name = "coalesce", .sql = "COALESCE", .min_args = 2, .max_args = variadic, .render = render.plain },
    .{ .name = "like", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "starts_with", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "ends_with", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "contains", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "abs", .sql = "ABS", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "floor", .sql = "FLOOR", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "sqrt", .sql = "SQRT", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "sign", .sql = "SIGN", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "reverse", .sql = "REVERSE", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "power", .sql = "POWER", .min_args = 2, .max_args = 2, .render = render.plain },
    .{ .name = "nullif", .sql = "NULLIF", .min_args = 2, .max_args = 2, .render = render.plain },
    .{ .name = "ceil", .min_args = 1, .max_args = 1, .render = render.ceil },
    .{ .name = "mod", .min_args = 2, .max_args = 2, .render = render.mod },
    .{ .name = "left", .min_args = 2, .max_args = 2, .render = render.counted },
    .{ .name = "right", .min_args = 2, .max_args = 2, .render = render.counted },
    .{ .name = "repeat", .min_args = 2, .max_args = 2, .render = render.counted },
};

fn lookupPushable(name: []const u8) ?*const Pushable {
    const map = comptime blk: {
        var kvs: [pushable.len]struct { []const u8, usize } = undefined;
        for (pushable, 0..) |p, i| kvs[i] = .{ p.name, i };
        break :blk std.StaticStringMap(usize).initComptime(kvs);
    };
    const i = map.get(name) orelse return null;
    return &pushable[i];
}

const render = struct {
    fn none(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = .{ arena, p, c, args, dialect };
        return null;
    }

    fn plain(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = c;
        _ = dialect;
        const joined = try std.mem.join(arena, ", ", args);
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ p.sql, joined });
    }

    fn trim(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = p;
        _ = c;
        return switch (dialect) {
            .sqlserver => try std.fmt.allocPrint(arena, "LTRIM(RTRIM({s}))", .{args[0]}),
            else => try std.fmt.allocPrint(arena, "TRIM({s})", .{args[0]}),
        };
    }

    fn ceil(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = p;
        _ = c;
        const f = switch (dialect) {
            .sqlserver => "CEILING",
            else => "CEIL",
        };
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ f, args[0] });
    }

    /// SQL Server has only `%`; both spellings take the dividend's sign everywhere,
    /// matching the engine.
    fn mod(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = p;
        _ = c;
        return switch (dialect) {
            .sqlserver => try std.fmt.allocPrint(arena, "({s} % {s})", .{ args[0], args[1] }),
            else => try std.fmt.allocPrint(arena, "MOD({s}, {s})", .{ args[0], args[1] }),
        };
    }

    /// Only a literal count >= 0 is pushed: negative counts diverge across dialects.
    fn counted(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        if (c.args[1].* != .int_lit or c.args[1].int_lit < 0) return null;
        const f: []const u8 = if (std.mem.eql(u8, p.name, "left"))
            "LEFT"
        else if (std.mem.eql(u8, p.name, "right"))
            "RIGHT"
        else if (dialect == .sqlserver)
            "REPLICATE"
        else
            "REPEAT";
        return try std.fmt.allocPrint(arena, "{s}({s}, {s})", .{ f, args[0], args[1] });
    }
};

test "translateExpr: equality with a string literal escapes quotes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = try a.create(ast.Expr);
    lit.* = .{ .str_lit = "O'Brien" };
    const e = try bin(a, .eq, try fld(a, "b"), lit);
    const sql = (try translateExpr(a, e, .mysql, testSchema(), true)).?;
    try testing.expectEqualStrings("(`b` = 'O''Brien')", sql);
}

test "translateExpr: AND of comparisons, per-dialect quoting" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit5 = try a.create(ast.Expr);
    lit5.* = .{ .int_lit = 5 };
    const lit9 = try a.create(ast.Expr);
    lit9.* = .{ .int_lit = 9 };
    const e = try bin(a, .@"and", try bin(a, .ge, try fld(a, "a"), lit5), try bin(a, .lt, try fld(a, "c"), lit9));
    try testing.expectEqualStrings("((\"a\" >= 5) AND (\"c\" < 9))", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("(([a] >= 5) AND ([c] < 9))", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);
}

test "translateExpr: is not null" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try a.create(ast.Expr);
    e.* = .{ .is_null = .{ .e = try fld(a, "a"), .negated = true } };
    try testing.expectEqualStrings("(`a` IS NOT NULL)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
}

test "translateExpr: a field the schema does not have is not pushed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = try a.create(ast.Expr);
    lit.* = .{ .int_lit = 1 };
    try testing.expect((try translateExpr(a, try bin(a, .eq, try fld(a, "zzz"), lit), .mysql, testSchema(), true)) == null);
}

test "translateExpr: bitwise operators are never pushed down" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const four = try a.create(ast.Expr);
    four.* = .{ .int_lit = 4 };

    const masked = try bin(a, .bit_and, try fld(a, "a"), four);
    const pred = try bin(a, .eq, masked, four);
    for ([_]Dialect{ .mysql, .postgres, .sqlserver }) |d| {
        try testing.expect((try translateExpr(a, pred, d, testSchema(), true)) == null);
        try testing.expect((try translateExpr(a, masked, d, testSchema(), true)) == null);
    }
    for ([_]ast.BinOp{ .bit_or, .bit_xor, .shl, .shr }) |op| {
        try testing.expect((try translateExpr(a, try bin(a, op, try fld(a, "a"), four), .mysql, testSchema(), true)) == null);
    }
    const notx = try a.create(ast.Expr);
    notx.* = .{ .unary = .{ .op = .bit_not, .e = try fld(a, "a") } };
    try testing.expect((try translateExpr(a, notx, .mysql, testSchema(), true)) == null);
}

test "translateExpr: NOT over an OR with a bool literal" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit1 = try a.create(ast.Expr);
    lit1.* = .{ .int_lit = 1 };
    const f = try a.create(ast.Expr);
    f.* = .{ .bool_lit = false };
    const or_e = try bin(a, .@"or", try bin(a, .gt, try fld(a, "a"), lit1), f);
    const not_e = try a.create(ast.Expr);
    not_e.* = .{ .unary = .{ .op = .not, .e = or_e } };
    try testing.expectEqualStrings("(NOT (((`a` > 1) OR (1=0))))", (try translateExpr(a, not_e, .mysql, testSchema(), true)).?);
}

test "translateExpr: extended constructs (is empty, CASE, CAST, functions)" {
    var arn = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arn.deinit();
    const a = arn.allocator();
    const sqlp = @import("../../lang/sql_parser.zig");

    const cases = [_]struct { src: []const u8, want: ?[]const u8, d: Dialect = .sqlserver }{
        .{ .src = "status IS EMPTY", .want = "([status] IS NULL OR ([status] = ''))" },
        .{ .src = "status IS EMPTY", .want = "(`status` IS NULL OR (`status` = ''))", .d = .mysql },
        .{ .src = "status IS NULL", .want = "(`status` IS NULL)", .d = .mysql },
        .{ .src = "status IS NOT EMPTY", .want = null },
        .{ .src = "IF(v > 1, 'a', 'b')", .want = "(CASE WHEN ([v] > 1) THEN 'a' ELSE 'b' END)" },
        .{ .src = "CASE status WHEN 'x', 'y' THEN 1 ELSE 0 END", .want = null },
        .{ .src = "CASE v WHEN 1 THEN 1 ELSE 0 END > 0", .want = "((CASE [v] WHEN 1 THEN 1 ELSE 0 END) > 0)" },
        .{ .src = "CAST(v AS INT) > 5", .want = null },
        .{ .src = "CAST(v AS INT) > 5", .want = null, .d = .mysql },
        .{ .src = "CAST(5 AS INT) > 1", .want = "(CAST(5 AS BIGINT) > 1)" },
        .{ .src = "CAST(v AS STRING) = 'x'", .want = "(CAST([v] AS VARCHAR(MAX)) = 'x')" },
        .{ .src = "lower(status) = 'ok'", .want = "(LOWER([status]) = 'ok')" },
        .{ .src = "length(status) > 2", .want = null },
        .{ .src = "length(status) > 2", .want = null, .d = .mysql },
        .{ .src = "strpos(status, 'a') = 2", .want = null },
        .{ .src = "trim(status) = 'x'", .want = "(LTRIM(RTRIM([status])) = 'x')" },
        .{ .src = "substr(status, 1, 2) = 'AB'", .want = "(SUBSTRING([status], 1, 2) = 'AB')" },
        .{ .src = "coalesce(status, 'n') = 'n'", .want = "(COALESCE([status], 'n') = 'n')" },
        .{ .src = "contains(status, 'ab')", .want = "([status] LIKE '%ab%')" },
        .{ .src = "starts_with(status, 'CT2')", .want = "([status] LIKE 'CT2%')" },
        .{ .src = "status LIKE 'a%'", .want = "([status] LIKE 'a%')" },
        .{ .src = "contains(status, '10%')", .want = null },
        .{ .src = "status LIKE 'a_b'", .want = null },
        .{ .src = "contains(status, '[x]')", .want = "([status] LIKE '%[[]x]%')" },
        .{ .src = "contains(status, '[x]')", .want = "(`status` LIKE '%[x]%')", .d = .mysql },
        .{ .src = "substr(status, 2, 2) = 'AB'", .want = null },
        .{ .src = "status >= 'B'", .want = null },
        .{ .src = "status <> 'x'", .want = null },
        .{ .src = "NOT (status = 'x')", .want = null },
        .{ .src = "status >= 'B' AND v > 1", .want = "([v] > 1)" },
        .{ .src = "status = '\u{e9}'", .want = null },
        .{ .src = "status = 'a\\b'", .want = null, .d = .mysql },
        .{ .src = "now() > v", .want = null },
        .{ .src = "v + 1 > 2", .want = null },
    };
    for (cases) |tc| {
        var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try sqlp.parseExprStr(a, tc.src, &diag);
        const got = try translateExpr(a, e, tc.d, .{ .fields = &.{} }, false);
        if (tc.want) |w| {
            std.testing.expectEqualStrings(w, got orelse "<null>") catch |err| {
                std.debug.print("case: {s}\n", .{tc.src});
                return err;
            };
        } else if (got) |g| {
            std.debug.print("case {s}: want null, got {s}\n", .{ tc.src, g });
            return error.TestUnexpectedResult;
        }
    }
}

test "translateCall: same-name numeric builtins render identically everywhere" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const abs_e = try callExpr(a, "abs", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("ABS([a])", (try translateExpr(a, abs_e, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("ABS(\"a\")", (try translateExpr(a, abs_e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("ABS(`a`)", (try translateExpr(a, abs_e, .mysql, testSchema(), true)).?);

    const floor_e = try callExpr(a, "floor", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("FLOOR([a])", (try translateExpr(a, floor_e, .sqlserver, testSchema(), true)).?);
    const sqrt_e = try callExpr(a, "sqrt", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("SQRT(`a`)", (try translateExpr(a, sqrt_e, .mysql, testSchema(), true)).?);
    const sign_e = try callExpr(a, "sign", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("SIGN(\"a\")", (try translateExpr(a, sign_e, .postgres, testSchema(), true)).?);
    const rev_e = try callExpr(a, "reverse", &[_]*ast.Expr{try fld(a, "b")});
    try testing.expectEqualStrings("REVERSE([b])", (try translateExpr(a, rev_e, .sqlserver, testSchema(), true)).?);
    const pow_e = try callExpr(a, "power", &[_]*ast.Expr{ try fld(a, "a"), try intLit(a, 2) });
    try testing.expectEqualStrings("POWER([a], 2)", (try translateExpr(a, pow_e, .sqlserver, testSchema(), true)).?);
    const nif_e = try callExpr(a, "nullif", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, "x") });
    try testing.expectEqualStrings("NULLIF(`b`, 'x')", (try translateExpr(a, nif_e, .mysql, testSchema(), true)).?);

    const bad = try callExpr(a, "abs", &[_]*ast.Expr{ try fld(a, "a"), try intLit(a, 1) });
    try testing.expect((try translateExpr(a, bad, .mysql, testSchema(), true)) == null);
}

test "translateCall: ceil is CEILING on sqlserver, CEIL elsewhere" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try callExpr(a, "ceil", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("CEILING([a])", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("CEIL(\"a\")", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("CEIL(`a`)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
}

test "translateCall: mod is the % operator on sqlserver, MOD elsewhere" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try callExpr(a, "mod", &[_]*ast.Expr{ try fld(a, "a"), try intLit(a, 3) });
    try testing.expectEqualStrings("([a] % 3)", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("MOD(\"a\", 3)", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("MOD(`a`, 3)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);

    const cmp = try bin(a, .eq, e, try intLit(a, 0));
    try testing.expectEqualStrings("(([a] % 3) = 0)", (try translateExpr(a, cmp, .sqlserver, testSchema(), true)).?);
}

test "translateCall: strpos and length stay in the engine, where the sources disagree on what they count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d| {
        const sp = try callExpr(a, "strpos", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, "x") });
        try testing.expect((try translateExpr(a, sp, d, testSchema(), true)) == null);
        const ln = try callExpr(a, "length", &[_]*ast.Expr{try fld(a, "b")});
        try testing.expect((try translateExpr(a, ln, d, testSchema(), true)) == null);
    }
}

test "translateCall: repeat is REPLICATE on sqlserver; left/right are portable" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rep = try callExpr(a, "repeat", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 3) });
    try testing.expectEqualStrings("REPLICATE([b], 3)", (try translateExpr(a, rep, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("REPEAT(\"b\", 3)", (try translateExpr(a, rep, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("REPEAT(`b`, 3)", (try translateExpr(a, rep, .mysql, testSchema(), true)).?);

    const lf = try callExpr(a, "left", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 2) });
    try testing.expectEqualStrings("LEFT([b], 2)", (try translateExpr(a, lf, .sqlserver, testSchema(), true)).?);
    const rt = try callExpr(a, "right", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 2) });
    try testing.expectEqualStrings("RIGHT(`b`, 2)", (try translateExpr(a, rt, .mysql, testSchema(), true)).?);

    const neg = try callExpr(a, "left", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, -2) });
    try testing.expect((try translateExpr(a, neg, .postgres, testSchema(), true)) == null);
    const dyn = try callExpr(a, "repeat", &[_]*ast.Expr{ try fld(a, "b"), try fld(a, "a") });
    try testing.expect((try translateExpr(a, dyn, .postgres, testSchema(), true)) == null);
}

test "translateCall: excluded builtins fall back to the engine" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const round_e = try callExpr(a, "round", &[_]*ast.Expr{try fld(a, "a")});
    const greatest_e = try callExpr(a, "greatest", &[_]*ast.Expr{ try fld(a, "a"), try fld(a, "c") });
    const lpad_e = try callExpr(a, "lpad", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 4), try strLit(a, "0") });
    const split_e = try callExpr(a, "split_part", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, ","), try intLit(a, 1) });
    const excluded = [_]*ast.Expr{ round_e, greatest_e, lpad_e, split_e };
    for (excluded) |e| {
        try testing.expect((try translateExpr(a, e, .postgres, testSchema(), true)) == null);
        try testing.expect((try translateExpr(a, e, .mysql, testSchema(), true)) == null);
        try testing.expect((try translateExpr(a, e, .sqlserver, testSchema(), true)) == null);
    }

    const cmp = try bin(a, .gt, round_e, try intLit(a, 1));
    try testing.expect((try translateExpr(a, cmp, .postgres, testSchema(), true)) == null);
}

test "translateCall: every pushable name is an engine builtin" {
    for (pushable) |p| {
        if (builtins.lookup(p.name) == null) {
            std.debug.print("pushdown table names `{s}`, which is not a builtin\n", .{p.name});
            return error.TestUnexpectedResult;
        }
    }
}

test "translateExpr: a safe (TRY_) cast is never pushed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const from_text = try a.create(ast.Expr);
    from_text.* = .{ .cast = .{ .e = try fld(a, "b"), .ty = types.Type.init(.int) } };
    try testing.expect((try translateExpr(a, from_text, .mysql, testSchema(), true)) == null);

    const plain = try a.create(ast.Expr);
    plain.* = .{ .cast = .{ .e = try fld(a, "a"), .ty = types.Type.init(.int) } };
    try testing.expectEqualStrings("CAST(`a` AS SIGNED)", (try translateExpr(a, plain, .mysql, testSchema(), true)).?);

    const safe = try a.create(ast.Expr);
    safe.* = .{ .cast = .{ .e = try fld(a, "a"), .ty = types.Type.init(.int), .safe = true } };
    try testing.expect((try translateExpr(a, safe, .mysql, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, safe, .postgres, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, safe, .sqlserver, testSchema(), true)) == null);
}

test "text comparisons against the column's collation: exact where it compares bytes, widened where it pads, refused where it folds" {
    var arn = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arn.deinit();
    const a = arn.allocator();
    const sqlp = @import("../../lang/sql_parser.zig");

    var facts = Facts.init(a);
    try facts.put("d", .{ .text = true, .byte_order = true, .pads = true });
    try facts.put("ci", .{ .text = true, .byte_order = false, .pads = true });
    try facts.put("c", .{ .text = true, .byte_order = true, .pads = false });
    try facts.put("n", .{ .text = false });

    const cases = [_]struct { src: []const u8, need: Need, want: ?[]const u8, d: Dialect = .sqlserver }{
        .{ .src = "d >= '20240105'", .need = .superset, .want = "([d] >= '20240105' OR [d] LIKE '20240105%')" },
        .{ .src = "d > '000123'", .need = .superset, .want = "([d] >= '000123' OR [d] LIKE '000123%')" },
        .{ .src = "d < 'Z'", .need = .superset, .want = "([d] <= 'Z')" },
        .{ .src = "d = '01'", .need = .superset, .want = "([d] = '01')" },
        .{ .src = "d = '01'", .need = .exact, .want = "([d] = '01' AND DATALENGTH([d]) = 2)" },
        .{ .src = "d > '000123'", .need = .exact, .want = "([d] > '000123' OR ([d] LIKE '000123%' AND DATALENGTH([d]) > 6))" },
        .{ .src = "d < '2024'", .need = .exact, .want = "(NOT ([d] >= '2024' OR [d] LIKE '2024%'))" },
        .{ .src = "NOT (d = 'x')", .need = .superset, .want = "(NOT (([d] = 'x' AND DATALENGTH([d]) = 1)))" },
        .{ .src = "d >= 'a b'", .need = .superset, .want = "([d] >= 'a b' OR [d] LIKE 'a b%')" },
        .{ .src = "d >= '10%'", .need = .superset, .want = "([d] >= '10%' OR [d] LIKE '10[%]%')" },
        .{ .src = "d = 'x '", .need = .exact, .want = null },
        .{ .src = "ci = 'x'", .need = .superset, .want = "([ci] = 'x')" },
        .{ .src = "ci = 'x'", .need = .exact, .want = null },
        .{ .src = "ci >= 'B'", .need = .superset, .want = null },
        .{ .src = "ci <> 'x'", .need = .superset, .want = null },
        .{ .src = "ci LIKE 'a%'", .need = .superset, .want = "([ci] LIKE 'a%')" },
        .{ .src = "ci LIKE 'a%'", .need = .exact, .want = null },
        .{ .src = "c >= 'b'", .need = .exact, .want = "(\"c\" >= 'b')", .d = .postgres },
        .{ .src = "c <> 'b'", .need = .exact, .want = "(\"c\" <> 'b')", .d = .postgres },
        .{ .src = "d = 'x'", .need = .exact, .want = null, .d = .postgres },
        .{ .src = "n >= '2024-01-01'", .need = .exact, .want = "([n] >= '2024-01-01')" },
        .{ .src = "d = '01'", .need = .exact, .want = "(`d` = '01' AND LENGTH(`d`) = 2)", .d = .mysql },
    };
    for (cases) |tc| {
        var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try sqlp.parseExprStr(a, tc.src, &diag);
        const got = try translatePred(a, e, tc.d, .{ .facts = &facts, .need = tc.need });
        if (tc.want) |w| {
            std.testing.expectEqualStrings(w, got orelse "<null>") catch |err| {
                std.debug.print("case: {s}\n", .{tc.src});
                return err;
            };
        } else if (got) |g| {
            std.debug.print("case {s}: want null, got {s}\n", .{ tc.src, g });
            return error.TestUnexpectedResult;
        }
    }

    var wants = false;
    var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try sqlp.parseExprStr(a, "x >= 'B'", &diag);
    try std.testing.expect((try translatePred(a, e, .sqlserver, .{ .wants_facts = &wants })) == null);
    try std.testing.expect(wants);
}

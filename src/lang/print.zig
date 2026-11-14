//! The one expression walker, `Printer`, and the policies (`Mode`) its callers print
//! under: `sql`, the details `EXPLAIN ANALYZE` prints beside an operator; the `${ }`
//! hole text (`parse/stmt.zig`); and the key GROUP BY, ORDER BY and HAVING bind by
//! (`parse/rewrite.zig`). `sql` is total over `Expr`, unlike the hole text, which
//! refuses what a hole cannot hold: a profile describes whatever ran. Under `sql` a
//! nested operator is parenthesised rather than placed by precedence, so the text
//! reads the way the expression groups, whatever the script wrote.

const std = @import("std");
const ast = @import("ast.zig");
const quote = @import("quote.zig");

const W = *std.Io.Writer;
const Error = std.Io.Writer.Error;

/// An expression node's kind, as a mode refuses it.
pub const Kind = std.meta.Tag(ast.Expr);

/// Keyword case and operator spelling: SQL (`NULL`, `NOT`, `=`, `<>`, `AND`) or
/// script (`null`, `not`, `==`, `!=`, `and`).
pub const Dialect = enum { sql, script };

/// Where parentheses go. `operands`: around an operator's operand that is itself an
/// operator. `script`: around every binary and every unary's operand. `whole`:
/// around every binary, unary and null test.
pub const Grouping = enum { operands, script, whole };

/// Node shapes: SQL text, or the compact key text (`f(*)`, lower-cased call names,
/// `cast(x as int?)`, `(x is is_null)`, `match(...)`, `let(n=v in b)`, `(x)->b`,
/// arguments joined by a bare comma).
pub const Layout = enum { sql, key };

/// How a walk prints, one per caller: `field` renders a column from the caller's
/// `Ctx`; `float` is `{d}` (`decimal`) or `{e}` (`scientific`); `refuses` lists the
/// kinds the walk fails on with `error.Refused`; `typed_text` reads a CAST of a string
/// literal back as that literal (`typedText`); `distinct` keeps a call's DISTINCT.
pub fn Mode(comptime Ctx: type) type {
    return struct {
        field: fn (Ctx, W, ast.QualName) Error!void,
        float: enum { decimal, scientific },
        dialect: Dialect,
        grouping: Grouping,
        layout: Layout = .sql,
        refuses: []const Kind = &.{},
        typed_text: bool = false,
        distinct: bool = true,
    };
}

/// The walker: prints an expression to `w` under `mode`, with `ctx` passed to
/// `mode.field`.
pub fn Printer(comptime Ctx: type, comptime mode: Mode(Ctx)) type {
    return struct {
        ctx: Ctx,
        w: W,

        const Self = @This();

        pub const Fail = if (mode.refuses.len > 0) Error || error{Refused} else Error;

        const sep = if (mode.layout == .key) "," else ", ";

        pub fn walk(self: Self, e0: *const ast.Expr) Fail!void {
            const e = if (mode.typed_text) typedText(e0) else e0;
            if (mode.refuses.len > 0) {
                if (std.mem.indexOfScalar(Kind, mode.refuses, std.meta.activeTag(e.*)) != null) return error.Refused;
            }
            const wrap = switch (mode.grouping) {
                .operands => false,
                .script => e.* == .binary,
                .whole => e.* == .binary or e.* == .unary or e.* == .is_null,
            };
            if (wrap) try self.w.writeByte('(');
            try self.node(e);
            if (wrap) try self.w.writeByte(')');
        }

        fn node(self: Self, e: *const ast.Expr) Fail!void {
            const w = self.w;
            switch (e.*) {
                .null_lit => try self.kw("NULL"),
                .bool_lit => |b| if (b) try self.kw("TRUE") else try self.kw("FALSE"),
                .int_lit => |v| try w.print("{d}", .{v}),
                .float_lit => |v| switch (mode.float) {
                    .decimal => try w.print("{d}", .{v}),
                    .scientific => try w.print("{e}", .{v}),
                },
                .dec_lit => |v| try w.print("{f}", .{v}),
                .str_lit => |s| try quote.strLit(w, s),
                .field => |q| try mode.field(self.ctx, w, q),
                .lambda_var => |n| try w.writeAll(n),
                .unary => |u| {
                    switch (u.op) {
                        .neg => try w.writeByte('-'),
                        .not => try self.kw("NOT "),
                        .bit_not => try w.writeByte('~'),
                    }
                    if (mode.grouping == .script) {
                        try w.writeByte('(');
                        try self.walk(u.e);
                        try w.writeByte(')');
                    } else try self.operand(u.e);
                },
                .binary => |b| {
                    try self.operand(b.l);
                    try w.print(" {s} ", .{binOp(b.op, mode.dialect)});
                    try self.operand(b.r);
                },
                .call => |c| {
                    if (mode.layout == .key) {
                        for (c.name) |ch| try w.writeByte(std.ascii.toLower(ch));
                    } else try w.writeAll(c.name);
                    try w.writeByte('(');
                    if (mode.distinct and c.distinct) try self.kw("DISTINCT ");
                    if (mode.layout == .key and c.args.len == 0) try w.writeByte('*');
                    try self.list(c.args);
                    try w.writeByte(')');
                },
                .cond => |c| {
                    try self.kw("IF(");
                    try self.walk(c.cond);
                    try w.writeAll(sep);
                    try self.walk(c.then);
                    try w.writeAll(sep);
                    try self.walk(c.els);
                    try w.writeByte(')');
                },
                .match => |m| switch (mode.layout) {
                    .sql => {
                        try self.kw("CASE");
                        if (m.subject) |s| {
                            try w.writeByte(' ');
                            try self.walk(s);
                        }
                        for (m.arms) |arm| {
                            if (arm.is_default) {
                                try self.kw(" ELSE ");
                            } else {
                                try self.kw(" WHEN ");
                                if (arm.pats.len > 0) try self.list(arm.pats) else if (arm.guard) |g| try self.walk(g);
                                try self.kw(" THEN ");
                            }
                            try self.walk(arm.value);
                        }
                        try self.kw(" END");
                    },
                    .key => {
                        try w.writeAll("match(");
                        if (m.subject) |s| try self.walk(s);
                        for (m.arms) |arm| {
                            try w.writeAll(if (arm.is_default) ";else" else ";when");
                            for (arm.pats) |p| {
                                try w.writeByte(' ');
                                try self.walk(p);
                            }
                            if (arm.guard) |g| {
                                try self.kw(" IF ");
                                try self.walk(g);
                            }
                            try self.kw(" THEN ");
                            try self.walk(arm.value);
                        }
                        try w.writeByte(')');
                    },
                },
                .index => |x| {
                    try self.operand(x.e);
                    try w.writeByte('[');
                    try self.walk(x.key);
                    try w.writeByte(']');
                },
                .cast => |c| {
                    switch (mode.layout) {
                        .sql => if (c.safe) try self.kw("TRY_CAST(") else try self.kw("CAST("),
                        .key => try self.kw("CAST("),
                    }
                    try self.walk(c.e);
                    try self.kw(" AS ");
                    switch (mode.layout) {
                        .sql => try c.ty.writeName(w, if (mode.dialect == .sql) .upper else .lower),
                        .key => {
                            try c.ty.writeShortName(w);
                            if (c.safe) try w.writeByte('?');
                        },
                    }
                    try w.writeByte(')');
                },
                .is_null => |n| {
                    try self.operand(n.e);
                    if (n.negated) try self.kw(" IS NOT ") else try self.kw(" IS ");
                    switch (mode.layout) {
                        .sql => switch (n.kind) {
                            .is_null => try self.kw("NULL"),
                            .is_empty => try self.kw("EMPTY"),
                        },
                        .key => try w.writeAll(@tagName(n.kind)),
                    }
                },
                .let_in => |l| {
                    switch (mode.layout) {
                        .sql => {
                            try self.kw("LET ");
                            try w.print("{s} = ", .{l.name});
                        },
                        .key => try w.print("let({s}=", .{l.name}),
                    }
                    try self.walk(l.value);
                    try self.kw(" IN ");
                    try self.walk(l.body);
                    if (mode.layout == .key) try w.writeByte(')');
                },
                .lambda => |l| {
                    if (mode.layout == .sql and l.params.len == 1) try w.writeAll(l.params[0]) else {
                        try w.writeByte('(');
                        for (l.params, 0..) |p, i| {
                            if (i > 0) try w.writeAll(sep);
                            try w.writeAll(p);
                        }
                        try w.writeByte(')');
                    }
                    try w.writeAll(if (mode.layout == .key) "->" else " -> ");
                    try self.walk(l.body);
                },
            }
        }

        fn operand(self: Self, e: *const ast.Expr) Fail!void {
            const grouped = mode.grouping == .operands and switch (e.*) {
                .binary, .unary, .is_null, .let_in, .lambda => true,
                else => false,
            };
            if (grouped) try self.w.writeByte('(');
            try self.walk(e);
            if (grouped) try self.w.writeByte(')');
        }

        fn list(self: Self, es: []const *ast.Expr) Fail!void {
            for (es, 0..) |a, i| {
                if (i > 0) try self.w.writeAll(sep);
                try self.walk(a);
            }
        }

        fn kw(self: Self, comptime text: []const u8) Error!void {
            try self.w.writeAll(comptime if (mode.dialect == .sql) text else lower(text));
        }
    };
}

/// The EXPLAIN ANALYZE text: SQL keywords and operators, names quoted by `name`.
pub const sql: Mode(void) = .{ .field = plainField, .float = .decimal, .dialect = .sql, .grouping = .operands };

pub fn expr(w: W, e: *const ast.Expr) Error!void {
    return Printer(void, sql).walk(.{ .ctx = {}, .w = w }, e);
}

/// A dotted name, each part double-quoted where a bare identifier could not spell it.
/// An internal part (`\x00lateral`) stays as is, as rewrites find its holes by text.
pub fn name(w: W, q: ast.QualName) Error!void {
    for (q.parts, 0..) |p, i| {
        if (i > 0) try w.writeByte('.');
        if (bareIdent(p) or (p.len > 0 and p[0] == 0)) try w.writeAll(p) else try quote.ident(w, p);
    }
}

/// `name` as a `Mode(void)` field.
pub fn plainField(_: void, w: W, q: ast.QualName) Error!void {
    return name(w, q);
}

/// A string literal a typed parameter took as a CAST (a table function's DATE
/// argument), read back as the text it was written as.
pub fn typedText(e: *const ast.Expr) *const ast.Expr {
    return if (e.* == .cast and e.cast.e.* == .str_lit) e.cast.e else e;
}

/// An operator's text in `d`; only equality and logic spell differently.
pub fn binOp(o: ast.BinOp, d: Dialect) []const u8 {
    const s = d == .sql;
    return switch (o) {
        .add => "+",
        .sub => "-",
        .mul => "*",
        .div => "/",
        .mod => "%",
        .bit_and => "&",
        .bit_or => "|",
        .bit_xor => "^",
        .shl => "<<",
        .shr => ">>",
        .eq => if (s) "=" else "==",
        .ne => if (s) "<>" else "!=",
        .lt => "<",
        .le => "<=",
        .gt => ">",
        .ge => ">=",
        .@"and" => if (s) "AND" else "and",
        .@"or" => if (s) "OR" else "or",
        .distinct => "IS DISTINCT FROM",
        .not_distinct => "IS NOT DISTINCT FROM",
    };
}

fn lower(comptime text: []const u8) []const u8 {
    var buf: [text.len]u8 = undefined;
    _ = std.ascii.lowerString(&buf, text);
    const out = buf;
    return &out;
}

fn bareIdent(s: []const u8) bool {
    if (s.len == 0 or std.ascii.isDigit(s[0])) return false;
    for (s) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '*') return false;
    return true;
}

test "print: nested operators are parenthesised, strings and odd names quoted" {
    var col_parts = [_][]const u8{ "s", "Valor Total" };
    var col = ast.Expr{ .field = .{ .parts = &col_parts } };
    var zero = ast.Expr{ .int_lit = 0 };
    var gt = ast.Expr{ .binary = .{ .op = .gt, .l = &col, .r = &zero } };
    var it = ast.Expr{ .str_lit = "it's" };
    var region_parts = [_][]const u8{"region"};
    var region = ast.Expr{ .field = .{ .parts = &region_parts } };
    var eq = ast.Expr{ .binary = .{ .op = .eq, .l = &region, .r = &it } };
    var both = ast.Expr{ .binary = .{ .op = .@"and", .l = &gt, .r = &eq } };
    var out = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer out.deinit();
    try expr(&out.writer, &both);
    try std.testing.expectEqualStrings("(s.\"Valor Total\" > 0) AND (region = 'it''s')", out.written());
}

test "print: calls, casts, null tests and CASE read back as SQL" {
    var x_parts = [_][]const u8{"x"};
    var x = ast.Expr{ .field = .{ .parts = &x_parts } };
    var args = [_]*ast.Expr{&x};
    var call = ast.Expr{ .call = .{ .name = "count", .args = &args, .distinct = true } };
    var cast = ast.Expr{ .cast = .{ .e = &x, .ty = .{ .kind = .decimal, .precision = 18, .scale = 2 } } };
    var isn = ast.Expr{ .is_null = .{ .e = &x, .negated = true } };
    var one = ast.Expr{ .int_lit = 1 };
    var label = ast.Expr{ .str_lit = "one" };
    var other = ast.Expr{ .str_lit = "other" };
    var pats = [_]*ast.Expr{&one};
    const arms = [_]ast.MatchArm{
        .{ .pats = &pats, .guard = null, .value = &label, .is_default = false },
        .{ .pats = &.{}, .guard = null, .value = &other, .is_default = true },
    };
    var case = ast.Expr{ .match = .{ .subject = &x, .arms = &arms } };
    var out = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer out.deinit();
    for ([_]*ast.Expr{ &call, &cast, &isn, &case }) |e| {
        try expr(&out.writer, e);
        try out.writer.writeAll(" | ");
    }
    try std.testing.expectEqualStrings("count(DISTINCT x) | CAST(x AS DECIMAL(18, 2)) | x IS NOT NULL | CASE x WHEN 1 THEN 'one' ELSE 'other' END | ", out.written());
}

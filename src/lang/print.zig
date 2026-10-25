//! SQL text of an expression and a qualified name, for the details `EXPLAIN ANALYZE`
//! prints beside an operator. Total over `Expr`, unlike `unparse`, which refuses what a
//! `${ }` hole cannot hold: a profile describes whatever ran. A nested operator is
//! parenthesised rather than placed by precedence, so the text reads the way the
//! expression groups, whatever the script wrote.

const std = @import("std");
const ast = @import("ast.zig");
const quote = @import("quote.zig");

const W = *std.Io.Writer;
const Error = std.Io.Writer.Error;

pub fn expr(w: W, e: *const ast.Expr) Error!void {
    switch (e.*) {
        .null_lit => try w.writeAll("NULL"),
        .bool_lit => |b| try w.writeAll(if (b) "TRUE" else "FALSE"),
        .int_lit => |v| try w.print("{d}", .{v}),
        .float_lit => |v| try w.print("{d}", .{v}),
        .dec_lit => |v| try w.print("{f}", .{v}),
        .str_lit => |s| try quote.strLit(w, s),
        .field => |q| try name(w, q),
        .lambda_var => |n| try w.writeAll(n),
        .unary => |u| {
            try w.writeAll(switch (u.op) {
                .neg => "-",
                .not => "NOT ",
                .bit_not => "~",
            });
            try operand(w, u.e);
        },
        .binary => |b| {
            try operand(w, b.l);
            try w.print(" {s} ", .{binOp(b.op)});
            try operand(w, b.r);
        },
        .call => |c| {
            try w.writeAll(c.name);
            try w.writeByte('(');
            if (c.distinct) try w.writeAll("DISTINCT ");
            try list(w, c.args);
            try w.writeByte(')');
        },
        .cond => |c| {
            try w.writeAll("IF(");
            try expr(w, c.cond);
            try w.writeAll(", ");
            try expr(w, c.then);
            try w.writeAll(", ");
            try expr(w, c.els);
            try w.writeByte(')');
        },
        .match => |m| {
            try w.writeAll("CASE");
            if (m.subject) |s| {
                try w.writeByte(' ');
                try expr(w, s);
            }
            for (m.arms) |arm| {
                if (arm.is_default) {
                    try w.writeAll(" ELSE ");
                } else {
                    try w.writeAll(" WHEN ");
                    if (arm.pats.len > 0) try list(w, arm.pats) else if (arm.guard) |g| try expr(w, g);
                    try w.writeAll(" THEN ");
                }
                try expr(w, arm.value);
            }
            try w.writeAll(" END");
        },
        .cast => |c| {
            try w.writeAll(if (c.safe) "TRY_CAST(" else "CAST(");
            try expr(w, c.e);
            try w.writeAll(" AS ");
            if (c.ty.kind == .decimal) try w.print("DECIMAL({d}, {d})", .{ c.ty.precision, c.ty.scale }) else try upper(w, @tagName(c.ty.kind));
            try w.writeByte(')');
        },
        .is_null => |n| {
            try operand(w, n.e);
            try w.writeAll(if (n.negated) " IS NOT " else " IS ");
            try w.writeAll(switch (n.kind) {
                .is_null => "NULL",
                .is_empty => "EMPTY",
            });
        },
        .let_in => |l| {
            try w.print("LET {s} = ", .{l.name});
            try expr(w, l.value);
            try w.writeAll(" IN ");
            try expr(w, l.body);
        },
        .lambda => |l| {
            if (l.params.len == 1) try w.writeAll(l.params[0]) else {
                try w.writeByte('(');
                for (l.params, 0..) |p, i| {
                    if (i > 0) try w.writeAll(", ");
                    try w.writeAll(p);
                }
                try w.writeByte(')');
            }
            try w.writeAll(" -> ");
            try expr(w, l.body);
        },
    }
}

/// A dotted name, each part double-quoted where a bare identifier could not spell it.
pub fn name(w: W, q: ast.QualName) Error!void {
    for (q.parts, 0..) |p, i| {
        if (i > 0) try w.writeByte('.');
        if (bareIdent(p)) try w.writeAll(p) else try quote.ident(w, p);
    }
}

fn operand(w: W, e: *const ast.Expr) Error!void {
    const grouped = switch (e.*) {
        .binary, .is_null, .let_in, .lambda => true,
        else => false,
    };
    if (grouped) try w.writeByte('(');
    try expr(w, e);
    if (grouped) try w.writeByte(')');
}

fn list(w: W, es: []const *ast.Expr) Error!void {
    for (es, 0..) |a, i| {
        if (i > 0) try w.writeAll(", ");
        try expr(w, a);
    }
}

fn upper(w: W, s: []const u8) Error!void {
    for (s) |c| try w.writeByte(std.ascii.toUpper(c));
}

fn bareIdent(s: []const u8) bool {
    if (s.len == 0 or std.ascii.isDigit(s[0])) return false;
    for (s) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '*') return false;
    return true;
}

fn binOp(o: ast.BinOp) []const u8 {
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
        .eq => "=",
        .ne => "<>",
        .lt => "<",
        .le => "<=",
        .gt => ">",
        .ge => ">=",
        .@"and" => "AND",
        .@"or" => "OR",
        .distinct => "IS DISTINCT FROM",
        .not_distinct => "IS NOT DISTINCT FROM",
    };
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

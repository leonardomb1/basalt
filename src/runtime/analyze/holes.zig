//! `${...}` holes in a script's strings: where one closes, whether it is a bare loop
//! variable, and whether its text parses as an expression a hole may hold. The run
//! renders holes per row; `check` asks the same questions of the text up front.

const ast = @import("../../lang/ast.zig");
const parser = @import("../../lang/sql_parser.zig");
const std = @import("std");

/// The `}` closing a `${` opened before `start`: brace-balanced and quote-aware
/// (`'...'` and `"..."`, both escaped by doubling).
pub fn close(s: []const u8, start: usize) ?usize {
    var depth: usize = 1;
    var k = start;
    while (k < s.len) {
        switch (s[k]) {
            '\'', '"' => {
                const q = s[k];
                k += 1;
                while (k < s.len) {
                    if (s[k] == q) {
                        if (k + 1 < s.len and s[k + 1] == q) {
                            k += 2;
                            continue;
                        }
                        k += 1;
                        break;
                    }
                    k += 1;
                }
            },
            '{' => {
                depth += 1;
                k += 1;
            },
            '}' => {
                depth -= 1;
                if (depth == 0) return k;
                k += 1;
            },
            else => k += 1,
        }
    }
    return null;
}

pub fn bare(inner: []const u8) bool {
    if (inner.len == 0 or !(std.ascii.isAlphabetic(inner[0]) or inner[0] == '_')) return false;
    for (inner[1..]) |c| if (!(std.ascii.isAlphanumeric(c) or c == '_')) return false;
    return true;
}

fn unsupported(e: *const ast.Expr) ?[]const u8 {
    return switch (e.*) {
        .let_in => "`let … in` is not supported inside ${...}; bind it in a select/fn instead",
        .field => |q| if (q.safe.len > 0) "`?.` safe navigation is not supported inside ${...}" else null,
        .unary => |u| unsupported(u.e),
        .binary => |b| unsupported(b.l) orelse unsupported(b.r),
        .cond => |c| unsupported(c.cond) orelse unsupported(c.then) orelse unsupported(c.els),
        .cast => |c| unsupported(c.e),
        .is_null => |n| unsupported(n.e),
        .call => |c| {
            for (c.args) |a| if (unsupported(a)) |why| return why;
            return null;
        },
        .match => |m| {
            if (m.subject) |s| if (unsupported(s)) |why| return why;
            for (m.arms) |arm| {
                for (arm.pats) |p| if (unsupported(p)) |why| return why;
                if (arm.guard) |g| if (unsupported(g)) |why| return why;
                if (unsupported(arm.value)) |why| return why;
            }
            return null;
        },
        else => null,
    };
}

pub const Parsed = union(enum) { expr: *const ast.Expr, fail: []const u8 };

/// A hole's text as an expression, or why it is not one a hole may hold.
pub fn parse(arena: std.mem.Allocator, text: []const u8) error{OutOfMemory}!Parsed {
    var diag = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const e = parser.parseExprStr(arena, text, &diag) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return .{ .fail = diag.msg };
    };
    if (unsupported(e)) |why| return .{ .fail = why };
    return .{ .expr = e };
}

pub fn failMsg(arena: std.mem.Allocator, text: []const u8, why: []const u8) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(arena, "`${{{s}}}`: {s}", .{ text, why });
}

/// The first hole in `s` whose text cannot be an expression, worded as the run words it.
pub fn problem(arena: std.mem.Allocator, s: []const u8) error{OutOfMemory}!?[]const u8 {
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, s, i, "${")) |at| {
        const end = close(s, at + 2) orelse return null;
        const inner = s[at + 2 .. end];
        if (!bare(inner)) switch (try parse(arena, inner)) {
            .fail => |why| return try failMsg(arena, inner, why),
            .expr => {},
        };
        i = end + 1;
    }
    return null;
}

test "a hole that cannot parse, or holds what a hole refuses, is named" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expect(try problem(a, "out_${name}_${upper(name)}.csv") == null);
    try std.testing.expect(std.mem.indexOf(u8, (try problem(a, "x_${n:lower}.csv")).?, "`${n:lower}`") != null);
    try std.testing.expect(std.mem.indexOf(u8, (try problem(a, "${let x = 1 in x}")).?, "not supported") != null);
    try std.testing.expect(try problem(a, "unclosed ${x") == null);
}

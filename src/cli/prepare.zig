//! An entry made runnable: the session's declarations as a prelude, LETs frozen to
//! the values they had, shared by the REPL and the notebook kernel.

const DeclId = @import("entry.zig").DeclId;
const DeclStore = @import("entry.zig").DeclStore;
const Value = @import("../exec/value.zig").Value;
const ast = @import("../lang/ast.zig");
const declOf = @import("entry.zig").declOf;
const entryStatements = @import("repl.zig").entryStatements;
const eval = @import("../exec/eval.zig");
const include = @import("../lang/include.zig");
const runtime = @import("../runtime/run.zig");
const splitStatements = @import("entry.zig").splitStatements;
const std = @import("std");
const parser = @import("../lang/sql_parser.zig");

pub const Pending = struct { id: DeclId, text: []const u8 };

pub const Prepared = struct {
    text: []const u8,
    entry_at: usize,
    prog: ast.Program,
    pending: []const Pending,
    executable: usize,
};

pub const PrepareError = error{ ParseFailed, EndpointInSession, OutOfMemory };

/// A declaration the entry makes again is left out of the prelude, so no name is declared twice.
fn withPrelude(a: std.mem.Allocator, decls: *const DeclStore, pending: []const Pending, block: []const u8) !struct { text: []const u8, entry_at: usize } {
    var buf = std.array_list.Managed(u8).init(a);
    for (decls.items.items) |e| {
        var shadowed = false;
        for (pending) |p| {
            if (p.id.kind == e.kind and std.ascii.eqlIgnoreCase(p.id.name, e.name)) shadowed = true;
        }
        if (shadowed) continue;
        try buf.appendSlice(e.text);
        try buf.appendSlice(";\n");
    }
    const entry_at = buf.items.len;
    try buf.appendSlice(block);
    return .{ .text = buf.items, .entry_at = entry_at };
}

pub fn sessionText(a: std.mem.Allocator, decls: *const DeclStore, block: []const u8) !struct { text: []const u8, entry_at: usize } {
    var pending = std.array_list.Managed(Pending).init(a);
    for (try splitStatements(a, block)) |st| {
        if (declOf(st)) |id| try pending.append(.{ .id = id, .text = st });
    }
    const j = try withPrelude(a, decls, pending.items, block);
    return .{ .text = j.text, .entry_at = j.entry_at };
}

/// Nothing is committed here: the caller commits `pending` once the entry stands, so a
/// typo cannot poison the session. Statements are re-read from the parse, as a function
/// body's `;`s fool the pre-scan, except under `@include`, whose positions are elsewhere.
pub fn prepareEntry(a: std.mem.Allocator, decls: *const DeclStore, block: []const u8, diag: *include.Diag, text_out: ?*[]const u8) PrepareError!Prepared {
    var pending = std.array_list.Managed(Pending).init(a);
    var executable: usize = 0;
    for (try splitStatements(a, block)) |st| {
        const id = declOf(st) orelse {
            executable += 1;
            continue;
        };
        if (id.kind == .endpoint) return error.EndpointInSession;
        try pending.append(.{ .id = id, .text = st });
    }

    const joined = try withPrelude(a, decls, pending.items, block);
    const text = joined.text;
    const entry_at = joined.entry_at;
    if (text_out) |t| t.* = text;

    const prog = include.loadProgram(a, text, "<repl>", ".", diag) catch |e| switch (e) {
        error.ParseFailed => return error.ParseFailed,
        error.OutOfMemory => return error.OutOfMemory,
    };

    if (std.mem.indexOf(u8, block, "@include") == null) {
        const parsed = try entryStatements(a, text, entry_at, prog);
        pending.clearRetainingCapacity();
        executable = 0;
        for (parsed) |p| {
            if (p.id) |id| try pending.append(.{ .id = id, .text = p.text }) else executable += 1;
        }
    }
    return .{ .text = text, .entry_at = entry_at, .prog = prog, .pending = pending.items, .executable = executable };
}

pub const LetFreezer = struct {
    arena: std.mem.Allocator,
    frozen: std.array_list.Managed(Frozen),

    const Frozen = struct { name: []const u8, text: []const u8 };

    pub fn init(arena: std.mem.Allocator) LetFreezer {
        return .{ .arena = arena, .frozen = std.array_list.Managed(Frozen).init(arena) };
    }

    pub fn hook(self: *LetFreezer) runtime.LetHook {
        return .{ .ctx = self, .f = onLet };
    }

    fn onLet(ctx: *anyopaque, name: []const u8, v: Value) void {
        const self: *LetFreezer = @ptrCast(@alignCast(ctx));
        const lit = letLiteral(self.arena, v) catch return orelse return;
        const text = std.fmt.allocPrint(self.arena, "LET {s} = {s}", .{ name, lit }) catch return;
        const n = self.arena.dupe(u8, name) catch return;
        self.frozen.append(.{ .name = n, .text = text }) catch {};
    }

    pub fn commit(self: *const LetFreezer, decls: *DeclStore) !void {
        for (self.frozen.items) |f| {
            for (decls.items.items) |e| {
                if (e.kind == .let and std.ascii.eqlIgnoreCase(e.name, f.name)) break;
            } else continue;
            try decls.put(.{ .kind = .let, .name = f.name }, f.text);
        }
    }
};

/// A SQL expression that folds back to exactly `v`, or null when there is no literal
/// form. A date, an exponent or the one overflowing int goes through a CAST of its text.
pub fn letLiteral(arena: std.mem.Allocator, v: Value) !?[]const u8 {
    return switch (v) {
        .null => "NULL",
        .bool => |b| if (b) "TRUE" else "FALSE",
        .int => |x| if (x == std.math.minInt(i64))
            "CAST('-9223372036854775808' AS BIGINT)"
        else
            try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "CAST('{e}' AS DOUBLE)", .{x}),
        .decimal => |d| try std.fmt.allocPrint(arena, "CAST('{s}' AS DECIMAL(38, {d}))", .{ try eval.valueToString(arena, v), d.scale }),
        .string => |s| blk: {
            var out = std.array_list.Managed(u8).init(arena);
            try out.append('\'');
            for (s) |c| {
                if (c == '\'') try out.append('\'');
                try out.append(c);
            }
            try out.append('\'');
            break :blk out.items;
        },
        .date => try std.fmt.allocPrint(arena, "CAST('{s}' AS DATE)", .{try eval.valueToString(arena, v)}),
        .time => try std.fmt.allocPrint(arena, "CAST('{s}' AS TIME)", .{try eval.valueToString(arena, v)}),
        .timestamp => try std.fmt.allocPrint(arena, "CAST('{s}' AS TIMESTAMP)", .{try eval.valueToString(arena, v)}),
        else => null,
    };
}

test "letLiteral: every value folds back to itself" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const values = [_]Value{
        .null,                                               .{ .bool = true },
        .{ .int = std.math.minInt(i64) },                    .{ .int = std.math.maxInt(i64) },
        .{ .int = -7 },                                      .{ .float = 0.1 },
        .{ .float = -1.5e300 },                              .{ .float = 5e-324 },
        .{ .decimal = .{ .unscaled = -12345, .scale = 3 } }, .{ .string = "it's\nfine" },
        .{ .string = "" },                                   .{ .date = -1 },
        .{ .time = 86_399_999_999 },                         .{ .timestamp = -500_000 },
        .{ .timestamp = 1_767_268_800_123_456 },
    };
    for (values) |v| {
        const lit = (try letLiteral(a, v)).?;
        const src = try std.fmt.allocPrint(a, "LET x = {s};", .{lit});
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, src, &diag);
        const back = try eval.constEval(a, prog.stmts[1].let_const.expr.?, &.{}, &.{});
        errdefer std.debug.print("{s} -> {any}\n", .{ lit, back });
        try std.testing.expectEqual(std.meta.activeTag(v), std.meta.activeTag(back));
        if (v != .null) try std.testing.expectEqual(std.math.Order.eq, eval.compareValues(v, back).?);
    }
    try std.testing.expect((try letLiteral(a, .{ .bytes = "\x00" })) == null);
}

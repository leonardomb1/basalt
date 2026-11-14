//! Message formatting for cold error paths. A comptime format of only `{s}` and `{d}`
//! over text and integers is rendered at run time by one shared function, so each
//! message costs a call instead of its own `std.fmt` instantiation; any other format
//! goes to `std.fmt`. Either way the text is what `std.fmt` prints.
const std = @import("std");
const Writer = std.Io.Writer;

const Arg = union(enum) { s: []const u8, u: usize, i: isize };

fn argKind(comptime T: type) ?std.meta.Tag(Arg) {
    return switch (@typeInfo(T)) {
        .comptime_int => .i,
        .int => |n| if (n.bits > @bitSizeOf(usize)) null else if (n.signedness == .signed) .i else .u,
        .pointer => |p| switch (p.size) {
            .slice => if (p.child == u8) .s else null,
            .one => switch (@typeInfo(p.child)) {
                .array => |a| if (a.child == u8) .s else null,
                else => null,
            },
            else => null,
        },
        else => null,
    };
}

/// Whether `fmt` over `Args` is literal text, `{{`, `}}`, and `{s}` / `{d}` placeholders
/// each taking the next argument, of text or of an integer.
fn plain(comptime fmt: []const u8, comptime Args: type) bool {
    @setEvalBranchQuota(fmt.len * 20 + 1000);
    const fields = @typeInfo(Args).@"struct".fields;
    var next: usize = 0;
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '{' and fmt[i] != '}') continue;
        if (i + 1 < fmt.len and fmt[i + 1] == fmt[i]) {
            i += 1;
            continue;
        }
        if (fmt[i] == '}' or i + 2 >= fmt.len or fmt[i + 2] != '}' or next >= fields.len) return false;
        const kind = argKind(fields[next].type) orelse return false;
        const spec = fmt[i + 1];
        if ((spec != 's' and spec != 'd') or (spec == 's') != (kind == .s)) return false;
        next += 1;
        i += 2;
    }
    return next == fields.len;
}

inline fn pack(args: anytype) [@typeInfo(@TypeOf(args)).@"struct".fields.len]Arg {
    var out: [@typeInfo(@TypeOf(args)).@"struct".fields.len]Arg = undefined;
    inline for (args, 0..) |v, k| out[k] = switch (comptime argKind(@TypeOf(v)).?) {
        .s => .{ .s = v },
        .u => .{ .u = v },
        .i => .{ .i = v },
    };
    return out;
}

fn render(w: *Writer, fmt: []const u8, args: []const Arg) Writer.Error!void {
    var rest = fmt;
    var next: usize = 0;
    while (std.mem.indexOfAny(u8, rest, "{}")) |k| {
        if (rest[k + 1] == rest[k]) {
            try w.writeAll(rest[0 .. k + 1]);
            rest = rest[k + 2 ..];
            continue;
        }
        try w.writeAll(rest[0..k]);
        switch (args[next]) {
            .s => |s| try w.writeAll(s),
            .u => |u| try w.printInt(u, 10, .lower, .{}),
            .i => |v| try w.printInt(v, 10, .lower, .{}),
        }
        next += 1;
        rest = rest[k + 3 ..];
    }
    try w.writeAll(rest);
}

fn bufRender(buf: []u8, fmt: []const u8, args: []const Arg) error{NoSpaceLeft}![]u8 {
    var w: Writer = .fixed(buf);
    render(&w, fmt, args) catch return error.NoSpaceLeft;
    return w.buffered();
}

fn allocRender(gpa: std.mem.Allocator, fmt: []const u8, args: []const Arg) error{OutOfMemory}![]u8 {
    var aw = try Writer.Allocating.initCapacity(gpa, fmt.len);
    defer aw.deinit();
    render(&aw.writer, fmt, args) catch return error.OutOfMemory;
    return aw.toOwnedSlice();
}

/// `std.fmt.bufPrint`.
pub inline fn bufPrint(buf: []u8, comptime fmt: []const u8, args: anytype) error{NoSpaceLeft}![]u8 {
    if (comptime !plain(fmt, @TypeOf(args))) return std.fmt.bufPrint(buf, fmt, args);
    return bufRender(buf, fmt, &pack(args));
}

/// `std.fmt.allocPrint`.
pub inline fn allocPrint(gpa: std.mem.Allocator, comptime fmt: []const u8, args: anytype) error{OutOfMemory}![]u8 {
    if (comptime !plain(fmt, @TypeOf(args))) return std.fmt.allocPrint(gpa, fmt, args);
    return allocRender(gpa, fmt, &pack(args));
}

/// `Writer.print`.
pub inline fn print(w: *Writer, comptime fmt: []const u8, args: anytype) Writer.Error!void {
    if (comptime !plain(fmt, @TypeOf(args))) return w.print(fmt, args);
    return render(w, fmt, &pack(args));
}

/// `bufPrint` filling `buf`, ending in `...` when the message does not fit.
pub inline fn clipPrint(buf: []u8, comptime fmt: []const u8, args: anytype) []u8 {
    return bufPrint(buf, fmt, args) catch clip(buf);
}

fn clip(buf: []u8) []u8 {
    @memcpy(buf[buf.len - 3 ..], "...");
    return buf;
}

fn expectSame(comptime fmt: []const u8, args: anytype) !void {
    comptime std.debug.assert(plain(fmt, @TypeOf(args)));
    var a: [64]u8 = undefined;
    var b: [64]u8 = undefined;
    try std.testing.expectEqualStrings(try std.fmt.bufPrint(&a, fmt, args), try bufPrint(&b, fmt, args));
}

test "runtime rendering prints what std.fmt prints" {
    const s: []const u8 = "x";
    const n: u8 = 200;
    const m: i64 = -42;
    try expectSame("plain", .{});
    try expectSame("{s} and {d}, {d}{{}}{s}", .{ s, n, m, "lit" });
    try expectSame("{{{d}}} {d}", .{ 7, @as(usize, 0) });
    try std.testing.expect(comptime !plain("{x}", @TypeOf(.{n})));
    try std.testing.expect(comptime !plain("{d}", @TypeOf(.{@as(f64, 1)})));
    try std.testing.expect(comptime !plain("{s}", @TypeOf(.{n})));
}

test "clipPrint ends a message too long for its buffer in ..." {
    var buf: [8]u8 = undefined;
    try std.testing.expectEqualStrings("abcde...", clipPrint(&buf, "abc{s}", .{"defghij"}));
    try std.testing.expectEqualStrings("ab1", clipPrint(&buf, "ab{d}", .{1}));
}

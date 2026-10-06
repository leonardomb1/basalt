//! Values as text: how dates, times, timestamps and decimals print.

const Value = @import("../value.zig").Value;
const civilFromDays = @import("time.zig").civilFromDays;
const formatDecimal = @import("support.zig").formatDecimal;
const std = @import("std");

pub fn writeValue(w: anytype, v: Value) !void {
    switch (v) {
        .null => {},
        .string, .bytes => |x| try w.writeAll(x),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .int => |x| try w.print("{d}", .{x}),
        .float => |x| try w.print("{d}", .{x}),
        .decimal => |d| try writeDecimal(w, d.unscaled, d.scale),
        .date => |x| try writeDate(w, x),
        .time => |x| try writeTime(w, x),
        .timestamp => |x| try writeTimestamp(w, x),
    }
}

pub fn writeDate(w: anytype, days: i64) !void {
    const c = civilFromDays(days);
    try writeYear(w, c.y);
    try w.print("-{d:0>2}-{d:0>2}", .{ c.m, c.d });
}

/// Four digits, zero-padded; a year before 0 carries a leading `-`, as ISO
/// 8601's expanded form does (printing one used to trap on the cast).
fn writeYear(w: anytype, y: i64) !void {
    if (y < 0) try w.writeByte('-');
    try w.print("{d:0>4}", .{@abs(y)});
}

pub fn writeTime(w: anytype, t: i64) !void {
    const us: u64 = @intCast(@mod(t, 86_400_000_000));
    const secs = us / 1_000_000;
    const frac = us % 1_000_000;
    if (frac != 0) {
        try w.print("{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ secs / 3600, (secs % 3600) / 60, secs % 60, frac });
    } else {
        try w.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ secs / 3600, (secs % 3600) / 60, secs % 60 });
    }
}

pub fn writeTimestamp(w: anytype, micros: i64) !void {
    const days = @divFloor(micros, 86_400_000_000);
    const us: u64 = @intCast(micros - days * 86_400_000_000);
    const secs = us / 1_000_000;
    const frac = us % 1_000_000;
    const c = civilFromDays(days);
    try writeYear(w, c.y);
    if (frac != 0) {
        try w.print("-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{
            c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60, frac,
        });
    } else {
        try w.print("-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
            c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
        });
    }
}

pub fn writeDecimal(w: anytype, unscaled: i128, scale: u8) !void {
    const neg = unscaled < 0;
    var mag: u128 = if (neg) @intCast(-unscaled) else @intCast(unscaled);

    var digits: [48]u8 = undefined;
    var n: usize = 0;
    if (mag == 0) {
        digits[0] = '0';
        n = 1;
    }
    while (mag > 0) : (mag /= 10) {
        digits[n] = @intCast('0' + mag % 10);
        n += 1;
    }
    while (n <= scale) : (n += 1) digits[n] = '0';

    if (neg) try w.writeByte('-');
    var k: usize = n;
    while (k > 0) {
        k -= 1;
        try w.writeByte(digits[k]);
        if (scale > 0 and k == scale) try w.writeByte('.');
    }
}

pub fn valueToString(arena: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .string => |s| s,
        .bytes => |s| s,
        .bool => |b| if (b) "true" else "false",
        .int => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .decimal => |d| try formatDecimal(arena, d.unscaled, d.scale),
        .date => |x| try formatDate(arena, x),
        .time => |x| try formatTime(arena, x),
        .timestamp => |x| try formatTimestamp(arena, x),
    };
}

pub const fmt_bound = 128;

/// Renders through `writeDate` into a fixed `fmt_bound` buffer, which cannot
/// overflow (the widest output is 17 bytes), so the catch is `unreachable`.
pub fn formatDate(arena: std.mem.Allocator, days: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeDate(&w, days) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub fn formatTime(arena: std.mem.Allocator, t: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeTime(&w, t) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub fn formatTimestamp(arena: std.mem.Allocator, micros: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeTimestamp(&w, micros) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

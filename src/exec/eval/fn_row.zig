//! The row form of each built-in function.

const Batch = @import("../batch.zig").Batch;
const EvalError = @import("../eval.zig").EvalError;
const Value = @import("../value.zig").Value;
const accNode = @import("row.zig").accNode;
const ast = @import("../../lang/ast.zig");
const bindParams = @import("row.zig").bindParams;
const castValueTyped = @import("cast.zig").castValueTyped;
const clip = @import("support.zig").clip;
const compareValues = @import("support.zig").compareValues;
const eq = @import("support.zig").eq;
const evalRow = @import("row.zig").evalRow;
const failWith = @import("support.zig").failWith;
const json = @import("../json.zig");
const jsontext = @import("../jsontext.zig");
const jsonArrayArg = @import("row.zig").jsonArrayArg;
const jsonElementValue = @import("row.zig").jsonElementValue;
const lambdaSlots = @import("row.zig").lambdaSlots;
const literalOf = @import("row.zig").literalOf;
const max_str_bytes = @import("functions.zig").max_str_bytes;
const reduceTypeAt = @import("row.zig").reduceTypeAt;
const regexpReplaceText = @import("fn_scalar.zig").regexpReplaceText;
const std = @import("std");
const valueToString = @import("format.zig").valueToString;
const writeJsonValue = @import("row.zig").writeJsonValue;

pub const per_row = struct {
    pub fn now(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        _ = arena;
        _ = c;
        _ = batch;
        _ = row;
        return .{ .timestamp = std.time.microTimestamp() };
    }

    pub fn today(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        _ = arena;
        _ = c;
        _ = batch;
        _ = row;
        const days = @divFloor(std.time.microTimestamp(), 86_400_000_000);
        return .{ .date = @intCast(days) };
    }

    /// Both the pattern and the replacement are evaluated before either is checked
    /// for NULL, and the flags only then.
    pub fn regexpReplace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const pat = try evalRow(arena, c.args[1], batch, row);
        const rep = try evalRow(arena, c.args[2], batch, row);
        if (pat.isNull() or rep.isNull()) return .null;
        var flags: ?[]const u8 = null;
        if (c.args.len == 4) {
            const fv = try evalRow(arena, c.args[3], batch, row);
            if (fv.isNull()) return .null;
            flags = try valueToString(arena, fv);
        }
        const s = try valueToString(arena, v);
        return .{ .string = try regexpReplaceText(arena, s, try valueToString(arena, pat), try valueToString(arena, rep), flags) };
    }

    /// Values are written as `json_transform` writes elements, so a nested object or
    /// array goes in as JSON. A null key is an error.
    pub fn jsonObject(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('{') catch return error.OutOfMemory;
        var i: usize = 0;
        while (i < c.args.len) : (i += 2) {
            const k = try evalRow(arena, c.args[i], batch, row);
            if (k.isNull()) return failWith(error.CastFailed, "json_object: key {d} is null", .{i / 2 + 1});
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            jsontext.key(w, try valueToString(arena, k)) catch return error.OutOfMemory;
            try writeJsonValue(arena, try evalRow(arena, c.args[i + 1], batch, row), w);
        }
        w.writeByte('}') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    pub fn jsonArray(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        for (c.args, 0..) |e, i| {
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            try writeJsonValue(arena, try evalRow(arena, e, batch, row), w);
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    pub fn coalesce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (!v.isNull()) return v;
        }
        return .null;
    }

    pub fn nullif(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const a = try evalRow(arena, c.args[0], batch, row);
        if (a.isNull()) return .null;
        const b = try evalRow(arena, c.args[1], batch, row);
        if (b.isNull()) return a;
        if (compareValues(a, b)) |ord| {
            if (ord == .eq) return .null;
        }
        return a;
    }

    /// Null arguments are ignored (Postgres); all-null yields null.
    pub fn greatestLeast(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const want_gt = eq(c.name, "greatest");
        var best: Value = .null;
        for (c.args) |ae| {
            const v = try evalRow(arena, ae, batch, row);
            if (v.isNull()) continue;
            if (best.isNull()) {
                best = v;
                continue;
            }
            const ord = compareValues(best, v) orelse return error.TypeMismatch;
            if (if (want_gt) ord == .lt else ord == .gt) best = v;
        }
        return best;
    }

    /// The JSON array functions: the body is bound once to slots each element then
    /// overwrites. An element the body cannot compare counts as null; a failed CAST
    /// still fails. A JSON cell that is not an array is an error.
    pub fn jsonLambda(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        if (c.args.len != 2 or c.args[1].* != .lambda) return error.TypeMismatch;
        const dv = try evalRow(arena, c.args[0], batch, row);
        if (dv.isNull()) return .null;
        const doc = try valueToString(arena, dv);
        try json.validate(arena, doc);
        if (json.rootKind(doc) != .array) return error.InvalidJson;
        const l = c.args[1].lambda;
        const Kind = enum { filter, transform, any, all };
        const kind: Kind = if (eq(c.name, "json_filter")) .filter else if (eq(c.name, "json_transform")) .transform else if (eq(c.name, "json_any")) .any else .all;
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var n: usize = 0;
        const slots = try lambdaSlots(arena, &.{ .null_lit, .{ .int_lit = 0 } });
        const body = try bindParams(arena, l, slots);
        var items = json.Elements.root(doc);
        var idx: i64 = 0;
        while (items.next()) |el| : (idx += 1) {
            slots[0].* = try literalOf(arena, try jsonElementValue(arena, el));
            slots[1].* = .{ .int_lit = idx };
            const r = evalRow(arena, body, batch, row) catch |e| switch (e) {
                error.TypeMismatch => Value.null,
                else => return e,
            };
            const holds = r == .bool and r.bool;
            switch (kind) {
                .filter, .transform => if (kind == .transform or holds) {
                    if (n > 0) w.writeByte(',') catch return error.OutOfMemory;
                    n += 1;
                    if (kind == .filter) try json.compactInto(arena, el, w) else try writeJsonValue(arena, r, w);
                },
                .any => if (holds) return Value{ .bool = true },
                .all => if (!holds) return Value{ .bool = false },
            }
        }
        return switch (kind) {
            .any => Value{ .bool = false },
            .all => Value{ .bool = true },
            .filter, .transform => blk: {
                w.writeByte(']') catch return error.OutOfMemory;
                break :blk Value{ .string = out.written() };
            },
        };
    }

    /// A fold from `initial`. Unlike `json_transform`, an element the body cannot
    /// compare fails the statement, and a float into an INT total is a CastFailed.
    pub fn jsonReduce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        if (c.args.len != 3 or c.args[2].* != .lambda) return error.TypeMismatch;
        const dv = try evalRow(arena, c.args[0], batch, row);
        if (dv.isNull()) return .null;
        const doc = try valueToString(arena, dv);
        try json.validate(arena, doc);
        if (json.rootKind(doc) != .array) return error.InvalidJson;
        const ty = try reduceTypeAt(arena, c, batch);
        var acc = try evalRow(arena, c.args[1], batch, row);
        if (!ty.unknown and !acc.isNull()) acc = try castValueTyped(arena, acc, ty);
        const slots = try lambdaSlots(arena, &.{ .null_lit, .null_lit, .{ .int_lit = 0 } });
        const body = try bindParams(arena, c.args[2].lambda, slots);
        var items = json.Elements.root(doc);
        var idx: i64 = 0;
        while (items.next()) |el| : (idx += 1) {
            slots[0].* = try accNode(arena, acc, ty);
            slots[1].* = try literalOf(arena, try jsonElementValue(arena, el));
            slots[2].* = .{ .int_lit = idx };
            const r = evalRow(arena, body, batch, row) catch |e| {
                if (e != error.TypeMismatch) return e;
                return failWith(e, "json_reduce: the lambda could not compute with element [{d}], {s} — text is not a number; CAST it or drop it with json_filter first", .{ idx, clip(el) });
            };
            if (r == .float and (ty.kind == .int or ty.kind == .decimal) and !ty.unknown)
                return failWith(error.CastFailed, "json_reduce: element [{d}], {s}, made the lambda return {d}, which {s} accumulator cannot hold — start from a FLOAT (0e0)", .{ idx, clip(el), r.float, if (ty.kind == .int) "an INT" else "a DECIMAL" });
            acc = if (r.isNull() or ty.unknown) r else try castValueTyped(arena, r, ty);
        }
        return acc;
    }

    pub fn jsonRange(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var bounds = [2]i64{ 0, 0 };
        for (c.args, bounds[2 - c.args.len ..]) |e, *b| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) return .null;
            if (v != .int) return error.TypeMismatch;
            b.* = v.int;
        }
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var k = bounds[0];
        while (k < bounds[1]) : (k += 1) {
            if (k > bounds[0]) w.writeByte(',') catch return error.OutOfMemory;
            w.print("{d}", .{k}) catch return error.OutOfMemory;
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "json_range: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    /// Elements `start` up to (not including) `stop`, from 0; negative bounds count
    /// from the end, as Python's slices do, and bounds past either end are clamped.
    pub fn jsonSlice(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const doc = try jsonArrayArg(arena, c.args[0], batch, row) orelse return .null;
        var len: i64 = 0;
        var count = json.Elements.root(doc);
        while (count.next()) |_| len += 1;
        var bounds = [2]i64{ 0, len };
        for (c.args[1..], bounds[0 .. c.args.len - 1]) |e, *b| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) return .null;
            if (v != .int) return error.TypeMismatch;
            b.* = std.math.clamp(if (v.int < 0) len + v.int else v.int, 0, len);
        }
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var items = json.Elements.root(doc);
        var k: i64 = 0;
        var n: usize = 0;
        while (items.next()) |el| : (k += 1) {
            if (k < bounds[0] or k >= bounds[1]) continue;
            if (n > 0) w.writeByte(',') catch return error.OutOfMemory;
            n += 1;
            try json.compactInto(arena, el, w);
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    pub fn jsonConcat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var n: usize = 0;
        for (c.args) |e| {
            const doc = try jsonArrayArg(arena, e, batch, row) orelse return .null;
            var items = json.Elements.root(doc);
            while (items.next()) |el| {
                if (n > 0) w.writeByte(',') catch return error.OutOfMemory;
                n += 1;
                try json.compactInto(arena, el, w);
            }
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "json_concat: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }
};

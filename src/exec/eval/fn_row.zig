//! The row form of each built-in function.

const Batch = @import("../batch.zig").Batch;
const EvalError = @import("../eval.zig").EvalError;
const Value = @import("../value.zig").Value;
const accNode = @import("row.zig").accNode;
const addUnits = @import("time.zig").addUnits;
const ast = @import("../../lang/ast.zig");
const bindParams = @import("row.zig").bindParams;
const cachedRegex = @import("support.zig").cachedRegex;
const cachedRegexOpts = @import("support.zig").cachedRegexOpts;
const caseMap = @import("strings.zig").caseMap;
const caseMapInto = @import("strings.zig").caseMapInto;
const castValueTyped = @import("cast.zig").castValueTyped;
const charCount = @import("strings.zig").charCount;
const charOffset = @import("strings.zig").charOffset;
const charWidth = @import("strings.zig").charWidth;
const clip = @import("support.zig").clip;
const compareValues = @import("support.zig").compareValues;
const dateDiff = @import("time.zig").dateDiff;
const daysFromCivil = @import("time.zig").daysFromCivil;
const daysInMonth = @import("time.zig").daysInMonth;
const endSlice = @import("strings.zig").endSlice;
const eq = @import("support.zig").eq;
const evalRow = @import("row.zig").evalRow;
const extractField = @import("time.zig").extractField;
const failWith = @import("support.zig").failWith;
const isAscii = @import("strings.zig").isAscii;
const isNum = @import("support.zig").isNum;
const isWordChar = @import("strings.zig").isWordChar;
const json = @import("../json.zig");
const jsonArrayArg = @import("row.zig").jsonArrayArg;
const jsonElementValue = @import("row.zig").jsonElementValue;
const lambdaSlots = @import("row.zig").lambdaSlots;
const likeMatch = @import("strings.zig").likeMatch;
const literalOf = @import("row.zig").literalOf;
const max_str_bytes = @import("functions.zig").max_str_bytes;
const mulI64 = @import("time.zig").mulI64;
const padChars = @import("strings.zig").padChars;
const parseHexI64 = @import("support.zig").parseHexI64;
const reduceTypeAt = @import("row.zig").reduceTypeAt;
const regex = @import("../regex.zig");
const regexpFind = @import("support.zig").regexpFind;
const reverseChars = @import("strings.zig").reverseChars;
const roundDecimal = @import("cast.zig").roundDecimal;
const roundHalfAway = @import("strings.zig").roundHalfAway;
const roundOutScale = @import("cast.zig").roundOutScale;
const std = @import("std");
const strftimeFmt = @import("time.zig").strftimeFmt;
const strptimeFmt = @import("time.zig").strptimeFmt;
const substrChars = @import("strings.zig").substrChars;
const temporalMicros = @import("time.zig").temporalMicros;
const timeUnit = @import("time.zig").timeUnit;
const toF64 = @import("support.zig").toF64;
const toI64 = @import("support.zig").toI64;
const trim = @import("support.zig").trim;
const truncMicros = @import("time.zig").truncMicros;
const unaccentCp = @import("strings.zig").unaccentCp;
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

    pub fn regexpReplace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const pat = try evalRow(arena, c.args[1], batch, row);
        const rep = try evalRow(arena, c.args[2], batch, row);
        if (pat.isNull() or rep.isNull()) return .null;
        var flags = regex.Flags{};
        if (c.args.len == 4) {
            const fv = try evalRow(arena, c.args[3], batch, row);
            if (fv.isNull()) return .null;
            flags = regex.parseFlags(try valueToString(arena, fv)) orelse return error.CastFailed;
        }
        const re = cachedRegexOpts(try valueToString(arena, pat), .{ .icase = flags.icase }) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.BadPattern => return error.CastFailed,
            error.PatternTooComplex => return error.PatternTooComplex,
        };
        const replaceFn = if (flags.global) &regex.replaceAllRe else &regex.replaceFirstRe;
        const out = replaceFn(
            arena,
            re,
            try valueToString(arena, v),
            try valueToString(arena, rep),
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.BadPattern => return error.CastFailed,
            error.PatternTooComplex => return error.PatternTooComplex,
        };
        return .{ .string = out };
    }

    pub fn regexpMatches(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const m = try regexpFind(arena, c, batch, row) orelse return .null;
        return .{ .bool = m.span != null };
    }

    /// Null where the pattern does not match, unlike DuckDB's '', which a load
    /// cannot tell from an empty field.
    pub fn regexpExtract(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const m = try regexpFind(arena, c, batch, row) orelse return .null;
        if (m.span == null) return .null;
        const g: usize = if (c.args.len == 3) @intCast(c.args[2].int_lit) else 0;
        const span = m.caps[g] orelse return .null;
        return .{ .string = m.s[span[0]..span[1]] };
    }

    pub fn digest(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const data = try valueToString(arena, v);
        if (eq(c.name, "xxhash64")) return .{ .int = @bitCast(std.hash.XxHash64.hash(0, data)) };
        if (eq(c.name, "md5")) {
            var d: [std.crypto.hash.Md5.digest_length]u8 = undefined;
            std.crypto.hash.Md5.hash(data, &d, .{});
            return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{&d}) };
        }
        var d: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data, &d, .{});
        return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{&d}) };
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
            std.json.Stringify.encodeJsonString(try valueToString(arena, k), .{}, w) catch return error.OutOfMemory;
            w.writeByte(':') catch return error.OutOfMemory;
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

    pub fn toBase64(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const data = try valueToString(arena, v);
        const enc = std.base64.standard.Encoder;
        const out = try arena.alloc(u8, enc.calcSize(data.len));
        return .{ .string = enc.encode(out, data) };
    }

    pub fn fromBase64(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const text = std.mem.trim(u8, try valueToString(arena, v), " \t\r\n");
        const dec = std.base64.standard.Decoder;
        const bad = "from_base64: '{s}' is not base64";
        const out = try arena.alloc(u8, dec.calcSizeForSlice(text) catch return failWith(error.CastFailed, bad, .{clip(text)}));
        dec.decode(out, text) catch return failWith(error.CastFailed, bad, .{clip(text)});
        return .{ .bytes = out };
    }

    /// RFC 3986 percent-encoding: all but letters, digits and `-._~` become `%XX`.
    /// Decoding leaves `+` alone and passes a stray `%` through, as DuckDB does.
    pub fn urlCode(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        if (eq(c.name, "url_encode")) {
            for (str) |b| {
                if (std.ascii.isAlphanumeric(b) or b == '-' or b == '.' or b == '_' or b == '~') {
                    try out.append(b);
                } else try out.writer().print("%{X:0>2}", .{b});
            }
        } else {
            var i: usize = 0;
            while (i < str.len) : (i += 1) {
                if (str[i] == '%' and i + 2 < str.len) {
                    if (std.fmt.parseInt(u8, str[i + 1 .. i + 3], 16)) |b| {
                        try out.append(b);
                        i += 2;
                        continue;
                    } else |_| {}
                }
                try out.append(str[i]);
            }
        }
        return .{ .string = out.items };
    }

    /// Postgres' `concat_ws`: nulls are skipped, where `concat` is null when any
    /// value is (which hashed a row with one empty column to null).
    pub fn concatWs(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const sep = try valueToString(arena, sv);
        var buf = std.array_list.Managed(u8).init(arena);
        var first = true;
        for (c.args[1..]) |e| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) continue;
            if (!first) try buf.appendSlice(sep);
            first = false;
            try buf.appendSlice(try valueToString(arena, v));
        }
        return .{ .string = buf.items };
    }

    pub fn dateTruncExtract(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[1], batch, row);
        if (v.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        const u = timeUnit(c.args[0].str_lit) orelse return error.TypeMismatch;
        return if (eq(c.name, "extract"))
            Value{ .int = extractField(us, u) }
        else
            Value{ .timestamp = truncMicros(us, u) };
    }

    pub fn coalesce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (!v.isNull()) return v;
        }
        return .null;
    }

    pub fn upperLower(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        var out = std.array_list.Managed(u8).init(arena);
        try caseMapInto(&out, try valueToString(arena, v), eq(c.name, "upper"));
        return .{ .string = out.items };
    }

    /// `length` counts characters, `strlen` bytes (DuckDB's split); a BYTES value
    /// is bytes either way.
    pub fn strlen(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const s = try valueToString(arena, v);
        return .{ .int = @intCast(if (eq(c.name, "length") and v != .bytes) charCount(s) else s.len) };
    }

    pub fn bitCount(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        return .{ .int = @intCast(@popCount(v.int)) };
    }

    pub fn toHex(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{@as(u64, @bitCast(v.int))}) };
    }

    pub fn fromHex(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .int = try parseHexI64(try valueToString(arena, v)) };
    }

    pub fn concat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var buf = std.array_list.Managed(u8).init(arena);
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (v.isNull()) return .null;
            try buf.appendSlice(try valueToString(arena, v));
        }
        return .{ .string = try buf.toOwnedSlice() };
    }

    pub fn affix(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const name = c.name;
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const p = try valueToString(arena, pv);
        const r = if (eq(name, "starts_with")) std.mem.startsWith(u8, s, p) else if (eq(name, "ends_with")) std.mem.endsWith(u8, s, p) else (std.mem.indexOf(u8, s, p) != null);
        return .{ .bool = r };
    }

    pub fn like(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        return .{ .bool = likeMatch(try valueToString(arena, sv), try valueToString(arena, pv)) };
    }

    pub fn trimSpace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .string = try arena.dupe(u8, trim(try valueToString(arena, v))) };
    }

    pub fn substr(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const startv = try evalRow(arena, c.args[1], batch, row);
        if (startv.isNull()) return .null;
        var len_opt: ?i64 = null;
        if (c.args.len > 2) {
            const lv = try evalRow(arena, c.args[2], batch, row);
            if (lv.isNull()) return .null;
            len_opt = toI64(lv);
        }
        return .{ .string = try substrChars(arena, try valueToString(arena, sv), toI64(startv), len_opt) };
    }

    pub fn replace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const fv = try evalRow(arena, c.args[1], batch, row);
        const tv = try evalRow(arena, c.args[2], batch, row);
        if (sv.isNull() or fv.isNull() or tv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const from = try valueToString(arena, fv);
        const to = try valueToString(arena, tv);
        if (from.len == 0) return .{ .string = try arena.dupe(u8, s) };
        const out = try arena.alloc(u8, std.mem.replacementSize(u8, s, from, to));
        _ = std.mem.replace(u8, s, from, to, out);
        return .{ .string = out };
    }

    pub fn abs(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        switch (v) {
            .int => |x| {
                if (x == std.math.minInt(i64)) return error.IntOverflow;
                return Value{ .int = if (x < 0) -x else x };
            },
            .float => |x| return Value{ .float = @abs(x) },
            .decimal => |d| return Value{ .decimal = .{
                .unscaled = if (d.unscaled < 0) -d.unscaled else d.unscaled,
                .scale = d.scale,
            } },
            else => return error.TypeMismatch,
        }
    }

    pub fn floorCeil(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v == .int) return v;
        if (!isNum(v)) return error.TypeMismatch;
        const x = toF64(v);
        return Value{ .float = if (eq(c.name, "floor")) @floor(x) else @ceil(x) };
    }

    pub fn round(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (!isNum(v)) return error.TypeMismatch;
        var digits: i64 = 0;
        if (c.args.len > 1) {
            const dv = try evalRow(arena, c.args[1], batch, row);
            if (dv.isNull()) return .null;
            digits = toI64(dv);
        }
        if (v == .int and c.args.len == 1) return v;
        if (v == .decimal) return Value{ .decimal = roundDecimal(v.decimal, digits, roundOutScale(c, v.decimal.scale)) orelse return error.IntOverflow };
        return Value{ .float = roundHalfAway(toF64(v), digits) };
    }

    pub fn mod(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const a = try evalRow(arena, c.args[0], batch, row);
        const b = try evalRow(arena, c.args[1], batch, row);
        if (a.isNull() or b.isNull()) return .null;
        const d = toI64(b);
        if (d == 0) return .null;
        if (d == -1) return Value{ .int = 0 };
        return Value{ .int = @rem(toI64(a), d) };
    }

    pub fn power(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const a = try evalRow(arena, c.args[0], batch, row);
        const b = try evalRow(arena, c.args[1], batch, row);
        if (a.isNull() or b.isNull()) return .null;
        return Value{ .float = std.math.pow(f64, toF64(a), toF64(b)) };
    }

    pub fn sqrt(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const x = toF64(v);
        if (x < 0) return .null;
        return Value{ .float = @sqrt(x) };
    }

    pub fn sign(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const x = toF64(v);
        return Value{ .int = if (x > 0) @as(i64, 1) else if (x < 0) @as(i64, -1) else @as(i64, 0) };
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

    pub fn pad(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return .null;
        var fill: []const u8 = " ";
        if (c.args.len > 2) {
            const fv = try evalRow(arena, c.args[2], batch, row);
            if (fv.isNull()) return .null;
            fill = try valueToString(arena, fv);
        }
        const s = try valueToString(arena, sv);
        return Value{ .string = try padChars(arena, s, toI64(nv), fill, eq(c.name, "lpad")) };
    }

    pub fn leftRight(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        return Value{ .string = try arena.dupe(u8, endSlice(s, toI64(nv), eq(c.name, "left"))) };
    }

    pub fn splitPart(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const dv = try evalRow(arena, c.args[1], batch, row);
        const nv = try evalRow(arena, c.args[2], batch, row);
        if (sv.isNull() or dv.isNull() or nv.isNull()) return .null;
        const delim = try valueToString(arena, dv);
        if (delim.len == 0) return .null;
        const want = toI64(nv);
        if (want < 1) return Value{ .string = "" };
        var it = std.mem.splitSequence(u8, try valueToString(arena, sv), delim);
        var k: i64 = 0;
        while (it.next()) |part| {
            k += 1;
            if (k == want) return Value{ .string = try arena.dupe(u8, part) };
        }
        return Value{ .string = "" };
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
            const r = try evalRow(arena, body, batch, row);
            if (r == .float and (ty.kind == .int or ty.kind == .decimal) and !ty.unknown)
                return failWith(error.CastFailed, "json_reduce: the lambda returned {d} into {s} accumulator — start from a FLOAT (0.0)", .{ r.float, if (ty.kind == .int) "an INT" else "a DECIMAL" });
            acc = if (r.isNull() or ty.unknown) r else try castValueTyped(arena, r, ty);
        }
        return acc;
    }

    pub fn chars(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var i: usize = 0;
        while (i < str.len) {
            const cw = charWidth(str, i);
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            std.json.Stringify.encodeJsonString(str[i..][0..cw], .{}, w) catch return error.OutOfMemory;
            i += cw;
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "chars: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
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

    pub fn jsonLength(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const doc = try jsonArrayArg(arena, c.args[0], batch, row) orelse return .null;
        var items = json.Elements.root(doc);
        var n: i64 = 0;
        while (items.next()) |_| n += 1;
        return .{ .int = n };
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

    pub fn jsonGet(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const dv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (dv.isNull() or pv.isNull()) return .null;
        const doc = try valueToString(arena, dv);
        try json.validate(arena, doc);
        const leaf = (try json.path(arena, doc, try valueToString(arena, pv))) orelse return .null;
        return switch (try json.cell(arena, leaf)) {
            .null => .null,
            .text => |t| .{ .string = t },
        };
    }

    pub fn strpos(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const sub = try valueToString(arena, pv);
        if (sub.len == 0) return Value{ .int = 1 };
        const at = std.mem.indexOf(u8, s, sub) orelse return Value{ .int = 0 };
        return Value{ .int = @as(i64, @intCast(charCount(s[0..at]))) + 1 };
    }

    pub fn repeat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return .null;
        const n = toI64(nv);
        if (n <= 0) return Value{ .string = "" };
        const s = try valueToString(arena, sv);
        const total = @as(u128, @intCast(n)) * @as(u128, s.len);
        if (total > max_str_bytes) return error.CastFailed;
        const out = try arena.alloc(u8, @intCast(total));
        var i: usize = 0;
        while (i < out.len) : (i += s.len) @memcpy(out[i..][0..s.len], s);
        return Value{ .string = out };
    }

    pub fn reverse(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        return Value{ .string = try reverseChars(arena, try valueToString(arena, sv)) };
    }

    pub fn dateAddDiff(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        if (c.args[0].* != .str_lit) return error.TypeMismatch;
        const u = timeUnit(c.args[0].str_lit) orelse return error.TypeMismatch;
        const a = try evalRow(arena, c.args[1], batch, row);
        const b = try evalRow(arena, c.args[2], batch, row);
        if (a.isNull() or b.isNull()) return .null;
        if (eq(c.name, "date_add")) return try addUnits(b, u, toI64(a));
        const a_us = temporalMicros(a) orelse return error.TypeMismatch;
        const b_us = temporalMicros(b) orelse return error.TypeMismatch;
        return Value{ .int = dateDiff(a_us, b_us, u) };
    }

    pub fn makeDate(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const yv = try evalRow(arena, c.args[0], batch, row);
        const mv = try evalRow(arena, c.args[1], batch, row);
        const dv = try evalRow(arena, c.args[2], batch, row);
        if (yv.isNull() or mv.isNull() or dv.isNull()) return .null;
        const y = toI64(yv);
        const m = toI64(mv);
        const d = toI64(dv);
        if (m < 1 or m > 12) return error.CastFailed;
        if (d < 1 or d > daysInMonth(y, @intCast(m))) return error.CastFailed;
        const days = daysFromCivil(y, @intCast(m), @intCast(d));
        return Value{ .date = std.math.cast(i32, days) orelse return error.CastFailed };
    }

    pub fn epoch(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        return Value{ .int = @divFloor(us, 1_000_000) };
    }

    pub fn toTimestamp(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return Value{ .timestamp = try mulI64(toI64(v), 1_000_000) };
    }

    pub fn strftime(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const fv = try evalRow(arena, c.args[1], batch, row);
        if (fv.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        return Value{ .string = try strftimeFmt(arena, us, try valueToString(arena, fv)) };
    }

    pub fn strptime(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const fv = try evalRow(arena, c.args[1], batch, row);
        if (fv.isNull()) return .null;
        const text = try valueToString(arena, v);
        const fmt = try valueToString(arena, fv);
        const us = strptimeFmt(text, fmt) orelse {
            if (eq(c.name, "try_strptime")) return .null;
            return failWith(error.CastFailed, "strptime: '{s}' is not a date in '{s}' (try_strptime gives null)", .{ clip(text), clip(fmt) });
        };
        return .{ .timestamp = us };
    }

    pub fn unaccent(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        if (isAscii(str)) return .{ .string = str };
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const base = if (w == 1) null else unaccentCp(std.unicode.utf8Decode(str[i..][0..w]) catch unreachable);
            try out.appendSlice(base orelse str[i..][0..w]);
            i += w;
        }
        return .{ .string = out.items };
    }

    /// Postgres' `translate`, by characters: each character of `from` becomes the one
    /// at the same place in `to`, or is deleted when `to` is shorter.
    pub fn translate(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var args: [3][]const u8 = undefined;
        for (&args, c.args) |*o, e| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) return .null;
            o.* = try valueToString(arena, v);
        }
        const str, const from, const to = args;
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const ch = str[i..][0..w];
            i += w;
            var at: usize = 0;
            var k: usize = 0;
            const hit = while (k < from.len) {
                const fw = charWidth(from, k);
                if (std.mem.eql(u8, from[k..][0..fw], ch)) break at;
                k += fw;
                at += 1;
            } else null;
            const idx = hit orelse {
                try out.appendSlice(ch);
                continue;
            };
            const off = charOffset(to, idx);
            if (off < to.len) try out.appendSlice(to[off..][0..charWidth(to, off)]);
        }
        return .{ .string = out.items };
    }

    pub fn initcap(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        var in_word = false;
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const ch = str[i..][0..w];
            i += w;
            const cp: u21 = if (w == 1) ch[0] else std.unicode.utf8Decode(ch) catch unreachable;
            const word = isWordChar(cp, w);
            if (!word) {
                try out.appendSlice(ch);
            } else {
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(caseMap(cp, !in_word), &buf) catch unreachable;
                try out.appendSlice(buf[0..n]);
            }
            in_word = word;
        }
        return .{ .string = out.items };
    }

    pub fn ascii(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        if (str.len == 0) return .{ .int = 0 };
        const w = charWidth(str, 0);
        return .{ .int = if (w == 1) str[0] else std.unicode.utf8Decode(str[0..w]) catch unreachable };
    }

    pub fn chr(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        const bad = "chr: {d} is not a character";
        const cp = std.math.cast(u21, v.int) orelse return failWith(error.CastFailed, bad, .{v.int});
        var buf: [4]u8 = undefined;
        if (cp == 0) return failWith(error.CastFailed, bad, .{v.int});
        const n = std.unicode.utf8Encode(cp, &buf) catch return failWith(error.CastFailed, bad, .{v.int});
        return .{ .string = try arena.dupe(u8, buf[0..n]) };
    }
};

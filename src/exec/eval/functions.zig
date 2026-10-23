//! The built-in scalar function table: each name with its typing rule, its row form
//! and, where one exists, its vector kernel. A function written once as math
//! (`fn_scalar.zig`) gets both forms from `scalar.zig`; the rest have a row form in
//! `fn_row.zig` and, a few, a hand-written kernel in `fn_vec.zig`.

const Batch = @import("../batch.zig").Batch;
const EvalError = @import("../eval.zig").EvalError;
const Type = @import("../../lang/types.zig").Type;
const TypeCtx = @import("../eval.zig").TypeCtx;
const TypeError = @import("../eval.zig").TypeError;
const Value = @import("../value.zig").Value;
const Vec = @import("vec.zig").Vec;
const VecError = @import("vec.zig").VecError;
const ast = @import("../../lang/ast.zig");
const fn_search = @import("fn_search.zig");
const per_row = @import("fn_row.zig").per_row;
const std = @import("std");
const typing = @import("fn_typing.zig").typing;
const vectorized = @import("fn_vec.zig").vectorized;
const f = @import("fn_scalar.zig");

fn S(comptime F: type) type {
    return @import("scalar.zig").Scalar(F);
}

/// An entry whose typing rule, row form and kernel all come from `F`'s declaration.
fn auto(comptime name: []const u8, comptime F: type) Builtin {
    return .{
        .name = name,
        .type_fn = @import("scalar.zig").typeRule(F),
        .eval_fn = S(F).eval,
        .vec_fn = if (@hasDecl(F, "vec")) F.vec else S(F).vec,
        .sig = name ++ F.signature,
        .io = @hasDecl(F, "io") and F.io,
    };
}

const r = @import("fn_records.zig");

/// The byte, charset, field-decoding and field-encoding functions (`fn_records.zig`).
const records = blk: {
    const ints = .{ .{ "u8", u8 }, .{ "i8", i8 }, .{ "u16", u16 }, .{ "i16", i16 }, .{ "u32", u32 }, .{ "i32", i32 }, .{ "u64", u64 }, .{ "i64", i64 } };
    const floats = .{ .{ "f32", f32 }, .{ "f64", f64 } };
    var out: []const Builtin = &.{
        auto("byte_len", r.ByteLen),           auto("byte_slice", r.ByteSlice),     auto("byte_at", r.ByteAt),
        auto("byte_find", r.ByteFind),         auto("hex", r.Hex),                  auto("unhex", r.Unhex),
        auto("decode", r.Decode),              auto("encode", r.Encode),            auto("text_at", r.TextAt),
        auto("int_at", r.IntAt),               auto("decimal_at", r.DecimalAt),     auto("date_at", r.DateAt),
        auto("time_at", r.TimeAt),             auto("timestamp_at", r.TimestampAt), auto("zoned_at", r.ZonedAt),
        auto("packed_at", r.PackedAt),         auto("fmt_text", r.FmtText),         auto("fmt_int", r.FmtInt),
        auto("fmt_decimal", r.FmtDecimal),     auto("fmt_date", r.FmtDate),         auto("fmt_time", r.FmtTime),
        auto("fmt_timestamp", r.FmtTimestamp), auto("fmt_zoned", r.FmtZoned),       auto("fmt_packed", r.FmtPacked),
        auto("read_bytes", r.ReadBytes),       auto("file_size", r.FileSize),
    };
    for (ints) |it| {
        const one = @sizeOf(it[1]) == 1;
        for (if (one) .{.little} else .{ .little, .big }) |e| {
            const suffix = if (one) "" else if (e == .little) "le" else "be";
            out = out ++ .{ auto(it[0] ++ suffix ++ "_at", r.BinIntAt(it[1], e)), auto("fmt_" ++ it[0] ++ suffix, r.FmtBinInt(it[1], e)) };
        }
    }
    for (floats) |ft| for (.{ .little, .big }) |e| {
        const suffix = if (e == .little) "le" else "be";
        out = out ++ .{ auto(ft[0] ++ suffix ++ "_at", r.BinFloatAt(ft[1], e)), auto("fmt_" ++ ft[0] ++ suffix, r.FmtBinFloat(ft[1], e)) };
    };
    break :blk out;
};

pub const max_str_bytes = 1 << 20;

/// `mod(a, b)` is `a % b`, in both forms.
const Mod = struct {
    fn node(c: ast.Expr.Call) ast.Expr {
        return .{ .binary = .{ .op = .mod, .l = c.args[0], .r = c.args[1] } };
    }

    fn eval(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var e = node(c);
        return @import("row.zig").evalRow(arena, &e, batch, row);
    }

    fn vec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        var e = node(c);
        return @import("vec.zig").evalVecNode(arena, &e, batch);
    }
};

pub const Builtin = struct {
    name: []const u8,
    type_fn: *const fn (*TypeCtx, ast.Expr.Call) TypeError!Type,
    eval_fn: *const fn (std.mem.Allocator, ast.Expr.Call, Batch, usize) EvalError!Value,
    vec_fn: ?*const fn (std.mem.Allocator, ast.Expr.Call, Batch) VecError!Vec = null,
    sig: ?[]const u8 = null,
    io: bool = false,
};

pub const builtins = base ++ records[0..records.len].*;

const base = [_]Builtin{
    .{ .name = "now", .type_fn = typing.now, .eval_fn = per_row.now, .vec_fn = vectorized.now },
    .{ .name = "today", .type_fn = typing.today, .eval_fn = per_row.today, .vec_fn = vectorized.today },
    .{ .name = "regexp_replace", .type_fn = typing.regexpReplace, .eval_fn = per_row.regexpReplace },
    .{ .name = "regexp_matches", .type_fn = typing.regexpMatches, .eval_fn = per_row.regexpMatches },
    .{ .name = "regexp_extract", .type_fn = typing.regexpExtract, .eval_fn = per_row.regexpExtract },
    .{ .name = "md5", .type_fn = typing.digest, .eval_fn = per_row.digest },
    .{ .name = "sha256", .type_fn = typing.digest, .eval_fn = per_row.digest },
    .{ .name = "xxhash64", .type_fn = typing.digest, .eval_fn = per_row.digest },
    .{ .name = "concat_ws", .type_fn = typing.concatWs, .eval_fn = per_row.concatWs },
    .{ .name = "date_trunc", .type_fn = typing.dateTruncExtract, .eval_fn = S(f.DateTrunc).eval, .vec_fn = S(f.DateTrunc).vec },
    .{ .name = "extract", .type_fn = typing.dateTruncExtract, .eval_fn = S(f.Extract).eval, .vec_fn = S(f.Extract).vec },
    .{ .name = "upper", .type_fn = typing.unaryString, .eval_fn = S(f.CaseMap(true)).eval, .vec_fn = S(f.CaseMap(true)).vec },
    .{ .name = "lower", .type_fn = typing.unaryString, .eval_fn = S(f.CaseMap(false)).eval, .vec_fn = S(f.CaseMap(false)).vec },
    .{ .name = "length", .type_fn = typing.strlen, .eval_fn = S(f.Length(true)).eval, .vec_fn = S(f.Length(true)).vec },
    .{ .name = "strlen", .type_fn = typing.strlen, .eval_fn = S(f.Length(false)).eval, .vec_fn = S(f.Length(false)).vec },
    .{ .name = "bit_count", .type_fn = typing.bitCountToHex, .eval_fn = S(f.BitCount).eval, .vec_fn = S(f.BitCount).vec },
    .{ .name = "to_hex", .type_fn = typing.bitCountToHex, .eval_fn = S(f.ToHex).eval, .vec_fn = S(f.ToHex).vec },
    .{ .name = "from_hex", .type_fn = typing.fromHex, .eval_fn = S(f.FromHex).eval, .vec_fn = S(f.FromHex).vec },
    .{ .name = "concat", .type_fn = typing.concat, .eval_fn = S(f.Concat).eval, .vec_fn = S(f.Concat).vec },
    .{ .name = "coalesce", .type_fn = typing.coalesce, .eval_fn = per_row.coalesce, .vec_fn = vectorized.coalesce },
    .{ .name = "starts_with", .type_fn = typing.strPredicate, .eval_fn = S(f.Match(.starts_with)).eval, .vec_fn = S(f.Match(.starts_with)).vec },
    .{ .name = "ends_with", .type_fn = typing.strPredicate, .eval_fn = S(f.Match(.ends_with)).eval, .vec_fn = S(f.Match(.ends_with)).vec },
    .{ .name = "contains", .type_fn = typing.strPredicate, .eval_fn = S(f.Match(.contains)).eval, .vec_fn = S(f.Match(.contains)).vec },
    .{ .name = "search", .type_fn = fn_search.typeSearch, .eval_fn = fn_search.rowSearch, .vec_fn = fn_search.vecSearch },
    .{ .name = "like", .type_fn = typing.strPredicate, .eval_fn = S(f.Match(.like)).eval, .vec_fn = S(f.Match(.like)).vec },
    .{ .name = "trim", .type_fn = typing.unaryString, .eval_fn = S(f.Trim).eval, .vec_fn = S(f.Trim).vec },
    .{ .name = "substr", .type_fn = typing.substr, .eval_fn = S(f.Substr).eval, .vec_fn = S(f.Substr).vec },
    .{ .name = "replace", .type_fn = typing.replace, .eval_fn = S(f.Replace).eval, .vec_fn = S(f.Replace).vec },
    .{ .name = "abs", .type_fn = typing.abs, .eval_fn = f.Abs.eval, .vec_fn = f.Abs.vec },
    .{ .name = "floor", .type_fn = typing.floorCeil, .eval_fn = f.Whole(true).eval, .vec_fn = f.Whole(true).vec },
    .{ .name = "ceil", .type_fn = typing.floorCeil, .eval_fn = f.Whole(false).eval, .vec_fn = f.Whole(false).vec },
    .{ .name = "round", .type_fn = typing.round, .eval_fn = f.Round.eval, .vec_fn = f.Round.vec },
    .{ .name = "mod", .type_fn = typing.mod, .eval_fn = Mod.eval, .vec_fn = Mod.vec },
    .{ .name = "power", .type_fn = typing.power, .eval_fn = S(f.Power).eval, .vec_fn = S(f.Power).vec },
    .{ .name = "sqrt", .type_fn = typing.sqrt, .eval_fn = S(f.Sqrt).eval, .vec_fn = S(f.Sqrt).vec },
    .{ .name = "sign", .type_fn = typing.sign, .eval_fn = f.Sign.eval, .vec_fn = f.Sign.vec },
    .{ .name = "nullif", .type_fn = typing.nullif, .eval_fn = per_row.nullif, .vec_fn = vectorized.nullif },
    .{ .name = "greatest", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast, .vec_fn = vectorized.Extreme(true).vec },
    .{ .name = "least", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast, .vec_fn = vectorized.Extreme(false).vec },
    .{ .name = "lpad", .type_fn = typing.pad, .eval_fn = S(f.Pad(true)).eval, .vec_fn = S(f.Pad(true)).vec },
    .{ .name = "rpad", .type_fn = typing.pad, .eval_fn = S(f.Pad(false)).eval, .vec_fn = S(f.Pad(false)).vec },
    .{ .name = "left", .type_fn = typing.leftRight, .eval_fn = S(f.End(true)).eval, .vec_fn = S(f.End(true)).vec },
    .{ .name = "right", .type_fn = typing.leftRight, .eval_fn = S(f.End(false)).eval, .vec_fn = S(f.End(false)).vec },
    .{ .name = "split_part", .type_fn = typing.splitPart, .eval_fn = S(f.SplitPart).eval, .vec_fn = S(f.SplitPart).vec },
    .{ .name = "strpos", .type_fn = typing.strpos, .eval_fn = S(f.Strpos).eval, .vec_fn = S(f.Strpos).vec },
    .{ .name = "repeat", .type_fn = typing.repeat, .eval_fn = S(f.Repeat).eval, .vec_fn = S(f.Repeat).vec },
    .{ .name = "reverse", .type_fn = typing.reverse, .eval_fn = S(f.Reverse).eval, .vec_fn = S(f.Reverse).vec },
    .{ .name = "date_add", .type_fn = typing.dateAdd, .eval_fn = f.DateAdd.eval, .vec_fn = f.DateAdd.vec },
    .{ .name = "date_diff", .type_fn = typing.dateDiff, .eval_fn = S(f.DateDiff).eval, .vec_fn = S(f.DateDiff).vec },
    .{ .name = "make_date", .type_fn = typing.makeDate, .eval_fn = S(f.MakeDate).eval, .vec_fn = S(f.MakeDate).vec },
    .{ .name = "epoch", .type_fn = typing.epoch, .eval_fn = S(f.Epoch).eval, .vec_fn = S(f.Epoch).vec },
    .{ .name = "to_timestamp", .type_fn = typing.toTimestamp, .eval_fn = f.ToTimestamp.eval, .vec_fn = f.ToTimestamp.vec },
    .{ .name = "strftime", .type_fn = typing.strftime, .eval_fn = S(f.Strftime).eval, .vec_fn = S(f.Strftime).vec },
    .{ .name = "strptime", .type_fn = typing.strptime, .eval_fn = S(f.Strptime(false)).eval, .vec_fn = S(f.Strptime(false)).vec },
    .{ .name = "try_strptime", .type_fn = typing.strptime, .eval_fn = S(f.Strptime(true)).eval, .vec_fn = S(f.Strptime(true)).vec },
    .{ .name = "unaccent", .type_fn = typing.unaryString, .eval_fn = S(f.Unaccent).eval, .vec_fn = S(f.Unaccent).vec },
    .{ .name = "strip_accents", .type_fn = typing.unaryString, .eval_fn = S(f.Unaccent).eval, .vec_fn = S(f.Unaccent).vec },
    .{ .name = "translate", .type_fn = typing.translate, .eval_fn = S(f.Translate).eval, .vec_fn = S(f.Translate).vec },
    .{ .name = "initcap", .type_fn = typing.unaryString, .eval_fn = S(f.Initcap).eval, .vec_fn = S(f.Initcap).vec },
    .{ .name = "ascii", .type_fn = typing.strlen, .eval_fn = S(f.Ascii).eval, .vec_fn = S(f.Ascii).vec },
    .{ .name = "chr", .type_fn = typing.chr, .eval_fn = S(f.Chr).eval, .vec_fn = S(f.Chr).vec },
    .{ .name = "json_get", .type_fn = typing.jsonGet, .eval_fn = per_row.jsonGet },
    .{ .name = "json_filter", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_transform", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_any", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_all", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_reduce", .type_fn = typing.jsonReduce, .eval_fn = per_row.jsonReduce },
    .{ .name = "chars", .type_fn = typing.chars, .eval_fn = per_row.chars },
    .{ .name = "json_range", .type_fn = typing.jsonRange, .eval_fn = per_row.jsonRange },
    .{ .name = "json_length", .type_fn = typing.jsonLength, .eval_fn = per_row.jsonLength },
    .{ .name = "json_slice", .type_fn = typing.jsonSlice, .eval_fn = per_row.jsonSlice },
    .{ .name = "json_concat", .type_fn = typing.jsonConcat, .eval_fn = per_row.jsonConcat },
    .{ .name = "json_object", .type_fn = typing.jsonBuild, .eval_fn = per_row.jsonObject },
    .{ .name = "json_array", .type_fn = typing.jsonBuild, .eval_fn = per_row.jsonArray },
    .{ .name = "to_base64", .type_fn = typing.unaryString, .eval_fn = S(f.ToBase64).eval, .vec_fn = S(f.ToBase64).vec },
    .{ .name = "from_base64", .type_fn = typing.fromBase64, .eval_fn = per_row.fromBase64 },
    .{ .name = "url_encode", .type_fn = typing.unaryString, .eval_fn = S(f.UrlCode(true)).eval, .vec_fn = S(f.UrlCode(true)).vec },
    .{ .name = "url_decode", .type_fn = typing.unaryString, .eval_fn = S(f.UrlCode(false)).eval, .vec_fn = S(f.UrlCode(false)).vec },
};

pub fn lookupBuiltin(name: []const u8) ?*const Builtin {
    const map = comptime blk: {
        var kvs: [builtins.len]struct { []const u8, usize } = undefined;
        for (builtins, 0..) |b, i| kvs[i] = .{ b.name, i };
        break :blk std.StaticStringMap(usize).initComptime(kvs);
    };
    const i = map.get(name) orelse return null;
    return &builtins[i];
}

const testing_util = @import("testing_util.zig");
const column = @import("../column.zig");
const types = @import("../../lang/types.zig");
const Decimal = @import("../value.zig").Decimal;

fn testBatch(a: std.mem.Allocator) !Batch {
    const nan = std.math.nan(f64);
    const inf = std.math.inf(f64);
    const fs = [_]?f64{ 2.5, -2.5, null, nan, -0.0, inf, -inf, 0.125, 1234.5678 };
    const ds = [_]?Decimal{ .{ .unscaled = 125, .scale = 2 }, .{ .unscaled = -15, .scale = 1 }, null, .{ .unscaled = 3, .scale = 0 }, .{ .unscaled = -5, .scale = 2 }, .{ .unscaled = 255, .scale = 2 }, .{ .unscaled = 5, .scale = 1 }, .{ .unscaled = -1235, .scale = 2 }, .{ .unscaled = 99999, .scale = 2 } };
    const ss = [_]?[]const u8{ "pear", null, "apple", "", "fig", "b", "zz", "b", "kiwi" };
    const dts = [_]?i32{ 18000, -1, null, 0, 19357, -719162, 11016, 18321, 2932896 };
    const ts = [_]?i64{ 1_600_000_123_456_789, -1, null, 0, 1_672_531_199_999_999, -62_135_596_800_000_000, 951_782_400_000_000, 1_000_000_000_000_000, 253_402_300_799_999_999 };
    const us = [_]?[]const u8{ "2024-02-29", "2023-02-29", null, "1999-12-31", "abc", "", "2000-01-01", "20-1-1", "2024-13-01" };
    var fb = column.Builder.init(a, Type.init(.float).asNullable());
    var db = column.Builder.init(a, Type.decimal(10, 2).asNullable());
    var sb = column.Builder.init(a, Type.init(.string).asNullable());
    var dtb = column.Builder.init(a, Type.init(.date).asNullable());
    var tb = column.Builder.init(a, Type.init(.timestamp).asNullable());
    var ub = column.Builder.init(a, Type.init(.string).asNullable());
    for (fs, ds, ss, dts, ts, us) |x, d, s, dt, t, u| {
        try dtb.append(if (dt) |v| .{ .date = v } else .null);
        try tb.append(if (t) |v| .{ .timestamp = v } else .null);
        try ub.append(if (u) |v| .{ .string = v } else .null);
        try fb.append(if (x) |v| .{ .float = v } else .null);
        try db.append(if (d) |v| .{ .decimal = v } else .null);
        try sb.append(if (s) |v| .{ .string = v } else .null);
    }
    const schema = try a.create(types.Schema);
    schema.* = .{ .fields = try a.dupe(types.Schema.Field, &.{
        .{ .name = "i", .ty = Type.init(.int).asNullable() },
        .{ .name = "z", .ty = Type.init(.int).asNullable() },
        .{ .name = "g", .ty = Type.init(.int).asNullable() },
        .{ .name = "m", .ty = Type.init(.int) },
        .{ .name = "f", .ty = Type.init(.float).asNullable() },
        .{ .name = "d", .ty = Type.decimal(10, 2).asNullable() },
        .{ .name = "s", .ty = Type.init(.string).asNullable() },
        .{ .name = "dt", .ty = Type.init(.date).asNullable() },
        .{ .name = "t", .ty = Type.init(.timestamp).asNullable() },
        .{ .name = "u", .ty = Type.init(.string).asNullable() },
    }) };
    const cols = try a.alloc(column.Column, 10);
    cols[0] = try column.intColumn(a, &.{ 5, -7, null, 0, 3, -2, 1000, 3, -45 });
    cols[1] = try column.intColumn(a, &.{ 0, 3, 1, null, 3, -2, 2, 0, 7 });
    cols[2] = try column.intColumn(a, &.{ 1, null, 0, -1, 2, 3, 1, 0, -2 });
    cols[3] = try column.intColumn(a, &.{ std.math.minInt(i64), 1, 2, 3, 4, 5, 6, 7, 8 });
    cols[4] = try fb.finish();
    cols[5] = try db.finish();
    cols[6] = try sb.finish();
    cols[7] = try dtb.finish();
    cols[8] = try tb.finish();
    cols[9] = try ub.finish();
    return .{ .schema = schema, .columns = cols, .len = fs.len };
}

test "numeric builtins: the kernels give the row forms' values, nulls, scales and errors" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const batch = try testBatch(a);
    try testing_util.expectVecMatchesRows(a, batch, &.{
        "abs(i)",           "abs(f)",           "abs(d)",          "abs(-3)",          "abs(-1.50)",
        "floor(i)",         "floor(f)",         "floor(d)",        "ceil(f)",          "ceil(d)",
        "round(i)",         "round(i, 1)",      "round(f)",        "round(f, 1)",      "round(f, -1)",
        "round(f, g)",      "round(d)",         "round(d, 1)",     "round(d, -1)",     "round(d, g)",
        "round(d, 5)",      "round(i, g)",      "sign(i)",         "sign(f)",          "sign(d)",
        "mod(i, 3)",        "mod(f, 2e0)",      "mod(d, 3)",       "mod(d, 0.4)",      "nullif(i, 3)",
        "nullif(i, z)",     "nullif(f, f)",     "nullif(d, 1.25)", "nullif(s, 'b')",   "greatest(i, z)",
        "least(i, z, 2)",   "greatest(f, 0e0)", "least(f, -1e0)",  "greatest(d, 1.5)", "least(i, null, z)",
        "greatest(s, 'b')", "least(s, s)",      "greatest(d, d)",  "least(z, i, g)",   "greatest(null, i)",
    }, true);
    try testing_util.expectVecMatchesRows(a, batch, &.{
        "abs(m)",                  "mod(i, z)",      "round(i, 1 / z)", "nullif(i, 1.0)",
        "greatest(i, f)",          "greatest(i, d)", "nullif(f, i)",    "round(d, 40)",
        "if(z = 0, 0, mod(i, z))",
    }, false);
}

test "temporal builtins: the kernels give the row forms' values, nulls and errors" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const batch = try testBatch(a);
    try testing_util.expectVecMatchesRows(a, batch, &.{
        "date_trunc('year', t)",                                "date_trunc('month', t)",                        "date_trunc('week', t)",
        "date_trunc('day', t)",                                 "date_trunc('hour', t)",                         "date_trunc('minute', t)",
        "date_trunc('second', t)",                              "date_trunc('MONTH', dt)",                       "date_trunc('week', dt)",
        "extract('year', t)",                                   "extract('month', dt)",                          "extract('week', t)",
        "extract('week', dt)",                                  "extract('day', t)",                             "extract('hour', t)",
        "extract('minute', t)",                                 "extract('second', t)",                          "extract('second', dt)",
        "date_add('day', i, dt)",                               "date_add('month', g, dt)",                      "date_add('year', z, t)",
        "date_add('hour', i, t)",                               "date_add('week', g, dt)",                       "date_add('second', i, t)",
        "date_add('month', 1, t)",                              "date_diff('day', t, dt)",                       "date_diff('month', dt, t)",
        "date_diff('week', t, t)",                              "date_diff('year', dt, date_add('day', i, dt))", "date_diff('hour', t, date_add('minute', i, t))",
        "make_date(2000 + i, 1 + mod(abs(z), 12), 1 + abs(g))", "epoch(t)",                                      "epoch(dt)",
        "to_timestamp(i)",                                      "to_timestamp(d)",                               "to_timestamp(if(f < 1e6 and f > -1e6, f, 0e0))",
        "to_timestamp(1.2345675)",                              "strftime(t, '%Y-%m-%d %H:%M:%S')",              "strftime(dt, '%y/%m/%d %%')",
        "strftime(t, s)",                                       "try_strptime(u, '%Y-%m-%d')",                   "try_strptime(s, u)",
        "strptime('2020-02-02', '%Y-%m-%d')",                   "greatest(dt, date_add('day', 3, dt))",          "least(t, date_trunc('day', t))",
    }, true);
    try testing_util.expectVecMatchesRows(a, batch, &.{
        "strptime(u, '%Y-%m-%d')",                                    "make_date(i, z, g)",                                        "make_date(nullif(z, 0), 1 / z, 1)",
        "date_add('day', 1 / z, dt)",                                 "date_add('day', nullif(z, 0), date_add('day', 1 / z, dt))", "to_timestamp(f)",
        "date_diff('day', nullif(t, t), date_add('day', 1 / z, dt))", "date_add('year', m, dt)",                                   "date_add('second', m, t)",
        "strftime(t, concat('%', s))",                                "date_add('month', i * 100000000, dt)",
    }, false);
}

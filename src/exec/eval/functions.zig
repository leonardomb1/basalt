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
    .{ .name = "date_trunc", .type_fn = typing.dateTruncExtract, .eval_fn = per_row.dateTruncExtract },
    .{ .name = "extract", .type_fn = typing.dateTruncExtract, .eval_fn = per_row.dateTruncExtract },
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
    .{ .name = "abs", .type_fn = typing.abs, .eval_fn = per_row.abs },
    .{ .name = "floor", .type_fn = typing.floorCeil, .eval_fn = per_row.floorCeil },
    .{ .name = "ceil", .type_fn = typing.floorCeil, .eval_fn = per_row.floorCeil },
    .{ .name = "round", .type_fn = typing.round, .eval_fn = per_row.round },
    .{ .name = "mod", .type_fn = typing.mod, .eval_fn = per_row.mod },
    .{ .name = "power", .type_fn = typing.power, .eval_fn = S(f.Power).eval, .vec_fn = S(f.Power).vec },
    .{ .name = "sqrt", .type_fn = typing.sqrt, .eval_fn = S(f.Sqrt).eval, .vec_fn = S(f.Sqrt).vec },
    .{ .name = "sign", .type_fn = typing.sign, .eval_fn = per_row.sign },
    .{ .name = "nullif", .type_fn = typing.nullif, .eval_fn = per_row.nullif },
    .{ .name = "greatest", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast },
    .{ .name = "least", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast },
    .{ .name = "lpad", .type_fn = typing.pad, .eval_fn = S(f.Pad(true)).eval, .vec_fn = S(f.Pad(true)).vec },
    .{ .name = "rpad", .type_fn = typing.pad, .eval_fn = S(f.Pad(false)).eval, .vec_fn = S(f.Pad(false)).vec },
    .{ .name = "left", .type_fn = typing.leftRight, .eval_fn = S(f.End(true)).eval, .vec_fn = S(f.End(true)).vec },
    .{ .name = "right", .type_fn = typing.leftRight, .eval_fn = S(f.End(false)).eval, .vec_fn = S(f.End(false)).vec },
    .{ .name = "split_part", .type_fn = typing.splitPart, .eval_fn = S(f.SplitPart).eval, .vec_fn = S(f.SplitPart).vec },
    .{ .name = "strpos", .type_fn = typing.strpos, .eval_fn = S(f.Strpos).eval, .vec_fn = S(f.Strpos).vec },
    .{ .name = "repeat", .type_fn = typing.repeat, .eval_fn = S(f.Repeat).eval, .vec_fn = S(f.Repeat).vec },
    .{ .name = "reverse", .type_fn = typing.reverse, .eval_fn = S(f.Reverse).eval, .vec_fn = S(f.Reverse).vec },
    .{ .name = "date_add", .type_fn = typing.dateAdd, .eval_fn = per_row.dateAddDiff },
    .{ .name = "date_diff", .type_fn = typing.dateDiff, .eval_fn = per_row.dateAddDiff },
    .{ .name = "make_date", .type_fn = typing.makeDate, .eval_fn = per_row.makeDate },
    .{ .name = "epoch", .type_fn = typing.epoch, .eval_fn = per_row.epoch },
    .{ .name = "to_timestamp", .type_fn = typing.toTimestamp, .eval_fn = per_row.toTimestamp },
    .{ .name = "strftime", .type_fn = typing.strftime, .eval_fn = per_row.strftime },
    .{ .name = "strptime", .type_fn = typing.strptime, .eval_fn = per_row.strptime },
    .{ .name = "try_strptime", .type_fn = typing.strptime, .eval_fn = per_row.strptime },
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

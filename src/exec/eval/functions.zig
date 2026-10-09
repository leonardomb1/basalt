//! The built-in scalar function table: each name with its typing rule, its row form
//! and, where one exists, its vector kernel (fn_typing, fn_row, fn_vec).

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

pub const max_str_bytes = 1 << 20;

pub const Builtin = struct {
    name: []const u8,
    type_fn: *const fn (*TypeCtx, ast.Expr.Call) TypeError!Type,
    eval_fn: *const fn (std.mem.Allocator, ast.Expr.Call, Batch, usize) EvalError!Value,
    vec_fn: ?*const fn (std.mem.Allocator, ast.Expr.Call, Batch) VecError!Vec = null,
};

pub const builtins = [_]Builtin{
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
    .{ .name = "upper", .type_fn = typing.unaryString, .eval_fn = per_row.upperLower, .vec_fn = vectorized.upperLower },
    .{ .name = "lower", .type_fn = typing.unaryString, .eval_fn = per_row.upperLower, .vec_fn = vectorized.upperLower },
    .{ .name = "length", .type_fn = typing.strlen, .eval_fn = per_row.strlen, .vec_fn = vectorized.strlen },
    .{ .name = "strlen", .type_fn = typing.strlen, .eval_fn = per_row.strlen, .vec_fn = vectorized.strlen },
    .{ .name = "bit_count", .type_fn = typing.bitCountToHex, .eval_fn = per_row.bitCount },
    .{ .name = "to_hex", .type_fn = typing.bitCountToHex, .eval_fn = per_row.toHex },
    .{ .name = "from_hex", .type_fn = typing.fromHex, .eval_fn = per_row.fromHex },
    .{ .name = "concat", .type_fn = typing.concat, .eval_fn = per_row.concat, .vec_fn = vectorized.concat },
    .{ .name = "coalesce", .type_fn = typing.coalesce, .eval_fn = per_row.coalesce, .vec_fn = vectorized.coalesce },
    .{ .name = "starts_with", .type_fn = typing.strPredicate, .eval_fn = per_row.affix, .vec_fn = vectorized.strPredicate },
    .{ .name = "ends_with", .type_fn = typing.strPredicate, .eval_fn = per_row.affix, .vec_fn = vectorized.strPredicate },
    .{ .name = "contains", .type_fn = typing.strPredicate, .eval_fn = per_row.affix, .vec_fn = vectorized.strPredicate },
    .{ .name = "search", .type_fn = fn_search.typeSearch, .eval_fn = fn_search.rowSearch, .vec_fn = fn_search.vecSearch },
    .{ .name = "like", .type_fn = typing.strPredicate, .eval_fn = per_row.like, .vec_fn = vectorized.strPredicate },
    .{ .name = "trim", .type_fn = typing.unaryString, .eval_fn = per_row.trimSpace, .vec_fn = vectorized.trimSpace },
    .{ .name = "substr", .type_fn = typing.substr, .eval_fn = per_row.substr, .vec_fn = vectorized.substr },
    .{ .name = "replace", .type_fn = typing.replace, .eval_fn = per_row.replace, .vec_fn = vectorized.replace },
    .{ .name = "abs", .type_fn = typing.abs, .eval_fn = per_row.abs },
    .{ .name = "floor", .type_fn = typing.floorCeil, .eval_fn = per_row.floorCeil },
    .{ .name = "ceil", .type_fn = typing.floorCeil, .eval_fn = per_row.floorCeil },
    .{ .name = "round", .type_fn = typing.round, .eval_fn = per_row.round },
    .{ .name = "mod", .type_fn = typing.mod, .eval_fn = per_row.mod },
    .{ .name = "power", .type_fn = typing.power, .eval_fn = per_row.power },
    .{ .name = "sqrt", .type_fn = typing.sqrt, .eval_fn = per_row.sqrt },
    .{ .name = "sign", .type_fn = typing.sign, .eval_fn = per_row.sign },
    .{ .name = "nullif", .type_fn = typing.nullif, .eval_fn = per_row.nullif },
    .{ .name = "greatest", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast },
    .{ .name = "least", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast },
    .{ .name = "lpad", .type_fn = typing.pad, .eval_fn = per_row.pad },
    .{ .name = "rpad", .type_fn = typing.pad, .eval_fn = per_row.pad },
    .{ .name = "left", .type_fn = typing.leftRight, .eval_fn = per_row.leftRight },
    .{ .name = "right", .type_fn = typing.leftRight, .eval_fn = per_row.leftRight },
    .{ .name = "split_part", .type_fn = typing.splitPart, .eval_fn = per_row.splitPart },
    .{ .name = "strpos", .type_fn = typing.strpos, .eval_fn = per_row.strpos },
    .{ .name = "repeat", .type_fn = typing.repeat, .eval_fn = per_row.repeat },
    .{ .name = "reverse", .type_fn = typing.reverse, .eval_fn = per_row.reverse },
    .{ .name = "date_add", .type_fn = typing.dateAdd, .eval_fn = per_row.dateAddDiff },
    .{ .name = "date_diff", .type_fn = typing.dateDiff, .eval_fn = per_row.dateAddDiff },
    .{ .name = "make_date", .type_fn = typing.makeDate, .eval_fn = per_row.makeDate },
    .{ .name = "epoch", .type_fn = typing.epoch, .eval_fn = per_row.epoch },
    .{ .name = "to_timestamp", .type_fn = typing.toTimestamp, .eval_fn = per_row.toTimestamp },
    .{ .name = "strftime", .type_fn = typing.strftime, .eval_fn = per_row.strftime },
    .{ .name = "strptime", .type_fn = typing.strptime, .eval_fn = per_row.strptime },
    .{ .name = "try_strptime", .type_fn = typing.strptime, .eval_fn = per_row.strptime },
    .{ .name = "unaccent", .type_fn = typing.unaryString, .eval_fn = per_row.unaccent },
    .{ .name = "strip_accents", .type_fn = typing.unaryString, .eval_fn = per_row.unaccent },
    .{ .name = "translate", .type_fn = typing.translate, .eval_fn = per_row.translate },
    .{ .name = "initcap", .type_fn = typing.unaryString, .eval_fn = per_row.initcap },
    .{ .name = "ascii", .type_fn = typing.strlen, .eval_fn = per_row.ascii },
    .{ .name = "chr", .type_fn = typing.chr, .eval_fn = per_row.chr },
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
    .{ .name = "to_base64", .type_fn = typing.unaryString, .eval_fn = per_row.toBase64 },
    .{ .name = "from_base64", .type_fn = typing.fromBase64, .eval_fn = per_row.fromBase64 },
    .{ .name = "url_encode", .type_fn = typing.unaryString, .eval_fn = per_row.urlCode },
    .{ .name = "url_decode", .type_fn = typing.unaryString, .eval_fn = per_row.urlCode },
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

//! The typing rules of the built-in functions: argument checks and result types,
//! applied when a script is checked, before any row is read.

const Type = @import("../../lang/types.zig").Type;
const TypeCtx = @import("../eval.zig").TypeCtx;
const TypeError = @import("../eval.zig").TypeError;
const ast = @import("../../lang/ast.zig");
const badStrftime = @import("time.zig").badStrftime;
const bindParams = @import("row.zig").bindParams;
const boolish = @import("../eval.zig").boolish;
const comparable = @import("../eval.zig").comparable;
const dateDiff = @import("time.zig").dateDiff;
const decimalArithType = @import("../decimal.zig").decimalArithType;
const eq = @import("support.zig").eq;
const intish = @import("../eval.zig").intish;
const lambdaSlots = @import("row.zig").lambdaSlots;
const numericish = @import("../eval.zig").numericish;
const regex = @import("../regex.zig");
const roundOutScale = @import("../decimal.zig").roundOutScale;
const std = @import("std");
const temporalish = @import("../eval.zig").temporalish;
const timeUnit = @import("time.zig").timeUnit;
const types = @import("../../lang/types.zig");

pub const typing = struct {
    pub fn now(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 0) return self.err("`now` takes no arguments", .{});
        return Type.init(.timestamp);
    }

    pub fn today(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 0) return self.err("`today` takes no arguments", .{});
        return Type.init(.date);
    }

    pub fn regexpReplace(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3 and c.args.len != 4) return self.err("`regexp_replace` takes (string, pattern, replacement[, flags])", .{});
        _ = try literalPattern(self, c);
        const a = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantText(c, 2);
        if (c.args.len == 4) {
            _ = try self.wantText(c, 3);
            if (c.args[3].* == .str_lit and regex.parseFlags(c.args[3].str_lit) == null)
                return self.err("`regexp_replace` flags are `g` (every match) and `i` (ignore case), not `{s}`", .{c.args[3].str_lit});
        }
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn literalPattern(self: *TypeCtx, c: ast.Expr.Call) TypeError!?u8 {
        if (c.args[1].* != .str_lit) return null;
        var pbuf: [16 * 1024]u8 = undefined;
        var pfba = std.heap.FixedBufferAllocator.init(&pbuf);
        const re = regex.Regex.compile(pfba.allocator(), c.args[1].str_lit) catch
            return self.err("invalid regular expression `{s}`", .{c.args[1].str_lit});
        return re.ngroups;
    }

    pub fn regexpMatches(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`regexp_matches` takes (string, pattern)", .{});
        _ = try literalPattern(self, c);
        const a = try self.wantText(c, 0);
        const p = try self.wantText(c, 1);
        return Type.init(.bool).withNull(a.nullable or p.nullable);
    }

    pub fn regexpExtract(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 and c.args.len != 3) return self.err("`regexp_extract` takes (string, pattern[, group])", .{});
        const groups = try literalPattern(self, c);
        _ = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        if (c.args.len == 3) {
            if (c.args[2].* != .int_lit) return self.err("`regexp_extract` needs a literal group number", .{});
            const g = c.args[2].int_lit;
            if (g < 0 or g >= regex.max_groups or (groups != null and g >= groups.?))
                return self.err("`regexp_extract` group {d} is not in the pattern", .{g});
        }
        return Type.init(.string).withNull(true);
    }

    pub fn digest(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{c.name});
        const a = try self.wantText(c, 0);
        return Type.init(if (eq(c.name, "xxhash64")) .int else .string).withNull(a.nullable);
    }

    pub fn jsonBuild(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (eq(c.name, "json_object") and c.args.len % 2 != 0)
            return self.err("`json_object` takes key, value pairs", .{});
        for (c.args, 0..) |_, i| _ = try self.wantText(c, i);
        return Type.init(.string);
    }

    pub fn fromBase64(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`from_base64` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.bytes).withNull(a.nullable);
    }

    pub fn concatWs(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len < 2) return self.err("`concat_ws` takes (separator, value, ...)", .{});
        for (c.args, 0..) |_, i| _ = try self.wantText(c, i);
        return Type.init(.string).withNull((try self.argType(c, 0)).nullable);
    }

    pub fn dateTruncExtract(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 2) return self.err("`{s}` takes (unit, timestamp)", .{name});
        if (c.args[0].* != .str_lit) return self.err("`{s}` needs a literal unit", .{name});
        if (timeUnit(c.args[0].str_lit) == null)
            return self.err("unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
        const a = try self.argType(c, 1);
        if (a.kind != .date and a.kind != .timestamp and !a.unknown)
            return self.err("`{s}` needs a date or timestamp", .{name});
        const out: types.TypeKind = if (eq(name, "extract")) .int else .timestamp;
        return Type.init(out).withNull(a.nullable);
    }

    pub fn unaryString(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{c.name});
        const a = try self.wantText(c, 0);
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn strlen(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{c.name});
        const a = try self.wantText(c, 0);
        return Type.init(.int).withNull(a.nullable);
    }

    pub fn bitCountToHex(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        const a = try self.argType(c, 0);
        if (c.args.len != 1 or !intish(a)) return self.err("`{s}` takes one INT argument", .{name});
        const out: types.TypeKind = if (eq(name, "to_hex")) .string else .int;
        return Type.init(out).withNull(a.nullable);
    }

    pub fn fromHex(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const a = try self.argType(c, 0);
        if (c.args.len != 1 or !(a.kind == .string or a.kind == .bytes or a.unknown))
            return self.err("`from_hex` takes one STRING argument", .{});
        return Type.init(.int).withNull(a.nullable);
    }

    pub fn concat(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len == 0) return self.err("`concat` needs at least one argument", .{});
        var nn = false;
        var bytes = false;
        for (c.args, 0..) |_, i| {
            const t = try self.wantText(c, i);
            nn = nn or t.nullable;
            bytes = bytes or t.kind == .bytes;
        }
        return Type.init(if (bytes) .bytes else .string).withNull(nn);
    }

    pub fn coalesce(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len == 0) return self.err("`coalesce` needs at least one argument", .{});
        var result: ?Type = null;
        var all_null = true;
        for (c.args) |a| {
            const t = try self.typeOf(a);
            all_null = all_null and t.nullable;
            result = if (result) |r| (Type.unify(r, t) orelse return self.err("`coalesce` args have incompatible types", .{})) else t;
        }
        return result.?.withNull(all_null);
    }

    pub fn strPredicate(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`{s}` takes (string, string)", .{c.name});
        const a = try self.wantText(c, 0);
        const b = try self.wantText(c, 1);
        return Type.init(.bool).withNull(a.nullable or b.nullable);
    }

    pub fn substr(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 and c.args.len != 3) return self.err("`substr` takes (string, start[, length])", .{});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "start");
        if (c.args.len > 2) _ = try self.wantInt(c, 2, "length");
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn replace(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`replace` takes (string, from, to)", .{});
        const a = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantText(c, 2);
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn abs(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`abs` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`abs` needs a numeric argument", .{});
        return a;
    }

    pub fn floorCeil(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{name});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`{s}` needs a numeric argument", .{name});
        if (a.unknown or a.kind == .int) return a;
        if (a.kind == .decimal) return Type.decimal(@min(38, (a.precision -| a.scale) + 1), 0).withNull(a.nullable);
        return Type.init(.float).withNull(a.nullable);
    }

    pub fn round(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1 and c.args.len != 2) return self.err("`round` takes (x) or (x, digits)", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`round` needs a numeric argument", .{});
        if (c.args.len == 2) {
            const d = try self.argType(c, 1);
            if (!numericish(d)) return self.err("`round` digits must be an integer", .{});
        }
        if (a.unknown or (a.kind == .int and c.args.len == 1)) return a;
        if (a.kind == .decimal) return Type.decimal(a.precision, roundOutScale(c, a.scale)).withNull(a.nullable);
        return Type.init(.float).withNull(a.nullable);
    }

    pub fn mod(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`mod` takes (a, b)", .{});
        const a = try self.argType(c, 0);
        const b = try self.argType(c, 1);
        if (!numericish(a) or !numericish(b)) return self.err("`mod` needs numeric arguments", .{});
        const nn = a.nullable or b.nullable or a.unknown or b.unknown;
        if (decimalArithType(.mod, a, b)) |dt| return dt.withNull(nn);
        return Type.init(if (a.kind == .float or b.kind == .float) .float else .int).withNull(nn);
    }

    pub fn power(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`power` takes (base, exponent)", .{});
        const a = try self.argType(c, 0);
        const b = try self.argType(c, 1);
        if (!numericish(a) or !numericish(b)) return self.err("`power` needs numeric arguments", .{});
        return Type.init(.float).withNull(a.nullable or b.nullable or a.unknown or b.unknown);
    }

    pub fn sqrt(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`sqrt` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`sqrt` needs a numeric argument", .{});
        return Type.init(.float).asNullable();
    }

    pub fn sign(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`sign` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`sign` needs a numeric argument", .{});
        if (a.kind == .decimal) return Type.decimal(1, 0).withNull(a.nullable);
        if (a.kind == .float) return a;
        return Type.init(.int).withNull(a.nullable or a.unknown);
    }

    pub fn nullif(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`nullif` takes (a, b)", .{});
        const a = try self.argType(c, 0);
        const b = try self.argType(c, 1);
        if (!comparable(a, b)) return self.err("`nullif` arguments are not comparable", .{});
        return a.asNullable();
    }

    pub fn greatestLeast(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len < 2) return self.err("`{s}` needs at least two arguments", .{name});
        var result: ?Type = null;
        for (c.args) |a| {
            const t = try self.typeOf(a);
            result = if (result) |r|
                (Type.unify(r, t) orelse return self.err("`{s}` arguments have incompatible types", .{name}))
            else
                t;
        }
        return result.?.asNullable();
    }

    pub fn pad(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 2 and c.args.len != 3) return self.err("`{s}` takes (string, length[, fill])", .{name});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "length");
        if (c.args.len > 2) _ = try self.wantText(c, 2);
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn leftRight(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 2) return self.err("`{s}` takes (string, n)", .{name});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "n");
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn splitPart(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`split_part` takes (string, delimiter, n)", .{});
        _ = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantInt(c, 2, "n");
        return Type.init(.string).asNullable();
    }

    pub fn jsonLambda(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 or c.args[1].* != .lambda)
            return self.err("`{s}` takes (json array, x -> {s})", .{ c.name, if (eq(c.name, "json_transform")) "value" else "condition" });
        const a = try self.wantText(c, 0);
        const l = c.args[1].lambda;
        if (l.params.len > 2) return self.err("`{s}`'s lambda takes (x) or (x, i), not {d} parameters", .{ c.name, l.params.len });
        const bt = try self.typeOf(try bindParams(self.arena, l, try lambdaSlots(self.arena, &.{ .null_lit, .{ .int_lit = 0 } })));
        if (eq(c.name, "json_transform")) return Type.init(.string).asNullable();
        if (!boolish(bt)) return self.err("`{s}`: the lambda must be a condition (BOOL), not {s}", .{ c.name, @tagName(bt.kind) });
        if (eq(c.name, "json_filter")) return Type.init(.string).asNullable();
        return Type.init(.bool).withNull(a.nullable or a.unknown);
    }

    pub fn jsonReduce(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3 or c.args[2].* != .lambda)
            return self.err("`json_reduce` takes (json array, initial, (acc, x) -> value)", .{});
        _ = try self.wantText(c, 0);
        const l = c.args[2].lambda;
        if (l.params.len < 2 or l.params.len > 3)
            return self.err("`json_reduce`'s lambda takes (acc, x) or (acc, x, i), not {d} parameter{s}", .{ l.params.len, if (l.params.len == 1) "" else "s" });
        return (try reduceAcc(self, c)).asNullable();
    }

    /// The accumulator's type: the initial value's, widened to hold what the lambda
    /// returns (an INT start summing floats is FLOAT) and settled before the run.
    pub fn reduceAcc(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const l = c.args[2].lambda;
        var acc = try self.typeOf(c.args[1]);
        var pass: usize = 0;
        while (pass < 3) : (pass += 1) {
            const null_node = try self.arena.create(ast.Expr);
            null_node.* = .null_lit;
            const acc_node: ast.Expr = if (acc.unknown) .null_lit else .{ .cast = .{ .e = null_node, .ty = acc } };
            const body = try bindParams(self.arena, l, try lambdaSlots(self.arena, &.{ acc_node, .null_lit, .{ .int_lit = 0 } }));
            const bt = try self.typeOf(body);
            var u = Type.unify(acc, bt) orelse
                return self.err("`json_reduce`: the lambda returns {s}, which an accumulator of {s} cannot hold", .{ @tagName(bt.kind), @tagName(acc.kind) });
            if (u.kind == .decimal) u.precision = 38;
            if (u.kind == acc.kind and u.unknown == acc.unknown and u.scale == acc.scale and u.precision == acc.precision) return u;
            acc = u;
        }
        return self.err("`json_reduce`: the accumulator's type keeps changing — give the initial value the type the lambda returns", .{});
    }

    pub fn chars(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`chars` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn jsonRange(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1 and c.args.len != 2) return self.err("`json_range` takes (n) or (start, stop)", .{});
        var nn = false;
        for (0..c.args.len) |i| nn = nn or (try self.wantInt(c, i, if (i + 1 == c.args.len) "stop" else "start")).nullable;
        return Type.init(.string).withNull(nn);
    }

    pub fn jsonLength(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`json_length` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.int).withNull(a.nullable or a.unknown);
    }

    pub fn jsonSlice(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 and c.args.len != 3) return self.err("`json_slice` takes (json array, start[, stop])", .{});
        var nn = (try self.wantText(c, 0)).nullable;
        for (1..c.args.len) |i| nn = nn or (try self.wantInt(c, i, if (i == 1) "start" else "stop")).nullable;
        return Type.init(.string).withNull(nn);
    }

    pub fn jsonConcat(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len < 2) return self.err("`json_concat` takes two or more JSON arrays", .{});
        var nn = false;
        for (0..c.args.len) |i| nn = nn or (try self.wantText(c, i)).nullable;
        return Type.init(.string).withNull(nn);
    }

    pub fn jsonGet(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`json_get` takes (json, path)", .{});
        _ = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        return Type.init(.string).asNullable();
    }

    pub fn strpos(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`strpos` takes (string, substring)", .{});
        const a = try self.wantText(c, 0);
        const b = try self.wantText(c, 1);
        return Type.init(.int).withNull(a.nullable or b.nullable);
    }

    pub fn repeat(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`repeat` takes (string, n)", .{});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "n");
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn reverse(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`reverse` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.string).withNull(a.nullable);
    }

    pub fn dateAdd(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`date_add` takes (unit, n, timestamp)", .{});
        if (c.args[0].* != .str_lit) return self.err("`date_add` needs a literal unit", .{});
        const u = timeUnit(c.args[0].str_lit) orelse
            return self.err("unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
        const nt = try self.argType(c, 1);
        if (!numericish(nt)) return self.err("`date_add` needs an integer amount", .{});
        const a = try self.argType(c, 2);
        if (a.unknown) return a;
        const nn = a.nullable or nt.nullable or nt.unknown;
        if (a.kind == .date) {
            if (u == .hour or u == .minute or u == .second)
                return self.err("`date_add` cannot add `{s}` to a date; cast it to a timestamp first", .{c.args[0].str_lit});
            return Type.init(.date).withNull(nn);
        }
        if (a.kind != .timestamp) return self.err("`date_add` needs a date or timestamp", .{});
        return Type.init(.timestamp).withNull(nn);
    }

    pub fn dateDiff(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`date_diff` takes (unit, start, end)", .{});
        if (c.args[0].* != .str_lit) return self.err("`date_diff` needs a literal unit", .{});
        if (timeUnit(c.args[0].str_lit) == null)
            return self.err("unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
        const a = try self.argType(c, 1);
        const b = try self.argType(c, 2);
        if (!temporalish(a) or !temporalish(b))
            return self.err("`date_diff` needs date or timestamp arguments", .{});
        return Type.init(.int).withNull(a.nullable or b.nullable or a.unknown or b.unknown);
    }

    pub fn makeDate(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`make_date` takes (year, month, day)", .{});
        var nn = false;
        for (c.args) |a| {
            const t = try self.typeOf(a);
            if (!numericish(t)) return self.err("`make_date` needs integer arguments", .{});
            nn = nn or t.nullable or t.unknown;
        }
        return Type.init(.date).withNull(nn);
    }

    pub fn epoch(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`epoch` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!temporalish(a)) return self.err("`epoch` needs a date or timestamp", .{});
        return Type.init(.int).withNull(a.nullable);
    }

    pub fn toTimestamp(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`to_timestamp` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`to_timestamp` needs a numeric argument", .{});
        return Type.init(.timestamp).withNull(a.nullable);
    }

    /// A literal format is validated here so an unsupported directive fails `check`.
    pub fn strftime(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`strftime` takes (timestamp, format)", .{});
        const a = try self.argType(c, 0);
        if (!temporalish(a)) return self.err("`strftime` needs a date or timestamp", .{});
        const f = try self.argType(c, 1);
        if (c.args[1].* == .str_lit) {
            if (badStrftime(c.args[1].str_lit)) |bad|
                return self.err("`strftime` does not support `%{s}` (supported: %Y %m %d %H %M %S %y %%)", .{bad});
        }
        return Type.init(.string).withNull(a.nullable or f.nullable);
    }

    pub fn strptime(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`{s}` takes (text, format)", .{c.name});
        const a = try self.wantText(c, 0);
        const f = try self.wantText(c, 1);
        if (c.args[1].* == .str_lit) {
            if (badStrftime(c.args[1].str_lit)) |bad|
                return self.err("`{s}` does not support `%{s}` (supported: %Y %m %d %H %M %S %y %%)", .{ c.name, bad });
        }
        return Type.init(.timestamp).withNull(eq(c.name, "try_strptime") or a.nullable or f.nullable);
    }

    pub fn translate(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`translate` takes (string, from, to)", .{});
        var nn = false;
        for (0..3) |i| nn = nn or (try self.wantText(c, i)).nullable;
        return Type.init(.string).withNull(nn);
    }

    pub fn chr(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const a = try self.argType(c, 0);
        if (c.args.len != 1 or !intish(a)) return self.err("`chr` takes one INT argument", .{});
        return Type.init(.string).withNull(a.nullable);
    }
};

const parser = @import("../../lang/sql_parser.zig");

const test_schema = types.Schema{ .fields = &.{
    .{ .name = "i", .ty = Type.init(.int) },
    .{ .name = "ni", .ty = Type.init(.int).asNullable() },
    .{ .name = "f", .ty = Type.init(.float) },
    .{ .name = "s", .ty = Type.init(.string) },
    .{ .name = "ns", .ty = Type.init(.string).asNullable() },
    .{ .name = "d", .ty = Type.init(.date) },
    .{ .name = "ts", .ty = Type.init(.timestamp) },
    .{ .name = "b", .ty = Type.init(.bool) },
    .{ .name = "m", .ty = Type.decimal(10, 2) },
    .{ .name = "arr", .ty = Type.init(.array) },
} };

fn typeOfSrc(ctx: *TypeCtx, src: []const u8) !Type {
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return ctx.typeOf(try parser.parseExprStr(ctx.arena, src, &diag));
}

test "builtin typing: a wrong argument count or kind is refused with the function's own message" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const cases = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "now(1)", .msg = "`now` takes no arguments" },
        .{ .src = "today(1)", .msg = "`today` takes no arguments" },
        .{ .src = "regexp_replace(s, 'a')", .msg = "`regexp_replace` takes (string, pattern, replacement[, flags])" },
        .{ .src = "regexp_replace(s, 'a', 'b', 'x')", .msg = "`regexp_replace` flags are `g` (every match) and `i` (ignore case), not `x`" },
        .{ .src = "regexp_replace(s, '(', 'b')", .msg = "invalid regular expression `(`" },
        .{ .src = "regexp_replace(arr, 'a', 'b')", .msg = "`regexp_replace` argument 1 must be text, got array" },
        .{ .src = "regexp_matches(s)", .msg = "`regexp_matches` takes (string, pattern)" },
        .{ .src = "regexp_extract(s)", .msg = "`regexp_extract` takes (string, pattern[, group])" },
        .{ .src = "regexp_extract(s, 'a', i)", .msg = "`regexp_extract` needs a literal group number" },
        .{ .src = "regexp_extract(s, 'a', -1)", .msg = "`regexp_extract` needs a literal group number" },
        .{ .src = "regexp_extract(s, ns, 10)", .msg = "`regexp_extract` group 10 is not in the pattern" },
        .{ .src = "regexp_extract(s, '(a)', 2)", .msg = "`regexp_extract` group 2 is not in the pattern" },
        .{ .src = "md5(s, s)", .msg = "`md5` takes one argument" },
        .{ .src = "xxhash64(arr)", .msg = "`xxhash64` argument 1 must be text, got array" },
        .{ .src = "json_object('a')", .msg = "`json_object` takes key, value pairs" },
        .{ .src = "json_array(s, arr)", .msg = "`json_array` argument 2 must be text, got array" },
        .{ .src = "from_base64()", .msg = "`from_base64` takes one argument" },
        .{ .src = "concat_ws('|')", .msg = "`concat_ws` takes (separator, value, ...)" },
        .{ .src = "date_trunc('day')", .msg = "`date_trunc` takes (unit, timestamp)" },
        .{ .src = "date_trunc(s, ts)", .msg = "`date_trunc` needs a literal unit" },
        .{ .src = "date_trunc('fortnight', ts)", .msg = "unknown time unit `fortnight` (units: year, month, week, day, hour, minute, second)" },
        .{ .src = "date_trunc('day', s)", .msg = "`date_trunc` needs a date or timestamp" },
        .{ .src = "upper(s, s)", .msg = "`upper` takes one argument" },
        .{ .src = "length()", .msg = "`length` takes one argument" },
        .{ .src = "bit_count(s)", .msg = "`bit_count` takes one INT argument" },
        .{ .src = "to_hex(i, i)", .msg = "`to_hex` takes one INT argument" },
        .{ .src = "from_hex(i)", .msg = "`from_hex` takes one STRING argument" },
        .{ .src = "from_hex()", .msg = "`from_hex` is missing an argument" },
        .{ .src = "concat()", .msg = "`concat` needs at least one argument" },
        .{ .src = "coalesce()", .msg = "`coalesce` needs at least one argument" },
        .{ .src = "coalesce(i, d)", .msg = "`coalesce` args have incompatible types" },
        .{ .src = "starts_with(s)", .msg = "`starts_with` takes (string, string)" },
        .{ .src = "substr(s)", .msg = "`substr` takes (string, start[, length])" },
        .{ .src = "substr(s, f)", .msg = "`substr` start must be an INT, got float" },
        .{ .src = "substr(s, 1, s)", .msg = "`substr` length must be an INT, got string" },
        .{ .src = "replace(s, 'a')", .msg = "`replace` takes (string, from, to)" },
        .{ .src = "abs(i, i)", .msg = "`abs` takes one argument" },
        .{ .src = "abs(s)", .msg = "`abs` needs a numeric argument" },
        .{ .src = "floor()", .msg = "`floor` takes one argument" },
        .{ .src = "ceil(d)", .msg = "`ceil` needs a numeric argument" },
        .{ .src = "round(f, 1, 2)", .msg = "`round` takes (x) or (x, digits)" },
        .{ .src = "round(s)", .msg = "`round` needs a numeric argument" },
        .{ .src = "round(f, s)", .msg = "`round` digits must be an integer" },
        .{ .src = "mod(i)", .msg = "`mod` takes (a, b)" },
        .{ .src = "mod(i, s)", .msg = "`mod` needs numeric arguments" },
        .{ .src = "power(i)", .msg = "`power` takes (base, exponent)" },
        .{ .src = "power(s, i)", .msg = "`power` needs numeric arguments" },
        .{ .src = "sqrt()", .msg = "`sqrt` takes one argument" },
        .{ .src = "sqrt(s)", .msg = "`sqrt` needs a numeric argument" },
        .{ .src = "sign()", .msg = "`sign` takes one argument" },
        .{ .src = "sign(d)", .msg = "`sign` needs a numeric argument" },
        .{ .src = "nullif(i)", .msg = "`nullif` takes (a, b)" },
        .{ .src = "nullif(i, d)", .msg = "`nullif` arguments are not comparable" },
        .{ .src = "greatest(i)", .msg = "`greatest` needs at least two arguments" },
        .{ .src = "least(i, d)", .msg = "`least` arguments have incompatible types" },
        .{ .src = "lpad(s)", .msg = "`lpad` takes (string, length[, fill])" },
        .{ .src = "rpad(s, s)", .msg = "`rpad` length must be an INT, got string" },
        .{ .src = "left(s)", .msg = "`left` takes (string, n)" },
        .{ .src = "right(s, f)", .msg = "`right` n must be an INT, got float" },
        .{ .src = "split_part(s, ',')", .msg = "`split_part` takes (string, delimiter, n)" },
        .{ .src = "split_part(s, ',', s)", .msg = "`split_part` n must be an INT, got string" },
        .{ .src = "json_filter(s, 1)", .msg = "`json_filter` takes (json array, x -> condition)" },
        .{ .src = "json_transform(s)", .msg = "`json_transform` takes (json array, x -> value)" },
        .{ .src = "json_any(s, (x, i, j) -> true)", .msg = "`json_any`'s lambda takes (x) or (x, i), not 3 parameters" },
        .{ .src = "json_all(s, x -> 1)", .msg = "`json_all`: the lambda must be a condition (BOOL), not int" },
        .{ .src = "json_reduce(s, 0)", .msg = "`json_reduce` takes (json array, initial, (acc, x) -> value)" },
        .{ .src = "json_reduce(s, 0, x -> x)", .msg = "`json_reduce`'s lambda takes (acc, x) or (acc, x, i), not 1 parameter" },
        .{ .src = "json_reduce(s, 0, (a, x, i, j) -> a)", .msg = "`json_reduce`'s lambda takes (acc, x) or (acc, x, i), not 4 parameters" },
        .{ .src = "json_reduce(s, 0, (a, x) -> d)", .msg = "`json_reduce`: the lambda returns date, which an accumulator of int cannot hold" },
        .{ .src = "chars()", .msg = "`chars` takes one argument" },
        .{ .src = "json_range()", .msg = "`json_range` takes (n) or (start, stop)" },
        .{ .src = "json_range(s)", .msg = "`json_range` stop must be an INT, got string" },
        .{ .src = "json_range(f, 3)", .msg = "`json_range` start must be an INT, got float" },
        .{ .src = "json_length()", .msg = "`json_length` takes one argument" },
        .{ .src = "json_slice(s)", .msg = "`json_slice` takes (json array, start[, stop])" },
        .{ .src = "json_slice(s, s)", .msg = "`json_slice` start must be an INT, got string" },
        .{ .src = "json_slice(s, 1, f)", .msg = "`json_slice` stop must be an INT, got float" },
        .{ .src = "json_concat(s)", .msg = "`json_concat` takes two or more JSON arrays" },
        .{ .src = "json_get(s)", .msg = "`json_get` takes (json, path)" },
        .{ .src = "strpos(s)", .msg = "`strpos` takes (string, substring)" },
        .{ .src = "repeat(s)", .msg = "`repeat` takes (string, n)" },
        .{ .src = "repeat(s, s)", .msg = "`repeat` n must be an INT, got string" },
        .{ .src = "reverse()", .msg = "`reverse` takes one argument" },
        .{ .src = "date_add('day', 1)", .msg = "`date_add` takes (unit, n, timestamp)" },
        .{ .src = "date_add(s, 1, ts)", .msg = "`date_add` needs a literal unit" },
        .{ .src = "date_add('eon', 1, ts)", .msg = "unknown time unit `eon` (units: year, month, week, day, hour, minute, second)" },
        .{ .src = "date_add('day', s, ts)", .msg = "`date_add` needs an integer amount" },
        .{ .src = "date_add('minute', 1, d)", .msg = "`date_add` cannot add `minute` to a date; cast it to a timestamp first" },
        .{ .src = "date_add('day', 1, s)", .msg = "`date_add` needs a date or timestamp" },
        .{ .src = "date_diff('day', d)", .msg = "`date_diff` takes (unit, start, end)" },
        .{ .src = "date_diff(s, d, d)", .msg = "`date_diff` needs a literal unit" },
        .{ .src = "date_diff('lustrum', d, d)", .msg = "unknown time unit `lustrum` (units: year, month, week, day, hour, minute, second)" },
        .{ .src = "date_diff('day', d, s)", .msg = "`date_diff` needs date or timestamp arguments" },
        .{ .src = "make_date(2024, 1)", .msg = "`make_date` takes (year, month, day)" },
        .{ .src = "make_date(2024, s, 1)", .msg = "`make_date` needs integer arguments" },
        .{ .src = "epoch()", .msg = "`epoch` takes one argument" },
        .{ .src = "epoch(i)", .msg = "`epoch` needs a date or timestamp" },
        .{ .src = "to_timestamp()", .msg = "`to_timestamp` takes one argument" },
        .{ .src = "to_timestamp(d)", .msg = "`to_timestamp` needs a numeric argument" },
        .{ .src = "strftime(ts)", .msg = "`strftime` takes (timestamp, format)" },
        .{ .src = "strftime(s, '%Y')", .msg = "`strftime` needs a date or timestamp" },
        .{ .src = "strftime(ts, '%Q')", .msg = "`strftime` does not support `%Q` (supported: %Y %m %d %H %M %S %y %%)" },
        .{ .src = "strptime(s)", .msg = "`strptime` takes (text, format)" },
        .{ .src = "try_strptime(s, '%Y-%q')", .msg = "`try_strptime` does not support `%q` (supported: %Y %m %d %H %M %S %y %%)" },
        .{ .src = "translate(s, 'a')", .msg = "`translate` takes (string, from, to)" },
        .{ .src = "chr(s)", .msg = "`chr` takes one INT argument" },
        .{ .src = "chr(i, i)", .msg = "`chr` takes one INT argument" },
    };
    for (cases) |cs| {
        var ctx = TypeCtx{ .schema = test_schema, .arena = ar.allocator() };
        if (typeOfSrc(&ctx, cs.src)) |t| {
            std.debug.print("`{s}` typed as {s}, expected the error `{s}`\n", .{ cs.src, @tagName(t.kind), cs.msg });
            return error.TestUnexpectedResult;
        } else |e| {
            try std.testing.expectEqual(error.TypeError, e);
            std.testing.expectEqualStrings(cs.msg, ctx.msg) catch |x| {
                std.debug.print("in `{s}`\n", .{cs.src});
                return x;
            };
        }
    }
}

test "builtin typing: result kinds, nullability and decimal shapes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const cases = [_]struct { src: []const u8, kind: types.TypeKind, nullable: bool, precision: u8 = 0, scale: u8 = 0 }{
        .{ .src = "now()", .kind = .timestamp, .nullable = false },
        .{ .src = "today()", .kind = .date, .nullable = false },
        .{ .src = "regexp_replace(ns, 'a', 'b', 'gi')", .kind = .string, .nullable = true },
        .{ .src = "regexp_matches(s, ns)", .kind = .bool, .nullable = true },
        .{ .src = "regexp_matches(s, 'a')", .kind = .bool, .nullable = false },
        .{ .src = "regexp_extract(s, '(a)', 1)", .kind = .string, .nullable = true },
        .{ .src = "xxhash64(s)", .kind = .int, .nullable = false },
        .{ .src = "sha256(ns)", .kind = .string, .nullable = true },
        .{ .src = "json_object('k', ns)", .kind = .string, .nullable = false },
        .{ .src = "from_base64(s)", .kind = .bytes, .nullable = false },
        .{ .src = "concat_ws(ns, s)", .kind = .string, .nullable = true },
        .{ .src = "concat_ws('|', ns)", .kind = .string, .nullable = false },
        .{ .src = "date_trunc('month', d)", .kind = .timestamp, .nullable = false },
        .{ .src = "extract('year', ts)", .kind = .int, .nullable = false },
        .{ .src = "length(ns)", .kind = .int, .nullable = true },
        .{ .src = "to_hex(i)", .kind = .string, .nullable = false },
        .{ .src = "bit_count(ni)", .kind = .int, .nullable = true },
        .{ .src = "from_hex(s)", .kind = .int, .nullable = false },
        .{ .src = "concat(s, from_base64(s))", .kind = .bytes, .nullable = false },
        .{ .src = "concat(s, ns)", .kind = .string, .nullable = true },
        .{ .src = "coalesce(ni, i)", .kind = .int, .nullable = false },
        .{ .src = "coalesce(ni, ni)", .kind = .int, .nullable = true },
        .{ .src = "contains(s, ns)", .kind = .bool, .nullable = true },
        .{ .src = "substr(ns, 1, 2)", .kind = .string, .nullable = true },
        .{ .src = "replace(s, 'a', 'b')", .kind = .string, .nullable = false },
        .{ .src = "abs(m)", .kind = .decimal, .nullable = false, .precision = 10, .scale = 2 },
        .{ .src = "floor(m)", .kind = .decimal, .nullable = false, .precision = 9, .scale = 0 },
        .{ .src = "ceil(f)", .kind = .float, .nullable = false },
        .{ .src = "floor(ni)", .kind = .int, .nullable = true },
        .{ .src = "round(m, 1)", .kind = .decimal, .nullable = false, .precision = 10, .scale = 1 },
        .{ .src = "round(i, 1)", .kind = .float, .nullable = false },
        .{ .src = "round(i)", .kind = .int, .nullable = false },
        .{ .src = "mod(i, f)", .kind = .float, .nullable = false },
        .{ .src = "mod(ni, i)", .kind = .int, .nullable = true },
        .{ .src = "power(i, ni)", .kind = .float, .nullable = true },
        .{ .src = "sqrt(i)", .kind = .float, .nullable = true },
        .{ .src = "sign(m)", .kind = .decimal, .nullable = false, .precision = 1, .scale = 0 },
        .{ .src = "sign(f)", .kind = .float, .nullable = false },
        .{ .src = "sign(ni)", .kind = .int, .nullable = true },
        .{ .src = "nullif(i, 0)", .kind = .int, .nullable = true },
        .{ .src = "greatest(i, ni)", .kind = .int, .nullable = true },
        .{ .src = "lpad(s, 5, '0')", .kind = .string, .nullable = false },
        .{ .src = "right(ns, 2)", .kind = .string, .nullable = true },
        .{ .src = "split_part(s, ',', 1)", .kind = .string, .nullable = true },
        .{ .src = "json_transform(s, x -> x)", .kind = .string, .nullable = true },
        .{ .src = "json_filter(s, (x, i) -> i > 0)", .kind = .string, .nullable = true },
        .{ .src = "json_any(s, x -> x = '1')", .kind = .bool, .nullable = false },
        .{ .src = "json_all(ns, x -> x = '1')", .kind = .bool, .nullable = true },
        .{ .src = "json_reduce(s, 0, (a, x) -> a + 1)", .kind = .int, .nullable = true },
        .{ .src = "json_reduce(s, 0, (a, x) -> a + 1.5e0)", .kind = .float, .nullable = true },
        .{ .src = "chars(ns)", .kind = .string, .nullable = true },
        .{ .src = "json_range(1, ni)", .kind = .string, .nullable = true },
        .{ .src = "json_length(s)", .kind = .int, .nullable = false },
        .{ .src = "json_slice(s, 1, ni)", .kind = .string, .nullable = true },
        .{ .src = "json_concat(s, ns)", .kind = .string, .nullable = true },
        .{ .src = "json_get(s, 'a')", .kind = .string, .nullable = true },
        .{ .src = "strpos(s, ns)", .kind = .int, .nullable = true },
        .{ .src = "repeat(s, 2)", .kind = .string, .nullable = false },
        .{ .src = "reverse(ns)", .kind = .string, .nullable = true },
        .{ .src = "date_add('week', ni, d)", .kind = .date, .nullable = true },
        .{ .src = "date_add('hour', 1, ts)", .kind = .timestamp, .nullable = false },
        .{ .src = "date_diff('day', d, ts)", .kind = .int, .nullable = false },
        .{ .src = "make_date(2024, ni, 1)", .kind = .date, .nullable = true },
        .{ .src = "epoch(ts)", .kind = .int, .nullable = false },
        .{ .src = "to_timestamp(f)", .kind = .timestamp, .nullable = false },
        .{ .src = "strftime(d, ns)", .kind = .string, .nullable = true },
        .{ .src = "strptime(s, '%Y')", .kind = .timestamp, .nullable = false },
        .{ .src = "try_strptime(s, '%Y')", .kind = .timestamp, .nullable = true },
        .{ .src = "translate(s, 'a', ns)", .kind = .string, .nullable = true },
        .{ .src = "chr(ni)", .kind = .string, .nullable = true },
    };
    for (cases) |cs| {
        var ctx = TypeCtx{ .schema = test_schema, .arena = ar.allocator() };
        const t = typeOfSrc(&ctx, cs.src) catch |e| {
            std.debug.print("`{s}` failed: {s}\n", .{ cs.src, ctx.msg });
            return e;
        };
        errdefer std.debug.print("in `{s}`\n", .{cs.src});
        try std.testing.expectEqual(cs.kind, t.kind);
        try std.testing.expectEqual(cs.nullable, t.nullable);
        try std.testing.expectEqual(cs.precision, t.precision);
        try std.testing.expectEqual(cs.scale, t.scale);
    }
}

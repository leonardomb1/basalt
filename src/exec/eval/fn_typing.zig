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
const eq = @import("support.zig").eq;
const intish = @import("../eval.zig").intish;
const lambdaSlots = @import("row.zig").lambdaSlots;
const numericish = @import("../eval.zig").numericish;
const regex = @import("../regex.zig");
const roundOutScale = @import("cast.zig").roundOutScale;
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
        if (c.args.len != 3) return self.err("`regexp_replace` takes (string, pattern, replacement)", .{});
        _ = try literalPattern(self, c);
        const a = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantText(c, 2);
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
        for (c.args, 0..) |_, i| nn = nn or (try self.wantText(c, i)).nullable;
        return Type.init(.string).withNull(nn);
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
        if (!(a.kind == .int or a.unknown) or !(b.kind == .int or b.unknown))
            return self.err("`mod` needs integer arguments", .{});
        return Type.init(.int).asNullable();
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

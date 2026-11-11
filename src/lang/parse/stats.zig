//! The statistics written as expressions over the basic aggregates, expanded where
//! they are parsed: the pair statistics `corr`, `covar_pop`, `covar_samp` and the
//! `regr_*` family (arguments `y, x`, as the SQL standard orders them),
//! `weighted_avg(x, w)` and `geomean(x)`. Being ordinary aggregates once expanded,
//! they group, run on lanes, spill and take `OVER` and `FILTER` with no machinery of
//! their own.
//!
//! A row counts only when both values of a pair are present (and the FILTER holds):
//! each value is masked to NULL otherwise and cast to DOUBLE, so an INT sum cannot
//! overflow. The covariance is the polarization identity over the stable variance,
//! `(var(x + y) - var(x) - var(y)) / 2`: its error is about the float epsilon times
//! `var(x) + var(y)`, fine unless the two spreads differ by many orders of
//! magnitude. A statistic whose variance is 0 is NULL, as in PostgreSQL; `regr_r2`
//! is 1 when only y is constant. `OVER` is parsed once and every piece takes it.

const Parser = @import("../sql_parser.zig").Parser;
const Error = @import("../sql_parser.zig").Error;
const Pos = @import("../sql_parser.zig").Pos;
const Window = @import("window.zig");
const ast = @import("../ast.zig");
const types = @import("../types.zig");
const std = @import("std");

pub const names = [_][]const u8{
    "corr",      "covar_pop", "covar_samp", "regr_slope", "regr_intercept", "regr_r2",      "regr_count",
    "regr_avgx", "regr_avgy", "regr_sxx",   "regr_syy",   "regr_sxy",       "weighted_avg", "geomean",
};

pub fn isStat(name: []const u8) bool {
    for (names) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

const Build = struct {
    p: *Parser,
    pos: Pos,
    over: ?Window.Spec,

    fn node(b: Build, e: ast.Expr) Error!*ast.Expr {
        return b.p.mk(e);
    }

    fn agg(b: Build, name: []const u8, arg: *ast.Expr) Error!*ast.Expr {
        const args = try b.p.arena.dupe(*ast.Expr, &.{arg});
        if (b.over) |spec| return Window.windowCall(b.p, name, args, false, null, b.pos, spec);
        return b.node(.{ .call = .{ .name = name, .args = args } });
    }

    fn call(b: Build, name: []const u8, args: []const *ast.Expr) Error!*ast.Expr {
        return b.node(.{ .call = .{ .name = name, .args = try b.p.arena.dupe(*ast.Expr, args) } });
    }

    fn bin(b: Build, op: ast.BinOp, l: *ast.Expr, r: *ast.Expr) Error!*ast.Expr {
        return b.node(.{ .binary = .{ .op = op, .l = l, .r = r } });
    }

    fn num(b: Build, x: f64) Error!*ast.Expr {
        return b.node(.{ .float_lit = x });
    }

    fn when(b: Build, c: *ast.Expr, then: *ast.Expr, els: *ast.Expr) Error!*ast.Expr {
        return b.node(.{ .cond = .{ .cond = c, .then = then, .els = els } });
    }

    fn nul(b: Build) Error!*ast.Expr {
        return b.node(.null_lit);
    }

    fn present(b: Build, e: *ast.Expr) Error!*ast.Expr {
        return b.node(.{ .is_null = .{ .e = e, .negated = true } });
    }

    /// `e` as a DOUBLE where `keep` holds, else NULL.
    fn masked(b: Build, e: *ast.Expr, keep: *ast.Expr) Error!*ast.Expr {
        return b.when(keep, try b.node(.{ .cast = .{ .e = e, .ty = types.Type.init(.float) } }), try b.nul());
    }

    /// `x / y`, NULL where `y` is 0.
    fn over0(b: Build, x: *ast.Expr, y: *ast.Expr) Error!*ast.Expr {
        return b.bin(.div, x, try b.call("nullif", &.{ y, try b.num(0) }));
    }
};

/// `name(args)`, one of `names`, as its expression; at `FILTER` or `OVER` when
/// either follows the call.
pub fn expand(self: *Parser, name: []const u8, args: []const *ast.Expr, distinct: bool, pos: Pos) Error!*ast.Expr {
    const want: usize = if (std.mem.eql(u8, name, "geomean")) 1 else 2;
    if (args.len != want) return self.fail(pos, "`{s}` takes {s}", .{ name, if (want == 1) "one argument" else if (std.mem.eql(u8, name, "weighted_avg")) "(x, weight)" else "(y, x)" });
    if (distinct) return self.fail(pos, "DISTINCT does not apply to `{s}`", .{name});
    const filter = try Window.parseFilterClause(self);
    const b = Build{ .p = self, .pos = pos, .over = if (self.isKw("over")) try Window.parseOver(self, pos) else null };

    var keep = try b.present(args[0]);
    if (want == 2) keep = try b.bin(.@"and", keep, try b.present(args[1]));
    if (filter) |c| keep = try b.bin(.@"and", keep, c);
    const first = try b.masked(args[0], keep);
    if (want == 1) {
        const bad = try b.agg("count_if", try b.bin(.le, first, try b.num(0)));
        const mean_ln = try b.agg("avg", try b.call("ln", &.{first}));
        return b.when(try b.bin(.gt, bad, try b.num(0)), try b.nul(), try b.call("exp", &.{mean_ln}));
    }
    const second = try b.masked(args[1], keep);
    if (std.mem.eql(u8, name, "weighted_avg"))
        return b.over0(try b.agg("sum", try b.bin(.mul, first, second)), try b.agg("sum", second));

    const y = first;
    const x = second;
    const s = try b.bin(.add, x, y);
    const samp = std.mem.eql(u8, name, "covar_samp");
    const v = if (samp) "var_samp" else "var_pop";
    const vx = try b.agg(v, x);
    const vy = try b.agg(v, y);
    const cov = try b.bin(.div, try b.bin(.sub, try b.bin(.sub, try b.agg(v, s), vx), vy), try b.num(2));
    if (std.mem.eql(u8, name, "covar_pop") or samp) return cov;
    if (std.mem.eql(u8, name, "regr_count")) return b.agg("count", x);
    if (std.mem.eql(u8, name, "regr_avgx")) return b.agg("avg", x);
    if (std.mem.eql(u8, name, "regr_avgy")) return b.agg("avg", y);
    if (std.mem.eql(u8, name, "regr_sxx")) return b.bin(.mul, vx, try b.agg("count", x));
    if (std.mem.eql(u8, name, "regr_syy")) return b.bin(.mul, vy, try b.agg("count", x));
    if (std.mem.eql(u8, name, "regr_sxy")) return b.bin(.mul, cov, try b.agg("count", x));
    const slope = try b.over0(cov, vx);
    if (std.mem.eql(u8, name, "regr_slope")) return slope;
    if (std.mem.eql(u8, name, "regr_intercept"))
        return b.bin(.sub, try b.agg("avg", y), try b.bin(.mul, slope, try b.agg("avg", x)));
    const r2 = try b.over0(try b.bin(.mul, cov, cov), try b.bin(.mul, vx, vy));
    if (std.mem.eql(u8, name, "regr_r2")) {
        const x_flat = try b.bin(.eq, vx, try b.num(0));
        const y_flat = try b.bin(.eq, vy, try b.num(0));
        return b.when(x_flat, try b.nul(), try b.when(y_flat, try b.num(1), try clamp(b, r2, 0)));
    }
    return clamp(b, try b.over0(cov, try b.call("sqrt", &.{try b.bin(.mul, vx, vy)})), -1);
}

/// `r` held to [`lo`, 1], so rounding never shows a correlation past its bounds.
fn clamp(b: Build, r: *ast.Expr, lo: f64) Error!*ast.Expr {
    const high = try b.when(try b.bin(.gt, r, try b.num(1)), try b.num(1), r);
    return b.when(try b.bin(.lt, r, try b.num(lo)), try b.num(lo), high);
}

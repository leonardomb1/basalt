//! Window calls. A call followed by `OVER` anywhere in a SELECT item, `f(args)
//! [IGNORE NULLS | RESPECT NULLS] [FILTER (WHERE c)] OVER (spec)` or `OVER w`, is
//! lifted out of its expression as it is parsed: it is recorded on the select core's
//! list (`Parser.win_sink`) and a hidden column `__w<n>` takes its place, the way a
//! scalar subquery leaves a `$__scalar<n>`. Outside a SELECT list (and ORDER BY)
//! there is no list, so OVER in WHERE, GROUP BY, HAVING or ON is refused.
//!
//! `Lower` turns the calls into stages: a projection computing every expression key
//! or argument into a hidden `__wa<n>` column (one per distinct expression) and
//! carrying the columns later items read, one window stage per distinct PARTITION
//! BY / ORDER BY in order of first appearance, each sorting its own input, and the
//! SELECT list's projection, which evaluates an expression around a window over its
//! hidden column. A call that is a whole item names its column after the item, so
//! `WHERE rn <= k` over a CTE still finds a lone ROW_NUMBER (`windowTopK`). Over a
//! GROUP BY the windows run after the aggregate and HAVING, their aggregates lifted
//! into the aggregate stage like any other (`SUM(SUM(x)) OVER ()`).
//!
//! A call's `Spec` is its own keys and frame, or a named window (`base`) taken whole
//! when `bare` (`OVER w`), else extended (`OVER (w ORDER BY t)`).
//!
//! FILTER is desugared where it is written: `agg(x) FILTER (WHERE c)` is
//! `agg(if(c, x, null))` and `COUNT(*) FILTER (WHERE c)` is `count_if(c)`, exact
//! since every aggregate skips nulls, so GROUP BY, the parallel lanes and windows
//! all see an ordinary aggregate.
//!
//! A frame is `ROWS|RANGE <start>` (ending at CURRENT ROW) or `ROWS|RANGE BETWEEN
//! <start> AND <end>`. Checked here: a start after its end, UNBOUNDED FOLLOWING as a
//! start or UNBOUNDED PRECEDING as an end, a ROWS offset that is not a whole number,
//! and a RANGE offset without exactly one ORDER BY key; the key's type is checked by
//! the analyzer. Over a date or timestamp key a RANGE offset is a number of days or
//! an `INTERVAL 'n' DAY|HOUR|MINUTE|SECOND`.

const Parser = @import("../sql_parser.zig").Parser;
const Error = @import("../sql_parser.zig").Error;
const Pos = @import("../sql_parser.zig").Pos;
const aggregates = @import("../aggregates.zig");
const ast = @import("../ast.zig");
const std = @import("std");

pub const OrderExpr = struct { e: *ast.Expr, desc: bool };

pub const Spec = struct {
    base: ?[]const u8 = null,
    bare: bool = false,
    partition: []const *ast.Expr = &.{},
    order: []const OrderExpr = &.{},
    frame: ?ast.WinFrame = null,
    pos: Pos,
};

pub const WinCall = struct {
    hidden: []const u8,
    name: []const u8,
    func: ast.WinFn,
    arg: ?*ast.Expr = null,
    offset: i64 = 1,
    default: ?*ast.Expr = null,
    distinct: bool = false,
    ignore_nulls: bool = false,
    spec: Spec,
    pos: Pos,
    out: ?[]const u8 = null,
};

pub const NamedWindow = struct { name: []const u8, spec: Spec };

/// `IGNORE NULLS` (true) or `RESPECT NULLS` (false), consumed, or null.
pub fn parseNullTreatment(self: *Parser) ?bool {
    if (!(self.isKw("ignore") or self.isKw("respect")) or !self.peekKw("nulls")) return null;
    const ignore = self.isKw("ignore");
    _ = self.advance();
    _ = self.advance();
    return ignore;
}

/// `FILTER (WHERE c)` after a call, or null with nothing consumed.
pub fn parseFilterClause(self: *Parser) Error!?*ast.Expr {
    if (!(self.isKw("filter") and self.peekTag() == .lparen)) return null;
    _ = self.advance();
    _ = self.advance();
    try self.expectKw("where");
    const c = try self.parseExpr();
    _ = try self.expect(.rparen);
    return c;
}

/// An aggregate call with its FILTER folded into the argument.
pub fn applyFilter(self: *Parser, name: []const u8, args: []*ast.Expr, c: *ast.Expr, pos: Pos) Error!struct { name: []const u8, args: []*ast.Expr } {
    if (aggregates.lookup(name) == null)
        return self.fail(pos, "FILTER (WHERE …) applies to an aggregate; `{s}` is not one", .{name});
    if (args.len == 0) {
        if (aggregates.lookup(name) != .count) return self.fail(pos, "`{s}` needs an argument", .{name});
        const one = try self.arena.alloc(*ast.Expr, 1);
        one[0] = c;
        return .{ .name = "count_if", .args = one };
    }
    const out = try self.arena.dupe(*ast.Expr, args);
    out[0] = try self.mk(.{ .cond = .{ .cond = c, .then = args[0], .els = try self.mk(.null_lit) } });
    return .{ .name = name, .args = out };
}

/// The call `name(args)` just parsed, at `OVER`: recorded on the select core's list,
/// and replaced by its hidden column.
pub fn parseWindowCall(self: *Parser, name: []const u8, args: []const *ast.Expr, distinct: bool, nulls: ?bool, pos: Pos) Error!*ast.Expr {
    const sink = self.win_sink orelse
        return self.fail(pos, "a window function is only allowed in the SELECT list or ORDER BY — compute it in a derived table or CTE to filter, group or join on it", .{});
    _ = self.advance();
    const func: ast.WinFn = if (std.meta.stringToEnum(ast.WinKind, name)) |k|
        .{ .win = k }
    else if (aggregates.lookup(name)) |a|
        .{ .agg = a }
    else
        return self.fail(pos, "`{s}` is not a window function — over a window, basalt computes every aggregate and row_number, rank, dense_rank, percent_rank, cume_dist, ntile, lag, lead, first_value, last_value and nth_value", .{name});
    var call = WinCall{ .hidden = "", .name = name, .func = func, .distinct = distinct, .ignore_nulls = nulls orelse false, .spec = .{ .pos = pos }, .pos = pos };
    try self.windowArgs(&call, args);
    if (nulls != null) {
        const takes = func == .win and switch (func.win) {
            .lag, .lead, .first_value, .last_value, .nth_value => true,
            else => false,
        };
        if (!takes) return self.fail(pos, "IGNORE NULLS and RESPECT NULLS apply to LAG, LEAD, FIRST_VALUE, LAST_VALUE and NTH_VALUE, not `{s}`", .{name});
    }
    if (self.eat(.lparen)) {
        call.spec = try self.parseSpecBody(pos);
    } else {
        call.spec = .{ .base = try self.expectIdent(), .bare = true, .pos = pos };
    }
    for (sink.items) |prev| {
        if (call.arg) |a| if (refsName(a, prev.hidden)) return nested(self, pos);
        for (call.spec.partition) |e| if (refsName(e, prev.hidden)) return nested(self, pos);
        for (call.spec.order) |o| if (refsName(o.e, prev.hidden)) return nested(self, pos);
    }
    self.derived_n += 1;
    call.hidden = try std.fmt.allocPrint(self.arena, "__w{d}", .{self.derived_n});
    try sink.append(call);
    return self.mk(.{ .field = try self.qualOne(call.hidden) });
}

fn nested(self: *Parser, pos: Pos) Error {
    return self.fail(pos, "window functions cannot be nested — compute the inner one in a derived table or CTE first", .{});
}

fn wholeNumber(e: *const ast.Expr) ?i64 {
    return if (e.* == .int_lit) e.int_lit else null;
}

pub fn windowArgs(self: *Parser, call: *WinCall, args: []const *ast.Expr) Error!void {
    const name = call.name;
    switch (call.func) {
        .win => |k| {
            if (call.distinct) return self.fail(call.pos, "DISTINCT applies to an aggregate, not `{s}`", .{name});
            switch (k) {
                .row_number, .rank, .dense_rank, .percent_rank, .cume_dist => if (args.len != 0)
                    return self.fail(call.pos, "`{s}()` takes no arguments", .{name}),
                .ntile => {
                    if (args.len != 1) return self.fail(call.pos, "NTILE takes one argument, the number of buckets", .{});
                    const n = wholeNumber(args[0]) orelse 0;
                    if (n < 1) return self.fail(call.pos, "NTILE takes a positive whole number of buckets", .{});
                    call.offset = n;
                },
                .lag, .lead => {
                    if (args.len < 1 or args.len > 3) return self.fail(call.pos, "`{s}` takes (column[, offset[, default]])", .{name});
                    call.arg = args[0];
                    if (args.len >= 2) {
                        const o = args[1];
                        if (o.* == .unary and o.unary.op == .neg and o.unary.e.* == .int_lit)
                            return self.fail(call.pos, "LAG/LEAD offset must not be negative — use the other function", .{});
                        call.offset = wholeNumber(o) orelse return self.fail(call.pos, "LAG/LEAD offset must be an integer", .{});
                    }
                    if (args.len == 3) call.default = args[2];
                },
                .first_value, .last_value => {
                    if (args.len != 1) return self.fail(call.pos, "`{s}` takes one argument", .{name});
                    call.arg = args[0];
                },
                .nth_value => {
                    if (args.len != 2) return self.fail(call.pos, "NTH_VALUE takes (column, n)", .{});
                    call.arg = args[0];
                    const n = wholeNumber(args[1]) orelse 0;
                    if (n < 1) return self.fail(call.pos, "NTH_VALUE's position must be a positive whole number", .{});
                    call.offset = n;
                },
            }
        },
        .agg => |a| {
            if (args.len == 0 and a != .count) return self.fail(call.pos, "`{s}` needs an argument", .{name});
            if (call.distinct and a != .count) return self.fail(call.pos, "DISTINCT over a window is only supported inside COUNT", .{});
            if (call.distinct and args.len == 0) return self.fail(call.pos, "COUNT(DISTINCT …) needs an argument", .{});
            if (args.len > 0) call.arg = args[0];
        },
    }
}

/// The inside of `OVER (...)` or `WINDOW w AS (...)`, past its `(`.
pub fn parseSpecBody(self: *Parser, pos: Pos) Error!Spec {
    var spec = Spec{ .pos = pos };
    if (self.atName() and !self.isKw("partition") and !self.isKw("order") and !self.isKw("rows") and !self.isKw("range") and !self.isKw("groups"))
        spec.base = try self.expectIdent();
    if (self.eatKw("partition")) {
        try self.expectKw("by");
        var keys = std.array_list.Managed(*ast.Expr).init(self.arena);
        while (true) {
            try keys.append(try self.parseExpr());
            if (!self.eat(.comma)) break;
        }
        spec.partition = try keys.toOwnedSlice();
    }
    if (self.eatKw("order")) {
        try self.expectKw("by");
        var keys = std.array_list.Managed(OrderExpr).init(self.arena);
        while (true) {
            const e = try self.parseExpr();
            var desc = false;
            if (self.eatKw("desc")) desc = true else _ = self.eatKw("asc");
            if (self.isKw("nulls"))
                return self.fail(self.curPos(), "NULLS FIRST / NULLS LAST is not supported inside OVER — nulls sort last in either direction", .{});
            try keys.append(.{ .e = e, .desc = desc });
            if (!self.eat(.comma)) break;
        }
        spec.order = try keys.toOwnedSlice();
    }
    if (self.isKw("rows") or self.isKw("range") or self.isKw("groups")) spec.frame = try self.parseFrame();
    _ = try self.expect(.rparen);
    return spec;
}

fn boundWord(b: ast.FrameBound) []const u8 {
    return switch (b) {
        .unbounded_preceding => "UNBOUNDED PRECEDING",
        .preceding => "PRECEDING",
        .current_row => "CURRENT ROW",
        .following => "FOLLOWING",
        .unbounded_following => "UNBOUNDED FOLLOWING",
    };
}

fn offsetNum(o: ast.FrameOffset) f64 {
    return std.fmt.parseFloat(f64, o.text) catch 0;
}

pub fn parseFrame(self: *Parser) Error!ast.WinFrame {
    const fpos = self.curPos();
    if (self.isKw("groups")) return self.fail(fpos, "GROUPS frames are not supported — use ROWS or RANGE", .{});
    const unit: ast.FrameUnit = if (self.eatKw("rows")) .rows else blk: {
        try self.expectKw("range");
        break :blk .range;
    };
    var start: ast.FrameBound = undefined;
    var end: ast.FrameBound = .current_row;
    if (self.eatKw("between")) {
        start = try self.parseBound();
        try self.expectKw("and");
        end = try self.parseBound();
    } else {
        start = try self.parseBound();
    }
    if (start == .unbounded_following) return self.fail(fpos, "a frame cannot start at UNBOUNDED FOLLOWING", .{});
    if (end == .unbounded_preceding) return self.fail(fpos, "a frame cannot end at UNBOUNDED PRECEDING", .{});
    if (start.rank() > end.rank())
        return self.fail(fpos, "this frame starts after it ends — {s} comes after {s}", .{ boundWord(start), boundWord(end) });
    if (start.rank() == end.rank() and start.hasOffset()) {
        const a = switch (start) {
            .preceding, .following => |o| o,
            else => unreachable,
        };
        const b = switch (end) {
            .preceding, .following => |o| o,
            else => unreachable,
        };
        if (a.unit == b.unit) {
            const later = if (start == .preceding) offsetNum(a) < offsetNum(b) else offsetNum(a) > offsetNum(b);
            if (later) return self.fail(fpos, "this frame starts after it ends — {s} {s} comes after {s} {s}", .{ a.text, boundWord(start), b.text, boundWord(end) });
        }
    }
    if (unit == .rows) for ([_]ast.FrameBound{ start, end }) |b| switch (b) {
        .preceding, .following => |o| if (o.unit != null or std.mem.indexOfAny(u8, o.text, ".eE") != null)
            return self.fail(fpos, "a ROWS offset is a whole number of rows", .{}),
        else => {},
    };
    return .{ .unit = unit, .start = start, .end = end };
}

fn intervalUnit(word: []const u8) ?ast.IntervalUnit {
    const units = [_]struct { []const u8, ast.IntervalUnit }{
        .{ "day", .day },       .{ "days", .day },
        .{ "hour", .hour },     .{ "hours", .hour },
        .{ "minute", .minute }, .{ "minutes", .minute },
        .{ "second", .second }, .{ "seconds", .second },
    };
    for (units) |u| if (std.ascii.eqlIgnoreCase(u[0], word)) return u[1];
    return null;
}

fn validNumber(text: []const u8) bool {
    const v = std.fmt.parseFloat(f64, text) catch return false;
    return v >= 0 and std.math.isFinite(v) and text.len > 0 and text[0] != '-' and text[0] != '+';
}

pub fn parseBound(self: *Parser) Error!ast.FrameBound {
    const bpos = self.curPos();
    if (self.eatKw("unbounded")) {
        if (self.eatKw("preceding")) return .unbounded_preceding;
        if (self.eatKw("following")) return .unbounded_following;
        return self.fail(self.curPos(), "expected PRECEDING or FOLLOWING after UNBOUNDED", .{});
    }
    if (self.eatKw("current")) {
        try self.expectKw("row");
        return .current_row;
    }
    var off: ast.FrameOffset = undefined;
    if (self.isKw("interval")) {
        _ = self.advance();
        const s = std.mem.trim(u8, (try self.expect(.string)).text, " ");
        if (std.mem.indexOfScalar(u8, s, ' ')) |sp| {
            const word = std.mem.trim(u8, s[sp + 1 ..], " ");
            off = .{ .text = s[0..sp], .unit = intervalUnit(word) orelse
                return self.fail(bpos, "unknown INTERVAL unit `{s}` in a frame — DAY, HOUR, MINUTE or SECOND", .{word}) };
        } else {
            const word = try self.expectIdent();
            off = .{ .text = s, .unit = intervalUnit(word) orelse
                return self.fail(bpos, "unknown INTERVAL unit `{s}` in a frame — DAY, HOUR, MINUTE or SECOND", .{word}) };
        }
        if (!validNumber(off.text)) return self.fail(bpos, "a frame's INTERVAL must be a non-negative number, not `{s}`", .{off.text});
    } else if (self.at(.int) or self.at(.float)) {
        off = .{ .text = self.advance().text };
        if (!validNumber(off.text)) return self.fail(bpos, "bad frame offset `{s}`", .{off.text});
    } else if (self.at(.minus)) {
        return self.fail(bpos, "a frame offset must not be negative", .{});
    } else {
        return self.fail(bpos, "expected a frame bound — UNBOUNDED PRECEDING, <n> PRECEDING, CURRENT ROW, <n> FOLLOWING or UNBOUNDED FOLLOWING", .{});
    }
    if (self.eatKw("preceding")) return .{ .preceding = off };
    if (self.eatKw("following")) return .{ .following = off };
    return self.fail(self.curPos(), "expected PRECEDING or FOLLOWING after a frame offset", .{});
}

/// `WINDOW w AS (...)[, ...]` after HAVING.
pub fn parseWindowClause(self: *Parser, named: *std.array_list.Managed(NamedWindow)) Error!void {
    while (true) {
        const pos = self.curPos();
        const name = try self.expectIdent();
        for (named.items) |nw| if (std.ascii.eqlIgnoreCase(nw.name, name))
            return self.fail(pos, "window `{s}` is defined twice", .{name});
        try self.expectKw("as");
        _ = try self.expect(.lparen);
        try named.append(.{ .name = name, .spec = try self.parseSpecBody(pos) });
        if (!self.eat(.comma)) break;
    }
}

/// `spec` with the named window it builds on merged in, as SQL copies one: `OVER w`
/// takes it whole; `OVER (w ...)` may add an ORDER BY it lacks and a frame, never a
/// PARTITION BY, and never extends a window that has a frame.
pub fn resolveSpec(self: *Parser, spec: Spec, named: []const NamedWindow, depth: usize) Error!Spec {
    const b = spec.base orelse return spec;
    if (depth > 16) return self.fail(spec.pos, "named windows refer to each other more than 16 deep", .{});
    const nw = for (named) |n| {
        if (std.ascii.eqlIgnoreCase(n.name, b)) break n;
    } else return self.fail(spec.pos, "unknown window `{s}` — define it after HAVING: WINDOW {s} AS (...)", .{ b, b });
    const base = try self.resolveSpec(nw.spec, named, depth + 1);
    if (spec.bare) return .{ .partition = base.partition, .order = base.order, .frame = base.frame, .pos = spec.pos };
    if (spec.partition.len > 0) return self.fail(spec.pos, "OVER ({s} ...) cannot add a PARTITION BY to the window it names", .{b});
    if (spec.order.len > 0 and base.order.len > 0) return self.fail(spec.pos, "OVER ({s} ...) cannot replace the ORDER BY of the window it names", .{b});
    if (base.frame != null) return self.fail(spec.pos, "window `{s}` has a frame, so OVER ({s} ...) cannot extend it — write OVER {s}", .{ b, b, b });
    return .{ .partition = base.partition, .order = if (spec.order.len > 0) spec.order else base.order, .frame = spec.frame, .pos = spec.pos };
}

/// What each function asks of its window: ranking and offsets an ORDER BY to number
/// by and no frame, a RANGE offset exactly one ORDER BY key.
pub fn checkCall(self: *Parser, call: WinCall, spec: Spec) Error!void {
    const ordered = call.func == .win and switch (call.func.win) {
        .first_value, .last_value, .nth_value => false,
        else => true,
    };
    if (ordered and spec.order.len == 0)
        return self.fail(call.pos, "a window function needs ORDER BY inside OVER (...) to number by", .{});
    const f = spec.frame orelse return;
    if (ordered)
        return self.fail(spec.pos, "a frame applies to an aggregate, FIRST_VALUE, LAST_VALUE or NTH_VALUE over a window; `{s}` does not take one", .{call.name});
    if (f.unit == .range and f.hasOffset() and spec.order.len != 1)
        return self.fail(spec.pos, "RANGE with an offset needs exactly one ORDER BY key inside OVER (...), a number, date or timestamp", .{});
}

/// Over a GROUP BY a window reads the aggregate's output: each column `e` names
/// must be a grouping key, an aggregate already lifted, or another window's.
pub fn groupedOnly(self: *Parser, e: *ast.Expr, group: []const ast.QualName, aggs: []const ast.AggItem, calls: []const WinCall, pos: Pos) Error!void {
    const Ctx = struct { p: *Parser, group: []const ast.QualName, aggs: []const ast.AggItem, calls: []const WinCall, bad: *?[]const u8 };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field) {
                const q = node.field;
                if (q.dollar) return @constCast(node);
                const n = q.last();
                for (cx.group) |k| if (std.mem.eql(u8, k.last(), n)) return @constCast(node);
                for (cx.aggs) |a| if (std.mem.eql(u8, a.name, n)) return @constCast(node);
                for (cx.calls) |c| if (std.mem.eql(u8, c.hidden, n)) return @constCast(node);
                if (cx.bad.* == null) cx.bad.* = n;
                return @constCast(node);
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    var bad: ?[]const u8 = null;
    _ = try S.recur(.{ .p = self, .group = group, .aggs = aggs, .calls = calls, .bad = &bad }, e);
    if (bad) |n| return self.fail(pos, "`{s}` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY", .{n});
}

/// Whether `e` reads the column `name`.
pub fn refsName(e: *const ast.Expr, name: []const u8) bool {
    return switch (e.*) {
        .field => |q| !q.dollar and q.parts.len == 1 and std.mem.eql(u8, q.parts[0], name),
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit, .lambda_var => false,
        .unary => |u| refsName(u.e, name),
        .binary => |b| refsName(b.l, name) or refsName(b.r, name),
        .call => |c| for (c.args) |a| {
            if (refsName(a, name)) break true;
        } else false,
        .cond => |c| refsName(c.cond, name) or refsName(c.then, name) or refsName(c.els, name),
        .cast => |c| refsName(c.e, name),
        .is_null => |n| refsName(n.e, name),
        .let_in => |l| refsName(l.value, name) or refsName(l.body, name),
        .lambda => |l| refsName(l.body, name),
        .match => |m| blk: {
            if (m.subject) |s| if (refsName(s, name)) break :blk true;
            for (m.arms) |arm| {
                for (arm.pats) |p| if (refsName(p, name)) break :blk true;
                if (arm.guard) |g| if (refsName(g, name)) break :blk true;
                if (refsName(arm.value, name)) break :blk true;
            }
            break :blk false;
        },
    };
}

/// Whether `e` reads any call's hidden column.
pub fn refsWindow(e: *const ast.Expr, calls: []const WinCall) bool {
    for (calls) |c| if (refsName(e, c.hidden)) return true;
    return false;
}

pub const Lower = struct {
    p: *Parser,
    calls: []WinCall,
    pre: std.array_list.Managed(ast.SelectItem),
    hidden: std.array_list.Managed([]const u8),
    carries: std.array_list.Managed([]const u8),
    seen: std.array_list.Managed([2][]const u8),

    pub fn init(p: *Parser, calls: []WinCall) Lower {
        return .{
            .p = p,
            .calls = calls,
            .pre = std.array_list.Managed(ast.SelectItem).init(p.arena),
            .hidden = std.array_list.Managed([]const u8).init(p.arena),
            .carries = std.array_list.Managed([]const u8).init(p.arena),
            .seen = std.array_list.Managed([2][]const u8).init(p.arena),
        };
    }

    fn carry(self: *Lower, name: []const u8) Error!void {
        for (self.calls) |c| if (std.mem.eql(u8, c.hidden, name)) return;
        for (self.carries.items) |c| if (std.mem.eql(u8, c, name)) return;
        try self.carries.append(name);
    }

    /// The column a window reads for `e`: itself when a bare column, else a hidden
    /// one computing it, shared by every equal expression.
    pub fn column(self: *Lower, e: *ast.Expr) Error!ast.QualName {
        if (e.* == .field and !e.field.dollar and e.field.parts.len == 1) {
            try self.carry(e.field.parts[0]);
            return e.field;
        }
        var buf = std.array_list.Managed(u8).init(self.p.arena);
        try self.p.exprKey(e, &buf);
        const key = buf.items;
        for (self.seen.items) |s| if (std.mem.eql(u8, s[0], key)) return self.p.qualOne(s[1]);
        self.p.derived_n += 1;
        const name = try std.fmt.allocPrint(self.p.arena, "__wa{d}", .{self.p.derived_n});
        try self.pre.append(.{ .computed = .{ .name = name, .expr = e } });
        try self.hidden.append(name);
        try self.seen.append(.{ key, name });
        return self.p.qualOne(name);
    }

    /// `e`, an item evaluated after the windows, with each column it reads carried
    /// through them; a qualified one (`b.amt`) is computed into a hidden column.
    pub fn carryExpr(self: *Lower, e: *ast.Expr) Error!*ast.Expr {
        const S = struct {
            fn recur(l: *Lower, node: *const ast.Expr) Error!*ast.Expr {
                if (node.* == .field) {
                    if (node.field.dollar) return @constCast(node);
                    if (node.field.parts.len == 1) {
                        try l.carry(node.field.parts[0]);
                        return @constCast(node);
                    }
                    return l.p.mk(.{ .field = try l.column(@constCast(node)) });
                }
                return ast.rebuildExpr(l.p.arena, node, l, recur);
            }
        };
        return S.recur(self, e);
    }

    /// The window stages, one per distinct PARTITION BY / ORDER BY, the calls'
    /// specs already resolved into `specs`.
    pub fn stages(self: *Lower, specs: []const Spec, pos: Pos) Error![]const ast.Stage {
        const Group = struct {
            key: []const u8,
            part: []const ast.QualName,
            ord: []const ast.SortKey,
            funcs: std.array_list.Managed(ast.WindowFunc),
        };
        var groups = std.array_list.Managed(Group).init(self.p.arena);
        for (self.calls, specs) |c, sp| {
            const part = try self.p.arena.alloc(ast.QualName, sp.partition.len);
            for (sp.partition, part) |e, *q| q.* = try self.column(e);
            const ord = try self.p.arena.alloc(ast.SortKey, sp.order.len);
            for (sp.order, ord) |o, *k| k.* = .{ .field = try self.column(o.e), .desc = o.desc };
            var key = std.array_list.Managed(u8).init(self.p.arena);
            for (part) |q| {
                try key.appendSlice(q.last());
                try key.append(',');
            }
            try key.append('|');
            for (ord) |k| {
                try key.appendSlice(k.field.last());
                try key.appendSlice(if (k.desc) " desc," else ",");
            }
            const arg: ?ast.QualName = if (c.arg) |a| try self.column(a) else null;
            const out = c.out orelse c.hidden;
            if (c.out == null) try self.hidden.append(c.hidden);
            const wf = ast.WindowFunc{
                .func = c.func,
                .out = out,
                .arg = arg,
                .offset = c.offset,
                .default = c.default,
                .distinct = c.distinct,
                .ignore_nulls = c.ignore_nulls,
                .frame = sp.frame,
            };
            const g = for (groups.items) |*g| {
                if (std.mem.eql(u8, g.key, key.items)) break g;
            } else blk: {
                try groups.append(.{ .key = key.items, .part = part, .ord = ord, .funcs = std.array_list.Managed(ast.WindowFunc).init(self.p.arena) });
                break :blk &groups.items[groups.items.len - 1];
            };
            try g.funcs.append(wf);
        }
        const out = try self.p.arena.alloc(ast.Stage, groups.items.len);
        for (groups.items, out) |*g, *st| st.* = .{
            .node = .{ .window = .{ .funcs = try g.funcs.toOwnedSlice(), .partition_by = g.part, .order_by = g.ord } },
            .hints = &.{},
            .pos = pos,
        };
        return out;
    }
};

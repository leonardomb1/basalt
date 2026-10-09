//! Nested-loop joins: what a join whose ON has no `=` between a left and a right
//! value runs as (`a.x < b.y`, `a.d BETWEEN b.from AND b.to`, `a.k = b.k OR
//! a.alt = b.k`). The right side is materialized once and only in memory: it never
//! spills, and past the join's `max_build` (else `join_build_byte_cap`) it fails
//! with `JoinBuildTooLarge`. Each probe row is paired with build rows and the ON is
//! evaluated over the pairs side by side (`pair_schema`, laid out as the inner
//! join's output), vectorized, at most `chunk` pairs at a time. One call emits at
//! most one chunk's matches plus the probe rows that finished unmatched, so neither
//! a pair batch nor an output batch grows with the product of the sides. Inner,
//! left, right, full, semi and anti; right and full flag the build rows that
//! matched and drain the rest once the probe side ends.
//!
//! Range path. When an AND conjunct of the ON compares a left value L with a right
//! value R of one orderable type, `rangeOf` finds it: `L >= R`, `L > R` make R a
//! `lower` bound (R at most L), `L <= R`, `L < R` an `upper` one, and
//! `L BETWEEN R1 AND R2` is both. The build rows are sorted by the lower bound's R
//! (else the upper's) and each probe row is paired only with the rows a binary
//! search leaves: a prefix for a lower bound, a suffix for an upper one, and with
//! both, the prefix cut at the front up to where the running maximum of the upper
//! bound reaches L. Searches are non-strict and order by `compareValues`, so they
//! keep every row the comparison could hold for; the whole ON is still evaluated on
//! the rows kept. A build row with a null bound is dropped, since its conjunct, and
//! so the ON, cannot hold; a probe row with a null L pairs with nothing.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const JoinIndex = @import("join.zig").JoinIndex;
const Op = @import("../op.zig").Op;
const Scan = @import("../op.zig").Scan;
const Stats = @import("../op.zig").Stats;
const TestSource = @import("testing_util.zig").TestSource;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const compareValues = @import("../eval/support.zig").compareValues;
const eval = @import("../eval.zig");
const failLabel = @import("../op.zig").failLabel;
const fieldIndex = @import("../eval/support.zig").fieldIndex;
const nullColumn = @import("join.zig").nullColumn;
const op_mod = @import("../op.zig");
const std = @import("std");
const takeCol = @import("join.zig").takeCol;
const testing = std.testing;
const types = @import("../../lang/types.zig");

pub const default_chunk: usize = 1 << 16;
const drain_chunk = 4096;

/// One side of a range: a left-side value and a right-side value compared in the
/// ON, both written over the pair schema, with their types there.
pub const Bound = struct {
    left: *const ast.Expr,
    right: *const ast.Expr,
    left_ty: types.Type,
    right_ty: types.Type,
};

/// `lower`: a conjunct that holds only where the right value is at most the left
/// one; `upper`: only where it is at least the left one.
pub const Range = struct {
    lower: ?Bound = null,
    upper: ?Bound = null,
};

/// The range shape of `cond` (resolved over `pair`, whose first `nleft` fields are
/// the left side's), or null when no AND conjunct compares one side's value with
/// the other's in a type the search can order: numbers of any kind, or one kind
/// among text, date, time and timestamp.
pub fn rangeOf(arena: std.mem.Allocator, cond: ?*const ast.Expr, pair: types.Schema, nleft: usize) std.mem.Allocator.Error!?Range {
    const c = cond orelse return null;
    var conj = std.array_list.Managed(*const ast.Expr).init(arena);
    try splitAnd(c, &conj);
    var out = Range{};
    for (conj.items) |e| {
        if (e.* != .binary) continue;
        const b = e.binary;
        const op = b.op;
        if (op != .lt and op != .le and op != .gt and op != .ge) continue;
        const sl = try sidesOf(arena, b.l, pair, nleft);
        const sr = try sidesOf(arena, b.r, pair, nleft);
        const l_left = sl == .left and sr == .right;
        const l_right = sl == .right and sr == .left;
        if (!l_left and !l_right) continue;
        const lv = if (l_left) b.l else b.r;
        const rv = if (l_left) b.r else b.l;
        var tc = eval.TypeCtx{ .schema = pair, .arena = arena };
        const lt = tc.typeOf(lv) catch continue;
        const rt = tc.typeOf(rv) catch continue;
        if (!orderable(lt, rt)) continue;
        const bound = Bound{ .left = lv, .right = rv, .left_ty = lt, .right_ty = rt };
        const right_at_most = if (l_left) (op == .gt or op == .ge) else (op == .lt or op == .le);
        if (right_at_most) {
            if (out.lower == null) out.lower = bound;
        } else if (out.upper == null) out.upper = bound;
    }
    if (out.lower == null and out.upper == null) return null;
    return out;
}

fn orderable(a: types.Type, b: types.Type) bool {
    if (a.unknown or b.unknown) return false;
    if (a.kind.isNumeric() and b.kind.isNumeric()) return true;
    if (a.kind != b.kind) return false;
    return switch (a.kind) {
        .string, .date, .time, .timestamp => true,
        else => false,
    };
}

fn splitAnd(e: *const ast.Expr, out: *std.array_list.Managed(*const ast.Expr)) std.mem.Allocator.Error!void {
    if (e.* == .binary and e.binary.op == .@"and") {
        try splitAnd(e.binary.l, out);
        try splitAnd(e.binary.r, out);
    } else try out.append(e);
}

const Side = enum { none, left, right, both };

/// Which side's columns `e` reads; `both` also for anything the search cannot
/// evaluate on one side alone (an unknown name, a `$` param, a lambda).
fn sidesOf(arena: std.mem.Allocator, e: *const ast.Expr, pair: types.Schema, nleft: usize) std.mem.Allocator.Error!Side {
    var left = false;
    var right = false;
    var other = false;
    const Walk = struct {
        arena: std.mem.Allocator,
        pair: types.Schema,
        nleft: usize,
        left: *bool,
        right: *bool,
        other: *bool,
        fn recur(w: @This(), x: *const ast.Expr) std.mem.Allocator.Error!*ast.Expr {
            switch (x.*) {
                .field => |q| {
                    if (q.dollar) {
                        w.other.* = true;
                    } else if (fieldIndex(w.pair, q)) |i| {
                        if (i < w.nleft) w.left.* = true else w.right.* = true;
                    } else w.other.* = true;
                    return @constCast(x);
                },
                .lambda, .lambda_var, .let_in => w.other.* = true,
                else => {},
            }
            return ast.rebuildExpr(w.arena, x, w, recur);
        }
    };
    _ = try Walk.recur(.{ .arena = arena, .pair = pair, .nleft = nleft, .left = &left, .right = &right, .other = &other }, e);
    if (other or (left and right)) return .both;
    if (left) return .left;
    if (right) return .right;
    return .none;
}

const Span = struct { lo: usize, hi: usize };

/// The build rows the range path keeps, in sort order, with their sort key and,
/// when there are both bounds, the running maximum of the upper bound.
const Sorted = struct {
    order: []usize,
    keys: []Value,
    pmax: ?[]Value,
    by_lower: bool,
};

const Cur = struct {
    batch: Batch,
    row: usize = 0,
    pos: usize = 0,
    hit: bool = false,
    spans: ?[]Span = null,
};

pub const NLJoin = struct {
    stats: Stats = .{},
    probe: Op,
    build: ?Op,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    /// The ON over `pair_schema`; null pairs every row with every row.
    cond: ?*const ast.Expr = null,
    pair_schema: ?*const types.Schema = null,
    range: ?Range = null,
    state: std.mem.Allocator,
    err: ?*ErrCtx = null,
    build_cap: ?usize = null,
    chunk: usize = default_chunk,

    index: ?*JoinIndex = null,
    matched: ?[]bool = null,
    sorted: ?Sorted = null,
    probe_mem: ?std.heap.ArenaAllocator = null,
    cur: ?Cur = null,
    probe_done: bool = false,
    drain_pos: usize = 0,

    pub fn next(self: *NLJoin, arena: std.mem.Allocator) anyerror!?Batch {
        const ix = try self.ensureBuild(arena);
        while (true) {
            if (self.cur == null) {
                if (self.probe_done) {
                    self.release();
                    return self.drain(arena, ix);
                }
                if (self.probe_mem == null) self.probe_mem = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                _ = self.probe_mem.?.reset(.retain_capacity);
                const pa = self.probe_mem.?.allocator();
                const lb = (try self.probe.next(pa)) orelse {
                    self.probe_done = true;
                    continue;
                };
                if (lb.len == 0) continue;
                self.cur = .{ .batch = lb, .spans = try self.spansFor(pa, ix, lb) };
            }
            const out = try self.step(arena, ix);
            if (self.cur.?.row >= self.cur.?.batch.len) self.cur = null;
            if (out.len > 0) return out;
        }
    }

    fn release(self: *NLJoin) void {
        if (self.probe_mem) |*m| m.deinit();
        self.probe_mem = null;
    }

    fn ensureBuild(self: *NLJoin, arena: std.mem.Allocator) anyerror!*JoinIndex {
        if (self.index) |ix| return ix;
        const build = self.build orelse return error.JoinHasNoBuildSide;
        const ix = JoinIndex.create(self.state, arena, build, self.right_schema, &.{}, self.build_cap orelse op_mod.join_build_byte_cap) catch |e| {
            if (self.err) |ec| ec.set("{s}", .{failLabel(e)});
            return e;
        };
        self.index = ix;
        if (self.kind == .right or self.kind == .full) {
            const m = try self.state.alloc(bool, ix.build_batch.len);
            @memset(m, false);
            self.matched = m;
        }
        return ix;
    }

    /// `b`'s columns after `n`-row null columns of `schema`, or before them when
    /// `nulls_first` is false: one side of a pair batch with the other side blank.
    fn halfPair(self: *NLJoin, arena: std.mem.Allocator, b: Batch, schema: *const types.Schema, nulls_first: bool) !Batch {
        const nn = schema.fields.len;
        const cols = try arena.alloc(column.Column, nn + b.columns.len);
        const at_nulls: usize = if (nulls_first) 0 else b.columns.len;
        const at_cols: usize = if (nulls_first) nn else 0;
        for (schema.fields, 0..) |f, i| cols[at_nulls + i] = try nullColumn(arena, f.ty, b.len);
        @memcpy(cols[at_cols..][0..b.columns.len], b.columns);
        return .{ .schema = self.pair_schema.?, .columns = cols, .len = b.len };
    }

    fn evalOver(self: *NLJoin, arena: std.mem.Allocator, e: *const ast.Expr, b: Batch, ty: types.Type) !column.Column {
        return eval.evalColumn(arena, e, b, ty) catch |err| {
            if (self.err) |ec| ec.set("{s}: in the join's ON condition", .{failLabel(err)});
            return err;
        };
    }

    /// The range path's sorted build rows, made on the first probe batch that
    /// meets a non-empty build side.
    fn ensureSorted(self: *NLJoin, ix: *JoinIndex) !Sorted {
        if (self.sorted) |s| return s;
        const r = self.range.?;
        var tmp = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer tmp.deinit();
        const pair = try self.halfPair(tmp.allocator(), ix.build_batch, self.left_schema, true);
        const by_lower = r.lower != null;
        const key_b = if (by_lower) r.lower.? else r.upper.?;
        const key_col = try self.evalOver(self.state, key_b.right, pair, key_b.right_ty);
        const up_col: ?column.Column = if (by_lower and r.upper != null)
            try self.evalOver(self.state, r.upper.?.right, pair, r.upper.?.right_ty)
        else
            null;
        const n = ix.build_batch.len;
        var order = std.array_list.Managed(usize).init(self.state);
        for (0..n) |i| {
            if (!key_col.validity.get(i)) continue;
            if (up_col) |u| if (!u.validity.get(i)) continue;
            try order.append(i);
        }
        const vals = try tmp.allocator().alloc(Value, n);
        for (order.items) |i| vals[i] = key_col.getValue(i);
        const Ctx = struct {
            vals: []const Value,
            fn less(c: @This(), a: usize, b: usize) bool {
                return compareValues(c.vals[a], c.vals[b]) == .lt;
            }
        };
        std.sort.pdq(usize, order.items, Ctx{ .vals = vals }, Ctx.less);
        const keys = try self.state.alloc(Value, order.items.len);
        for (order.items, keys) |i, *k| k.* = key_col.getValue(i);
        var pmax: ?[]Value = null;
        if (up_col) |u| {
            const pm = try self.state.alloc(Value, order.items.len);
            for (order.items, 0..) |i, k| {
                const v = u.getValue(i);
                pm[k] = if (k == 0 or compareValues(v, pm[k - 1]) == .gt) v else pm[k - 1];
            }
            pmax = pm;
        }
        self.sorted = .{ .order = order.items, .keys = keys, .pmax = pmax, .by_lower = by_lower };
        return self.sorted.?;
    }

    /// Each probe row's candidate span in sort order, or null when every build row
    /// is a candidate.
    fn spansFor(self: *NLJoin, pa: std.mem.Allocator, ix: *JoinIndex, lb: Batch) !?[]Span {
        const r = self.range orelse return null;
        if (ix.build_batch.len == 0) return null;
        const s = try self.ensureSorted(ix);
        const pair = try self.halfPair(pa, lb, self.right_schema, false);
        const lo_col: ?column.Column = if (r.lower) |b| try self.evalOver(pa, b.left, pair, b.left_ty) else null;
        const hi_col: ?column.Column = if (r.upper != null and (s.pmax != null or !s.by_lower))
            try self.evalOver(pa, r.upper.?.left, pair, r.upper.?.left_ty)
        else
            null;
        const n = s.order.len;
        const spans = try pa.alloc(Span, lb.len);
        for (spans, 0..) |*sp, row| {
            sp.* = .{ .lo = 0, .hi = n };
            if (lo_col) |c| {
                const v = c.getValue(row);
                if (v.isNull()) {
                    sp.* = .{ .lo = 0, .hi = 0 };
                    continue;
                }
                sp.hi = firstAbove(s.keys, v);
            }
            if (hi_col) |c| {
                const v = c.getValue(row);
                if (v.isNull()) {
                    sp.* = .{ .lo = 0, .hi = 0 };
                    continue;
                }
                sp.lo = firstAtLeast(if (s.by_lower) s.pmax.? else s.keys, v);
            }
            if (sp.lo > sp.hi) sp.lo = sp.hi;
        }
        return spans;
    }

    /// Pairs from where the current probe batch stands, up to `chunk` of them, the
    /// ON evaluated over them, and the output: the pairs it holds for, and each
    /// probe row finished here that matched nothing, as its join kind wants it.
    fn step(self: *NLJoin, arena: std.mem.Allocator, ix: *JoinIndex) anyerror!Batch {
        const c = &self.cur.?;
        const lb = c.batch;
        const nb = ix.build_batch.len;
        var cl = std.array_list.Managed(usize).init(arena);
        var cr = std.array_list.Managed(usize).init(arena);
        const start_row = c.row;
        while (c.row < lb.len and cl.items.len < self.chunk) {
            const sp: Span = if (c.spans) |s| s[c.row] else .{ .lo = 0, .hi = nb };
            const from = sp.lo + c.pos;
            const take = @min(sp.hi -| from, self.chunk - cl.items.len);
            for (from..from + take) |i| {
                try cl.append(c.row);
                try cr.append(if (c.spans != null) self.sorted.?.order[i] else i);
            }
            c.pos += take;
            if (from + take >= sp.hi) {
                c.row += 1;
                c.pos = 0;
            }
        }

        const pass = try arena.alloc(bool, cl.items.len);
        @memset(pass, true);
        if (self.cond != null and cl.items.len > 0) {
            const nl = lb.columns.len;
            const cols = try arena.alloc(column.Column, nl + ix.build_batch.columns.len);
            for (lb.columns, 0..) |col, i| cols[i] = try takeCol(arena, col, cl.items, &.{});
            for (ix.build_batch.columns, 0..) |col, k| cols[nl + k] = try takeCol(arena, col, cr.items, &.{});
            const pairs = Batch{ .schema = self.pair_schema.?, .columns = cols, .len = cl.items.len };
            const mask = try self.evalOver(arena, self.cond.?, pairs, types.Type.init(.bool));
            for (pass, 0..) |*p, i| p.* = mask.validity.get(i) and mask.data.b[i];
        }

        const fill_right = self.kind == .left or self.kind == .full;
        const pairs_out = self.kind != .semi and self.kind != .anti;
        var lidx = std.array_list.Managed(usize).init(arena);
        var ridx = std.array_list.Managed(usize).init(arena);
        var rnull = std.array_list.Managed(bool).init(arena);
        const last = if (c.pos > 0) c.row + 1 else c.row;
        var hit = c.hit;
        var p: usize = 0;
        var row = start_row;
        while (row < last) : (row += 1) {
            while (p < cl.items.len and cl.items[p] == row) : (p += 1) {
                if (!pass[p]) continue;
                hit = true;
                if (!pairs_out) continue;
                try lidx.append(row);
                try ridx.append(cr.items[p]);
                if (fill_right) try rnull.append(false);
                if (self.matched) |m| m[cr.items[p]] = true;
            }
            if (row == c.row) break;
            switch (self.kind) {
                .semi => if (hit) try lidx.append(row),
                .anti => if (!hit) try lidx.append(row),
                else => if (!hit and fill_right) {
                    try lidx.append(row);
                    try ridx.append(0);
                    try rnull.append(true);
                },
            }
            hit = false;
        }
        c.hit = hit;
        return self.gather(arena, ix, lb, lidx.items, ridx.items, rnull.items);
    }

    /// right/full: once the probe side ends, every build row nothing matched, the
    /// left columns null, in chunks.
    fn drain(self: *NLJoin, arena: std.mem.Allocator, ix: *JoinIndex) anyerror!?Batch {
        const matched = self.matched orelse return null;
        var ridx = std.array_list.Managed(usize).init(arena);
        while (self.drain_pos < ix.build_batch.len and ridx.items.len < drain_chunk) : (self.drain_pos += 1) {
            if (!matched[self.drain_pos]) try ridx.append(self.drain_pos);
        }
        if (ridx.items.len == 0) return null;
        return try self.gather(arena, ix, null, &.{}, ridx.items, &.{});
    }

    /// The output rows, one gather per column; `lb == null` is the drain, whose
    /// left side is all null.
    fn gather(self: *NLJoin, arena: std.mem.Allocator, ix: *JoinIndex, lb: ?Batch, lidx: []const usize, ridx: []const usize, rnull: []const bool) anyerror!Batch {
        const n = if (lb != null) lidx.len else ridx.len;
        const emit_right = self.kind != .semi and self.kind != .anti;
        const nleft = self.left_schema.fields.len;
        const cols = try arena.alloc(column.Column, nleft + if (emit_right) ix.build_batch.columns.len else 0);
        for (0..nleft) |i| {
            cols[i] = if (lb) |b|
                try takeCol(arena, b.columns[i], lidx, &.{})
            else
                try nullColumn(arena, self.left_schema.fields[i].ty, n);
        }
        if (emit_right) for (ix.build_batch.columns, 0..) |col, k| {
            cols[nleft + k] = try takeCol(arena, col, ridx, rnull);
        };
        return .{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

/// The first index whose key orders after `v`: the end of the rows with a key at
/// most `v`. A key that does not compare with `v` counts as not after it.
fn firstAbove(keys: []const Value, v: Value) usize {
    var lo: usize = 0;
    var hi: usize = keys.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareValues(keys[mid], v) == .gt) hi = mid else lo = mid + 1;
    }
    return lo;
}

/// The first index whose key is at least `v`. A key that does not compare with
/// `v` counts as at least it, so nothing is cut that might match.
fn firstAtLeast(keys: []const Value, v: Value) usize {
    var lo: usize = 0;
    var hi: usize = keys.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareValues(keys[mid], v) == .lt) lo = mid + 1 else hi = mid;
    }
    return lo;
}

const t_int = types.Type.init(.int).asNullable();
const t_str = types.Type.init(.string).asNullable();

const nl_left_schema = types.Schema{ .fields = &.{
    .{ .name = "x", .ty = t_int },
    .{ .name = "lv", .ty = t_str },
} };

const nl_right_schema = types.Schema{ .fields = &.{
    .{ .name = "lo", .ty = t_int },
    .{ .name = "hi", .ty = t_int },
    .{ .name = "rv", .ty = t_str },
} };

const nl_pair_schema = types.Schema{ .fields = &.{
    .{ .name = "x", .ty = t_int },
    .{ .name = "lv", .ty = t_str },
    .{ .name = "lo", .ty = t_int },
    .{ .name = "hi", .ty = t_int },
    .{ .name = "rv", .ty = t_str },
} };

const LRow = struct { x: ?i64, lv: ?[]const u8 };
const RRow = struct { lo: ?i64, hi: ?i64, rv: ?[]const u8 };

const Cond = enum { less, between, or_eq, above_hi, all };

fn mkE(a: std.mem.Allocator, e: ast.Expr) !*ast.Expr {
    const p = try a.create(ast.Expr);
    p.* = e;
    return p;
}

fn fld(a: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const parts = try a.alloc([]const u8, 1);
    parts[0] = name;
    return mkE(a, .{ .field = .{ .parts = parts } });
}

fn bin(a: std.mem.Allocator, op: ast.BinOp, l: *ast.Expr, r: *ast.Expr) !*ast.Expr {
    return mkE(a, .{ .binary = .{ .op = op, .l = l, .r = r } });
}

fn condExpr(a: std.mem.Allocator, c: Cond) !?*const ast.Expr {
    return switch (c) {
        .less => try bin(a, .lt, try fld(a, "x"), try fld(a, "lo")),
        .between => try bin(a, .@"and", try bin(a, .ge, try fld(a, "x"), try fld(a, "lo")), try bin(a, .le, try fld(a, "x"), try fld(a, "hi"))),
        .or_eq => try bin(a, .@"or", try bin(a, .eq, try fld(a, "x"), try fld(a, "lo")), try bin(a, .eq, try fld(a, "lv"), try fld(a, "rv"))),
        .above_hi => try bin(a, .gt, try fld(a, "x"), try fld(a, "hi")),
        .all => null,
    };
}

fn holds(c: Cond, l: LRow, r: RRow) bool {
    return switch (c) {
        .less => l.x != null and r.lo != null and l.x.? < r.lo.?,
        .between => l.x != null and r.lo != null and r.hi != null and l.x.? >= r.lo.? and l.x.? <= r.hi.?,
        .or_eq => (l.x != null and r.lo != null and l.x.? == r.lo.?) or
            (l.lv != null and r.rv != null and std.mem.eql(u8, l.lv.?, r.rv.?)),
        .above_hi => l.x != null and r.hi != null and l.x.? > r.hi.?,
        .all => true,
    };
}

fn optInt(a: std.mem.Allocator, v: ?i64) ![]const u8 {
    return if (v) |x| std.fmt.allocPrint(a, "{d}", .{x}) else "-";
}

fn rowText(a: std.mem.Allocator, l: ?LRow, r: ?RRow, emit_right: bool) ![]const u8 {
    const lx = try optInt(a, if (l) |x| x.x else null);
    const lv = if (l) |x| x.lv orelse "-" else "-";
    if (!emit_right) return std.fmt.allocPrint(a, "{s}|{s}", .{ lx, lv });
    const lo = try optInt(a, if (r) |x| x.lo else null);
    const hi = try optInt(a, if (r) |x| x.hi else null);
    const rv = if (r) |x| x.rv orelse "-" else "-";
    return std.fmt.allocPrint(a, "{s}|{s}|{s}|{s}|{s}", .{ lx, lv, lo, hi, rv });
}

fn lessText(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.order(u8, x, y) == .lt;
}

fn expected(a: std.mem.Allocator, kind: ast.JoinKind, c: Cond, ls: []const LRow, rs: []const RRow) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(a);
    const emit_right = kind != .semi and kind != .anti;
    const rhit = try a.alloc(bool, rs.len);
    @memset(rhit, false);
    for (ls) |l| {
        var hit = false;
        for (rs, 0..) |r, i| {
            if (!holds(c, l, r)) continue;
            hit = true;
            rhit[i] = true;
            if (emit_right) try out.append(try rowText(a, l, r, true));
        }
        switch (kind) {
            .semi => if (hit) try out.append(try rowText(a, l, null, false)),
            .anti => if (!hit) try out.append(try rowText(a, l, null, false)),
            .left, .full => if (!hit) try out.append(try rowText(a, l, null, true)),
            else => {},
        }
    }
    if (kind == .right or kind == .full) for (rs, rhit) |r, h| {
        if (!h) try out.append(try rowText(a, null, r, true));
    };
    std.mem.sort([]const u8, out.items, {}, lessText);
    return out.items;
}

fn cellText(a: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .null => "-",
        .int => |x| std.fmt.allocPrint(a, "{d}", .{x}),
        .string => |s| s,
        else => error.TestUnexpectedResult,
    };
}

fn leftBatches(a: std.mem.Allocator, ls: []const LRow, per: usize) ![]const Batch {
    var out = std.array_list.Managed(Batch).init(a);
    var i: usize = 0;
    while (i < ls.len) : (i += per) {
        const part = ls[i..@min(ls.len, i + per)];
        var xb = column.Builder.init(a, t_int);
        var vb = column.Builder.init(a, t_str);
        for (part) |l| {
            try xb.append(if (l.x) |x| Value{ .int = x } else .null);
            try vb.append(if (l.lv) |s| Value{ .string = s } else .null);
        }
        const cols = try a.alloc(column.Column, 2);
        cols[0] = try xb.finish();
        cols[1] = try vb.finish();
        try out.append(.{ .schema = &nl_left_schema, .columns = cols, .len = part.len });
    }
    return out.items;
}

fn rightBatches(a: std.mem.Allocator, rs: []const RRow, per: usize) ![]const Batch {
    var out = std.array_list.Managed(Batch).init(a);
    var i: usize = 0;
    while (i < rs.len) : (i += per) {
        const part = rs[i..@min(rs.len, i + per)];
        var lb = column.Builder.init(a, t_int);
        var hb = column.Builder.init(a, t_int);
        var vb = column.Builder.init(a, t_str);
        for (part) |r| {
            try lb.append(if (r.lo) |x| Value{ .int = x } else .null);
            try hb.append(if (r.hi) |x| Value{ .int = x } else .null);
            try vb.append(if (r.rv) |s| Value{ .string = s } else .null);
        }
        const cols = try a.alloc(column.Column, 3);
        cols[0] = try lb.finish();
        cols[1] = try hb.finish();
        cols[2] = try vb.finish();
        try out.append(.{ .schema = &nl_right_schema, .columns = cols, .len = part.len });
    }
    return out.items;
}

const RunOpts = struct { range: bool = false, chunk: usize = default_chunk, per: usize = 1000, cap: ?usize = null };

fn runNL(a: std.mem.Allocator, kind: ast.JoinKind, c: Cond, ls: []const LRow, rs: []const RRow, o: RunOpts) ![]const []const u8 {
    var lts = TestSource{ .schema_ = nl_left_schema, .batches = try leftBatches(a, ls, o.per) };
    var rts = TestSource{ .schema_ = nl_right_schema, .batches = try rightBatches(a, rs, o.per) };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    const cond = try condExpr(a, c);
    var jn = NLJoin{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_schema = &nl_left_schema,
        .right_schema = &nl_right_schema,
        .out_schema = &nl_pair_schema,
        .kind = kind,
        .cond = cond,
        .pair_schema = &nl_pair_schema,
        .range = if (o.range) try rangeOf(a, cond, nl_pair_schema, 2) else null,
        .state = a,
        .chunk = o.chunk,
        .build_cap = o.cap,
    };
    const top = Op{ .nl_join = &jn };
    var out = std.array_list.Managed([]const u8).init(a);
    while (try top.next(a)) |b| {
        for (0..b.len) |r| {
            var cells = std.array_list.Managed([]const u8).init(a);
            for (b.columns) |col| try cells.append(try cellText(a, col.getValue(r)));
            try out.append(try std.mem.join(a, "|", cells.items));
        }
    }
    std.mem.sort([]const u8, out.items, {}, lessText);
    return out.items;
}

fn expectRows(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

const all_kinds = [_]ast.JoinKind{ .inner, .left, .right, .full, .semi, .anti };

test "nljoin: `<`, BETWEEN, OR of equalities and no condition, every kind; a null comparison never matches" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ls = [_]LRow{ .{ .x = 1, .lv = "a" }, .{ .x = 5, .lv = "b" }, .{ .x = null, .lv = "c" }, .{ .x = 9, .lv = null } };
    const rs = [_]RRow{ .{ .lo = 2, .hi = 6, .rv = "c" }, .{ .lo = 5, .hi = 5, .rv = "q" }, .{ .lo = null, .hi = 3, .rv = "a" }, .{ .lo = 0, .hi = null, .rv = null } };
    for ([_]Cond{ .less, .between, .or_eq, .above_hi, .all }) |c| for (all_kinds) |kind| for ([_]bool{ false, true }) |rg| {
        const want = try expected(a, kind, c, &ls, &rs);
        try expectRows(want, try runNL(a, kind, c, &ls, &rs, .{ .range = rg }));
    };
    try expectRows(&.{ "1|a|2|6|c", "1|a|5|5|q" }, try runNL(a, .inner, .less, &ls, &rs, .{}));
    try expectRows(&.{ "-|c|-|-|-", "1|a|-|-|-", "5|b|2|6|c", "5|b|5|5|q", "9|-|-|-|-" }, try runNL(a, .left, .between, &ls, &rs, .{ .range = true }));
}

test "nljoin: an empty side gives every kind its unmatched rows or nothing" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ls = [_]LRow{ .{ .x = 1, .lv = "a" }, .{ .x = 2, .lv = "b" } };
    const rs = [_]RRow{.{ .lo = 2, .hi = 6, .rv = "c" }};
    for (all_kinds) |kind| for ([_]bool{ false, true }) |rg| {
        try expectRows(try expected(a, kind, .less, &ls, &.{}), try runNL(a, kind, .less, &ls, &.{}, .{ .range = rg }));
        try expectRows(try expected(a, kind, .less, &.{}, &rs), try runNL(a, kind, .less, &.{}, &rs, .{ .range = rg }));
    };
    try testing.expectEqual(@as(usize, 2), (try runNL(a, .left, .less, &ls, &.{}, .{})).len);
    try testing.expectEqual(@as(usize, 1), (try runNL(a, .right, .less, &.{}, &rs, .{})).len);
}

test "nljoin: a build side past its cap fails with JoinBuildTooLarge" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ls = [_]LRow{.{ .x = 1, .lv = "a" }};
    const rs = [_]RRow{ .{ .lo = 2, .hi = 6, .rv = "c" }, .{ .lo = 3, .hi = 7, .rv = "d" } };
    try testing.expectError(error.JoinBuildTooLarge, runNL(a, .inner, .less, &ls, &rs, .{ .cap = 8 }));
}

test "nljoin: the range path and the plain loop agree with a reference on random data, across batches and chunk boundaries" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rnd = prng.random();
    const tags = [_][]const u8{ "a", "b", "c", "d" };
    for (0..6) |round| {
        const nl = rnd.uintLessThan(usize, 60);
        const nr = rnd.uintLessThan(usize, 40);
        const ls = try a.alloc(LRow, nl);
        for (ls) |*l| l.* = .{
            .x = if (rnd.uintLessThan(u8, 10) == 0) null else rnd.intRangeAtMost(i64, -5, 50),
            .lv = if (rnd.uintLessThan(u8, 8) == 0) null else tags[rnd.uintLessThan(usize, tags.len)],
        };
        const rs = try a.alloc(RRow, nr);
        for (rs) |*r| {
            const lo = rnd.intRangeAtMost(i64, -5, 50);
            r.* = .{
                .lo = if (rnd.uintLessThan(u8, 10) == 0) null else lo,
                .hi = if (rnd.uintLessThan(u8, 10) == 0) null else lo + rnd.intRangeAtMost(i64, -2, 8),
                .rv = if (rnd.uintLessThan(u8, 8) == 0) null else tags[rnd.uintLessThan(usize, tags.len)],
            };
        }
        for ([_]Cond{ .less, .between, .or_eq, .above_hi }) |c| for (all_kinds) |kind| {
            const want = try expected(a, kind, c, ls, rs);
            for ([_]usize{ 1, 7, default_chunk }) |chunk| for ([_]bool{ false, true }) |rg| {
                const per = 1 + (round * 5) % 13;
                try expectRows(want, try runNL(a, kind, c, ls, rs, .{ .range = rg, .chunk = chunk, .per = per }));
            };
        };
    }
}

test "nljoin: the range path pairs a probe row with only the intervals that can hold it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var rs: [10]RRow = undefined;
    for (&rs, 0..) |*r, i| {
        const lo: i64 = @intCast((9 - i) * 10);
        r.* = .{ .lo = lo, .hi = lo + 9, .rv = "r" };
    }
    const ls = [_]LRow{ .{ .x = 55, .lv = "a" }, .{ .x = -1, .lv = "b" }, .{ .x = null, .lv = "c" }, .{ .x = 99, .lv = "d" } };
    var lts = TestSource{ .schema_ = nl_left_schema, .batches = try leftBatches(a, &ls, 10) };
    var rts = TestSource{ .schema_ = nl_right_schema, .batches = try rightBatches(a, &rs, 10) };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    const cond = try condExpr(a, .between);
    var jn = NLJoin{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_schema = &nl_left_schema,
        .right_schema = &nl_right_schema,
        .out_schema = &nl_pair_schema,
        .kind = .inner,
        .cond = cond,
        .pair_schema = &nl_pair_schema,
        .range = try rangeOf(a, cond, nl_pair_schema, 2),
        .state = a,
    };
    const ix = try jn.ensureBuild(a);
    const lb = (try lscan.next(a)).?;
    const spans = (try jn.spansFor(a, ix, lb)).?;
    try testing.expectEqual(@as(usize, 1), spans[0].hi - spans[0].lo);
    try testing.expectEqual(@as(usize, 4), jn.sorted.?.order[spans[0].lo]);
    try testing.expectEqual(@as(usize, 0), spans[1].hi - spans[1].lo);
    try testing.expectEqual(@as(usize, 0), spans[2].hi - spans[2].lo);
    try testing.expectEqual(@as(usize, 1), spans[3].hi - spans[3].lo);
}

test "nljoin: rangeOf finds bounds of a left value against right values, and nothing else" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const between = (try rangeOf(a, try condExpr(a, .between), nl_pair_schema, 2)).?;
    try testing.expect(between.lower != null and between.upper != null);
    try testing.expectEqualStrings("lo", between.lower.?.right.field.parts[0]);
    try testing.expectEqualStrings("hi", between.upper.?.right.field.parts[0]);
    const less = (try rangeOf(a, try condExpr(a, .less), nl_pair_schema, 2)).?;
    try testing.expect(less.lower == null and less.upper != null);
    const above = (try rangeOf(a, try condExpr(a, .above_hi), nl_pair_schema, 2)).?;
    try testing.expect(above.lower != null and above.upper == null);
    try testing.expect(try rangeOf(a, try condExpr(a, .or_eq), nl_pair_schema, 2) == null);
    try testing.expect(try rangeOf(a, null, nl_pair_schema, 2) == null);
    const same_side = try bin(a, .lt, try fld(a, "lo"), try fld(a, "hi"));
    try testing.expect(try rangeOf(a, same_side, nl_pair_schema, 2) == null);
    const mixed = try bin(a, .lt, try fld(a, "lv"), try fld(a, "lo"));
    try testing.expect(try rangeOf(a, mixed, nl_pair_schema, 2) == null);
}

//! Abstract syntax tree for the DSL. All nodes are arena-allocated by the parser;
//! recursive expression nodes use `*Expr` pointers into that same arena. Nothing
//! here is freed individually: the parser owns one arena for the whole program,
//! and the tree is immutable, so passes rebuild through `rebuildExpr`.
//!
//! Names and scope. A `Span` is 1-based with `end` just past the last character,
//! as an editor's range takes it. In a `QualName`, `safe[i]` says the separator
//! before `parts[i+1]` was `?.`; empty means none was. Only JSON-param paths honor
//! `?.` (a missing key yields null); on a plain column it is a type-check error.
//! A `$name` (`dollar`) is script scope: a PARAM, LET, loop variable or JSON
//! param. A bare name is a column, a `LET … IN` or a function binding, and nothing
//! script-scoped may stand in for it. A `lambda_var` is never a column, so no pass
//! collecting a query's columns takes `t` in `t -> t > 1` for one; lambdas are
//! values only as arguments of the JSON array functions.
//!
//! Expressions. `LetIn` is inlined by `expand.zig`, so the type-checker and
//! evaluator never see it. `TRY_CAST` is `Cast` with `safe`: a failed conversion
//! is null, so it is always nullable. `is empty` is true for null or an empty
//! string; both null tests are total. A `Match` has a subject with `,`-alternated
//! patterns per arm, or no subject and a boolean guard per arm; `_` has neither.
//!
//! Set by the runtime, never surface syntax: `Read.where` (raw SQL pushed down
//! from a union stage's `@[where]`, untranslated), `Read.cols` (the columns later
//! stages provably need; empty is all, since a narrow `SELECT` once fetched all
//! 300 columns of a wide table), `Join.right_filter` (a WHERE on the right side's
//! columns pushed into where that side is read) and a window's `top_k`.
//!
//! Semantics the nodes carry. A null-aware anti join (`NOT IN (SELECT ...)`),
//! unlike `NOT EXISTS`, keeps no row when the subquery returned a NULL, and keeps
//! a NULL `x` only against an empty one. `INTERSECT`/`EXCEPT` compare NULLs equal,
//! unlike join keys. A `UNION ALL BY NAME` reconciles arms to a canon schema by
//! name (take, NULL-fill, drop, cast); plain `UNION ALL` aligns by position. A
//! window stage is a breaker, bounded by its largest partition; without `ROWS
//! BETWEEN` the frame is the whole partition, or with `ORDER BY` everything up to
//! the current row's peers. `FROM RANGE` bounds and `CALL` arguments resolve at
//! plan time; an empty `FROM BUFFER` dir resolves from the `INTO BUFFER`
//! declaration, whose endpoint acks after fsync and answers 503 past `MAX`.
//!
//! Statements. A statement `LET` is folded once at plan time in declaration order
//! and is sealed: never bound from outside, never part of an endpoint's parameter
//! surface. Its query form takes one column of one row, and zero rows read as
//! NULL. `PRINT` writes through the run logger to stderr, never stdout, which is
//! the data contract. A fired `THROW` aborts before a row is read and is never
//! transient. A function body is an expression (inlined by `expand.zig`, recursion
//! rejected), a statement block run by `CALL` with its parameters as loop
//! variables, or a table query whose tokens re-parse at every `FROM f(args)`, so
//! nothing downstream sees the call. Without `OR REPLACE` a duplicate function is
//! an error; a parameter's declared type is checked only against literal
//! arguments, and defaults may only trail. `FOR EACH` runs its body per row of a
//! discovery read, a JSON-array param or an in-engine query; declared loop types
//! let a `match` compare as that type rather than as strings. A statement `match`
//! with no matching arm and no `_` is a no-op. `EXPLAIN COSTS` is refused, as
//! there is no cost model.

const std = @import("std");
const types = @import("types.zig");
const token = @import("token.zig");

pub const Pos = struct { line: u32, col: u32 };

pub const Span = struct { start: Pos, end: Pos };

pub const QualName = struct {
    parts: []const []const u8,
    safe: []const bool = &.{},
    dollar: bool = false,
    span: ?Span = null,

    pub fn single(self: QualName) ?[]const u8 {
        return if (self.parts.len == 1) self.parts[0] else null;
    }
    pub fn last(self: QualName) []const u8 {
        return self.parts[self.parts.len - 1];
    }
};

pub const BinOp = enum { add, sub, mul, div, mod, bit_and, bit_or, bit_xor, shl, shr, eq, ne, lt, le, gt, ge, @"and", @"or" };
pub const UnOp = enum { neg, not, bit_not };

pub const Expr = union(enum) {
    null_lit,
    bool_lit: bool,
    int_lit: i64,
    float_lit: f64,
    str_lit: []const u8,
    field: QualName,
    unary: Unary,
    binary: Binary,
    call: Call,
    cond: Cond,
    match: Match,
    cast: Cast,
    is_null: IsNull,
    let_in: LetIn,
    lambda: Lambda,
    lambda_var: []const u8,

    pub const Unary = struct { op: UnOp, e: *Expr };
    pub const Binary = struct { op: BinOp, l: *Expr, r: *Expr };
    pub const Call = struct { name: []const u8, args: []const *Expr, distinct: bool = false, span: ?Span = null };
    pub const Cond = struct { cond: *Expr, then: *Expr, els: *Expr };
    pub const LetIn = struct { name: []const u8, value: *Expr, body: *Expr };
    pub const Lambda = struct { params: []const []const u8, body: *Expr };
    pub const Cast = struct { e: *Expr, ty: types.Type, safe: bool = false };
    pub const NullTest = enum { is_null, is_empty };
    pub const IsNull = struct { e: *Expr, negated: bool, kind: NullTest = .is_null };
};

pub const Match = struct {
    subject: ?*Expr,
    arms: []const MatchArm,
};

pub const MatchArm = struct {
    pats: []const *Expr,
    guard: ?*Expr,
    value: *Expr,
    is_default: bool,
};

fn mkExpr(arena: std.mem.Allocator, e: Expr) !*Expr {
    const p = try arena.create(Expr);
    p.* = e;
    return p;
}

/// Rebuilds `e` with each child replaced by `recur(ctx, child)`, copying every own
/// field exactly; leaves are shared. The single place that enumerates `Expr`'s
/// fields, so no pass can drop one on rebuild (that bug hit `is_null.kind` three times).
pub fn rebuildExpr(arena: std.mem.Allocator, e: *const Expr, ctx: anytype, comptime recur: anytype) !*Expr {
    return switch (e.*) {
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit, .field, .lambda_var => @constCast(e),
        .lambda => |l| try mkExpr(arena, .{ .lambda = .{ .params = l.params, .body = try recur(ctx, l.body) } }),
        .unary => |u| try mkExpr(arena, .{ .unary = .{ .op = u.op, .e = try recur(ctx, u.e) } }),
        .binary => |b| try mkExpr(arena, .{ .binary = .{ .op = b.op, .l = try recur(ctx, b.l), .r = try recur(ctx, b.r) } }),
        .cond => |c| try mkExpr(arena, .{ .cond = .{ .cond = try recur(ctx, c.cond), .then = try recur(ctx, c.then), .els = try recur(ctx, c.els) } }),
        .cast => |c| try mkExpr(arena, .{ .cast = .{ .e = try recur(ctx, c.e), .ty = c.ty, .safe = c.safe } }),
        .is_null => |n| try mkExpr(arena, .{ .is_null = .{ .e = try recur(ctx, n.e), .negated = n.negated, .kind = n.kind } }),
        .let_in => |l| try mkExpr(arena, .{ .let_in = .{ .name = l.name, .value = try recur(ctx, l.value), .body = try recur(ctx, l.body) } }),
        .call => |c| blk: {
            const args = try arena.alloc(*Expr, c.args.len);
            for (c.args, args) |a, *out| out.* = try recur(ctx, a);
            break :blk try mkExpr(arena, .{ .call = .{ .name = c.name, .args = args, .distinct = c.distinct, .span = c.span } });
        },
        .match => |m| blk: {
            const subject = if (m.subject) |s| try recur(ctx, s) else null;
            const arms = try arena.alloc(MatchArm, m.arms.len);
            for (m.arms, arms) |arm, *out| {
                const pats = try arena.alloc(*Expr, arm.pats.len);
                for (arm.pats, pats) |p, *po| po.* = try recur(ctx, p);
                out.* = .{
                    .pats = pats,
                    .guard = if (arm.guard) |g| try recur(ctx, g) else null,
                    .value = try recur(ctx, arm.value),
                    .is_default = arm.is_default,
                };
            }
            break :blk try mkExpr(arena, .{ .match = .{ .subject = subject, .arms = arms } });
        },
    };
}

pub const Hint = struct { key: []const u8, value: HintVal, pos: Pos };

pub const HintVal = union(enum) {
    flag,
    str: []const u8,
    int: i64,
    ident: []const u8,
};

pub const Read = struct {
    connector: []const u8,
    form: ReadForm,
    where: []const u8 = "",
    cols: []const []const u8 = &.{},
};

pub const ReadForm = union(enum) {
    table: QualName,
    query: []const u8,
    path: []const u8,
    request: ?[]const types.BodyCol,
    buffer: BufferRef,
    range: RangeSpec,
    unit,
};

pub const RangeSpec = struct { lo: *Expr, hi: *Expr };

pub const BufferRef = struct { name: []const u8, dir: []const u8 = "" };

pub const BufferDecl = struct {
    name: []const u8,
    dir: []const u8,
    segment_bytes: u64 = 16 << 20,
    retain_hours: ?u32 = null,
    max_bytes: u64 = 1 << 30,
    schema: []const types.BodyCol,
    pos: Pos,
};

pub const SelectItem = union(enum) {
    star,
    star_except: []const []const u8,
    star_rename: []const Rename,
    field: QualName,
    computed: Computed,

    pub const Computed = struct { name: []const u8, expr: *Expr };
    pub const Rename = struct { from: []const u8, to: []const u8 };
};

pub const Explode = struct { field: []const u8, as_name: ?[]const u8, delim: ?[]const u8 = null, json: bool = false };

pub const Limit = struct { count: u64, offset: u64 = 0 };

pub const Distinct = struct { on: ?[]const QualName };

pub const SortKey = struct { field: QualName, desc: bool };
pub const Sort = struct { keys: []const SortKey };

pub const AggFunc = enum {
    count,
    sum,
    avg,
    min,
    max,
    median,
    count_if,
    bool_and,
    bool_or,
    bit_and,
    bit_or,
    bit_xor,
    var_samp,
    var_pop,
    stddev_samp,
    stddev_pop,
};
pub const AggItem = struct { name: []const u8, func: AggFunc, arg: ?*Expr, distinct: bool = false };
pub const Aggregate = struct { aggs: []const AggItem, by: []const QualName };

pub const JoinKind = enum { inner, left, semi, anti, right, full, cross };
/// An `=` of the ON whose sides the parser cannot tell apart, its columns written
/// without a table (`trim(code) = cast(cr AS string)`). The plan decides by where
/// the columns resolve: one side each makes a computed key, named `left_name` and
/// `right_name` on the side that computes it; both on one side make a filter.
pub const DeferredKey = struct {
    a: *Expr,
    b: *Expr,
    left_name: []const u8,
    right_name: []const u8,
    pos: Pos,
};

pub const Join = struct {
    kind: JoinKind,
    binding: []const u8,
    alias: []const u8 = "",
    left_keys: []const QualName,
    right_keys: []const QualName,
    null_aware: bool = false,
    right_filter: ?*Expr = null,
    deferred: []const DeferredKey = &.{},
    /// The rest of an outer join's ON, over a left row and a right row together:
    /// a pair joins only where it holds, so an unmatched row still comes out. With
    /// no key it is the whole ON, of any join kind.
    residual: ?*Expr = null,

    /// An ON with no `=` key, run as a nested-loop join.
    pub fn keyless(self: Join) bool {
        return self.kind != .cross and self.left_keys.len == 0 and self.deferred.len == 0;
    }

    pub fn rightStages(self: Join, arena: std.mem.Allocator, stages: []const Stage) ![]const Stage {
        const f = self.right_filter orelse return stages;
        const out = try arena.alloc(Stage, stages.len + 1);
        @memcpy(out[0..stages.len], stages);
        out[stages.len] = .{ .node = .{ .filter = f }, .hints = &.{}, .pos = if (stages.len > 0) stages[0].pos else .{ .line = 0, .col = 0 } };
        return out;
    }
};

pub const Write = struct {
    connector: []const u8,
    form: ?[]const u8,
    target: []const u8,
    mode: WriteMode,
};

pub const WriteMode = union(enum) {
    default,
    append,
    overwrite,
    upsert: Upsert,

    pub const Upsert = struct {
        keys: []const []const u8,
        partial: ?[]const []const u8 = null,
    };
};

pub const UnionBranch = struct { read: Read, tag: ?[]const u8, pipeline: ?Pipeline = null };

pub const Union = struct {
    branches: []const UnionBranch = &.{},
    discover_conn: []const u8 = "",
    discover_query: []const u8 = "",
    discover_json: []const u8 = "",
    discover_pipeline: ?Pipeline = null,
    positional: bool = false,
    set: SetOp = .union_all,
    pos: Pos,
};

pub const SetOp = enum { union_all, intersect, except };

pub const Stage = struct {
    node: Node,
    hints: []const Hint,
    pos: Pos,

    pub const Node = union(enum) {
        ref: []const u8,
        read: Read,
        union_: Union,
        filter: *Expr,
        select: []const SelectItem,
        explode: Explode,
        limit: Limit,
        distinct: Distinct,
        sort: Sort,
        aggregate: Aggregate,
        join: Join,
        window: Window,
        write: Write,
    };
};

pub const WinKind = enum { row_number, rank, dense_rank, lag, lead, sum, count, min, max, avg };
/// `default` is LAG/LEAD's third argument, the value past the partition's edge.
pub const WindowFunc = struct { kind: WinKind, out: []const u8, arg: ?QualName = null, offset: i64 = 1, default: ?*Expr = null, frame: WinFrame = .{} };
pub const WinFrame = struct { rows: bool = false, unbounded: bool = false, preceding: i64 = 0 };

pub const Window = struct {
    funcs: []const WindowFunc,
    top_k: ?u64 = null,
    partition_by: []const QualName = &.{},
    order_by: []const SortKey = &.{},
};

pub const Pipeline = struct {
    stages: []const Stage,
    pos: Pos,
    show: bool = false,
};

pub const ParamSource = enum { query, body, header };

pub const Param = struct {
    name: []const u8,
    ty: types.Type,
    default: ?*Expr,
    source: ?ParamSource,
    header_name: ?[]const u8 = null,
    pos: Pos,
    is_json: bool = false,
};

pub const Attr = struct { key: []const u8, value: *Expr, pos: Pos };

pub const Connection = struct {
    name: []const u8,
    connector: []const u8,
    config: []const Attr,
    pos: Pos,
    hints: []const Hint = &.{},
};

pub const Let = struct { name: []const u8, pipeline: Pipeline, pos: Pos };

pub const LetConst = struct { name: []const u8, expr: ?*Expr, query: ?Pipeline = null, pos: Pos };

pub const Print = struct { expr: *Expr, pos: Pos };

pub const FnParam = struct {
    name: []const u8,
    ty: ?types.Type = null,
    default: ?*Expr = null,
};

pub const FnBody = union(enum) {
    expr: *Expr,
    stmts: []const Stmt,
    table: []const token.Token,
};

pub const FnDecl = struct {
    name: []const u8,
    params: []const FnParam,
    body: FnBody,
    replace: bool = false,
    pos: Pos,
};

pub const CallStmt = struct {
    name: []const u8,
    args: []const *Expr,
    pos: Pos,
};

pub const Throw = struct {
    message: *Expr,
    when: ?*Expr = null,
    pos: Pos,
};

pub const ForSource = union(enum) {
    read: Read,
    json_path: QualName,
    pipeline: Pipeline,
};

pub const ForEach = struct {
    var_names: []const []const u8,
    var_types: []const ?types.Type = &.{},
    source: ForSource,
    hints: []const Hint,
    body: []const Stmt,
    pos: Pos,
};

pub const Kind = enum { batch, http };

pub const KindDecl = struct {
    kind: Kind,
    config: []const Attr,
    buffer: ?BufferDecl = null,
    pos: Pos,
};

pub const StmtMatch = struct {
    subject: ?*Expr,
    arms: []const StmtArm,
    pos: Pos,
};

pub const StmtArm = struct {
    pats: []const *Expr,
    guard: ?*Expr,
    body: []const Stmt,
    is_default: bool,
};

pub const Stmt = union(enum) {
    kind: KindDecl,
    param: Param,
    connection: Connection,
    binding: Let,
    output: Pipeline,
    for_each: ForEach,
    match: StmtMatch,
    func: FnDecl,
    let_const: LetConst,
    print: Print,
    call: CallStmt,
    throw: Throw,
    explain: ExplainStmt,
};

pub const ExplainMode = enum { none, plan, analyze, describe };

pub const ExplainStmt = struct {
    mode: ExplainMode,
    pipeline: Pipeline,
    pos: Pos,
};

pub const Program = struct { stmts: []const Stmt, explain: ExplainMode = .none };

test "rebuildExpr identity copies every field — no silent drop" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Id = struct {
        fn r(arena: std.mem.Allocator, e: *const Expr) anyerror!*Expr {
            return rebuildExpr(arena, e, arena, r);
        }
    };
    const x = try mkExpr(a, .{ .field = .{ .parts = &[_][]const u8{"x"} } });
    const empty = try mkExpr(a, .{ .is_null = .{ .e = x, .negated = true, .kind = .is_empty } });
    const casted = try mkExpr(a, .{ .cast = .{ .e = empty, .ty = types.Type.init(.int), .safe = true } });

    const out = try Id.r(a, casted);
    try std.testing.expect(out.* == .cast);
    try std.testing.expectEqual(types.TypeKind.int, out.cast.ty.kind);
    try std.testing.expect(out.cast.safe);
    try std.testing.expect(out.cast.e.* == .is_null);
    try std.testing.expectEqual(Expr.NullTest.is_empty, out.cast.e.is_null.kind);
    try std.testing.expect(out.cast.e.is_null.negated);

    const bound = try mkExpr(a, .{ .let_in = .{ .name = "v", .value = casted, .body = x } });
    const out2 = try Id.r(a, bound);
    try std.testing.expect(out2.* == .let_in);
    try std.testing.expectEqualStrings("v", out2.let_in.name);
    try std.testing.expect(out2.let_in.value.* == .cast);

    const args = try a.alloc(*Expr, 2);
    args[0] = x;
    args[1] = empty;
    const span: Span = .{ .start = .{ .line = 3, .col = 5 }, .end = .{ .line = 3, .col = 17 } };
    const call = try mkExpr(a, .{ .call = .{ .name = "concat", .args = args, .distinct = true, .span = span } });
    const out3 = try Id.r(a, call);
    try std.testing.expect(out3.* == .call);
    try std.testing.expectEqualStrings("concat", out3.call.name);
    try std.testing.expectEqual(@as(usize, 2), out3.call.args.len);
    try std.testing.expect(out3.call.args[1].* == .is_null);
    try std.testing.expect(out3.call.distinct);
    try std.testing.expectEqual(span, out3.call.span.?);

    const one = try mkExpr(a, .{ .int_lit = 1 });
    const two = try mkExpr(a, .{ .int_lit = 2 });
    const pats = try a.alloc(*Expr, 2);
    pats[0] = one;
    pats[1] = two;
    const arms = try a.alloc(MatchArm, 3);
    arms[0] = .{ .pats = pats, .guard = null, .value = x, .is_default = false };
    arms[1] = .{ .pats = &.{}, .guard = empty, .value = one, .is_default = false };
    arms[2] = .{ .pats = &.{}, .guard = null, .value = two, .is_default = true };
    const m = try mkExpr(a, .{ .match = .{ .subject = x, .arms = arms } });
    const out4 = try Id.r(a, m);
    try std.testing.expect(out4.* == .match);
    try std.testing.expect(out4.match.subject.?.* == .field);
    try std.testing.expectEqual(@as(usize, 3), out4.match.arms.len);
    try std.testing.expectEqual(@as(usize, 2), out4.match.arms[0].pats.len);
    try std.testing.expectEqual(@as(i64, 1), out4.match.arms[0].pats[0].int_lit);
    try std.testing.expectEqual(@as(i64, 2), out4.match.arms[0].pats[1].int_lit);
    try std.testing.expect(out4.match.arms[0].guard == null);
    try std.testing.expect(!out4.match.arms[0].is_default);
    try std.testing.expect(out4.match.arms[0].value.* == .field);
    try std.testing.expect(out4.match.arms[1].guard.?.* == .is_null);
    try std.testing.expect(!out4.match.arms[1].is_default);
    try std.testing.expectEqual(@as(i64, 1), out4.match.arms[1].value.int_lit);
    try std.testing.expect(out4.match.arms[2].is_default);
    try std.testing.expect(out4.match.arms[2].guard == null);
    try std.testing.expectEqual(@as(usize, 0), out4.match.arms[2].pats.len);
    try std.testing.expectEqual(@as(i64, 2), out4.match.arms[2].value.int_lit);

    const bin = try mkExpr(a, .{ .binary = .{ .op = .ge, .l = x, .r = two } });
    const out5 = try Id.r(a, bin);
    try std.testing.expect(out5.* == .binary);
    try std.testing.expectEqual(BinOp.ge, out5.binary.op);
    try std.testing.expect(out5.binary.l.* == .field);
    try std.testing.expectEqual(@as(i64, 2), out5.binary.r.int_lit);

    const un = try mkExpr(a, .{ .unary = .{ .op = .bit_not, .e = one } });
    const out6 = try Id.r(a, un);
    try std.testing.expect(out6.* == .unary);
    try std.testing.expectEqual(UnOp.bit_not, out6.unary.op);
    try std.testing.expectEqual(@as(i64, 1), out6.unary.e.int_lit);

    const lam = try mkExpr(a, .{ .lambda = .{ .params = &[_][]const u8{ "acc", "t" }, .body = bin } });
    const out7 = try Id.r(a, lam);
    try std.testing.expect(out7.* == .lambda);
    try std.testing.expectEqual(@as(usize, 2), out7.lambda.params.len);
    try std.testing.expectEqualStrings("acc", out7.lambda.params[0]);
    try std.testing.expectEqualStrings("t", out7.lambda.params[1]);
    try std.testing.expect(out7.lambda.body.* == .binary);
    try std.testing.expectEqual(BinOp.ge, out7.lambda.body.binary.op);
}

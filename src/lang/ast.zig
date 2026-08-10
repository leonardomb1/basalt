//! Abstract syntax tree for the DSL. All nodes are arena-allocated by the parser;
//! recursive expression nodes use `*Expr` pointers into that same arena. Nothing
//! here is freed individually — the parser owns one arena for the whole program.

const std = @import("std");
const types = @import("types.zig");

pub const Pos = struct { line: u32, col: u32 };

/// A possibly-qualified name: `id`, `public.orders`, `a.b.c`. `safe` (when present)
/// is parallel to the *separators*: `safe[i]` is true when the separator between
/// `parts[i]` and `parts[i+1]` was `?.` (safe navigation) rather than `.` — so its
/// length is `parts.len - 1`, and it aligns one-to-one with `parts[1..]`. An empty
/// `safe` means no `?.` was used (every separator is a plain `.`). Only JSON-param
/// paths honor `?.` (a missing intermediate key resolves the path to null instead of
/// erroring); a `?.` on a plain column reference is rejected at type-check.
pub const QualName = struct {
    parts: []const []const u8,
    safe: []const bool = &.{},

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

    pub const Unary = struct { op: UnOp, e: *Expr };
    pub const Binary = struct { op: BinOp, l: *Expr, r: *Expr };
    pub const Call = struct { name: []const u8, args: []const *Expr, distinct: bool = false };
    pub const Cond = struct { cond: *Expr, then: *Expr, els: *Expr };
    /// `let name = value in body`: a local binding inside an expression. Inlined at
    /// plan time (`expand.zig`) by substituting `value` for `name` in `body`, so the
    /// type-checker and evaluator never see it — like a single-use `fn`. Lets a `fn`
    /// body (or any computed column) name an intermediate instead of repeating it.
    pub const LetIn = struct { name: []const u8, value: *Expr, body: *Expr };
    /// `CAST(e AS ty)`, and — with `safe` set — `TRY_CAST(e AS ty)`, which has
    /// identical syntax and type rules except that a failed conversion yields
    /// null instead of raising. A safe cast is therefore always nullable.
    pub const Cast = struct { e: *Expr, ty: types.Type, safe: bool = false };
    /// `x is null` / `x is not null` (`.is_null`), and the additive
    /// `x is empty` / `x is not empty` (`.is_empty`) which is true when the
    /// operand is null OR an empty string. Both forms are total (never null).
    pub const NullTest = enum { is_null, is_empty };
    pub const IsNull = struct { e: *Expr, negated: bool, kind: NullTest = .is_null };
};

/// `match [subject] arm... end`. Subject form: each arm has `pats` (alternation
/// via `,`). Guard form (no subject): each arm has a boolean `guard`. A default
/// arm (`_`) has empty `pats` and null `guard`.
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

/// The structural recursion every expression-transforming pass shares: rebuild `e`
/// by replacing each child sub-expression with `recur(ctx, child)`, copying every
/// own field (`op`, `ty`, `kind`, `name`, `negated`, `is_default`, …) exactly. Leaf
/// nodes have no children, so they are shared unchanged (the AST is immutable).
///
/// This is the SINGLE place that enumerates `Expr`'s variants and fields, so a pass
/// can't silently drop a field on rebuild (the bug that hit `is_null.kind` three
/// times). A pass handles the few node kinds it cares about, then delegates the rest
/// here; adding a new `Expr` field or variant is a one-line change guarded by Zig's
/// exhaustive switch. `recur` is the pass's own transform, so `ctx`/error set flow
/// through unchanged (the error set is inferred per instantiation).
pub fn rebuildExpr(arena: std.mem.Allocator, e: *const Expr, ctx: anytype, comptime recur: anytype) !*Expr {
    return switch (e.*) {
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit, .field => @constCast(e),
        .unary => |u| try mkExpr(arena, .{ .unary = .{ .op = u.op, .e = try recur(ctx, u.e) } }),
        .binary => |b| try mkExpr(arena, .{ .binary = .{ .op = b.op, .l = try recur(ctx, b.l), .r = try recur(ctx, b.r) } }),
        .cond => |c| try mkExpr(arena, .{ .cond = .{ .cond = try recur(ctx, c.cond), .then = try recur(ctx, c.then), .els = try recur(ctx, c.els) } }),
        .cast => |c| try mkExpr(arena, .{ .cast = .{ .e = try recur(ctx, c.e), .ty = c.ty, .safe = c.safe } }),
        .is_null => |n| try mkExpr(arena, .{ .is_null = .{ .e = try recur(ctx, n.e), .negated = n.negated, .kind = n.kind } }),
        .let_in => |l| try mkExpr(arena, .{ .let_in = .{ .name = l.name, .value = try recur(ctx, l.value), .body = try recur(ctx, l.body) } }),
        .call => |c| blk: {
            const args = try arena.alloc(*Expr, c.args.len);
            for (c.args, args) |a, *out| out.* = try recur(ctx, a);
            break :blk try mkExpr(arena, .{ .call = .{ .name = c.name, .args = args, .distinct = c.distinct } });
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
    /// Optional raw SQL predicate pushed down to the source (no dialect
    /// translation). Not surface syntax on `read` itself — set by the runtime
    /// from a union stage's `@[where = "..."]` hint. Empty = no predicate.
    where: []const u8 = "",
};

pub const ReadForm = union(enum) {
    table: QualName,
    query: []const u8,
    path: []const u8,
    /// The HTTP request body as rows. Payload = the declared schema
    /// (`FROM BODY (col TYPE [NOT NULL], ...)`, enforced at bind time), or
    /// null to infer it from the first object (BSL `read request`).
    request: ?[]const types.BodyCol,
    /// `FROM BUFFER 'name' [AT 'dir']` — replay/drain a durable WAL buffer.
    /// An empty `dir` resolves from the program's `INTO BUFFER` declaration.
    buffer: BufferRef,
    /// `FROM RANGE(lo, hi)` — generated integers lo..hi-1. Bounds must
    /// resolve to int literals at plan time (params and loop vars allowed).
    range: RangeSpec,
    /// A `SELECT` with no FROM — one empty row the projection fills.
    unit,
};

pub const RangeSpec = struct { lo: *Expr, hi: *Expr };

pub const BufferRef = struct { name: []const u8, dir: []const u8 = "" };

/// `CREATE ENDPOINT ... ACCEPT BODY (schema) INTO BUFFER 'name' AT 'dir'
/// SEGMENT n MB [RETAIN UNTIL LOADED | RETAIN n HOURS]` — the durable-buffer
/// declaration (language.md §9). The endpoint acks 200 after fsync; the
/// pipeline drains the buffer asynchronously via `FROM BUFFER`.
pub const BufferDecl = struct {
    name: []const u8,
    dir: []const u8,
    segment_bytes: u64 = 16 << 20,
    retain_hours: ?u32 = null,
    /// Backpressure limit (`MAX n MB|GB`): bytes on disk beyond this ⇒ the
    /// endpoint answers 503 + Retry-After.
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

pub const Explode = struct { field: []const u8, as_name: ?[]const u8, delim: ?[]const u8 = null };

pub const Limit = struct { count: u64, offset: u64 = 0 };

pub const Distinct = struct { on: ?[]const QualName };

pub const SortKey = struct { field: QualName, desc: bool };
pub const Sort = struct { keys: []const SortKey };

pub const AggFunc = enum { count, sum, avg, min, max };
pub const AggItem = struct { name: []const u8, func: AggFunc, arg: ?*Expr, distinct: bool = false };
pub const Aggregate = struct { aggs: []const AggItem, by: []const QualName };

pub const JoinKind = enum { inner, left, semi, anti, right, full, cross };
/// `left_keys[i] = right_keys[i]` for every i — the equi-join conjunction.
/// Both sides are plain (possibly qualified) column names; `cross` carries none.
pub const Join = struct {
    kind: JoinKind,
    binding: []const u8,
    left_keys: []const QualName,
    right_keys: []const QualName,
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

/// One arm of a `UNION ALL BY NAME`. Usually a bare source (`read`), which is the
/// reconciliation case the feature was built for: N similar tables aligned by column
/// name. `pipeline` is set instead when the arm is a general query — a file, a
/// projection, an aggregate — in which case `read` is unused and the arm is built like
/// any other pipeline.
pub const UnionBranch = struct { read: Read, tag: ?[]const u8, pipeline: ?Pipeline = null };

/// A leading source that reconciles N tables to a canon schema and concatenates
/// them. Explicit: `union from <conn> <table|query|path> as "<tag>" ...`. Discovered:
/// `union <conn> tables "<query returning (table_name, tag)>"`. Reconciliation is by
/// name (take / NULL-fill missing / drop extra / cast type diffs); a `tag` column
/// (per-branch value) is optional. `tag` and `canon` come from the stage's `@[...]`.
pub const Union = struct {
    branches: []const UnionBranch = &.{},
    discover_conn: []const u8 = "",
    discover_query: []const u8 = "",
    discover_json: []const u8 = "",
    /// `EACH TABLE OF (SELECT ...)`: the branch list comes from a full basalt
    /// query run in-engine (its first two columns are table_name, tag) rather
    /// than from raw SQL sent to `discover_conn`. The discovered tables still
    /// live on `discover_conn` — inferred from the query's leading source, or
    /// named by a trailing `IN <conn>`.
    discover_pipeline: ?Pipeline = null,
    pos: Pos,
};

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

/// A window stage: compute one or more ranking functions over the rows, numbered
/// within each `partition_by` group in `order_by` order, and append them as columns.
/// It is a breaker — the whole partition has to be present before a row's number is
/// known — so memory is bounded by the largest partition rather than by batch size.
pub const WinKind = enum { row_number, rank, dense_rank, lag, lead, sum, count, min, max, avg };
/// `arg` is the column `LAG`/`LEAD` reads and `offset` how many rows back or forward;
/// the ranking functions take neither.
pub const WindowFunc = struct { kind: WinKind, out: []const u8, arg: ?QualName = null, offset: i64 = 1 };
pub const Window = struct {
    funcs: []const WindowFunc,
    partition_by: []const QualName = &.{},
    order_by: []const SortKey = &.{},
};

pub const Pipeline = struct { stages: []const Stage, pos: Pos };

pub const ParamSource = enum { query, body, header };

pub const Param = struct {
    name: []const u8,
    ty: types.Type,
    default: ?*Expr,
    source: ?ParamSource,
    /// `FROM HEADER('X-Tenant')`: which request header binds this param.
    /// Null (bare `FROM HEADER`) means the header is named like the param.
    header_name: ?[]const u8 = null,
    pos: Pos,
    /// `param x json from body`: the value is a JSON document (parsed into a
    /// separate binding namespace, navigated via `x.a.b` paths), not a scalar
    /// column value. `ty` is an unused placeholder when this is set.
    is_json: bool = false,
};

pub const Attr = struct { key: []const u8, value: *Expr, pos: Pos };

pub const Connection = struct {
    name: []const u8,
    connector: []const u8,
    config: []const Attr,
    pos: Pos,
};

pub const Let = struct { name: []const u8, pipeline: Pipeline, pos: Pos };

/// `LET name = <expr>;` at statement level: a script-scoped constant, folded ONCE
/// at plan time (in declaration order) and referenced as `$name` through the same
/// substitution machinery as a PARAM. Sealed — it can never be bound from outside
/// (`-p`, query string, header) and never joins an endpoint's parameter surface.
/// Distinct from `Expr.LetIn` (`LET x = v IN body`), which binds a bare name inside
/// one expression and disappears during expansion.
pub const LetConst = struct { name: []const u8, expr: *Expr, pos: Pos };

/// `PRINT <expr>;` — a script-authored progress line. The expression is evaluated
/// at plan time against the params, LETs and — inside a `FOR EACH` or statement
/// function body — the loop variables bound for that row, then written to stderr
/// through the run logger. Never stdout: that stream is the data contract.
pub const Print = struct { expr: *Expr, pos: Pos };

/// One declared parameter of a `CREATE FUNCTION`. `ty` is the optional declared
/// type — checked at expansion against *literal* arguments only (nothing else is
/// decidable there) and, for a statement-form function, used to coerce the
/// argument cell the way a `for` header's `name:type` does. `default` fills the
/// argument when the call omits it; defaults may only trail.
pub const FnParam = struct {
    name: []const u8,
    ty: ?types.Type = null,
    default: ?*Expr = null,
};

/// The two things a `CREATE FUNCTION` body can be.
///   * `.expr` — a scalar function, inlined at plan time (`expand.zig`) so the
///     type-checker and evaluator never see it. Recursion is rejected there.
///   * `.stmts` — a statement macro, invoked with `CALL f(args)`. The declaration
///     survives expansion; `run.zig` renders the block per call through the same
///     `${var}` machinery a `for` body uses, with the parameters as loop vars.
pub const FnBody = union(enum) {
    expr: *Expr,
    stmts: []const Stmt,
};

/// `CREATE [OR REPLACE] FUNCTION name(params) AS <body>`.
pub const FnDecl = struct {
    name: []const u8,
    params: []const FnParam,
    body: FnBody,
    /// `OR REPLACE`: the sanctioned overwrite of an earlier same-name
    /// declaration. Without it, a duplicate is an expansion error.
    replace: bool = false,
    pos: Pos,
};

/// `CALL name(args);` — invoke a statement-form function. Arguments are resolved
/// to text cells at plan time (literals, `$params`, or the loop variables in
/// scope at the call site).
pub const CallStmt = struct {
    name: []const u8,
    args: []const *Expr,
    pos: Pos,
};

/// `THROW <message> [WHEN <condition>];` — a script's own precondition, the half of
/// validation the engine cannot infer. Both operands are ordinary expressions over
/// PARAMs and LETs, so they are decidable at plan time: an absent or true condition
/// aborts before a row is read, with `message` as the error text verbatim. A fired
/// guard is permanent by construction — it is never classified as transient.
pub const Throw = struct {
    message: *Expr,
    when: ?*Expr = null,
    pos: Pos,
};

/// `for <var,...> in <source> @[...] <body>`: a plan-time fan-out. `source` is a
/// discovery read; the planner runs it once, mapping its first N columns onto the
/// N `var_names`, then runs `body` per row with each `${var}` interpolated into the
/// read/write targets. The body is a statement block — a bare pipeline is sugar for
/// a one-statement block — so it may hold `match` statements that branch per row on
/// the loop variables (e.g. picking an upsert key). `hints` carry `mode`
/// (sequential|parallel) and `on_error` (stop|continue).
/// A for-each source: a discovery `read` (`for x in <conn> query "..."`), a
/// JSON-array param path (`for x in job.tables`, with each object element's
/// fields bound to the loop variables by name), or a full basalt query run
/// in-engine (`FOR EACH ROW OF (SELECT ...)`), whose first N columns map onto
/// the N loop variables positionally.
pub const ForSource = union(enum) {
    read: Read,
    json_path: QualName,
    pipeline: Pipeline,
};

pub const ForEach = struct {
    var_names: []const []const u8,
    /// Optional declared type per loop variable (parallel to `var_names`; `null` =
    /// untyped/string). `for name, port:int in ...` lets a `match` over the loop
    /// values compare them as the declared type instead of as strings. An empty
    /// slice means every variable is untyped.
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
    /// Set by `ACCEPT ... INTO BUFFER` on a CREATE ENDPOINT (http only).
    buffer: ?BufferDecl = null,
    pos: Pos,
};

/// Plan-time structural dispatch: `match [subject] arm... end`, where each arm's
/// body is a `{ ... }` block of statements. Mirrors the expression `Match` arm
/// shapes (subject + `,` alternation, guard form, `_` default) but runs whole
/// statements. Evaluated once at plan time over params / loop variables; an
/// unmatched value with no `_` arm is a no-op.
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

/// `EXPLAIN` prefix on a program: print the plan instead of running it, or
/// (with `ANALYZE`) run it and print the plan back with measured actuals.
/// `COSTS` is rejected at parse time — there is no cost model to report.
pub const ExplainMode = enum { none, plan, analyze };

/// `EXPLAIN [ANALYZE] <query>;` in statement position: explain one pipeline where
/// it stands, against whatever the statements above it declared. `plan` renders
/// the static plan and executes nothing; `analyze` runs the pipeline into a
/// discarded sink and prints the operator tree with its measured actuals. Never
/// `.none` — a statement only exists because `EXPLAIN` was written.
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
    const casted = try mkExpr(a, .{ .cast = .{ .e = empty, .ty = types.Type.init(.int) } });

    const out = try Id.r(a, casted);
    try std.testing.expect(out.* == .cast);
    try std.testing.expectEqual(types.TypeKind.int, out.cast.ty.kind);
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
    const call = try mkExpr(a, .{ .call = .{ .name = "concat", .args = args } });
    const out3 = try Id.r(a, call);
    try std.testing.expect(out3.* == .call);
    try std.testing.expectEqualStrings("concat", out3.call.name);
    try std.testing.expectEqual(@as(usize, 2), out3.call.args.len);
    try std.testing.expect(out3.call.args[1].* == .is_null);
}

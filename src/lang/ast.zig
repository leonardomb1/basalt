//! Abstract syntax tree for the DSL. All nodes are arena-allocated by the parser;
//! recursive expression nodes use `*Expr` pointers into that same arena. Nothing
//! here is freed individually — the parser owns one arena for the whole program.

const std = @import("std");
const types = @import("types.zig");

pub const Pos = struct { line: u32, col: u32 };

/// A possibly-qualified name: `id`, `public.orders`, `a.b.c`.
pub const QualName = struct {
    parts: []const []const u8,

    pub fn single(self: QualName) ?[]const u8 {
        return if (self.parts.len == 1) self.parts[0] else null;
    }
    pub fn last(self: QualName) []const u8 {
        return self.parts[self.parts.len - 1];
    }
};

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

pub const BinOp = enum { add, sub, mul, div, mod, eq, ne, lt, le, gt, ge, @"and", @"or" };
pub const UnOp = enum { neg, not };

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
    cond: Cond, // if(c, a, b)
    match: Match,
    cast: Cast,
    is_null: IsNull,

    pub const Unary = struct { op: UnOp, e: *Expr };
    pub const Binary = struct { op: BinOp, l: *Expr, r: *Expr };
    pub const Call = struct { name: []const u8, args: []const *Expr };
    pub const Cond = struct { cond: *Expr, then: *Expr, els: *Expr };
    pub const Cast = struct { e: *Expr, ty: types.Type };
    pub const IsNull = struct { e: *Expr, negated: bool };
};

/// `match [subject] arm... end`. Subject form: each arm has `pats` (alternation
/// via `|`). Guard form (no subject): each arm has a boolean `guard`. A default
/// arm (`_`) has empty `pats` and null `guard`.
pub const Match = struct {
    subject: ?*Expr,
    arms: []const MatchArm,
};

pub const MatchArm = struct {
    pats: []const *Expr, // subject form patterns (empty for guard/default arms)
    guard: ?*Expr, // guard form condition (null otherwise)
    value: *Expr,
    is_default: bool,
};

// ---------------------------------------------------------------------------
// Hints  (@[ key [= value] , ... ])  — parsed, unused in v1
// ---------------------------------------------------------------------------

pub const Hint = struct { key: []const u8, value: HintVal, pos: Pos };

pub const HintVal = union(enum) {
    flag, // bare `key`
    str: []const u8,
    int: i64,
    ident: []const u8,
    size: Size, // `100mb`
};

pub const Size = struct { n: i64, unit: []const u8 };

// ---------------------------------------------------------------------------
// Operators / stages
// ---------------------------------------------------------------------------

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
    request,
};

pub const SelectItem = union(enum) {
    star,
    star_except: []const []const u8,
    field: QualName,
    computed: Computed,

    pub const Computed = struct { name: []const u8, expr: *Expr };
};

pub const Explode = struct { field: []const u8, as_name: ?[]const u8, delim: ?[]const u8 = null };

pub const Limit = struct { count: u64, offset: u64 = 0 };

pub const Distinct = struct { on: ?[]const QualName };

pub const SortKey = struct { field: QualName, desc: bool };
pub const Sort = struct { keys: []const SortKey };

pub const AggFunc = enum { count, sum, avg, min, max };
pub const AggItem = struct { name: []const u8, func: AggFunc, arg: ?*Expr };
pub const Aggregate = struct { aggs: []const AggItem, by: []const QualName };

pub const JoinKind = enum { inner, left, right, full, semi, anti, cross };
pub const Join = struct {
    kind: JoinKind,
    binding: []const u8, // right side: a `let` binding referenced by name
    left_key: QualName,
    right_key: QualName,
};

pub const Write = struct {
    connector: []const u8,
    form: ?[]const u8, // e.g. "stream_load"
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

pub const UnionBranch = struct { read: Read, tag: ?[]const u8 };

/// A leading source that reconciles N tables to a canon schema and concatenates
/// them. Explicit: `union from <conn> <table|query|path> as "<tag>" ...`. Discovered:
/// `union <conn> tables "<query returning (table_name, tag)>"`. Reconciliation is by
/// name (take / NULL-fill missing / drop extra / cast type diffs); a `tag` column
/// (per-branch value) is optional. `tag` and `canon` come from the stage's `@[...]`.
pub const Union = struct {
    branches: []const UnionBranch = &.{}, // explicit form
    discover_conn: []const u8 = "", // discovered/json form: connection holding the tables
    discover_query: []const u8 = "", // discovered form: query -> (table_name, tag)
    discover_json: []const u8 = "", // json form: a JSON array of {table, tag} objects
    pos: Pos,
};

pub const Stage = struct {
    node: Node,
    hints: []const Hint,
    pos: Pos,

    pub const Node = union(enum) {
        ref: []const u8, // a binding used as a source
        read: Read,
        union_: Union, // multi-source union (leading)
        filter: *Expr,
        select: []const SelectItem,
        explode: Explode,
        limit: Limit,
        distinct: Distinct,
        sort: Sort,
        aggregate: Aggregate,
        join: Join,
        write: Write,
    };
};

pub const Pipeline = struct { stages: []const Stage, pos: Pos };

// ---------------------------------------------------------------------------
// Top-level declarations
// ---------------------------------------------------------------------------

pub const ParamSource = enum { query, body, header };

pub const Param = struct {
    name: []const u8,
    ty: types.Type,
    default: ?*Expr,
    source: ?ParamSource,
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

/// `fn name(a, b) = <expr>`: a user-defined scalar function. Expanded inline at
/// plan time (`expand.zig`) — each call is replaced by the body with arguments
/// substituted for the parameters — so the type-checker and evaluator never see
/// user functions. Recursion is rejected during expansion.
pub const FnDecl = struct {
    name: []const u8,
    params: []const []const u8,
    body: *Expr,
    pos: Pos,
};

/// `for <var,...> in <source> @[...] <body>`: a plan-time fan-out. `source` is a
/// discovery read; the planner runs it once, mapping its first N columns onto the
/// N `var_names`, then runs `body` per row with each `${var}` interpolated into the
/// read/write targets. The body is a statement block — a bare pipeline is sugar for
/// a one-statement block — so it may hold `match` statements that branch per row on
/// the loop variables (e.g. picking an upsert key). `hints` carry `mode`
/// (sequential|parallel) and `on_error` (stop|continue).
/// A for-each source: either a discovery `read` (`for x in <conn> query "..."`)
/// or a JSON-array param path (`for x in job.tables`, with each object element's
/// fields bound to the loop variables by name).
pub const ForSource = union(enum) {
    read: Read,
    json_path: QualName,
};

pub const ForEach = struct {
    var_names: []const []const u8,
    source: ForSource,
    hints: []const Hint,
    body: []const Stmt,
    pos: Pos,
};

pub const Kind = enum { batch, http };

pub const KindDecl = struct { kind: Kind, config: []const Attr, pos: Pos };

/// Plan-time structural dispatch: `match [subject] arm... end`, where each arm's
/// body is a `{ ... }` block of statements. Mirrors the expression `Match` arm
/// shapes (subject + `|` alternation, guard form, `_` default) but runs whole
/// statements. Evaluated once at plan time over params / loop variables; an
/// unmatched value with no `_` arm is a no-op.
pub const StmtMatch = struct {
    subject: ?*Expr,
    arms: []const StmtArm,
    pos: Pos,
};

pub const StmtArm = struct {
    pats: []const *Expr, // subject-form patterns (empty for guard/default arms)
    guard: ?*Expr, // guard-form condition (null otherwise)
    body: []const Stmt, // `{ ... }` block to run when this arm matches
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
};

pub const Program = struct { stmts: []const Stmt };

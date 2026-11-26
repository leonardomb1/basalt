//! `basalt catalog` and the kernel's `catalog` op: every built-in function, keyword
//! and type the engine answers to, read from the tables the parser, the binder and
//! Tab already use, so an editor keeps no list of its own.
//!
//! A function is each of Tab's built-ins (`complete.builtin_functions`, which
//! `cli.zig`'s tests hold to the engine's registries both ways), its kind the
//! registry that knows it. Its parameters are read from its signature: a `[, x]` is
//! optional, the name before a `…` repeats, `x FROM y` and `x AS y` are two. Its
//! return type is what the binder answers for it over a spread of argument types,
//! null when the answers differ, as they do when the type follows the arguments,
//! or when the binder takes none of them. Its doc is the first sentence the reference
//! pages give it (`tools/fn_docs.zig`), or, for one of a family the pages describe by
//! a few members (`u16le_at` by `u8_at`'s row), its family's. Its aliases are the
//! other names an aggregate answers to and the other dialects' spellings the parser
//! reads as it (`year`, `datediff`).
//!
//! Keywords are the words the REPL colours as such and the words that end a
//! source, less the names listed as functions or types. Types are every name CAST
//! and PARAM read, with what each is and how it takes arguments. WITH options are
//! read where each format reads them, from no one table, and are not listed.
//!
//! The answer is the same for the life of the process, so the kernel builds its
//! JSON once.

const std = @import("std");
const build_options = @import("build_options");
const fn_docs = @import("fn_docs");
const complete = @import("repl/complete.zig");
const hilite = @import("repl/hilite.zig");
const eval = @import("../exec/eval.zig");
const jsontext = @import("../exec/jsontext.zig");
const aggregates = @import("../lang/aggregates.zig");
const stats = @import("../lang/parse/stats.zig");
const decl = @import("../lang/parse/decl.zig");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const expand = @import("../lang/expand.zig");
const parser = @import("../lang/sql_parser.zig");
const table_builtins = @import("../runtime/connect/table_builtins.zig");
const analyze = @import("../runtime/analyze.zig");
const binder = @import("../runtime/analyze/schema.zig");
const nextVal = @import("args.zig").nextVal;
const unknownOption = @import("args.zig").unknownOption;

pub const Kind = enum { scalar, aggregate, window, table };

pub const Param = struct { name: []const u8, optional: bool = false, variadic: bool = false };

pub const Function = struct {
    name: []const u8,
    kind: Kind,
    signature: []const u8,
    params: []const Param,
    returns: ?[]const u8,
    doc: ?[]const u8,
    aliases: []const []const u8,
};

pub const TypeName = struct { name: []const u8, is: []const u8, form: []const u8 };

pub const Catalog = struct {
    version: []const u8,
    functions: []const Function,
    keywords: []const []const u8,
    types: []const TypeName,
};

/// The nested types and the PARAM-only `JSON`, which `parseTypeName` reads by name.
const nested_types = [_]TypeName{
    .{ .name = "list", .is = "list", .form = "list(t)" },
    .{ .name = "map", .is = "map", .form = "map(k, v)" },
    .{ .name = "struct", .is = "struct", .form = "struct(name t, ...)" },
    .{ .name = "json", .is = "json", .form = "json (PARAM only)" },
};

fn kindOf(name: []const u8) Kind {
    if (aggregates.lookup(name) != null or stats.isStat(name)) return .aggregate;
    if (std.meta.stringToEnum(ast.WinKind, name) != null) return .window;
    if (table_builtins.lookup(name) != null) return .table;
    return .scalar;
}

/// Whether `name` is an aggregate's other name, listed under the first.
fn isAggAlias(name: []const u8) bool {
    const f = aggregates.lookup(name) orelse return false;
    return !std.mem.eql(u8, aggregates.spec(f).names[0], name);
}

pub fn build(gpa: std.mem.Allocator, a: std.mem.Allocator) !Catalog {
    var fns = std.array_list.Managed(Function).init(a);
    for (complete.builtin_functions) |b| {
        if (isAggAlias(b.name)) continue;
        const kind = kindOf(b.name);
        const params = try paramsOf(a, b.sig);
        var aliases = std.array_list.Managed([]const u8).init(a);
        if (aggregates.lookup(b.name)) |f| try aliases.appendSlice(aggregates.spec(f).names[1..]);
        for (expand.spellings) |sp| if (std.mem.eql(u8, sp[1], b.name)) try aliases.append(sp[0]);
        try fns.append(.{
            .name = b.name,
            .kind = kind,
            .signature = b.sig,
            .params = params,
            .returns = try returnsOf(gpa, a, b.name, kind, params),
            .doc = docOf(b.name, aliases.items),
            .aliases = aliases.items,
        });
    }
    std.sort.pdq(Function, fns.items, {}, struct {
        fn lt(_: void, x: Function, y: Function) bool {
            return std.mem.lessThan(u8, x.name, y.name);
        }
    }.lt);

    var tys = std.array_list.Managed(TypeName).init(a);
    for (decl.simple_types) |t| try tys.append(.{ .name = t.n, .is = @tagName(t.k), .form = t.n });
    for (decl.sized_text_types) |n| try tys.append(.{ .name = n, .is = "string", .form = try std.fmt.allocPrint(a, "{s}[(n)]", .{n}) });
    for (decl.decimal_types) |n| try tys.append(.{ .name = n, .is = "decimal", .form = try std.fmt.allocPrint(a, "{s}[(p[, s])]", .{n}) });
    try tys.appendSlice(&nested_types);

    var kws = std.array_list.Managed([]const u8).init(a);
    for ([2][]const []const u8{ &hilite.keywords, &parser.reserved_after_source }) |list| for (list) |k| {
        if (named(fns.items, tys.items, k) or inList(kws.items, k)) continue;
        try kws.append(k);
    };
    std.sort.pdq([]const u8, kws.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return .{ .version = build_options.version, .functions = fns.items, .keywords = kws.items, .types = tys.items };
}

fn inList(list: []const []const u8, w: []const u8) bool {
    for (list) |x| if (std.ascii.eqlIgnoreCase(x, w)) return true;
    return false;
}

/// Whether `w` is listed as a function, an alias of one, or a type.
fn named(fns: []const Function, tys: []const TypeName, w: []const u8) bool {
    for (fns) |f| if (std.ascii.eqlIgnoreCase(f.name, w) or inList(f.aliases, w)) return true;
    for (tys) |t| if (std.ascii.eqlIgnoreCase(t.name, w)) return true;
    return false;
}

fn findDoc(name: []const u8) ?[]const u8 {
    for (fn_docs.docs) |d| if (std.mem.eql(u8, d.name, name)) return d.doc;
    return null;
}

fn docOf(name: []const u8, aliases: []const []const u8) ?[]const u8 {
    if (findDoc(name)) |d| return d;
    for (aliases) |al| if (findDoc(al)) |d| return d;
    var buf: [64]u8 = undefined;
    const fam = family(&buf, name) orelse return null;
    var buf2: [64]u8 = undefined;
    for (fn_docs.docs) |d| if (family(&buf2, d.name)) |f| if (std.mem.eql(u8, f, fam)) return d.doc;
    return null;
}

/// A binary reader's or writer's family: its width as `#`, its sign as `I` (or `F`
/// for a float), its byte order dropped — `u16le_at` and `i8_at` are `I#_at`,
/// `fmt_f64be` is `fmt_F#`. Null for a name of no such family.
fn family(buf: []u8, name: []const u8) ?[]const u8 {
    const pre = if (std.mem.startsWith(u8, name, "fmt_")) "fmt_" else "";
    var rest = name[pre.len..];
    if (rest.len < 2 or std.mem.indexOfScalar(u8, "uif", rest[0]) == null or !std.ascii.isDigit(rest[1])) return null;
    const sign: u8 = if (rest[0] == 'f') 'F' else 'I';
    var i: usize = 1;
    while (i < rest.len and std.ascii.isDigit(rest[i])) i += 1;
    rest = rest[i..];
    if (std.mem.startsWith(u8, rest, "le") or std.mem.startsWith(u8, rest, "be")) rest = rest[2..];
    if (!(rest.len == 0 or std.mem.eql(u8, rest, "_at"))) return null;
    return std.fmt.bufPrint(buf, "{s}{c}#{s}", .{ pre, sign, rest }) catch null;
}

/// The parameters a signature names, between the call's first `(` and its match.
pub fn paramsOf(a: std.mem.Allocator, sig: []const u8) ![]const Param {
    const open = std.mem.indexOfScalar(u8, sig, '(') orelse return &.{};
    var depth: usize = 0;
    var close = sig.len;
    for (sig[open..], open..) |c, i| switch (c) {
        '(' => depth += 1,
        ')' => {
            depth -= 1;
            if (depth == 0) {
                close = i;
                break;
            }
        },
        else => {},
    };
    const body = sig[open + 1 .. close];
    var out = std.array_list.Managed(Param).init(a);
    var piece = std.array_list.Managed(u8).init(a);
    var optional_depth: usize = 0;
    var piece_optional = false;
    var paren: usize = 0;
    var quote = false;
    var i: usize = 0;
    while (i <= body.len) : (i += 1) {
        const c: u8 = if (i < body.len) body[i] else ',';
        if (quote) {
            if (c == '\'') quote = false;
            try piece.append(c);
            continue;
        }
        switch (c) {
            '\'' => {
                quote = true;
                try piece.append(c);
            },
            '(' => {
                paren += 1;
                try piece.append(c);
            },
            ')' => {
                paren -|= 1;
                try piece.append(c);
            },
            '[' => if (paren == 0 and std.mem.trimLeft(u8, body[i + 1 ..], " ").len > 0 and std.mem.trimLeft(u8, body[i + 1 ..], " ")[0] == ',') {
                optional_depth += 1;
                try flushParam(a, &out, &piece, piece_optional);
                piece_optional = true;
                i = std.mem.indexOfScalarPos(u8, body, i, ',').?;
            } else if (paren == 0 and clauseEnd(body, i) != null) {
                i = clauseEnd(body, i).?;
            } else {
                optional_depth += 1;
                piece_optional = true;
            },
            ']' => if (paren == 0) {
                optional_depth -|= 1;
                if (std.mem.trim(u8, piece.items, " ").len == 0) piece_optional = optional_depth > 0;
            } else try piece.append(c),
            ',' => if (paren == 0) {
                try flushParam(a, &out, &piece, piece_optional);
                piece_optional = optional_depth > 0;
            } else try piece.append(c),
            else => try piece.append(c),
        }
    }
    return out.toOwnedSlice();
}

/// Where a `[…]` opened at `at` holding a clause rather than parameters (`[ORDER BY
/// y]`) closes; null for one that holds a parameter.
fn clauseEnd(body: []const u8, at: usize) ?usize {
    const close = std.mem.indexOfScalarPos(u8, body, at, ']') orelse return null;
    const inner = std.mem.trim(u8, body[at + 1 .. close], " ");
    if (inner.len == 0 or !std.ascii.isUpper(inner[0])) return null;
    return close;
}

fn flushParam(a: std.mem.Allocator, out: *std.array_list.Managed(Param), piece: *std.array_list.Managed(u8), optional: bool) !void {
    defer piece.clearRetainingCapacity();
    const text = std.mem.trim(u8, piece.items, " ");
    if (text.len == 0) return;
    if (std.mem.eql(u8, text, "…") or std.mem.eql(u8, text, "...")) {
        if (out.items.len > 0) out.items[out.items.len - 1].variadic = true;
        return;
    }
    for ([_][]const u8{ " FROM ", " AS " }) |sep| if (std.mem.indexOf(u8, text, sep)) |k| {
        try out.append(.{ .name = try a.dupe(u8, std.mem.trim(u8, text[0..k], " ")), .optional = optional });
        try out.append(.{ .name = try a.dupe(u8, std.mem.trim(u8, text[k + sep.len ..], " ")), .optional = optional });
        return;
    };
    try out.append(.{ .name = try a.dupe(u8, text), .optional = optional });
}

/// The column types each return type is probed over, the likeliest first, as a
/// call of many parameters reaches only the first few in its first positions.
fn probeTypes(a: std.mem.Allocator) ![]const types.Type {
    const int_elem = try a.create(types.Type);
    int_elem.* = types.Type.init(.int).asNullable();
    const str_elem = try a.create(types.Type);
    str_elem.* = types.Type.init(.string).asNullable();
    return a.dupe(types.Type, &.{
        types.Type.init(.string),    types.Type.init(.int),                 types.Type.init(.bytes),
        types.Type.init(.float),     types.Type.decimal(10, 2),             types.Type.decimal(18, 4),
        types.Type.init(.bool),      types.Type.init(.date),                types.Type.init(.time),
        types.Type.init(.timestamp), .{ .kind = .array, .elem = int_elem }, .{ .kind = .array, .elem = str_elem },
    });
}

/// The arguments a scalar is probed with besides a column of each probe type: text
/// a unit or a pattern must be (`'day'`, `'YYYYMMDD'`), two integers, so a type a
/// literal decides (`decimal_at`'s length) shows as differing, and lambdas.
fn probeLiterals(a: std.mem.Allocator) ![]const *ast.Expr {
    const x = try ast.mk(a, .{ .lambda_var = "x" });
    const acc = try ast.mk(a, .{ .lambda_var = "acc" });
    const yes = try ast.mk(a, .{ .bool_lit = true });
    const forms = [_]ast.Expr{
        .{ .str_lit = "day" },
        .{ .int_lit = 1 },
        .{ .int_lit = 2 },
        .{ .str_lit = "YYYYMMDD" },
        .{ .lambda = .{ .params = &.{"x"}, .body = yes } },
        .{ .lambda = .{ .params = &.{"x"}, .body = x } },
        .{ .lambda = .{ .params = &.{ "acc", "x" }, .body = acc } },
    };
    const out = try a.alloc(*ast.Expr, forms.len);
    for (forms, out) |f, *o| o.* = try ast.mk(a, f);
    return out;
}

/// One type name over every probe that types, or null when two differ or none does.
const Agree = struct {
    a: std.mem.Allocator,
    seen: ?[]const u8 = null,
    split: bool = false,

    fn add(self: *Agree, t: types.Type, scratch: std.mem.Allocator) !void {
        if (self.split) return;
        const n = try t.name(scratch);
        if (self.seen) |s| {
            if (!std.mem.eql(u8, s, n)) self.split = true;
        } else self.seen = try self.a.dupe(u8, n);
    }

    fn result(self: Agree) ?[]const u8 {
        return if (self.split) null else self.seen;
    }
};

fn returnsOf(gpa: std.mem.Allocator, a: std.mem.Allocator, name: []const u8, kind: Kind, params: []const Param) !?[]const u8 {
    var scratch = std.heap.ArenaAllocator.init(gpa);
    defer scratch.deinit();
    const s = scratch.allocator();
    const tys = try probeTypes(s);
    var agree = Agree{ .a = a };
    switch (kind) {
        .table => {
            const b = table_builtins.lookup(name).?;
            var aw = std.Io.Writer.Allocating.init(a);
            try aw.writer.writeAll("table(");
            for (b.schema.fields, 0..) |f, i| {
                if (i > 0) try aw.writer.writeAll(", ");
                try aw.writer.print("{s} {s}", .{ f.name, try f.ty.name(s) });
            }
            try aw.writer.writeByte(')');
            return aw.written();
        },
        .window => {
            const k = std.meta.stringToEnum(ast.WinKind, name).?;
            for (tys) |t| try agree.add(binder.windowFuncType(.{ .win = k }, t), s);
        },
        .aggregate => if (aggregates.lookup(name)) |f| {
            const arg = try fieldExpr(s, "c0");
            for (tys) |t| {
                var d = analyze.Diag{};
                const schema = types.Schema{ .fields = &.{.{ .name = "c0", .ty = t.asNullable() }} };
                const got = binder.aggResultType(s, f, arg, schema, &d) catch |e| switch (e) {
                    error.OutOfMemory => return e,
                    else => continue,
                };
                try agree.add(got, s);
            }
        } else try probeQuery(gpa, s, name, params.len, &agree),
        .scalar => if (eval.lookupBuiltin(name) != null) try probeScalar(s, name, params, tys, &agree),
    }
    return agree.result();
}

fn fieldExpr(a: std.mem.Allocator, name: []const u8) !*ast.Expr {
    return ast.mk(a, .{ .field = .{ .parts = try a.dupe([]const u8, &.{name}) } });
}

/// Types a call over every arity its parameters allow and, at each, over a column
/// of each probe type and each probe literal in every position, up to a bound per
/// arity: a call of up to three parameters is tried every way.
fn probeScalar(s: std.mem.Allocator, name: []const u8, params: []const Param, tys: []const types.Type, agree: *Agree) !void {
    const lits = try probeLiterals(s);
    var fields = try s.alloc(types.Schema.Field, tys.len);
    var exprs = try s.alloc(*ast.Expr, tys.len + lits.len);
    for (tys, 0..) |t, i| {
        const n = try std.fmt.allocPrint(s, "c{d}", .{i});
        fields[i] = .{ .name = n, .ty = t.asNullable() };
        exprs[i] = try fieldExpr(s, n);
    }
    @memcpy(exprs[tys.len..], lits);
    var lo: usize = 0;
    for (params) |p| {
        if (!p.optional) lo += 1;
    }
    const hi = params.len + @intFromBool(params.len > 0 and params[params.len - 1].variadic);
    var probe = std.heap.ArenaAllocator.init(s);
    defer probe.deinit();
    for (lo..hi + 1) |arity| {
        if (arity > 6) break;
        var idx = [_]usize{0} ** 6;
        const args = try s.alloc(*ast.Expr, arity);
        var budget: usize = 8000;
        while (budget > 0) : (budget -= 1) {
            for (args, idx[0..arity]) |*x, k| x.* = exprs[k];
            var call = ast.Expr{ .call = .{ .name = name, .args = args } };
            var ctx = eval.TypeCtx{ .schema = .{ .fields = fields }, .arena = probe.allocator() };
            if (ctx.typeOf(&call)) |t| try agree.add(t, probe.allocator()) else |e| if (e == error.OutOfMemory) return e;
            _ = probe.reset(.retain_capacity);
            if (agree.split) return;
            var pos = arity;
            while (pos > 0) {
                pos -= 1;
                idx[pos] += 1;
                if (idx[pos] < exprs.len) break;
                idx[pos] = 0;
            } else break;
        }
    }
}

/// Types a call through a whole `SELECT`, for a statistic the parser writes out as
/// aggregates: over numeric columns, every pair of them.
fn probeQuery(gpa: std.mem.Allocator, s: std.mem.Allocator, name: []const u8, arity: usize, agree: *Agree) !void {
    const nums = [_]types.Type{ types.Type.init(.int), types.Type.init(.float), types.Type.decimal(10, 2) };
    const text = try std.fmt.allocPrint(s, "SELECT {s}(c0{s}) AS r FROM t;", .{ name, if (arity > 1) ", c1" else "" });
    for (nums) |x| for (nums) |y| {
        var run = std.heap.ArenaAllocator.init(gpa);
        defer run.deinit();
        const r = run.allocator();
        var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = parser.parseSourceOpts(r, text, &pd, .{ .known_tables = &.{"t"} }) catch continue;
        const fields = [_]types.Schema.Field{ .{ .name = "c0", .ty = x.asNullable() }, .{ .name = "c1", .ty = y.asNullable() } };
        const known = [_]analyze.KnownTable{.{ .name = "t", .schema = .{ .fields = &fields } }};
        var d = analyze.Diag{};
        const plan = analyze.analyzeOpts(r, prog, .{ .known_tables = &known }, &d) catch continue;
        if (plan.outputs.len == 0) continue;
        const o = plan.outputs[0];
        const out = (if (o.stages.len > 0) o.stages[o.stages.len - 1].out_schema else o.source.schema) orelse continue;
        for (out.fields) |f| if (std.mem.eql(u8, f.name, "r")) try agree.add(f.ty, r);
        if (agree.split) return;
    };
}

pub fn writeJson(w: *std.Io.Writer, c: Catalog) !void {
    try w.writeAll("{\"version\":");
    try jsontext.string(w, c.version);
    try w.writeAll(",\"functions\":[");
    for (c.functions, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"name\":");
        try jsontext.string(w, f.name);
        try w.print(",\"kind\":\"{s}\",\"signature\":", .{@tagName(f.kind)});
        try jsontext.string(w, f.signature);
        try w.writeAll(",\"params\":[");
        for (f.params, 0..) |p, k| {
            if (k > 0) try w.writeByte(',');
            try w.writeAll("{\"name\":");
            try jsontext.string(w, p.name);
            try w.print(",\"optional\":{},\"variadic\":{}}}", .{ p.optional, p.variadic });
        }
        try w.writeAll("],\"returns\":");
        try optString(w, f.returns);
        try w.writeAll(",\"doc\":");
        try optString(w, f.doc);
        try w.writeAll(",\"aliases\":");
        try strings(w, f.aliases);
        try w.writeByte('}');
    }
    try w.writeAll("],\"keywords\":");
    try strings(w, c.keywords);
    try w.writeAll(",\"types\":[");
    for (c.types, 0..) |t, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"name\":");
        try jsontext.string(w, t.name);
        try w.writeAll(",\"is\":");
        try jsontext.string(w, t.is);
        try w.writeAll(",\"form\":");
        try jsontext.string(w, t.form);
        try w.writeByte('}');
    }
    try w.writeAll("]}");
}

fn optString(w: *std.Io.Writer, s: ?[]const u8) !void {
    if (s) |x| try jsontext.string(w, x) else try w.writeAll("null");
}

fn strings(w: *std.Io.Writer, list: []const []const u8) !void {
    try w.writeByte('[');
    for (list, 0..) |x, i| {
        if (i > 0) try w.writeByte(',');
        try jsontext.string(w, x);
    }
    try w.writeByte(']');
}

/// A person's reading: each function's signature, kind, return type and doc, then
/// the keywords and the types.
pub fn writeText(w: *std.Io.Writer, c: Catalog) !void {
    try w.print("basalt {s}\n\nfunctions\n", .{c.version});
    for (c.functions) |f| {
        try w.print("  {s}  [{s}", .{ f.signature, @tagName(f.kind) });
        if (f.returns) |r| try w.print(" → {s}", .{r});
        try w.writeAll("]");
        if (f.aliases.len > 0) {
            try w.writeAll(" also");
            for (f.aliases) |al| try w.print(" {s}", .{al});
        }
        try w.writeByte('\n');
        if (f.doc) |d| try w.print("      {s}\n", .{d});
    }
    try w.writeAll("\nkeywords\n ");
    var col: usize = 1;
    for (c.keywords) |k| {
        if (col + k.len + 1 > 80) {
            try w.writeAll("\n ");
            col = 1;
        }
        try w.print(" {s}", .{k});
        col += k.len + 1;
    }
    try w.writeAll("\n\ntypes\n");
    for (c.types) |t| try w.print("  {s}  [{s}]\n", .{ t.form, t.is });
}

var kept: ?Catalog = null;
var kept_json: ?[]const u8 = null;
var kept_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);

/// The catalog, built at the first call and kept for the process.
pub fn get() !Catalog {
    if (kept) |c| return c;
    kept = try build(std.heap.page_allocator, kept_arena.allocator());
    return kept.?;
}

/// The catalog as JSON, written at the first call and kept for the process.
pub fn json() ![]const u8 {
    if (kept_json) |j| return j;
    var aw = std.Io.Writer.Allocating.init(kept_arena.allocator());
    try writeJson(&aw.writer, try get());
    kept_json = aw.written();
    return kept_json.?;
}

/// The built-in function named `name` or answering to it, in any case.
pub fn lookup(c: Catalog, name: []const u8) ?Function {
    for (c.functions) |f| if (std.ascii.eqlIgnoreCase(f.name, name) or inList(f.aliases, name)) return f;
    return null;
}

/// `basalt catalog [--format json|text]`: text by default, JSON as the kernel's
/// `catalog` op answers.
pub fn cmdCatalog(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writerStreaming(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [1024]u8 = undefined;
    var err_file = std.fs.File.stderr().writerStreaming(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var as_json = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--format")) {
            const v = (try nextVal(args, &i, "--format", stderr)) orelse return 2;
            if (std.mem.eql(u8, v, "json")) {
                as_json = true;
            } else if (std.mem.eql(u8, v, "text")) {
                as_json = false;
            } else {
                try stderr.print("error: `catalog` --format takes json or text, not `{s}`\n", .{v});
                return 2;
            }
        } else if (try unknownOption(arg, "catalog", stderr)) {
            return 2;
        } else {
            try stderr.print("error: unexpected argument `{s}`: `catalog` takes only --format\n", .{arg});
            return 2;
        }
    }
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    const c = try build(alloc, ar.allocator());
    if (as_json) {
        try writeJson(stdout, c);
        try stdout.writeByte('\n');
    } else try writeText(stdout, c);
    return 0;
}

fn testCatalog() !struct { ar: std.heap.ArenaAllocator, c: Catalog } {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    errdefer ar.deinit();
    const c = try build(std.testing.allocator, ar.allocator());
    return .{ .ar = ar, .c = c };
}

fn fnNamed(c: Catalog, name: []const u8) ?Function {
    for (c.functions) |f| if (std.mem.eql(u8, f.name, name)) return f;
    return null;
}

test "paramsOf: optional, variadic and keyword-separated parameters, and clauses that are none" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Case = struct { sig: []const u8, want: []const u8 };
    for ([_]Case{
        .{ .sig = "substr(s, start[, length])", .want = "s start length?" },
        .{ .sig = "lag(x[, n[, default]]) [IGNORE NULLS] OVER (…)", .want = "x n? default?" },
        .{ .sig = "json_range([start,] stop)", .want = "start? stop" },
        .{ .sig = "concat(a, …)", .want = "a*" },
        .{ .sig = "greatest(a, b, …)", .want = "a b*" },
        .{ .sig = "extract(unit FROM ts)", .want = "unit ts" },
        .{ .sig = "cast(x AS type)", .want = "x type" },
        .{ .sig = "array_agg(x [ORDER BY y])", .want = "x" },
        .{ .sig = "string_agg(x, separator [ORDER BY y])", .want = "x separator" },
        .{ .sig = "json_reduce(arr, initial, (acc, x) -> value)", .want = "arr initial (acc, x) -> value" },
        .{ .sig = "text_chunks(text, size [, overlap])", .want = "text size overlap?" },
        .{ .sig = "percentile_cont(fraction) WITHIN GROUP (ORDER BY x)", .want = "fraction" },
        .{ .sig = "now()", .want = "" },
        .{ .sig = "range([lo,] hi) → range", .want = "lo? hi" },
        .{ .sig = "count(* | x) [OVER (…)]", .want = "* | x" },
    }) |cs| {
        var aw = std.Io.Writer.Allocating.init(a);
        for (try paramsOf(a, cs.sig), 0..) |p, i| {
            if (i > 0) try aw.writer.writeByte(' ');
            try aw.writer.print("{s}{s}{s}", .{ p.name, if (p.optional) "?" else "", if (p.variadic) "*" else "" });
        }
        std.testing.expectEqualStrings(cs.want, aw.written()) catch |e| {
            std.debug.print("signature `{s}`\n", .{cs.sig});
            return e;
        };
    }
}

test "family: binary readers and writers share their row's doc, other names have none" {
    var b1: [64]u8 = undefined;
    var b2: [64]u8 = undefined;
    try std.testing.expectEqualStrings(family(&b1, "u8_at").?, family(&b2, "i64be_at").?);
    try std.testing.expectEqualStrings("fmt_F#", family(&b1, "fmt_f64be").?);
    try std.testing.expect(!std.mem.eql(u8, family(&b1, "f32le_at").?, family(&b2, "u16le_at").?));
    try std.testing.expect(family(&b1, "upper") == null);
    try std.testing.expect(family(&b1, "int_at") == null);
}

test "catalog: every function once, by kind, each with a doc; returns where the binder fixes them" {
    var t = try testCatalog();
    defer t.ar.deinit();
    const c = t.c;
    var undocumented: usize = 0;
    for (c.functions, 0..) |f, i| {
        for (c.functions[0..i]) |prev| try std.testing.expect(!std.mem.eql(u8, prev.name, f.name));
        if (f.doc == null) {
            std.debug.print("function `{s}` has no doc in the reference pages\n", .{f.name});
            undocumented += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 0), undocumented);
    const Want = struct { name: []const u8, kind: Kind, returns: ?[]const u8 };
    for ([_]Want{
        .{ .name = "upper", .kind = .scalar, .returns = "string" },
        .{ .name = "length", .kind = .scalar, .returns = "int" },
        .{ .name = "abs", .kind = .scalar, .returns = null },
        .{ .name = "date_diff", .kind = .scalar, .returns = "int" },
        .{ .name = "now", .kind = .scalar, .returns = "timestamp" },
        .{ .name = "cast", .kind = .scalar, .returns = null },
        .{ .name = "count", .kind = .aggregate, .returns = "int" },
        .{ .name = "sum", .kind = .aggregate, .returns = null },
        .{ .name = "avg", .kind = .aggregate, .returns = "float" },
        .{ .name = "corr", .kind = .aggregate, .returns = "float" },
        .{ .name = "row_number", .kind = .window, .returns = "int" },
        .{ .name = "lag", .kind = .window, .returns = null },
        .{ .name = "lines", .kind = .table, .returns = "table(n int, offset int, rec bytes)" },
        .{ .name = "range", .kind = .table, .returns = "table(range int)" },
    }) |w| {
        const f = fnNamed(c, w.name) orelse return error.TestUnexpectedResult;
        errdefer std.debug.print("function `{s}`\n", .{w.name});
        try std.testing.expectEqual(w.kind, f.kind);
        if (w.returns) |r| try std.testing.expectEqualStrings(r, f.returns orelse "null") else try std.testing.expect(f.returns == null);
    }
    try std.testing.expect(fnNamed(c, "variance") == null);
    try std.testing.expect(inList(fnNamed(c, "var_samp").?.aliases, "variance"));
    try std.testing.expect(inList(fnNamed(c, "extract").?.aliases, "year"));
    try std.testing.expect(inList(fnNamed(c, "date_diff").?.aliases, "datediff"));
    try std.testing.expect(fnNamed(c, "__agg_args") == null);
}

test "catalog: every function the reference pages describe is in it, under its name or an alias" {
    var t = try testCatalog();
    defer t.ar.deinit();
    const syntax = [_][]const u8{ "rollup", "cube", "identifier" };
    var unknown: usize = 0;
    for (fn_docs.docs) |d| {
        if (!named(t.c.functions, &.{}, d.name) and !inList(&syntax, d.name)) {
            std.debug.print("docs/{s} describes `{s}`, which the catalog does not list\n", .{ d.page, d.name });
            unknown += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 0), unknown);
}

test "catalog: keywords hold every word the parser reads as one, and no function or type twice" {
    var t = try testCatalog();
    defer t.ar.deinit();
    const c = t.c;
    for (c.keywords) |k| try std.testing.expect(!named(c.functions, c.types, k));
    try std.testing.expect(inList(c.keywords, "select"));
    try std.testing.expect(inList(c.keywords, "natural"));
    var dir = try std.fs.cwd().openDir("src/lang", .{ .iterate = true });
    defer dir.close();
    var walker = try dir.walk(std.testing.allocator);
    defer walker.deinit();
    var scraped: usize = 0;
    while (try walker.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".zig")) continue;
        const src = try dir.readFileAlloc(t.ar.allocator(), entry.path, 4 << 20);
        for ([_][]const u8{ "isKw(\"", "peekKw(\"", "eatKw(\"", "expectKw(\"" }) |call| {
            var k: usize = 0;
            while (std.mem.indexOfPos(u8, src, k, call)) |at| {
                const start = at + call.len;
                const end = std.mem.indexOfScalarPos(u8, src, start, '"') orelse break;
                k = end;
                const word = src[start..end];
                scraped += 1;
                if (inList(c.keywords, word) or named(c.functions, c.types, word)) continue;
                std.debug.print("src/lang/{s} reads `{s}` as a keyword, which the catalog does not list\n", .{ entry.path, word });
                return error.TestUnexpectedResult;
            }
        }
    }
    try std.testing.expect(scraped > 100);
}

test "catalog: every type name CAST and PARAM read, aliases with what they are" {
    var t = try testCatalog();
    defer t.ar.deinit();
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    for (t.c.types) |ty| {
        if (std.mem.eql(u8, ty.name, "json")) continue;
        const text = if (std.mem.eql(u8, ty.name, "list")) "list(int)" else if (std.mem.eql(u8, ty.name, "map")) "map(string, int)" else if (std.mem.eql(u8, ty.name, "struct")) "struct(a int)" else ty.name;
        const got = parser.parseTypeStr(ar.allocator(), text) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(ty.is, if (got.kind == .array) "list" else @tagName(got.kind));
    }
    var found = false;
    for (t.c.types) |ty| if (std.mem.eql(u8, ty.name, "bigint")) {
        found = true;
        try std.testing.expectEqualStrings("int", ty.is);
    };
    try std.testing.expect(found);
}

test "catalog: the JSON parses, carrying the version" {
    var t = try testCatalog();
    defer t.ar.deinit();
    var aw = std.Io.Writer.Allocating.init(t.ar.allocator());
    try writeJson(&aw.writer, t.c);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, t.ar.allocator(), aw.written(), .{});
    try std.testing.expectEqualStrings(build_options.version, parsed.object.get("version").?.string);
    try std.testing.expect(parsed.object.get("functions").?.array.items.len > 200);
}

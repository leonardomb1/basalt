//! `basalt check`: parse and validate a script without running it, every problem in
//! script order, as text or JSON for an editor.

const ErrOut = @import("args.zig").ErrOut;
const analyze = @import("../runtime/analyze.zig");
const ast = @import("../lang/ast.zig");
const include = @import("../lang/include.zig");
const remote = @import("../lang/remote.zig");
const include_fetch = @import("../net/include_fetch.zig");
const loadSource = @import("args.zig").loadSource;
const loadExit = @import("args.zig").loadExit;
const nextVal = @import("args.zig").nextVal;
const paramProblem = @import("args.zig").paramProblem;
const parser = @import("../lang/sql_parser.zig");
const repeatedKey = @import("args.zig").repeatedKey;
const std = @import("std");
const unknownOption = @import("args.zig").unknownOption;
const types = @import("../lang/types.zig");

pub const CheckIssue = struct { msg: []const u8, pos: ?ast.Pos = null, end: ?ast.Pos = null, file: ?[]const u8 = null, warning: bool = false };

pub const CheckOpts = struct {
    overrides: []const analyze.ParamOverride = &.{},
    known: []const analyze.KnownTable = &.{},
    declarations_only: bool = false,
    strict_params: bool = false,
    remote: ?*remote.Resolver = null,
};

/// Every problem in `text`, in script order: each statement that does not parse
/// (parsing resumes at the next), then each that does not check. Nothing runs or connects.
/// `strict_params` holds each override to a PARAM of this script, as `-p` is; the
/// kernel leaves it off, since a notebook's cells share their params.
pub fn checkText(a: std.mem.Allocator, text: []const u8, label: []const u8, dir: []const u8, opts: CheckOpts) ![]CheckIssue {
    var issues = std.array_list.Managed(CheckIssue).init(a);
    var names = std.array_list.Managed([]const u8).init(a);
    for (opts.known) |k| try names.append(k.name);
    var perrs = std.array_list.Managed(parser.Diagnostic).init(a);
    var idiag: include.Diag = .{};
    const prog = include.loadProgramOpts(a, text, label, dir, &idiag, .{ .parse = .{ .known_tables = names.items, .errors = &perrs }, .remote = opts.remote }) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ParseFailed => {
            const p = idiag.parse;
            try issues.append(.{ .msg = p.msg, .pos = .{ .line = p.line, .col = p.col }, .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null, .file = if (idiag.label.len > 0 and !std.mem.eql(u8, idiag.label, label)) idiag.label else null });
            return issues.toOwnedSlice();
        },
    };
    for (perrs.items) |p| try issues.append(.{ .msg = p.msg, .pos = .{ .line = p.line, .col = p.col }, .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null, .file = prog.fileOf(.{ .line = p.line, .col = p.col, .file = p.file }) });
    if (opts.strict_params) for (opts.overrides) |o| if (try paramProblem(a, prog, o.name)) |why| try issues.append(.{ .msg = why });
    for (prog.notes) |n| try issues.append(.{ .msg = n.msg, .pos = n.pos, .file = prog.fileOf(n.pos), .warning = true });

    var found = std.array_list.Managed(analyze.Issue).init(a);
    var adiag = analyze.Diag{};
    _ = try analyze.analyzeOpts(a, prog, .{
        .overrides = opts.overrides,
        .known_tables = opts.known,
        .issues = &found,
        .declarations_only = opts.declarations_only or perrs.items.len > 0,
    }, &adiag);
    for (found.items) |f| try issues.append(.{ .msg = f.msg, .pos = f.pos, .end = f.end, .file = prog.fileOf(f.pos) });

    std.sort.insertion(CheckIssue, issues.items, {}, struct {
        fn lt(_: void, x: CheckIssue, y: CheckIssue) bool {
            const px = x.pos orelse ast.Pos{ .line = 0, .col = 0 };
            const py = y.pos orelse ast.Pos{ .line = 0, .col = 0 };
            return px.line < py.line or (px.line == py.line and px.col < py.col);
        }
    }.lt);
    return issues.toOwnedSlice();
}

pub fn cmdCheck(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writerStreaming(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writerStreaming(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var overrides = std.array_list.Managed(analyze.ParamOverride).init(a);
    var known = std.array_list.Managed(analyze.KnownTable).init(a);
    var json = false;
    var pin = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--pin")) {
            pin = true;
            continue;
        }
        if (std.mem.eql(u8, args[i], "-c") or std.mem.eql(u8, args[i], "--command")) {
            i += 1;
            continue;
        }
        if (std.mem.eql(u8, args[i], "--format")) {
            const v = (try nextVal(args, &i, "--format", stderr)) orelse return 2;
            if (std.mem.eql(u8, v, "json")) json = true else if (!std.mem.eql(u8, v, "text")) {
                try stderr.print("error: `check --format` must be text|json\n", .{});
                return 2;
            }
            continue;
        }
        if (std.mem.eql(u8, args[i], "--known")) {
            const v = (try nextVal(args, &i, "--known", stderr)) orelse return 2;
            var it = std.mem.splitScalar(u8, v, ',');
            while (it.next()) |n| {
                const name = std.mem.trim(u8, n, " ");
                if (name.len > 0) try known.append(.{ .name = name });
            }
            continue;
        }
        if (!std.mem.eql(u8, args[i], "-p") and !std.mem.eql(u8, args[i], "--param")) {
            if (try unknownOption(args[i], "check", stderr)) return 2;
            continue;
        }
        i += 1;
        if (i >= args.len) {
            try stderr.print("error: missing key=value after `-p`\n", .{});
            return 2;
        }
        const eq = std.mem.indexOfScalar(u8, args[i], '=') orelse {
            try stderr.print("error: param must be key=value, got `{s}`\n", .{args[i]});
            return 2;
        };
        try overrides.append(.{ .name = args[i][0..eq], .value = args[i][eq + 1 ..] });
    }

    const src = loadSource(a, "check", args, stderr) catch |e| switch (e) {
        error.Usage, error.Unreadable => |le| return loadExit(le),
        else => return e,
    };
    const keys = try a.alloc([]const u8, overrides.items.len);
    for (overrides.items, keys) |o, *k| k.* = o.name;
    if (repeatedKey(keys)) |k| {
        try stderr.print("error: `-p {s}` given more than once\n", .{k});
        return 2;
    }
    const eo = ErrOut{ .w = if (json) stdout else stderr, .json = json, .bare = true, .label = src.label };
    var resolver = try remote.Resolver.init(a, src.dir, include_fetch.fetch(), pin);
    const issues = try checkText(a, src.text, src.label, src.dir, .{ .overrides = overrides.items, .known = known.items, .strict_params = true, .remote = &resolver });
    if (pin) {
        resolver.save() catch |e| {
            try stderr.print("error: cannot write `{s}`: {s}\n", .{ resolver.sum_path, @errorName(e) });
            return 1;
        };
        for (resolver.added.items) |k| try stderr.print("pinned {s} sha256:{s} in {s}\n", .{ k, resolver.pins.get(k).?, resolver.sum_path });
    }
    if (json) try stdout.writeAll("[");
    for (issues, 0..) |is, k| {
        if (json and k > 0) try stdout.writeByte(',');
        try eo.report(.{ .msg = is.msg, .pos = is.pos, .end = is.end, .file = is.file, .level = if (is.warning) "warning" else "error" });
    }
    if (json) try stdout.writeAll("]\n");
    for (issues) |is| if (!is.warning) return 1;
    if (!json) try stdout.print("ok: {s} checks out\n", .{src.label});
    return 0;
}

test "checkText: every problem in script order, known tables typed or not, nothing stops at the first" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const text =
        \\SELECT nope FROM RANGE(3);
        \\SELECT id, amt * 2 AS a2 FROM enrich WHERE day >= '2024-01-01';
        \\SELECT 1 +;
        \\SELECT missing FROM enrich;
        \\SELECT whatever FROM loose;
    ;
    const fields = [_]types.Schema.Field{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2) },
        .{ .name = "day", .ty = types.Type.init(.date) },
    };
    const known = [_]analyze.KnownTable{ .{ .name = "enrich", .schema = .{ .fields = &fields } }, .{ .name = "loose" } };
    const issues = try checkText(a, text, "t.sql", ".", .{ .known = &known });
    try std.testing.expectEqual(@as(usize, 3), issues.len);
    try std.testing.expectEqual(@as(u32, 1), issues[0].pos.?.line);
    try std.testing.expect(std.mem.indexOf(u8, issues[0].msg, "`nope`") != null);
    try std.testing.expectEqual(@as(u32, 3), issues[1].pos.?.line);
    try std.testing.expectEqual(@as(u32, 4), issues[2].pos.?.line);
    try std.testing.expect(std.mem.indexOf(u8, issues[2].msg, "`missing`") != null);

    const unknown = try checkText(a, "SELECT * FROM enrich;\nSELECT nope FROM RANGE(1);", "t.sql", ".", .{});
    try std.testing.expectEqual(@as(usize, 2), unknown.len);
    try std.testing.expect(std.mem.indexOf(u8, unknown[0].msg, "unknown source `enrich`") != null);

    try std.testing.expectEqual(@as(usize, 0), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{ .declarations_only = true })).len);
    try std.testing.expectEqual(@as(usize, 1), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{})).len);
}

test "checkText: a problem inside an @include'd file names that file with its own line" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makePath("lib");
    try tmp.dir.writeFile(.{ .sub_path = "lib/bad.sql", .data = "CREATE FUNCTION ok(x) AS x;\n\nSELECT nope FROM RANGE(3);\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const issues = try checkText(a, "@include 'lib/bad.sql';\nSELECT 1 AS v;", "main.sql", base, .{});
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expect(std.mem.endsWith(u8, issues[0].file.?, "lib/bad.sql"));
    try std.testing.expectEqual(@as(u32, 3), issues[0].pos.?.line);
}

test "checkText: an error inside an included table function's body, reached from a call, names that file" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "lib.sql", .data = "CREATE FUNCTION g() RETURNS TABLE AS SELECT * FROM 'x.csv';\n\nCREATE OR REPLACE FUNCTION g() RETURNS TABLE AS SELECT * FROM g();\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const issues = try checkText(a, "@include 'lib.sql';\nSELECT * FROM g();", "main.sql", base, .{});
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expect(std.mem.endsWith(u8, issues[0].file.?, "lib.sql"));
    try std.testing.expectEqual(@as(u32, 3), issues[0].pos.?.line);
    try std.testing.expectEqual(@as(u32, 63), issues[0].pos.?.col);
}

test "check warns where PUSHDOWN is written, without failing" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const issues = try checkText(a, "SELECT * FROM 'x.csv' PUSHDOWN($$a > 1$$);", "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expect(issues[0].warning);
    try std.testing.expect(std.mem.indexOf(u8, issues[0].msg, "`PUSHDOWN(...)` is deprecated") != null);
    try std.testing.expectEqual(@as(u32, 1), issues[0].pos.?.line);
}

test "check warns where USING stream_load is written, and takes the load without it silently" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', fe_port = 9030, be_url = 'http://h:8040', database = 'db', user = 'u', password = 'p');\n";
    const old = try checkText(a, try std.mem.concat(a, u8, &.{ conn, "LOAD INTO sr.db.t USING stream_load AS SELECT 1 AS a;" }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 1), old.len);
    try std.testing.expect(old[0].warning and std.mem.indexOf(u8, old[0].msg, "`USING stream_load` is deprecated") != null);
    try std.testing.expectEqual(@as(u32, 2), old[0].pos.?.line);
    const new = try checkText(a, try std.mem.concat(a, u8, &.{ conn, "LOAD INTO sr.db.t AS SELECT 1 AS a;" }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 0), new.len);
}

test "check takes USING changes into any table by key, and refuses it into a file or without a key" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conns = "CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', fe_port = 9030, be_url = 'http://h:8040', database = 'db', user = 'u', password = 'p');\nCREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'db', user = 'u', password = 'p');\n";
    const feed = " USING changes (sequence_by = seq, delete_when = op = 'd', position = lsn) AS SELECT 1 AS id, 'i' AS op, 1 AS seq, 10 AS lsn;";
    for ([_][]const u8{ "LOAD INTO sr.orders UPSERT ON (id)", "LOAD INTO pg.public.orders UPSERT ON (id)" }) |head| {
        const ok = try checkText(a, try std.mem.concat(a, u8, &.{ conns, head, feed }), "t.sql", ".", .{ .declarations_only = true });
        try std.testing.expectEqual(@as(usize, 0), ok.len);
    }
    const file = try checkText(a, try std.mem.concat(a, u8, &.{ conns, "LOAD INTO 'out.csv'", feed }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expect(file.len == 1 and std.mem.indexOf(u8, file[0].msg, "as an UPSERT into a table") != null);
    const keyless = try checkText(a, try std.mem.concat(a, u8, &.{ conns, "LOAD INTO sr.orders UPSERT", feed }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expect(keyless.len == 1 and std.mem.indexOf(u8, keyless[0].msg, "write it with `UPSERT ON (<key>)`") != null);
}

test "check takes a SQL table's change feed options, a $param as the position, and refuses an unknown one" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION db TYPE sqlserver OPTIONS (host = 'h', database = 'd', user = 'u', password = 'p');\nPARAM since STRING DEFAULT '';\n";
    const ok = try checkText(a, try std.mem.concat(a, u8, &.{ conn, "SELECT * FROM db.dbo.orders WITH (changes_from = $since, changes_to = '0000002B000004300004', capture_instance = 'dbo_orders');" }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 0), ok.len);
    const bad = try checkText(a, try std.mem.concat(a, u8, &.{ conn, "SELECT * FROM db.dbo.orders WITH (changes_since = $since);" }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 1), bad.len);
    try std.testing.expect(std.mem.indexOf(u8, bad[0].msg, "unknown read option `changes_since`") != null);
    const pg = "CREATE CONNECTION shop TYPE postgres OPTIONS (host = 'h', database = 'd', user = 'u', password = 'p');\nPARAM since STRING DEFAULT '';\n";
    const pg_ok = try checkText(a, try std.mem.concat(a, u8, &.{ pg, "SELECT * FROM shop.public.orders WITH (changes_from = $since, slot = 'basalt_orders', publication = 'basalt_pub');" }), "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 0), pg_ok.len);
}

test "check warns where ANCHOR SCHEMA is written, and takes schema_from silently" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const old = try checkText(a, "SELECT * FROM 'a.csv' UNION ALL BY NAME SELECT * FROM 'b.csv' ANCHOR SCHEMA first;", "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 1), old.len);
    try std.testing.expect(old[0].warning and std.mem.indexOf(u8, old[0].msg, "`ANCHOR SCHEMA` is deprecated") != null);
    const new = try checkText(a, "SELECT * FROM 'a.csv' UNION ALL BY NAME SELECT * FROM 'b.csv' WITH (schema_from = 'first');", "t.sql", ".", .{ .declarations_only = true });
    try std.testing.expectEqual(@as(usize, 0), new.len);
}

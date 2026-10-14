//! End-to-end scripts: FOR EACH, PARAM and LET, IDENTIFIER, CALL, THROW, PRINT,
//! WITH scoping, EXPLAIN and what `check` accepts against what `run` executes.

const std = @import("std");
const basalt = @import("basalt");
const ast = basalt.ast;
const analyze = basalt.analyze;
const obs = basalt.obs;
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const isTransient = basalt.env.isTransient;
const LogConfig = basalt.env.LogConfig;
const OutcomeSink = basalt.env.OutcomeSink;
const ParamArg = basalt.env.ParamArg;
const run = basalt.runtime.run;
const runScript = @import("harness.zig").runScript;
const checkAndRun = @import("harness.zig").checkAndRun;
const expectFile = @import("harness.zig").expectFile;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;

/// Points fd 2 at a file in `dir` until `end`, which restores it and returns what
/// was written there — where `PRINT`, an `EXPLAIN` statement's plan and the run
/// logger all go. Tests run one at a time, so the swap is seen by nothing else.
/// `beginFd` takes stdout the same way, which a printed result would otherwise
/// write into the test runner's own channel.
const StderrCapture = struct {
    file: std.fs.File,
    saved: std.posix.fd_t,
    fd: std.posix.fd_t,

    fn begin(dir: std.fs.Dir) !StderrCapture {
        return beginFd(dir, std.posix.STDERR_FILENO, "stderr.txt");
    }

    fn beginFd(dir: std.fs.Dir, fd: std.posix.fd_t, name: []const u8) !StderrCapture {
        const file = try dir.createFile(name, .{ .read = true, .truncate = true });
        errdefer file.close();
        const saved = try std.posix.dup(fd);
        errdefer std.posix.close(saved);
        try std.posix.dup2(file.handle, fd);
        return .{ .file = file, .saved = saved, .fd = fd };
    }

    fn end(self: *StderrCapture, alloc: std.mem.Allocator) ![]u8 {
        std.posix.dup2(self.saved, self.fd) catch {};
        std.posix.close(self.saved);
        defer self.file.close();
        try self.file.seekTo(0);
        return self.file.readToEndAlloc(alloc, 1 << 20);
    }
};

test "a shown result, CTE or derived table repeats a name as SQL does; a reference to it is ambiguous; LOAD INTO refuses one, naming it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "g,x\nab,1\nAb,5\ncd,2\ncd,10\nef,3\n" });
    try tmp.dir.writeFile(.{ .sub_path = "dup.csv", .data = "a,a,b\n1,2,3\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc,
        \\SELECT x AS a, g AS a, x + 1 AS a FROM '{s}/t.csv' ORDER BY x DESC LIMIT 2;
        \\SELECT SUM(x) AS s, MAX(x) AS s, COUNT(*), COUNT(*) FROM '{s}/t.csv' GROUP BY g ORDER BY g;
        \\SELECT g, g FROM '{s}/t.csv' ORDER BY 2 LIMIT 1;
        \\SELECT * FROM '{s}/dup.csv';
        \\WITH c AS (SELECT g, x AS g, x FROM '{s}/t.csv') SELECT * FROM c WHERE x > 4 ORDER BY x;
        \\SELECT b, count(*) AS n FROM (SELECT x AS a, g AS a, 1 AS b FROM '{s}/t.csv') d GROUP BY b;
    , .{ base, base, base, base, base, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    var out = try StderrCapture.beginFd(tmp.dir, std.posix.STDOUT_FILENO, "stdout.txt");
    const res = run(alloc, prog, .{ .stdout_format = .csv, .log = .{ .quiet = true, .summary = .none } }, &rdiag);
    const printed = try out.end(alloc);
    defer alloc.free(printed);
    _ = try res;
    try std.testing.expectEqualStrings(
        "a,a,a\n10,cd,11\n5,Ab,6\n" ++
            "s,s,count(*),count(*)\n5,5,1,1\n1,1,1,1\n12,10,2,2\n3,3,1,1\n" ++
            "g,g\nAb,Ab\n" ++
            "a,a,b\n1,2,3\n" ++
            "g,g,x\nAb,5,5\ncd,10,10\n" ++
            "b,n\n1,5\n",
        printed,
    );

    const refuse = @import("harness.zig").expectRefusedAlike;
    try refuse(alloc, &tmp, "SELECT g FROM (SELECT g, x AS g FROM '$B/t.csv') d;", "column reference `g` is ambiguous");
    try refuse(alloc, &tmp, "WITH c AS (SELECT g, x AS g, x FROM '$B/t.csv') SELECT * FROM c WHERE g > 1;", "column reference `g` is ambiguous");
    try refuse(alloc, &tmp, "SELECT a FROM '$B/dup.csv';", "column reference `a` is ambiguous");
    try refuse(alloc, &tmp, "SELECT x FROM '$B/t.csv' WHERE x IN (SELECT a FROM (SELECT x AS a, x AS a FROM '$B/t.csv') d);", "column reference `a` is ambiguous");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/t.csv' t JOIN (SELECT g, g FROM '$B/t.csv') d ON t.g = d.g;", "column reference `g` is ambiguous");
    try refuse(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT * FROM (SELECT g, g FROM '$B/t.csv') d;", "two columns are named `g`");

    const load = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/o.csv' AS SELECT * FROM '{s}/dup.csv';", .{ base, base });
    defer alloc.free(load);
    try std.testing.expectError(error.PlanFailed, run(alloc, try parser.parseSource(parena.allocator(), load, &pdiag), .{ .log = .{ .quiet = true } }, &rdiag));
    try std.testing.expectEqualStrings("LOAD INTO writes each column under its own name, and two columns are named `a` — alias one apart in the SELECT list", rdiag.msg);
}

test "EXPLAIN mid-script explains that query without running it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n2,pending\n" });

    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const ran_path = try std.fs.path.join(alloc, &.{ base, "ran.csv" });
    defer alloc.free(ran_path);
    const never_path = try std.fs.path.join(alloc, &.{ base, "never.csv" });
    defer alloc.free(never_path);

    const script = try std.fmt.allocPrint(alloc,
        \\LOAD INTO '{s}' AS SELECT id FROM '{s}';
        \\EXPLAIN LOAD INTO '{s}' AS
        \\WITH paid AS (SELECT id FROM '{s}' WHERE status = 'paid')
        \\SELECT id FROM paid;
    , .{ ran_path, in_path, never_path, in_path });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    try std.testing.expectEqual(ast.ExplainMode.none, prog.explain);

    var rdiag: Diag = .{};
    var cap = try StderrCapture.begin(tmp.dir);
    const res = run(alloc, prog, .{ .log = .{ .quiet = true, .summary = .none } }, &rdiag);
    const plan = try cap.end(alloc);
    defer alloc.free(plan);
    _ = try res;
    const write_line = try std.fmt.allocPrint(alloc, "write  csv  {s}", .{never_path});
    defer alloc.free(write_line);
    const scan_line = try std.fmt.allocPrint(alloc, "scan  csv  {s} (via binding paid)", .{in_path});
    defer alloc.free(scan_line);
    try std.testing.expect(std.mem.startsWith(u8, plan, "plan\n"));
    try std.testing.expect(std.mem.indexOf(u8, plan, write_line) != null);
    try std.testing.expect(std.mem.indexOf(u8, plan, "filter") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan, scan_line) != null);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, plan, "scan  csv"));

    const ran = try tmp.dir.readFileAlloc(alloc, "ran.csv", 1 << 20);
    defer alloc.free(ran);
    try std.testing.expectEqualStrings("id\n1\n2\n", ran);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("never.csv", .{}));
}

test "for-each over a JSON array param iterates and binds fields by name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,status\n1,paid\n2,pending\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,status\n3,paid\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"tables\":[{{\"name\":\"{s}/a.csv\",\"emp\":\"01\"}},{{\"name\":\"{s}/b.csv\",\"emp\":\"02\"}}]}}",
        .{ base, base },
    );
    defer alloc.free(body);
    const script = try std.fmt.allocPrint(alloc, "PARAM job JSON;\n" ++
        "FOR EACH ROW OF ($job.tables) AS (name, emp) SEQUENTIAL\n" ++
        "  LOAD INTO '{s}/out_${{emp}}.csv' AS SELECT id, '${{emp}}' AS EMPRESA FROM '${{name}}';\n" ++
        "END FOR;", .{base});
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{ .params = &.{.{ .key = "job", .val = body }} }, &rdiag);
    const first = try tmp.dir.readFileAlloc(alloc, "out_01.csv", 1 << 20);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("id,EMPRESA\n1,01\n2,01\n", first);
    const second = try tmp.dir.readFileAlloc(alloc, "out_02.csv", 1 << 20);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("id,EMPRESA\n3,02\n", second);
}

test "for-each loop var used as an expression value binds per row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
        "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, $name AS empresa FROM '{s}/${{name}}.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,empresa\n1,alpha\n", a);
    try std.testing.expectEqualStrings("id,empresa\n2,beta\n", b);
}

test "for-each: a typed loop var used as a value binds as its declared type" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "nums.csv", .data = "n\n5\n" });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/nums.csv') AS (n:INT)\n" ++
        "  LOAD INTO '{s}/out.csv' AS SELECT id, $n * 2 AS twice FROM '{s}/in.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,twice\n7,10\n", out);

    try tmp.dir.writeFile(.{ .sub_path = "nums.csv", .data = "n\nana\n" });
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "loop variable `n`: `ana` is not int") != null);
}

test "param substitution filters by a CLI-bound value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n2,200\n3,50\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM min INT DEFAULT 0;\nLOAD INTO '{s}' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= $min;",
        .{ out_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "min", .val = "100" }});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "PARAM: a FLOAT defaulting to 0.5 is a float, a DECIMAL one keeps its scale" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM rate FLOAT DEFAULT 0.5;\nPARAM fee DECIMAL(10,2) DEFAULT 2.5;\nLOAD INTO '{s}/out.csv' AS SELECT id, $rate * amount AS r, $fee AS f FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,r,f\n1,50,2.50\n", out);
}

test "IDENTIFIER path: a PARAM resolves outside any FOR EACH" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM dir STRING DEFAULT '{s}';\nPARAM name STRING DEFAULT 'in';\n" ++
            "LOAD INTO '{s}' AS SELECT id FROM IDENTIFIER($dir || '/' || $name || '.csv');",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "IDENTIFIER path: a PARAM resolves inside a CTE body" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n3\n4\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM dir STRING DEFAULT '{s}';\nPARAM name STRING DEFAULT 'in';\n" ++
            "LOAD INTO '{s}' AS WITH src AS (SELECT id FROM IDENTIFIER($dir || '/' || $name || '.csv'))\n" ++
            "SELECT id FROM src;",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n3\n4\n", out);
}

test "IDENTIFIER path: a LET resolves, and an expression hole sees script scope" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LET dir = '{s}';\nPARAM name STRING DEFAULT 'IN';\n" ++
            "LOAD INTO '{s}' AS SELECT id FROM IDENTIFIER($dir || '/' || lower($name) || '.csv');",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n7\n", out);
}

test "IDENTIFIER path: a loop variable shadows a same-named PARAM" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n5\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM dir STRING DEFAULT '{s}';\nPARAM name STRING DEFAULT 'absent';\n" ++
            "FOR EACH ROW OF (SELECT 'in' AS name) AS (name)\n" ++
            "  LOAD INTO '{s}' AS SELECT id FROM IDENTIFIER($dir || '/' || $name || '.csv');\n" ++
            "END FOR;",
        .{ base, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n5\n", out);
}

test "LOAD INTO IDENTIFIER: one output file per for-each row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "cat.csv", .data = "r\nnorth\nsouth\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LET dir = '{s}';\n" ++
            "FOR EACH ROW OF (SELECT r FROM '{s}/cat.csv') AS (r)\n" ++
            "  LOAD INTO IDENTIFIER($dir || '/out_' || $r || '.csv') AS SELECT $r AS region;\n" ++
            "END FOR;",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };

    const north = try tmp.dir.readFileAlloc(alloc, "out_north.csv", 1 << 16);
    defer alloc.free(north);
    try std.testing.expectEqualStrings("region\nnorth\n", north);
    const south = try tmp.dir.readFileAlloc(alloc, "out_south.csv", 1 << 16);
    defer alloc.free(south);
    try std.testing.expectEqualStrings("region\nsouth\n", south);
}

test "for-each fans out over a discovered list with interpolation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
}

test "log defaults to warn: a healthy run writes nothing to stderr" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';", .{ base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    var cap = try StderrCapture.begin(tmp.dir);
    const res = run(alloc, prog, .{}, &rdiag);
    const logged = try cap.end(alloc);
    defer alloc.free(logged);
    _ = try res;
    try std.testing.expectEqual(obs.Level.warn, (LogConfig{}).level);
    try std.testing.expectEqualStrings("", logged);
}

test "a lone LOAD logs load_complete under --log-format json at info" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';", .{ base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    var cap = try StderrCapture.begin(tmp.dir);
    const res = run(alloc, prog, .{ .items = true, .log = .{ .format = .json, .level = .info, .summary = .none } }, &rdiag);
    const logged = try cap.end(alloc);
    defer alloc.free(logged);
    _ = try res;
    try std.testing.expect(std.mem.indexOf(u8, logged, "\"event\":\"load_complete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, logged, "\"rows_written\":2") != null);
}

test "summary: --max-rows counts the printed rows, and a parallel FOR EACH names its source and sink" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n3\n4\n" });
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\na\nb\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc,
        \\SELECT id FROM '{s}/in.csv';
        \\LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base, base });
    defer alloc.free(script);
    const loop = try std.fmt.allocPrint(alloc,
        \\FOR EACH ROW OF (SELECT name FROM '{s}/names.csv') AS (nm) PARALLEL
        \\  LOAD INTO IDENTIFIER('{s}/o_' || $nm || '.parquet') AS SELECT id FROM '{s}/in.csv';
        \\END FOR;
    , .{ base, base, base });
    defer alloc.free(loop);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    var summary: obs.Summary = .{ .run_id = 0 };
    var out = try StderrCapture.beginFd(tmp.dir, std.posix.STDOUT_FILENO, "stdout.txt");
    const res = run(alloc, prog, .{ .threads = 2, .max_rows = 2, .stdout_format = .csv, .summary_out = &summary, .log = .{ .quiet = true, .summary = .none } }, &rdiag);
    const printed = try out.end(alloc);
    defer alloc.free(printed);
    _ = try res;
    try std.testing.expectEqualStrings("id\n1\n2\n", printed);
    try std.testing.expectEqual(@as(u64, 2 + 4), summary.rows_written);

    var loop_summary: obs.Summary = .{ .run_id = 0 };
    _ = try run(alloc, try parser.parseSource(parena.allocator(), loop, &pdiag), .{ .threads = 2, .summary_out = &loop_summary, .log = .{ .quiet = true, .summary = .none } }, &rdiag);
    try std.testing.expectEqual(@as(u64, 8), loop_summary.rows_written);
    try std.testing.expectEqualStrings("csv", loop_summary.source);
    try std.testing.expectEqualStrings("parquet", loop_summary.sink);
}

test "for-each discovers its rows from an in-engine SELECT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "catalog.csv", .data = "name,active\nALPHA,1\nBETA,1\nGAMMA,0\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    try tmp.dir.writeFile(.{ .sub_path = "gamma.csv", .data = "id,v\n9,90\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF (SELECT name, lower(name) AS slug FROM '{s}/catalog.csv' WHERE active = 1) AS (name, slug)\n  LOAD INTO '{s}/out_${{slug}}.csv' AS SELECT id, v FROM '{s}/${{slug}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out_gamma.csv", .{}));
}

test "for-each SELECT discovery with fewer columns than loop variables fails to plan" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "catalog.csv", .data = "name\nalpha\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF (SELECT name FROM '{s}/catalog.csv') AS (name, slug)\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "fewer columns than loop variables") != null);
}

test "for-each parallel + on_error=continue isolates a failing table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nghost\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name) PARALLEL ON ERROR CONTINUE\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .threads = 2 }, &rdiag));
    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id\n7\n", a);
}

test "statement-level CASE dispatches on a resolved param (default arm otherwise)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,v\n1,10\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "PARAM mode STRING DEFAULT 'small';\nCASE $mode\n" ++
        "  WHEN 'big' THEN LOAD INTO '{s}/out.csv' AS SELECT id, v FROM '{s}/in.csv';\n" ++
        "  ELSE LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';\nEND CASE;", .{ base, base, base, base });
    defer alloc.free(script);

    const dflt = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(dflt);
    try std.testing.expectEqualStrings("id\n1\n", dflt);
    const big = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "mode", .val = "big" }});
    defer alloc.free(big);
    try std.testing.expectEqualStrings("id,v\n1,10\n", big);
}

test "a file the script rewrites is read with the columns it writes, not a stale copy's" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n" });
    try tmp.dir.writeFile(.{ .sub_path = "staging.csv", .data = "old\n9\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{0s}/staging.csv' AS SELECT id, status FROM '{0s}/in.csv';\n" ++
        "LOAD INTO '{0s}/out.csv' AS SELECT id, status FROM '{0s}/staging.csv';", .{base});
    defer alloc.free(script);
    const got = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(got);
    try std.testing.expectEqualStrings("id,status\n1,paid\n", got);
}

test "a FOR EACH that only PRINTs is something to run" {
    const alloc = std.testing.allocator;
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), "FOR EACH ROW OF (SELECT 1 AS a) AS (a) PRINT 'x'; END FOR;", &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag);
}

test "statement-level CASE: a NULL subject matches no arm, not WHEN NULL nor WHEN ''" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "PARAM e STRING DEFAULT NULL;\nCASE $e\n" ++
        "  WHEN NULL THEN LOAD INTO '{0s}/out.csv' AS SELECT 'null' AS arm FROM '{0s}/in.csv';\n" ++
        "  WHEN '' THEN LOAD INTO '{0s}/out.csv' AS SELECT 'empty' AS arm FROM '{0s}/in.csv';\n" ++
        "  ELSE LOAD INTO '{0s}/out.csv' AS SELECT 'else' AS arm FROM '{0s}/in.csv';\nEND CASE;", .{base});
    defer alloc.free(script);

    const dflt = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(dflt);
    try std.testing.expectEqualStrings("arm\nelse\n", dflt);
    const empty = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "e", .val = "" }});
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("arm\nempty\n", empty);
}

test "for-each on_error=continue with an OutcomeSink: run succeeds, failure recorded" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nghost\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name) SEQUENTIAL ON ERROR CONTINUE\n" ++
        "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var oc_arena = std.heap.ArenaAllocator.init(alloc);
    defer oc_arena.deinit();
    var outcomes = OutcomeSink.init(oc_arena.allocator());
    defer outcomes.deinit();

    var rdiag: Diag = .{};
    const stats = try run(alloc, prog, .{ .outcomes = &outcomes }, &rdiag);
    try std.testing.expectEqual(@as(usize, 1), stats.rows_out);
    try std.testing.expectEqual(@as(usize, 2), outcomes.list.items.len);
    try std.testing.expectEqual(@as(usize, 1), outcomes.failures());
    for (outcomes.list.items) |o| {
        if (o.ok) {
            try std.testing.expectEqualStrings("alpha", o.item);
        } else {
            try std.testing.expectEqualStrings("ghost", o.item);
            try std.testing.expect(o.err.len > 0);
            try std.testing.expect(!o.retryable);
        }
    }
}

test "for-each with an empty discovery list is a no-op" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
        "  LOAD INTO '{s}/out.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;", .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    const stats = try run(alloc, prog, .{}, &rdiag);
    try std.testing.expectEqual(@as(usize, 0), stats.rows_out);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20));
}

fn letScript(alloc: std.mem.Allocator, base: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc,
        \\PARAM days INT DEFAULT 3;
        \\LET span = $days * 10;
        \\LET label = concat('n', $span);
        \\LOAD INTO '{s}/a.csv' AS SELECT id, $span AS s, $label AS l FROM '{s}/in.csv';
        \\LOAD INTO '{s}/b.csv' AS SELECT $label AS l FROM '{s}/in.csv';
    , .{ base, base, base, base });
}

fn runLetScript(alloc: std.mem.Allocator, script: []const u8, cli: []const ParamArg, rdiag: *Diag) !void {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    _ = try run(alloc, prog, .{ .params = cli, .log = .{ .summary = .none, .quiet = true } }, rdiag);
}

test "LET value is shared across statements" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try runLetScript(alloc, script, &[_]ParamArg{}, &rdiag);

    const a = try tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,s,l\n1,30,n30\n2,30,n30\n", a);
    const b = try tmp.dir.readFileAlloc(alloc, "b.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("l\nn30\nn30\n", b);
}

test "LET reads a param, so `-p` reaches it indirectly" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try runLetScript(alloc, script, &[_]ParamArg{.{ .key = "days", .val = "7" }}, &rdiag);

    const a = try tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,s,l\n1,70,n70\n", a);
}

test "a LET is sealed: `-p` naming one is a plan error, not a silent override" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, runLetScript(alloc, script, &[_]ParamArg{.{ .key = "span", .val = "99" }}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "`span` is a LET, not a PARAM") != null);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20));

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyzeWith(parena.allocator(), prog, &.{.{ .name = "span", .value = "99" }}, &adiag));
    try std.testing.expect(std.mem.indexOf(u8, adiag.msg, "`span` is a LET, not a PARAM") != null);
}

test "a LET and a PARAM may not share a name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        \\PARAM span INT DEFAULT 1;
        \\LET span = 5;
        \\LOAD INTO '{s}/a.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, runLetScript(alloc, script, &[_]ParamArg{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "declared twice") != null);
}

test "CALL renders a statement function per call (defaults fill the omitted arg)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "CREATE FUNCTION sync(name, tag DEFAULT 'Z') AS\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v, '${{tag}}' AS tag FROM '{s}/${{name}}.csv';\n" ++
            "END;\n" ++
            "CALL sync('alpha', 'A');\n" ++
            "CALL sync('beta');\n",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v,tag\n1,10,A\n2,20,A\n", a);
    try std.testing.expectEqualStrings("id,v,tag\n3,30,Z\n", b);
}

test "CALL nesting is depth-guarded (mutual recursion)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "CREATE FUNCTION ping(n) AS\n  CALL pong($n);\nEND;\n" ++
            "CREATE FUNCTION pong(n) AS\n  LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';\n  CALL ping($n);\nEND;\n" ++
            "CALL ping('x');\n",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "too deep") != null);
}

test "THROW: a fired guard is the verbatim, permanent error; a false WHEN is a no-op" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const guarded = try std.fmt.allocPrint(alloc,
        \\PARAM tbl STRING DEFAULT '';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(guarded);
    const gprog = try parser.parseSource(parena.allocator(), guarded, &pdiag);

    var gdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, gprog, .{}, &gdiag));
    try std.testing.expectEqualStrings("tbl is required (e.g. -p tbl=SC5)", gdiag.msg);
    try std.testing.expect(!gdiag.retryable);
    try std.testing.expect(!isTransient(error.PlanFailed));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20));

    var okdiag: Diag = .{};
    _ = try run(alloc, gprog, .{ .params = &[_]ParamArg{.{ .key = "tbl", .val = "SC5" }} }, &okdiag);
    const wrote = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(wrote);
    try std.testing.expect(std.mem.indexOf(u8, wrote, "id") != null);

    const bare = try std.fmt.allocPrint(alloc,
        \\LET tag = 'zz';
        \\THROW 'unreachable branch: ' || $tag;
        \\LOAD INTO '{s}/never.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(bare);
    const bprog = try parser.parseSource(parena.allocator(), bare, &pdiag);

    var bdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, bprog, .{}, &bdiag));
    try std.testing.expectEqualStrings("unreachable branch: zz", bdiag.msg);
    try std.testing.expect(!bdiag.retryable);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "never.csv", 1 << 20));

    const late = try std.fmt.allocPrint(alloc,
        \\LOAD INTO '{s}/early.csv' AS SELECT id FROM '{s}/in.csv';
        \\THROW 'late';
    , .{ base, base });
    defer alloc.free(late);
    const lprog = try parser.parseSource(parena.allocator(), late, &pdiag);
    var ldiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, lprog, .{}, &ldiag));
    try std.testing.expectEqualStrings("late", ldiag.msg);
    try std.testing.expectEqual(@as(u32, 2), ldiag.pos.?.line);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "early.csv", 1 << 20));

    try expectRefusedAlike(alloc, &tmp, "THROW 'x' WHEN 1;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "THROW condition must be a boolean, got int");
}

test "PRINT runs at the top level and per row inside a FOR EACH body" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM tag STRING DEFAULT 'nightly';\n" ++
            "PRINT 'run ' || $tag;\n" ++
            "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
            "  PRINT 'company ' || $name;\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v FROM '{s}/${{name}}.csv';\n" ++
            "END FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    var cap = try StderrCapture.begin(tmp.dir);
    const res = run(alloc, prog, .{}, &rdiag);
    const printed = try cap.end(alloc);
    defer alloc.free(printed);
    const stats = try res;
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);
    try std.testing.expectEqualStrings("run nightly\ncompany alpha\ncompany beta\n", printed);
}

test "PRINT over an unbound name is a plan error, not a silent blank" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "PRINT 'x ' || $nope;\nLOAD INTO '{s}/a.csv' AS SELECT id FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "PRINT:") != null);
}

test "a plan failure carries the failing stage's position" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS\nSELECT id\nFROM '{s}'\nWHERE nosuch > 1;", .{ base, in_path });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expectEqualStrings("unknown field `nosuch`", rdiag.msg);
    try std.testing.expectEqual(@as(u32, 4), rdiag.pos.?.line);
    try std.testing.expectEqual(@as(u32, 7), rdiag.pos.?.col);
}

test "script-level refusals point at the offending declaration or name, in check and run alike" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "u.csv", .data = "name\nana\nbob\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const Case = struct { src: []const u8, line: u32, col: u32, check: bool = true };
    const cases = [_]Case{
        .{ .src = "LET b = 1 +\n  $a;\nLET a = 2;\nSELECT $b AS v;", .line = 2, .col = 3 },
        .{ .src = "PARAM a INT DEFAULT 2;\nPARAM a INT DEFAULT 3;\nSELECT $a AS v;", .line = 2, .col = 1 },
        .{ .src = "PARAM a INT DEFAULT 2;\n  LET a = 3;\nSELECT $a AS v;", .line = 2, .col = 3 },
        .{ .src = "SELECT 1 AS v;\n  LET bad = 1 / 0;", .line = 2, .col = 3 },
        .{ .src = "FOR EACH ROW OF ('$B/u.csv') AS (nm)\n  THROW 'boom ' || $nm WHEN $nm = 'bob';\nEND FOR;\nSELECT 1 AS v;", .line = 2, .col = 3, .check = false },
    };
    for (cases) |c| {
        const script = try std.mem.replaceOwned(u8, alloc, c.src, "$B", base);
        defer alloc.free(script);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
        var rdiag: Diag = .{};
        try std.testing.expect(std.meta.isError(run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)));
        try std.testing.expectEqual(c.line, rdiag.pos.?.line);
        try std.testing.expectEqual(c.col, rdiag.pos.?.col);
        if (!c.check) continue;
        var adiag = analyze.Diag{};
        try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
        try std.testing.expectEqual(c.line, adiag.pos.?.line);
        try std.testing.expectEqual(c.col, adiag.pos.?.col);
    }
}

test "a FOR EACH discovery query may read a table function, a derived table or a CTE" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "o.csv", .data = "id,g,x\n1,a,10\n2,a,20\n3,b,30\n4,c,5\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc,
        \\CREATE FUNCTION groups(minx INT) RETURNS TABLE AS SELECT DISTINCT g FROM '{0s}/o.csv' WHERE x >= $minx;
        \\FOR EACH ROW OF (WITH s AS (SELECT g FROM groups(10)) SELECT g FROM (SELECT g FROM s) d) AS (gg)
        \\  LOAD INTO '{0s}/out_${{gg}}.csv' AS SELECT id FROM '{0s}/o.csv' WHERE g = $gg;
        \\END FOR;
    , .{base});
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var loop: ?ast.ForEach = null;
    for (prog.stmts) |st| if (st == .for_each) {
        loop = st.for_each;
    };
    for (loop.?.body) |st| try std.testing.expect(st != .binding);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    const a = try tmp.dir.readFileAlloc(alloc, "out_a.csv", 1 << 16);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id\n1\n2\n", a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_b.csv", 1 << 16);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id\n3\n", b);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out_c.csv", .{}));
}

const nested_loop_script =
    \\FOR EACH ROW OF ('$B/outer.csv') AS (name) MODE
    \\  FOR EACH ROW OF ('$B/inner.csv') AS (suffix)
    \\    THROW 'never ' || $name WHEN $name = 'zzz';
    \\    CASE $name WHEN 'a' THEN
    \\      LOAD INTO IDENTIFIER('$B/out_' || $name || '_' || $suffix || '.csv') AS
    \\      WITH src AS (SELECT grp, amt FROM IDENTIFIER('$B/' || $name || '.csv'))
    \\      SELECT grp, SUM(amt) AS total, $name AS src_name, $suffix AS sfx FROM src GROUP BY grp ORDER BY grp;
    \\    END CASE;
    \\  END FOR;
    \\END FOR;
;

test "nested for-each: an outer loop variable is a name, a value, a THROW operand and a CASE subject" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "SEQUENTIAL", "PARALLEL" }) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const script = try std.mem.replaceOwned(u8, alloc, nested_loop_script, "MODE", mode);
        defer alloc.free(script);
        try checkAndRun(alloc, &tmp, script, 4, &.{});
        try expectFile(&tmp, "out_a_one.csv", "grp,total,src_name,sfx\na,30,a,one\nb,5,a,one\n");
        try expectFile(&tmp, "out_a_two.csv", "grp,total,src_name,sfx\na,30,a,two\nb,5,a,two\n");
        try std.testing.expectError(error.FileNotFound, tmp.dir.access("out_b_one.csv", .{}));

        const base = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(base);
        const firing = try std.mem.replaceOwned(u8, alloc, script, "'zzz'", "'b'");
        defer alloc.free(firing);
        const fscript = try std.mem.replaceOwned(u8, alloc, firing, "$B", base);
        defer alloc.free(fscript);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), fscript, &pdiag);
        var rdiag: Diag = .{};
        try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .threads = 4, .log = .{ .quiet = true } }, &rdiag));
        try std.testing.expectEqualStrings("for-each row name=b: for-each row suffix=one: never b", rdiag.msg);
    }
}

test "for-each body WITH: rendered per row, and a same-named top-level WITH is left alone" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "SEQUENTIAL", "PARALLEL" }) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const tmpl =
            \\LOAD INTO '$B/out_first.csv' AS WITH src AS (SELECT id FROM '$B/a.csv') SELECT COUNT(*) AS n FROM src;
            \\FOR EACH ROW OF ('$B/outer.csv') AS (name) MODE
            \\  LOAD INTO IDENTIFIER('$B/out_' || $name || '.csv') AS
            \\  WITH src AS (SELECT id FROM IDENTIFIER('$B/' || $name || '.csv') WHERE id <> 1)
            \\  SELECT COUNT(*) AS n FROM src;
            \\END FOR;
            \\LOAD INTO '$B/out_last.csv' AS SELECT COUNT(*) AS n FROM src;
        ;
        const script = try std.mem.replaceOwned(u8, alloc, tmpl, "MODE", mode);
        defer alloc.free(script);
        try checkAndRun(alloc, &tmp, script, 4, &.{});
        try expectFile(&tmp, "out_first.csv", "n\n3\n");
        try expectFile(&tmp, "out_a.csv", "n\n2\n");
        try expectFile(&tmp, "out_b.csv", "n\n2\n");
        try expectFile(&tmp, "out_last.csv", "n\n3\n");
    }
}

test "top-level WITH: two statements reusing a CTE name each read their own" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/out_1.csv' AS WITH src AS (SELECT id FROM '$B/a.csv') SELECT COUNT(*) AS n FROM src;
        \\LOAD INTO '$B/out_2.csv' AS WITH src AS (SELECT id FROM '$B/b.csv') SELECT COUNT(*) AS n FROM src;
    , 1, &.{});
    try expectFile(&tmp, "out_1.csv", "n\n3\n");
    try expectFile(&tmp, "out_2.csv", "n\n2\n");
}

test "a WITH declared in a for-each body is not visible after it, to check and run alike" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id\n1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "outer.csv", .data = "name\na\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc,
        \\FOR EACH ROW OF ('$B/outer.csv') AS (name)
        \\  LOAD INTO '$B/out.csv' AS WITH src AS (SELECT id FROM '$B/a.csv') SELECT id FROM src;
        \\END FOR;
        \\LOAD INTO '$B/out_2.csv' AS SELECT id FROM src;
    , "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
    try std.testing.expect(std.mem.indexOf(u8, adiag.msg, "src") != null);
    var rdiag: Diag = .{};
    try std.testing.expect(std.meta.isError(run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "src") != null);
}

test "CALL: a nested FOR EACH in a statement function sees its argument and the script PARAMs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\PARAM tag STRING;
        \\CREATE FUNCTION fan(src) AS
        \\  FOR EACH ROW OF ('$B/inner.csv') AS (suffix)
        \\    LOAD INTO IDENTIFIER('$B/' || $tag || '_' || $src || '_' || $suffix || '.csv') AS
        \\    SELECT COUNT(*) AS n, $src AS s, $suffix AS sfx FROM IDENTIFIER('$B/' || $src || '.csv');
        \\  END FOR;
        \\END;
        \\FOR EACH ROW OF ('$B/outer.csv') AS (name)
        \\  CALL fan($name);
        \\END FOR;
    , 1, &[_]ParamArg{.{ .key = "tag", .val = "out" }});
    try expectFile(&tmp, "out_a_one.csv", "n,s,sfx\n3,a,one\n");
    try expectFile(&tmp, "out_b_two.csv", "n,s,sfx\n2,b,two\n");
}

test "LET inside a for-each body is refused by check and run with the same message" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "outer.csv", .data = "name\na\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc,
        \\FOR EACH ROW OF ('$B/outer.csv') AS (name)
        \\  LET x = 1;
        \\  LOAD INTO '$B/out.csv' AS SELECT $name AS n;
        \\END FOR;
    , "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    const want = "LET `x` must be declared at the top level of the script";
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
    try std.testing.expectEqualStrings(want, adiag.msg);
    try std.testing.expectEqual(@as(u32, 2), adiag.pos.?.line);
    var rdiag: Diag = .{};
    try std.testing.expect(std.meta.isError(run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, want) != null);
}

test "IDENTIFIER names a column per row: SELECT list, WHERE, GROUP BY, ORDER BY, DISTINCT ON, an expression" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "cols.csv", .data = "c\ngrp\nid\n" });
    try checkAndRun(alloc, &tmp,
        \\FOR EACH ROW OF ('$B/cols.csv') AS (c)
        \\  LOAD INTO IDENTIFIER('$B/agg_' || $c || '.csv') AS
        \\  SELECT IDENTIFIER($c), COUNT(*) AS n FROM '$B/a.csv'
        \\  WHERE IDENTIFIER($c) IS NOT NULL GROUP BY IDENTIFIER($c) ORDER BY IDENTIFIER($c) DESC;
        \\  LOAD INTO IDENTIFIER('$B/dist_' || $c || '.csv') AS
        \\  SELECT DISTINCT ON (IDENTIFIER($c)) IDENTIFIER($c), upper(CAST(IDENTIFIER($c) AS STRING)) AS u, $c AS col FROM '$B/a.csv';
        \\END FOR;
    , 1, &.{});
    try expectFile(&tmp, "agg_grp.csv", "grp,n\nb,1\na,2\n");
    try expectFile(&tmp, "agg_id.csv", "id,n\n3,1\n2,1\n1,1\n");
    try expectFile(&tmp, "dist_grp.csv", "grp,u,col\na,A,grp\nb,B,grp\n");
}

test "IDENTIFIER names a column from a PARAM outside any loop" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\PARAM col STRING;
        \\LOAD INTO '$B/out.csv' AS
        \\SELECT IDENTIFIER($col) AS k, SUM(amt) AS s FROM '$B/a.csv' GROUP BY IDENTIFIER($col) ORDER BY k;
    , 1, &[_]ParamArg{.{ .key = "col", .val = "grp" }});
    try expectFile(&tmp, "out.csv", "k,s\na,30\nb,5\n");
}

test "EXCEPT (IDENTIFIER($cols)): a comma list excludes each name, an empty one excludes nothing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\CREATE FUNCTION slim(ex) AS
        \\  LOAD INTO IDENTIFIER('$B/out_' || length($ex) || '.csv') AS SELECT * EXCEPT (IDENTIFIER($ex)) FROM '$B/a.csv' WHERE id = 1;
        \\END;
        \\CALL slim('grp, amt');
        \\CALL slim('');
        \\CALL slim('nope');
    , 1, &.{});
    try expectFile(&tmp, "out_8.csv", "id\n1\n");
    try expectFile(&tmp, "out_0.csv", "id,grp,amt\n1,a,10\n");
    try expectFile(&tmp, "out_4.csv", "id,grp,amt\n1,a,10\n");
}

test "a bare name is a column even when a PARAM or loop variable shares it; `$name` is the binding" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\PARAM grp STRING DEFAULT 'b';
        \\LOAD INTO '$B/bare.csv' AS SELECT COUNT(*) AS n FROM '$B/a.csv' WHERE grp = 'a';
        \\LOAD INTO '$B/dollar.csv' AS SELECT COUNT(*) AS n FROM '$B/a.csv' WHERE grp = $grp;
        \\LOAD INTO '$B/both.csv' AS SELECT grp, $grp AS p FROM '$B/a.csv' ORDER BY id;
        \\FOR EACH ROW OF ('$B/outer.csv') AS (grp)
        \\  LOAD INTO IDENTIFIER('$B/loop_' || $grp || '.csv') AS SELECT COUNT(*) AS n, $grp AS lv FROM '$B/a.csv' WHERE grp = 'b';
        \\END FOR;
    , 1, &.{});
    try expectFile(&tmp, "bare.csv", "n\n2\n");
    try expectFile(&tmp, "dollar.csv", "n\n1\n");
    try expectFile(&tmp, "both.csv", "grp,p\na,b\na,b\nb,b\n");
    try expectFile(&tmp, "loop_a.csv", "n,lv\n1,a\n");
    try expectFile(&tmp, "loop_b.csv", "n,lv\n1,b\n");
}

test "DATE, TIMESTAMP, TIME and DECIMAL params bind from text as a CAST reads it, and keep their type" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const script =
        \\PARAM d DATE DEFAULT '2026-01-01';
        \\PARAM t TIMESTAMP DEFAULT '2026-01-01 08:00:00';
        \\PARAM tm TIME DEFAULT '08:00';
        \\PARAM m DECIMAL(10,2) DEFAULT 1.5;
        \\LET x = CAST('1.5' AS DECIMAL(10,2));
        \\LOAD INTO '$B/p.csv' AS SELECT $d AS d, date_add('day', 1, $d) AS d1, $t AS t, $tm AS tm, $m AS m, $x + 1 AS x1;
    ;
    try checkAndRun(alloc, &tmp, script, 1, &.{});
    try expectFile(&tmp, "p.csv", "d,d1,t,tm,m,x1\n2026-01-01,2026-01-02,2026-01-01 08:00:00,08:00:00,1.50,2.50\n");
    try checkAndRun(alloc, &tmp, script, 1, &.{
        .{ .key = "d", .val = "2026-02-01" },
        .{ .key = "t", .val = "2026-02-01" },
        .{ .key = "tm", .val = "10:30:15" },
        .{ .key = "m", .val = "12.345" },
    });
    try expectFile(&tmp, "p.csv", "d,d1,t,tm,m,x1\n2026-02-01,2026-02-02,2026-02-01 00:00:00,10:30:15,12.35,2.50\n");

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), "PARAM d DATE DEFAULT 'nope'; SELECT $d AS d;", &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expectEqualStrings("PARAM `d`: `nope` is not a date (YYYY-MM-DD)", rdiag.msg);
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
    try std.testing.expectEqualStrings("PARAM `d`: `nope` is not a date (YYYY-MM-DD)", adiag.msg);
    var bdiag: Diag = .{};
    const bound = try parser.parseSource(parena.allocator(), "PARAM d DATE DEFAULT '2026-01-01'; SELECT $d AS d;", &pdiag);
    try std.testing.expectError(error.PlanFailed, run(alloc, bound, .{ .params = &.{.{ .key = "d", .val = "01/02/2026" }} }, &bdiag));
    try std.testing.expect(std.mem.indexOf(u8, bdiag.msg, "PARAM `d`: `01/02/2026` is not a date (YYYY-MM-DD)") != null);
}

test "check validates a literal date unit or strftime format where the columns' types are not known" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const cases = [_]struct { sql: []const u8, bad: ?[]const u8 }{
        .{ .sql = "SELECT date_trunc('fortnight', ts) AS w FROM p.dbo.t;", .bad = "unknown time unit `fortnight`" },
        .{ .sql = "SELECT * FROM p.dbo.t WHERE date_diff('quarter', a, b) > 1;", .bad = "unknown time unit `quarter`" },
        .{ .sql = "WITH c AS (SELECT strftime(ts, '%Q') AS s FROM p.dbo.t) SELECT * FROM c;", .bad = "does not support `%Q`" },
        .{ .sql = "SELECT max(date_add('weeks', 1, ts)) AS m FROM p.dbo.t;", .bad = "unknown time unit `weeks`" },
        .{ .sql = "SELECT date_trunc('week', ts) AS w FROM p.dbo.t;", .bad = null },
    };
    for (cases) |tc| {
        const src = try std.fmt.allocPrint(a, "CREATE CONNECTION p TYPE sqlserver OPTIONS (host = 'h');\n{s}", .{tc.sql});
        const prog = try parser.parseSource(a, src, &pdiag);
        var d = analyze.Diag{};
        if (tc.bad) |want| {
            try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(a, prog, &d));
            try std.testing.expect(std.mem.indexOf(u8, d.msg, want) != null);
        } else _ = try analyze.analyze(a, prog, &d);
    }
}

test "check and run refuse alike: declarations out of place, bad connection options, sink options no sink reads, PARAM text that is not its type, LETs that cannot fold" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "x\n1\n" });
    const cases = [_][2][]const u8{
        .{ "FOR EACH ROW OF ('$B/a.csv') AS (x)\n  CREATE FUNCTION f(v) AS v;\n  LOAD INTO '$B/o.csv' AS SELECT 1 AS a;\nEND FOR;", "may contain only" },
        .{ "CASE WHEN 1 = 1 THEN PARAM z INT; END CASE;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "PARAM `z` must be declared at the top level" },
        .{ "CASE WHEN 1 = 1 THEN CREATE FUNCTION f(v) AS v; END CASE;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "function `f` must be declared at the top level" },
        .{ "CREATE CONNECTION x TYPE foo OPTIONS (host = 'h');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "unknown connection type `foo`" },
        .{ "LOAD INTO '$B/o.csv' USING stream_load AS SELECT 1 AS a;", "only a starrocks or doris target" },
        .{ "LOAD INTO '$B/o.csv' WITH (encoding = 'latin1') AS SELECT * FROM '$B/a.csv';", "a CSV sink always writes UTF-8" },
        .{ "LOAD INTO 'smb://fs/share/o.csv' APPEND AS SELECT 1 AS a;", "renamed over the target" },
        .{ "LOAD INTO '$B/o.zip' AS SELECT 1 AS a;", "does not write them" },
        .{ "LOAD INTO '$B/o.zip :: a.csv' AS SELECT 1 AS a;", "does not write them" },
        .{ "LOAD INTO '$B/o.dat' WITH (format = 'parquet') APPEND AS SELECT 1 AS a;", "a parquet file's footer" },
        .{ "LOAD INTO '$B/o.csv' UPSERT ON (a) AS SELECT 1 AS a;", "`UPSERT` needs a table to merge into" },
        .{ "LOAD INTO '$B/o.csv' WITH (bogus = 1) AS SELECT 1 AS a;", "unknown `LOAD INTO` option `bogus`" },
        .{ "LOAD INTO '$B/o.csv.gz' WITH (compression = 'gzip') AS SELECT 1 AS a;", "`compression` applies to a Parquet file target only" },
        .{ "LOAD INTO '$B/o.parquet' WITH (compression = 'zstd') AS SELECT 1 AS a;", "unknown Parquet compression `zstd`" },
        .{ "PARAM n INT DEFAULT 'abc';\nLOAD INTO '$B/o.csv' AS SELECT $n AS a;", "PARAM `n`: `abc` is not an integer" },
        .{ "LET q = (SELECT COUNT(*) AS c FROM '$B/a.csv');\nLET m = $q + 1;\nLOAD INTO '$B/o.csv' AS SELECT $m AS a;", "LET `m` reads `$q`, a query LET" },
        .{ "LET m = $zz + 1;\nLOAD INTO '$B/o.csv' AS SELECT $m AS a;", "unknown `$zz`" },
        .{ "FOR EACH ROW OF (SELECT x FROM '$B/a.csv' WHERE 1 = 0) AS (x)\n  LET y = 1;\n  LOAD INTO '$B/o.csv' AS SELECT 1 AS a;\nEND FOR;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "LET `y` must be declared at the top level" },
        .{ "CREATE FUNCTION p(v) AS\n  LOAD INTO '$B/o.csv' AS SELECT 1 AS a;\n  LET y = 1;\nEND;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "LET `y` must be declared at the top level" },
        .{ "PARAM a INT DEFAULT 2;\nPARAM a INT DEFAULT 3;\nLOAD INTO '$B/o.csv' AS SELECT $a AS a;", "duplicate PARAM `a`" },
        .{ "LET a = 1;\nLET a = 2;\nLOAD INTO '$B/o.csv' AS SELECT $a AS a;", "duplicate LET `a`" },
        .{ "PARAM n INT DEFAULT 5;\nCASE $n WHEN 5 THEN LOAD INTO '$B/o.csv' AS SELECT 1 AS a; WHEN 'x' THEN LOAD INTO '$B/o.csv' AS SELECT 2 AS a; END CASE;", "CASE pattern 'x' is not int" },
        .{ "CREATE CONNECTION db TYPE sqlserver OPTIONS (host = 'h', auth = 'ntlm', tls = 'off');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "requires an encrypted channel" },
        .{ "CREATE CONNECTION sr TYPE starrocks OPTIONS (host = '127.0.0.1', user = 'u', password = 'p', database = 'd', load_size = 'abc');\nLOAD INTO sr.t AS SELECT 1 AS a;", "`load_size` must be a positive size like '128MB', not 'abc'" },
        .{ "CREATE CONNECTION sr TYPE starrocks OPTIONS (host = '127.0.0.1', user = 'u', password = 'p', database = 'd', compression = 'lz4');\nLOAD INTO sr.t AS SELECT 1 AS a;", "`compression` must be 'none' or 'gzip', not 'lz4'" },
        .{ "CREATE CONNECTION sr TYPE doris OPTIONS (host = '127.0.0.1', user = 'u', password = 'p');\nLOAD INTO sr.t AS SELECT 1 AS a;", "doris connection needs a `database`" },
        .{ "CREATE CONNECTION sr TYPE starrocks OPTIONS (host = '127.0.0.1', user = 'u', password = 'p', database = 'd', buckets = 'x');\nLOAD INTO sr.t AS SELECT 1 AS a;", "invalid integer config value" },
        .{ "CREATE CONNECTION h TYPE http OPTIONS (base_url = 'http://127.0.0.1');\nLOAD INTO h.t AS SELECT 1 AS a;", "unsupported sink connector `http`" },
        .{ "CREATE CONNECTION f TYPE ftp OPTIONS (host = 'h', bogus = 1);\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "ftp connection `f`: unknown option `bogus`" },
        .{ "CREATE CONNECTION f TYPE sftp OPTIONS (port = 22);\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "sftp connection `f` needs a `host`" },
        .{ "CREATE CONNECTION f TYPE sftp OPTIONS (host = 'h', port = 99999);\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "sftp connection `port` is out of range" },
        .{ "CREATE CONNECTION f TYPE smb OPTIONS (host = 'h', auth = 'basic');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "`auth` is kerberos or ntlm, not `basic`" },
        .{ "CREATE CONNECTION f TYPE smb OPTIONS (host = 'h', auth = 'kerberos', user = 'me');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "Kerberos needs the `realm`" },
        .{ "SELECT * FROM nope.t;", "unknown connection `nope` in read" },
        .{ "CREATE CONNECTION sr TYPE starrocks OPTIONS (host = '127.0.0.1', user = 'u', password = 'p', database = 'd', port = 99999);\nLOAD INTO sr.t AS SELECT 1 AS a;", "starrocks connection `port` is out of range" },
        .{ "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', user = 'u', password = 'p', database = 'd');\nLOAD INTO pg.t UPSERT AS SELECT * FROM '$B/a.csv';", "a bare `UPSERT` infers the primary key from the source" },
        .{ "PARAM n INT DEFAULT 1;\nTHROW 'boom' WHEN $n > 'x';\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "THROW condition: type mismatch" },
        .{ "THROW 1 / 0 WHEN true;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "THROW message: division by zero" },
        .{ "PARAM n INT DEFAULT 1;\nCASE WHEN $n = 'x' THEN LOAD INTO '$B/o.csv' AS SELECT 1 AS a; END CASE;", "match guard: type mismatch" },
        .{ "PARAM n INT DEFAULT 1;\nCASE $n WHEN 1 / 0 THEN LOAD INTO '$B/o.csv' AS SELECT 1 AS a; END CASE;", "match pattern: division by zero" },
        .{ "PRINT 1 / 0;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "PRINT: division by zero" },
        .{ "CREATE FUNCTION f(a) AS\n  LOAD INTO '$B/o.csv' AS SELECT $a AS a;\nEND;\nCALL f(1 / 0);", "CALL f: argument `a`: division by zero" },
        .{ "PARAM n STRING DEFAULT upper('x');\nLOAD INTO '$B/o.csv' AS SELECT $n AS a;", "param default must be a literal" },
        .{ "LET q = (SELECT x, x AS y FROM '$B/a.csv');\nLOAD INTO '$B/o.csv' AS SELECT $q AS a;", "LET `q`: must produce exactly one column, got 2" },
        .{ "FOR EACH ROW OF (SELECT x FROM '$B/a.csv' WHERE 1 = 0) AS (x)\n  LOAD INTO '$B/o.csv' WITH (bogus = 1) AS SELECT 1 AS a;\nEND FOR;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "unknown `LOAD INTO` option `bogus`" },
        .{ "CASE WHEN 1 = 0 THEN LOAD INTO '$B/o.xlsx' AS SELECT 1 AS a; END CASE;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "does not write them" },
        .{ "CREATE FUNCTION p(v) AS\n  LOAD INTO '$B/o.csv' USING stream_load AS SELECT 1 AS a;\nEND;\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "only a starrocks or doris target" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' a JOIN '$B/a.csv' b ON a.x = b.x WITH (join_order = 'bad');", "join_order is 'auto' or 'written'" },
        .{ "LOAD INTO '$B/o.csv' WITH (jobs = 2) AS SELECT * FROM '$B/a.csv';", "unknown `LOAD INTO` option `jobs`" },
        .{ "LOAD INTO '$B/o.csv' WITH (split = 'x') AS SELECT * FROM '$B/a.csv';", "unknown `LOAD INTO` option `split`" },
        .{ "LOAD INTO '$B/o.csv' WITH (label_prefix = 'x') AS SELECT * FROM '$B/a.csv';", "option `label_prefix` does not apply to a file target" },
        .{ "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', database = 'd');\nLOAD INTO pg.t WITH (delimiter = ';') AS SELECT * FROM '$B/a.csv';", "option `delimiter` does not apply to a postgres target" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' WITH (delimeter = ';');", "unknown read option `delimeter`: a CSV read takes" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' WITH (sheet = 'a');", "read option `sheet` does not apply to a CSV read" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' UNION ALL SELECT * FROM '$B/a.csv' WITH (nope = 1);", "unknown read option `nope`" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' WITH (max_build = '1GB');", "`max_build` is an option of a JOIN clause" },
        .{ "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', database = 'd');\nLOAD INTO '$B/o.csv' AS SELECT * FROM pg.t WITH (fetch_size = 10);", "unknown read option `fetch_size`: a SQL table read takes split, splits, split_kind" },
        .{ "CREATE CONNECTION h TYPE http OPTIONS (base_url = 'http://127.0.0.1');\nLOAD INTO '$B/o.csv' AS SELECT * FROM h.GET('/x', a = 1) WITH (itmes = 'data');", "unknown read option `itmes`: a REST read" },
        .{ "LOAD INTO '$B/o.csv' SPLIT BY (x) AS SELECT * FROM '$B/a.csv';", "`SPLIT BY` cuts a SQL table read into key ranges" },
        .{ "LOAD INTO '$B/o.csv' SPLIT BY (nope) JOBS 2 AS WITH b AS (SELECT * FROM '$B/a.csv') SELECT * FROM b;", "`SPLIT BY` cuts a SQL table read into key ranges" },
        .{ "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', bogus = 'x');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "postgres connection `pg`: unknown option `bogus`" },
        .{ "CREATE CONNECTION pg TYPE postgres OPTIONS (database = 'd');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "postgres connection `pg` needs a `host`" },
        .{ "CREATE CONNECTION ms TYPE sqlserver OPTIONS (host = 'h', auth = 'kerberos', user = 'me', tls = 'require');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "`auth = 'kerberos'` needs the `realm`" },
        .{ "PRINT 1;\nFOR EACH ROW OF (SELECT 1 AS a) AS (a, b)\n  PRINT $a;\nEND FOR;", "returns fewer columns than loop variables (1 for 2)" },
        .{ "PRINT 1;\nFOR EACH ROW OF ($j) AS (x)\n  PRINT $x;\nEND FOR;", "for-each: `j` is not a JSON param" },
        .{ "FOR EACH ROW OF (SELECT x FROM '$B/a.csv') AS (x)\n  LOAD INTO '$B/o_${x:lower}.csv' AS SELECT 1 AS a;\nEND FOR;", "`${x:lower}`: unexpected trailing input" },
        .{ "PRINT 1;\nSELECT t.nope FROM '$B/a.csv' t;", "unknown field `nope`" },
        .{ "PRINT 1;\nSELECT IDENTIFIER('nope') FROM '$B/a.csv';", "unknown field `nope`" },
        .{ "PRINT 1;\nSELECT * FROM '$B/a.csv' p JOIN '$B/a.csv' q ON p.nope = q.x;", "unknown column `p.nope`" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' p JOIN '$B/a.csv' q ON p.x = q.x WITH (max_build = 'lots');", "bad max_build `lots`" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' p JOIN '$B/a.csv' q ON p.x = q.x WITH (max_build = 0);", "max_build must be positive" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM range('a', 3);", "RANGE bounds must be integer literals" },
        .{ "CREATE CONNECTION f TYPE sftp OPTIONS (host = 'h');\nLOAD INTO '$B/o.csv' AS SELECT * FROM f.t;", "unsupported source connector `sftp`" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/a.csv' UNION ALL BY NAME SELECT * FROM '$B/a.csv' ANCHOR SCHEMA nope;", "union canon `nope` is not one of the source tables" },
        .{ "CREATE CONNECTION h TYPE http OPTIONS (url = 'http://127.0.0.1');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "http connection `h`: unknown attribute `url`" },
        .{ "CREATE CONNECTION h TYPE http OPTIONS (base_url = 'http://127.0.0.1', auth = 'bearer');\nLOAD INTO '$B/o.csv' AS SELECT 1 AS a;", "http connection `h`: auth 'bearer' needs a `token`" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT x, LAG(x, 1, 'no') OVER (ORDER BY x) AS l FROM '$B/a.csv';", "LAG/LEAD default does not fit `x`" },
        .{ "PRINT 1;\nCALL nope(1);", "CALL: no function named `nope`" },
        .{ "CREATE FUNCTION f(a) AS a + 1;\nCALL f(1);", "`f` is a scalar function" },
        .{ "CREATE FUNCTION f(a) RETURNS TABLE AS SELECT $a AS x;\nCALL f(1);", "`f` is a table function" },
        .{ "CREATE FUNCTION f(a) AS\n  PRINT $a;\nEND;\nCALL f(1, 2);", "`f` expects 1 argument(s), got 2" },
        .{ "FOR EACH ROW OF (SELECT x FROM '$B/a.csv' WHERE 1 = 0) AS (x)\n  CALL nope($x);\nEND FOR;", "CALL: no function named `nope`" },
        .{ "LET x = 1;\nPARAM x INT DEFAULT 2;\nLOAD INTO '$B/o.csv' AS SELECT $x AS a;", "`x` is declared twice: LET and PARAM share one name space" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT p.x FROM '$B/a.csv' p WHERE EXISTS (SELECT 1 FROM '$B/a.csv' q WHERE CAST(q.x AS string) = p.x);", "a correlated subquery compares `x` (int) with a column of its own (string)" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT p.x, (SELECT count(*) FROM '$B/a.csv' q WHERE CAST(q.x AS string) = p.x) AS n FROM '$B/a.csv' p;", "a correlated subquery compares `x` (int) with a column of its own (string)" },
    };
    for (cases) |c| try expectRefusedAlike(alloc, &tmp, c[0], c[1]);
}

test "check and run refuse alike a LET bound on the command line" {
    const alloc = std.testing.allocator;
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), "LET x = 1;\nSELECT $x AS a;", &pdiag);
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyzeWith(parena.allocator(), prog, &.{.{ .name = "x", .value = "2" }}, &adiag));
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .params = &.{.{ .key = "x", .val = "2" }}, .log = .{ .quiet = true } }, &rdiag));
    try std.testing.expectEqualStrings(adiag.msg, rdiag.msg);
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "is a LET, not a PARAM") != null);
}

test "a PARAM DEFAULT may be a negative number" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc,
        \\PARAM n INT DEFAULT -5;
        \\PARAM f FLOAT DEFAULT -2.5;
        \\LOAD INTO '{s}/out.csv' AS SELECT $n AS n, $f AS f;
    , .{base});
    defer alloc.free(script);
    const got = try runScript(alloc, &tmp, script, &.{});
    defer alloc.free(got);
    try std.testing.expectEqualStrings("n,f\n-5,-2.5\n", got);
}

test "a Parquet sink's `compression` picks gzip or no codec, and the file reads back whole" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    for ([_][]const u8{ "'gzip'", "none", "'snappy'" }) |c| {
        const script = try std.fmt.allocPrint(alloc,
            \\LOAD INTO '{s}/p.parquet' WITH (compression = {s}) AS SELECT range AS id, 'n' || CAST(range % 7 AS VARCHAR) AS s FROM RANGE(5000);
            \\LOAD INTO '{s}/out.csv' AS SELECT COUNT(*) AS n, SUM(id) AS t, COUNT(DISTINCT s) AS d FROM '{s}/p.parquet';
        , .{ base, c, base, base });
        defer alloc.free(script);
        const got = try runScript(alloc, &tmp, script, &.{});
        defer alloc.free(got);
        try std.testing.expectEqualStrings("n,t,d\n5000,12497500,7\n", got);
    }
}

test "a JSON PARAM's paths bind from -p in a batch run, a missing key is an error, a string DEFAULT is the fallback" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc,
        \\PARAM job JSON;
        \\PARAM cfg JSON DEFAULT '{{"n":9}}';
        \\LOAD INTO '{s}/out.csv' AS SELECT $job.a.b AS b, $job.n + $cfg.n AS n;
    , .{base});
    defer alloc.free(script);
    const got = try runScript(alloc, &tmp, script, &.{.{ .key = "job", .val = "{\"a\":{\"b\":\"x\"},\"n\":5}" }});
    defer alloc.free(got);
    try std.testing.expectEqualStrings("b,n\nx,14\n", got);

    try std.testing.expectError(error.PlanFailed, runScript(alloc, &tmp, script, &.{.{ .key = "job", .val = "{\"n\":5}" }}));
}

test "a FOR EACH over a JSON PARAM iterates its string DEFAULT; one given nothing is missing unless read with ?." {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc,
        \\PARAM rows JSON DEFAULT '[{{"x":"a"}},{{"x":"b"}}]';
        \\FOR EACH ROW OF ($rows) AS (x)
        \\  LOAD INTO '{s}/out_${{x}}.csv' AS SELECT '${{x}}' AS v;
        \\END FOR;
    , .{base});
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag);
    try expectFile(&tmp, "out_b.csv", "v\nb\n");

    const bare = try parser.parseSource(parena.allocator(), "PARAM rows JSON;\nFOR EACH ROW OF ($rows) AS (x)\n  PRINT $x;\nEND FOR;", &pdiag);
    try std.testing.expectError(error.PlanFailed, run(alloc, bare, .{ .log = .{ .quiet = true } }, &rdiag));
    try std.testing.expectEqualStrings("missing required param `rows`", rdiag.msg);
    try std.testing.expectEqual(@as(u32, 2), rdiag.pos.?.line);

    const optional = try parser.parseSource(parena.allocator(), "PARAM rows JSON;\nFOR EACH ROW OF ($rows?.items) AS (x)\n  PRINT $x;\nEND FOR;\nPRINT 'done';", &pdiag);
    _ = try run(alloc, optional, .{ .log = .{ .quiet = true } }, &rdiag);
}

test "an EXPLAIN statement's analyze error keeps check's position" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc, "PRINT 'x';\nEXPLAIN SELECT id FROM '{s}/in.csv'\nWHERE nope > 1;", .{base});
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var adiag = analyze.Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze.analyze(parena.allocator(), prog, &adiag));
    var rdiag: Diag = .{};
    var cap = try StderrCapture.begin(tmp.dir);
    const res = run(alloc, prog, .{ .log = .{ .quiet = true, .summary = .none } }, &rdiag);
    const out = try cap.end(alloc);
    defer alloc.free(out);
    try std.testing.expectError(error.PlanFailed, res);
    try std.testing.expectEqualStrings(adiag.msg, rdiag.msg);
    try std.testing.expectEqual(adiag.pos.?, rdiag.pos.?);
}

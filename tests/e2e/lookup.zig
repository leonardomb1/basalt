//! End-to-end `JOIN LATERAL f(args)`: a table function whose body reads an http
//! connection, called per row, in batches, against a loopback server that answers
//! each request from its body, as an embeddings API does, and counts what it got.

const std = @import("std");
const basalt = @import("basalt");
const ParamArg = basalt.env.ParamArg;
const EchoServer = basalt.http_fake.EchoServer;
const runScriptOpts = @import("harness.zig").runScriptOpts;
const expectRunRefuses = @import("harness.zig").expectRunRefuses;

/// `{"input": [...]}` answered as `{"data": [{"embedding": [len, first byte], "index": i}]}`,
/// one item per input, `[-1, 0]` for a JSON null; a string `input` (one row per request) as one item per
/// comma-separated part, none for an empty string.
fn embeddings(a: std.mem.Allocator, _: []const u8, body: []const u8) anyerror![]const u8 {
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{});
    const input = parsed.object.get("input") orelse return error.NoInput;
    var w = std.Io.Writer.Allocating.init(a);
    try w.writer.writeAll("{\"data\": [");
    switch (input) {
        .array => |arr| for (arr.items, 0..) |v, i| {
            if (i > 0) try w.writer.writeAll(", ");
            if (v == .null) {
                try w.writer.print("{{\"embedding\": [-1, 0], \"index\": {d}}}", .{i});
                continue;
            }
            const s = if (v == .string) v.string else "";
            try w.writer.print("{{\"embedding\": [{d}, {d}], \"index\": {d}}}", .{ s.len, if (s.len > 0) s[0] else 0, i });
        },
        .string => |s| {
            var it = std.mem.splitScalar(u8, s, ',');
            var i: usize = 0;
            if (s.len > 0) while (it.next()) |part| : (i += 1) {
                if (i > 0) try w.writer.writeAll(", ");
                try w.writer.print("{{\"embedding\": [{d}, 0], \"index\": {d}}}", .{ part.len, i });
            };
        },
        else => {},
    }
    try w.writer.writeAll("]}");
    return w.written();
}

fn inputCsv(a: std.mem.Allocator, n: usize) ![]const u8 {
    var w = std.Io.Writer.Allocating.init(a);
    try w.writer.writeAll("id,txt\n");
    for (0..n) |i| try w.writer.print("{d},{s}\n", .{ i, "abcdefghijklmnopqrstuvwxyz"[0 .. i % 26 + 1] });
    return w.written();
}

fn embedFn(a: std.mem.Allocator, url: []const u8, batch: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a,
        \\CREATE CONNECTION ai TYPE http OPTIONS (base_url = '{s}');
        \\CREATE FUNCTION embed(input) RETURNS TABLE AS
        \\  SELECT CAST(embedding AS LIST(DOUBLE)) AS embedding, CAST(index AS INT) AS index FROM ai.POST('/v1/embeddings')
        \\  WITH (body_type = 'json', body = '{{"model": "m", "input": ${{input}}}}', items = 'data'{s});
        \\
    , .{ url, batch });
}

test "a function called per row sends its rows in batches, in input order, and answers each row with its own item, the same at every -j" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 10) });
    var want: ?[]const u8 = null;
    for ([_]usize{ 1, 8 }) |threads| {
        var srv: EchoServer = undefined;
        try srv.start(embeddings, 0);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding, e.index FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e ORDER BY t.id;", .{ try embedFn(a, try srv.url(a), ", batch = 4, concurrency = 2"), base, base });
        const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = threads });
        defer std.testing.allocator.free(out);
        try std.testing.expectEqual(@as(usize, 3), srv.requests());
        if (want) |w| try std.testing.expectEqualStrings(w, out) else want = try a.dupe(u8, out);
    }
    try std.testing.expectEqualStrings(
        "id,embedding,index\n" ++
            "0,\"[1,97]\",0\n1,\"[2,97]\",1\n2,\"[3,97]\",2\n3,\"[4,97]\",3\n" ++
            "4,\"[5,97]\",0\n5,\"[6,97]\",1\n6,\"[7,97]\",2\n7,\"[8,97]\",3\n" ++
            "8,\"[9,97]\",0\n9,\"[10,97]\",1\n",
        want.?,
    );
}

test "rows the WHERE drops are never sent: a rerun that skips what the target holds sends only the new rows" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 10) });
    try tmp.dir.writeFile(.{ .sub_path = "done.csv", .data = "id\n0\n1\n2\n3\n4\n5\n6\n" });
    var srv: EchoServer = undefined;
    try srv.start(embeddings, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e WHERE t.id NOT IN (SELECT id FROM '{s}/done.csv') AND t.id < 9 ORDER BY t.id;", .{ try embedFn(a, try srv.url(a), ", batch = 4"), base, base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 4 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("id,embedding\n7,\"[8,97]\"\n8,\"[9,97]\"\n", out);
    try std.testing.expectEqual(@as(usize, 1), srv.requests());
}

test "without batch each row is its own request and takes every item its reply holds; LEFT JOIN LATERAL keeps a row with none" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,txt\n1,ab\n2,\n3,\"x,yz\"\n" });
    for ([_][]const u8{ "CROSS JOIN LATERAL embed(t.txt) e", "LEFT JOIN LATERAL embed(t.txt) e ON TRUE" }, [_][]const u8{
        "id,n,i\n1,2,0\n3,1,0\n3,2,1\n",
        "id,n,i\n1,2,0\n2,,\n3,1,0\n3,2,1\n",
    }) |join, want| {
        var srv: EchoServer = undefined;
        try srv.start(embeddings, 0);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding[1] AS n, e.index AS i FROM '{s}/in.csv' t {s} ORDER BY t.id, i;", .{ try embedFn(a, try srv.url(a), ""), base, base, join });
        const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 4 });
        defer std.testing.allocator.free(out);
        try std.testing.expectEqualStrings(want, out);
        try std.testing.expectEqual(@as(usize, 3), srv.requests());
    }
}

fn short(a: std.mem.Allocator, _: []const u8, _: []const u8) anyerror![]const u8 {
    return a.dupe(u8, "{\"data\": [{\"embedding\": [1, 1]}]}");
}

test "a batched reply holding too few items fails the run, naming the function; a 429 is retried" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 3) });
    {
        var srv: EchoServer = undefined;
        try srv.start(short, 0);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '$B/out.csv' AS SELECT t.id, e.embedding FROM '$B/in.csv' t CROSS JOIN LATERAL embed(t.txt) e;", .{try embedFn(a, try srv.url(a), ", batch = 4")});
        try expectRunRefuses(a, &tmp, script, 1, "request of 3 rows got 1 items back");
    }
    {
        var srv: EchoServer = undefined;
        try srv.start(embeddings, 2);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e ORDER BY t.id;", .{ try embedFn(a, try srv.url(a), ", batch = 4"), base, base });
        const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 2 });
        defer std.testing.allocator.free(out);
        try std.testing.expectEqualStrings("id,embedding\n0,\"[1,97]\"\n1,\"[2,97]\"\n2,\"[3,97]\"\n", out);
        try std.testing.expectEqual(@as(usize, 3), srv.requests());
    }
}

test "a lookup in a CTE joined as a right side, with little memory, still sends each row once" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 40) });
    var srv: EchoServer = undefined;
    try srv.start(embeddings, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS WITH v AS (SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e) SELECT count(*) AS n, sum(v.embedding[1]) AS s FROM '{s}/in.csv' x JOIN v ON v.id = x.id;", .{ try embedFn(a, try srv.url(a), ", batch = 8"), base, base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 8, .op_memory = 1 << 10 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 5), srv.requests());
    try std.testing.expectEqualStrings("n,s\n40,456\n", out);
}

test "a condition on what an UNNEST makes runs between the UNNEST and the lookup: a rerun keyed on a chunk's hash sends only new chunks" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,txt\n1,aaaabbbb\n2,ccccdddd\n" });
    try tmp.dir.writeFile(.{ .sub_path = "done.csv", .data = "h\n61be55a8e2f6b4e172338bddf184d6dbee29c98853e0a0485ecee7f27b9af0b4\n81cc5b17018674b401b42f35ba07bb79e211239c23bffe658da1577e3e646877\n" });
    try tmp.dir.writeFile(.{ .sub_path = "done_text.csv", .data = "t\naaaa\nbbbb\n" });
    const chunks = try std.fmt.allocPrint(a, "'{s}/in.csv' d CROSS JOIN UNNEST(text_chunks(d.txt, 4, 0)) AS c", .{base});
    const done = try std.fmt.allocPrint(a, "(SELECT h FROM '{s}/done.csv')", .{base});
    for ([_][]const u8{
        try std.fmt.allocPrint(a, "SELECT d.id, c.no, e.embedding FROM {s} CROSS JOIN LATERAL embed(c.text) e WHERE sha256(c.text) NOT IN {s} ORDER BY d.id, c.no", .{ chunks, done }),
        try std.fmt.allocPrint(a, "SELECT d.id, c.no, e.embedding FROM {s} CROSS JOIN LATERAL embed(c.text) e WHERE c.text NOT IN (SELECT t FROM '{s}/done_text.csv') ORDER BY d.id, c.no", .{ chunks, base }),
        try std.fmt.allocPrint(a, "WITH k AS (SELECT d.id, c.no, c.text, sha256(c.text) AS h FROM {s}) SELECT k.id, k.no, e.embedding FROM k CROSS JOIN LATERAL embed(k.text) e WHERE k.h NOT IN {s} ORDER BY k.id, k.no", .{ chunks, done }),
    }) |query| {
        var srv: EchoServer = undefined;
        try srv.start(embeddings, 0);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS {s};", .{ try embedFn(a, try srv.url(a), ", batch = 1"), base, query });
        const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 4 });
        defer std.testing.allocator.free(out);
        try std.testing.expectEqual(@as(usize, 2), srv.requests());
        try std.testing.expectEqualStrings("id,no,embedding\n2,1,\"[4,99]\"\n2,2,\"[4,100]\"\n", out);
    }
}

fn twoParams(a: std.mem.Allocator, _: []const u8, body: []const u8) anyerror![]const u8 {
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{});
    const input = parsed.object.get("input").?.array.items;
    const label = parsed.object.get("label").?.array.items;
    var w = std.Io.Writer.Allocating.init(a);
    try w.writer.writeAll("{\"data\": [");
    for (input, label, 0..) |v, l, i| {
        if (i > 0) try w.writer.writeAll(", ");
        try w.writer.print("{{\"embedding\": [{d}, {d}]}}", .{ v.string.len, l.integer });
    }
    try w.writer.writeAll("]}");
    return w.written();
}

test "each parameter fills its own hole: a value holding another's `${...}` goes out as written; a NULL goes out as null" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,txt\n1,${label}\n2,ab\n" });
    {
        var srv: EchoServer = undefined;
        try srv.start(twoParams, 0);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a,
            \\CREATE CONNECTION ai TYPE http OPTIONS (base_url = '{s}');
            \\CREATE FUNCTION tag(input, label) RETURNS TABLE AS
            \\  SELECT CAST(embedding AS LIST(DOUBLE)) AS embedding FROM ai.POST('/v1/tag')
            \\  WITH (body_type = 'json', body = '{{"input": ${{input}}, "label": ${{label}}}}', items = 'data', batch = 8);
            \\LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL tag(t.txt, t.id * 10) e ORDER BY t.id;
        , .{ try srv.url(a), base, base });
        const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 2 });
        defer std.testing.allocator.free(out);
        try std.testing.expectEqualStrings("id,embedding\n1,\"[8,10]\"\n2,\"[2,20]\"\n", out);
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,txt\n1,ab\n2,\n" });
    var srv: EchoServer = undefined;
    try srv.start(embeddings, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e ORDER BY t.id;", .{ try embedFn(a, try srv.url(a), ", batch = 4"), base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 2 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("id,embedding\n1,\"[2,97]\"\n2,\"[-1,0]\"\n", out);
}

test "a request still failing after its retries fails the run, naming the function and its rows; EXPLAIN calls nothing" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 3) });
    {
        var srv: EchoServer = undefined;
        try srv.start(embeddings, 100);
        defer srv.stop();
        const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '$B/out.csv' AS SELECT t.id, e.embedding FROM '$B/in.csv' t CROSS JOIN LATERAL embed(t.txt) e;", .{try embedFn(a, try srv.url(a), ", batch = 4, retries = 1")});
        try expectRunRefuses(a, &tmp, script, 1, "embed: a request for 3 rows failed");
        try std.testing.expectEqual(@as(usize, 2), srv.requests());
    }
    var srv: EchoServer = undefined;
    try srv.start(embeddings, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a, "{s}EXPLAIN LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e;", .{ try embedFn(a, try srv.url(a), ", batch = 4"), base, base });
    var diag: basalt.env.Diag = .{};
    _ = try @import("harness.zig").exec(std.testing.allocator, script, .{ .log = .{ .quiet = true } }, &diag);
    try std.testing.expectEqual(@as(usize, 0), srv.requests());
}

fn byTarget(a: std.mem.Allocator, target: []const u8, _: []const u8) anyerror![]const u8 {
    return std.fmt.allocPrint(a, "{{\"results\": [{{\"t\": {f}}}]}}", .{std.json.fmt(target, .{})});
}

test "a parameter in the path is the row's text, written `${p}` or `$p`; any expression may be passed; a constant call is one request" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,addr\n1,Lisbon\n2,Porto\n" });
    var srv: EchoServer = undefined;
    try srv.start(byTarget, 0);
    defer srv.stop();
    const decl = try std.fmt.allocPrint(a,
        \\CREATE CONNECTION g TYPE http OPTIONS (base_url = '{s}');
        \\CREATE FUNCTION geo(addr) RETURNS TABLE AS SELECT CAST(t AS STRING) AS t FROM g.GET('/geo/${{addr}}') WITH (items = 'results');
        \\CREATE FUNCTION geo2(addr) RETURNS TABLE AS SELECT CAST(t AS STRING) AS t FROM g.GET('/geo2/' || $addr) WITH (items = 'results');
        \\
    , .{try srv.url(a)});
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT d.id, x.t AS t, y.t AS t2 FROM '{s}/in.csv' d CROSS JOIN LATERAL geo(lower(d.addr)) x CROSS JOIN LATERAL geo2(d.addr) AS y ORDER BY d.id;", .{ decl, base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 2 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("id,t,t2\n1,/geo/lisbon,/geo2/Lisbon\n2,/geo/porto,/geo2/Porto\n", out);
    try std.testing.expectEqual(@as(usize, 4), srv.requests());

    const one = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t FROM geo('faro');", .{ decl, base });
    const got = try runScriptOpts(std.testing.allocator, &tmp, one, .{ .threads = 2 });
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("t\n/geo/faro\n", got);
    try std.testing.expectEqual(@as(usize, 5), srv.requests());
}

test "a function called per row is refused when planning would need a call or a batch cannot carry its holes; a plain read takes no batch" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION g TYPE http OPTIONS (base_url = 'http://127.0.0.1:9');\n";
    const cases = [_]struct { decl: []const u8, query: []const u8, want: []const u8 }{
        .{ .decl = "CREATE FUNCTION f(addr) RETURNS TABLE AS SELECT t FROM g.GET('/geo/${addr}');", .query = "SELECT x.t FROM 'in.csv' d CROSS JOIN LATERAL f(d.addr) x", .want = "CAST(<field> AS <type>) AS name" },
        .{ .decl = "CREATE FUNCTION f(addr) RETURNS TABLE AS SELECT * FROM g.GET('/geo/${addr}');", .query = "SELECT x.t FROM 'in.csv' d CROSS JOIN LATERAL f(d.addr) x", .want = "CAST(<field> AS <type>) AS name" },
        .{ .decl = "CREATE FUNCTION f(addr) RETURNS TABLE AS SELECT CAST(t AS STRING) AS t FROM g.GET('/geo/${addr}') WITH (batch = 8);", .query = "SELECT x.t FROM 'in.csv' d CROSS JOIN LATERAL f(d.addr) x", .want = "outside the body cannot carry a batch" },
        .{ .decl = "CREATE FUNCTION f() RETURNS TABLE AS SELECT * FROM g.GET('/all') WITH (batch = 8);", .query = "SELECT * FROM f()", .want = "is for a function called per row" },
        .{ .decl = "", .query = "SELECT * FROM g.GET('/all') WITH (concurrency = 2)", .want = "is for a function called per row" },
    };
    for (cases) |c| {
        const script = try std.fmt.allocPrint(a, "{s}{s}\nLOAD INTO 'out.csv' AS {s};", .{ conn, c.decl, c.query });
        var diag: basalt.sql_parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        try std.testing.expectError(error.ParseFailed, basalt.sql_parser.parseSource(a, script, &diag));
        if (std.mem.indexOf(u8, diag.msg, c.want) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ c.want, diag.msg });
            return error.TestUnexpectedResult;
        }
    }
}

test "a row's text is percent-encoded in the path and in a query argument; ON TRUE may follow any lookup join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,addr\n1,São Paulo & co\n" });
    var srv: EchoServer = undefined;
    try srv.start(byTarget, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a,
        \\CREATE CONNECTION g TYPE http OPTIONS (base_url = '{s}');
        \\CREATE FUNCTION geo(addr) RETURNS TABLE AS SELECT CAST(t AS STRING) AS t FROM g.GET('/geo/${{addr}}', q = '${{addr}}') WITH (items = 'results');
        \\LOAD INTO '{s}/out.csv' AS SELECT x.t FROM '{s}/in.csv' d CROSS JOIN LATERAL geo(d.addr) x ON TRUE;
    , .{ try srv.url(a), base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 1 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("t\n/geo/S%C3%A3o%20Paulo%20%26%20co?q=S%C3%A3o%20Paulo%20%26%20co\n", out);
}

fn modelEcho(a: std.mem.Allocator, _: []const u8, body: []const u8) anyerror![]const u8 {
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{});
    const o = parsed.object;
    return std.fmt.allocPrint(a, "{{\"data\": [{{\"m\": {f}, \"i\": {f}}}]}}", .{ std.json.fmt(o.get("model").?, .{}), std.json.fmt(o.get("input").?, .{}) });
}

test "a script PARAM fills its hole in a per-row request, and one named like the function's parameter does not take that parameter's hole" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,txt\n1,hello\n" });
    var srv: EchoServer = undefined;
    try srv.start(modelEcho, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a,
        \\PARAM model STRING DEFAULT 'small-1';
        \\PARAM input STRING DEFAULT 'not this';
        \\CREATE CONNECTION ai TYPE http OPTIONS (base_url = '{s}');
        \\CREATE FUNCTION embed(input) RETURNS TABLE AS SELECT CAST(m AS STRING) AS m, CAST(i AS STRING) AS i FROM ai.POST('/e')
        \\  WITH (body_type = 'json', body = '{{"model": "${{model}}", "input": ${{input}}}}', items = 'data');
        \\LOAD INTO '{s}/out.csv' AS SELECT e.m, e.i FROM '{s}/in.csv' d CROSS JOIN LATERAL embed(d.txt) e;
    , .{ try srv.url(a), base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 1 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("m,i\nsmall-1,hello\n", out);
}

test "a row's value cannot put a line break in a header" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,txt\n1,\"a\nb\"\n" });
    var srv: EchoServer = undefined;
    try srv.start(byTarget, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a,
        \\CREATE CONNECTION g TYPE http OPTIONS (base_url = '{s}');
        \\CREATE FUNCTION tagged(v) RETURNS TABLE AS SELECT CAST(t AS STRING) AS t FROM g.GET('/x') WITH (header = 'X-Tag: ${{v}}', items = 'results');
        \\LOAD INTO '$B/out.csv' AS SELECT x.t FROM '$B/in.csv' d CROSS JOIN LATERAL tagged(d.txt) x;
    , .{try srv.url(a)});
    try expectRunRefuses(a, &tmp, script, 1, "a row's value puts a line break in a header");
    try std.testing.expectEqual(@as(usize, 0), srv.requests());
}

test "a CTE holding a lookup, read twice, calls once per row" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 3) });
    var srv: EchoServer = undefined;
    try srv.start(embeddings, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS WITH v AS (SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e) SELECT p.id, q.embedding FROM v p JOIN v q ON q.id = p.id ORDER BY p.id;", .{ try embedFn(a, try srv.url(a), ""), base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 4 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("id,embedding\n0,\"[1,0]\"\n1,\"[2,0]\"\n2,\"[3,0]\"\n", out);
    try std.testing.expectEqual(@as(usize, 3), srv.requests());
}

test "a lookup in each UNION branch sends each branch's rows once at any -j" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = try inputCsv(a, 3) });
    var srv: EchoServer = undefined;
    try srv.start(embeddings, 0);
    defer srv.stop();
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(t.txt) e UNION ALL SELECT t.id, e.embedding FROM '{s}/in.csv' t CROSS JOIN LATERAL embed(upper(t.txt)) e ORDER BY id, embedding;", .{ try embedFn(a, try srv.url(a), ""), base, base, base });
    const out = try runScriptOpts(std.testing.allocator, &tmp, script, .{ .threads = 8 });
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 6), srv.requests());
    try std.testing.expectEqualStrings("id,embedding\n0,\"[1,0]\"\n0,\"[1,0]\"\n1,\"[2,0]\"\n1,\"[2,0]\"\n2,\"[3,0]\"\n2,\"[3,0]\"\n", out);
}

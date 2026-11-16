//! The run's connection pool end to end, through `run` against the in-process
//! PostgreSQL fake, which records each login: the statements of a run share one
//! connection, and one the server dropped while idle is dialed again.

const std = @import("std");
const msgfmt = @import("../../base/msgfmt.zig");
const FakePg = @import("../../testing/postgres_fake.zig").FakePg;
const Table = @import("../../testing/postgres_fake.zig").Table;
const env_mod = @import("../env.zig");
const parser = @import("../../lang/sql_parser.zig");
const run = @import("../run.zig").run;

const testing = std.testing;

const rows = [_][]const ?[]const u8{ &.{ "1", "alpha" }, &.{ "2", "beta" }, &.{ "3", "gamma" } };
const cols = [_]@import("../../testing/postgres_fake.zig").Col{ .{ .name = "id", .oid = 20 }, .{ .name = "name" } };
const tables = [_]Table{ .{ .name = "items", .cols = &cols, .rows = &rows }, .{ .name = "others", .cols = &cols, .rows = &rows } };

/// Runs `body`, its `$B` the tmp dir, after a connection `pg` to the fake; the
/// error it failed with, if any.
fn runScript(a: std.mem.Allocator, tmp: *std.testing.TmpDir, port: u16, body: []const u8) !?anyerror {
    const base = try tmp.dir.realpathAlloc(a, ".");
    const conn = try msgfmt.allocPrint(a, "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');\n", .{port});
    const script = try msgfmt.allocPrint(a, "{s}{s}", .{ conn, try std.mem.replaceOwned(u8, a, body, "$B", base) });
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var diag = env_mod.Diag{};
    _ = run(testing.allocator, prog, .{ .threads = 1, .log = .{ .quiet = true } }, &diag) catch |e| return @as(?anyerror, e);
    return null;
}

fn logins(srv: *FakePg) usize {
    srv.mutex.lock();
    defer srv.mutex.unlock();
    return srv.startups.items.len;
}

const three =
    \\LOAD INTO '$B/a.csv' AS SELECT id, name FROM pg.public.items WHERE id > 1;
    \\LOAD INTO '$B/b.csv' AS SELECT id + 1 AS x FROM pg.public.items;
    \\LOAD INTO '$B/c.csv' AS SELECT id FROM pg.public.others WHERE id < 3 ORDER BY id LIMIT 2;
;

test "pool: a run's statements, their probes and reads, log in once" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();
    try testing.expect(try runScript(a, &tmp, srv.port(), three) == null);
    try testing.expectEqual(@as(usize, 1), logins(srv));
    try testing.expectEqualStrings("id,name\n2,beta\n3,gamma\n", try tmp.dir.readFileAlloc(a, "a.csv", 1 << 10));
    try testing.expectEqualStrings("id\n1\n2\n", try tmp.dir.readFileAlloc(a, "c.csv", 1 << 10));
}

test "pool: a connection the server dropped while idle is dialed again; one dropped on every try fails, as the network failure it is" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();
    srv.drop_on = "others";
    srv.drop_once = true;
    try testing.expect(try runScript(a, &tmp, srv.port(), three) == null);
    try testing.expectEqual(@as(usize, 2), logins(srv));
    try testing.expectEqualStrings("id\n1\n2\n", try tmp.dir.readFileAlloc(a, "c.csv", 1 << 10));

    srv.drop_on = "others";
    srv.drop_once = false;
    try testing.expect(try runScript(a, &tmp, srv.port(), three) != null);
}

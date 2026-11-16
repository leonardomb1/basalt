//! REPLACE into a SQL database without ever emptying the target on a failure. The
//! rows go to a staging table first, from every lane in parallel with their own
//! commits and redials; once all of them are in, one transaction on the target
//! itself clears it and copies the staging rows in, and the staging table is
//! dropped. The target stays the same object, so its grants, views, foreign keys,
//! triggers, indexes and owner stay; readers see the old rows until the commit; a
//! run that fails before the cutover leaves the target as it was and drops the
//! staging table. Each run's staging table has a name of its own, so two REPLACEs
//! into one target at once never load or drop each other's rows: each cutover is
//! one transaction, and the last to commit holds the target. A run killed before
//! it could drop its staging table leaves it behind, named for its target.
//!
//! The sequence is written once here; what differs by database is `Intrinsics`, a
//! few statements each dialect supplies. A `TRUNCATE` is the default clear, being
//! transactional and cheap on PostgreSQL and SQL Server; `cutover = 'delete'`
//! serves a target it is refused on (one a foreign key references, or a login with
//! DELETE but not TRUNCATE rights). MySQL's TRUNCATE commits on its own, so MySQL
//! always deletes.

const std = @import("std");
const msgfmt = @import("../base/msgfmt.zig");
const sql = @import("sql.zig");
const types = @import("../lang/types.zig");

pub const Cutover = enum { truncate, delete };

pub const Intrinsics = struct {
    begin: []const u8,
    commit: []const u8,
    truncate_in_txn: bool,
    createLike: *const fn (std.mem.Allocator, []const u8, []const u8) error{OutOfMemory}![]const u8,
};

pub fn intrinsics(d: sql.Dialect) Intrinsics {
    return switch (d) {
        .postgres => .{ .begin = "BEGIN", .commit = "COMMIT", .truncate_in_txn = true, .createLike = struct {
            fn f(a: std.mem.Allocator, stg: []const u8, target: []const u8) error{OutOfMemory}![]const u8 {
                return msgfmt.allocPrint(a, "CREATE TABLE {s} (LIKE {s})", .{ stg, target });
            }
        }.f },
        .sqlserver => .{ .begin = "BEGIN TRANSACTION", .commit = "COMMIT TRANSACTION", .truncate_in_txn = true, .createLike = struct {
            fn f(a: std.mem.Allocator, stg: []const u8, target: []const u8) error{OutOfMemory}![]const u8 {
                return msgfmt.allocPrint(a, "SELECT * INTO {s} FROM {s} WHERE 1 = 0 UNION ALL SELECT * FROM {s} WHERE 1 = 0", .{ stg, target, target });
            }
        }.f },
        .mysql, .starrocks, .doris => .{ .begin = "START TRANSACTION", .commit = "COMMIT", .truncate_in_txn = false, .createLike = struct {
            fn f(a: std.mem.Allocator, stg: []const u8, target: []const u8) error{OutOfMemory}![]const u8 {
                return msgfmt.allocPrint(a, "CREATE TABLE {s} LIKE {s}", .{ stg, target });
            }
        }.f },
    };
}

/// The staging table's raw name: the target's own schema, then `_basalt_stage_`,
/// the table name cut so the whole fits PostgreSQL's 63-byte identifier limit, and
/// this run's `tag` (8 hex digits).
pub fn stagingName(a: std.mem.Allocator, target: []const u8, tag: u32) ![]const u8 {
    const dot = std.mem.lastIndexOfScalar(u8, target, '.');
    const schema = if (dot) |i| target[0 .. i + 1] else "";
    const table = if (dot) |i| target[i + 1 ..] else target;
    const room = 63 - "_basalt_stage_".len - "_00000000".len;
    return msgfmt.allocPrint(a, "{s}_basalt_stage_{s}_{x:0>8}", .{ schema, table[0..@min(table.len, room)], tag });
}

/// The statement that makes this run's staging table `staging`, shaped like the
/// target, which must already exist.
pub fn setupSql(a: std.mem.Allocator, d: sql.Dialect, target: []const u8, staging: []const u8) ![]const []const u8 {
    const out = try a.alloc([]const u8, 1);
    out[0] = try intrinsics(d).createLike(a, try sql.quoteIdent(a, d, staging), try sql.quoteIdent(a, d, target));
    return out;
}

/// The cutover: one transaction that clears the target and copies the staging rows
/// in by column name, so a target with its columns in another order still lines up.
/// On SQL Server a target that fills a loaded column by IDENTITY takes the staged
/// values as given (`sql.identityInsertSql`), as a bulk APPEND keeps them; its staging table
/// (`createLike`'s UNION ALL) has no IDENTITY of its own to renumber them. There the
/// copy after a TRUNCATE takes the table lock the TRUNCATE already holds, so it may
/// be minimally logged; after a DELETE it does not, the lock being refused on a
/// memory-optimized table, the one target a TRUNCATE cannot clear.
pub fn cutoverSql(a: std.mem.Allocator, d: sql.Dialect, target: []const u8, staging: []const u8, schema: types.Schema, how: Cutover) ![]const []const u8 {
    const in = intrinsics(d);
    const qt = try sql.quoteIdent(a, d, target);
    const qs = try sql.quoteIdent(a, d, staging);
    const cols = try sql.colList(a, d, schema);
    var out = std.array_list.Managed([]const u8).init(a);
    try out.append(in.begin);
    try out.append(if (how == .truncate and in.truncate_in_txn)
        try msgfmt.allocPrint(a, "TRUNCATE TABLE {s}", .{qt})
    else
        try msgfmt.allocPrint(a, "DELETE FROM {s}", .{qt}));
    const lock = d == .sqlserver and how == .truncate;
    const insert = try msgfmt.allocPrint(a, "INSERT INTO {s}{s} ({s}) SELECT {s} FROM {s}", .{ qt, if (lock) " WITH (TABLOCK)" else "", cols, cols, qs });
    if (d == .sqlserver) {
        try out.append(try sql.identityInsertSql(a, qt, schema, true));
        try out.append(insert);
        try out.append(try sql.identityInsertSql(a, qt, schema, false));
    } else try out.append(insert);
    try out.append(in.commit);
    return out.items;
}

pub fn dropSql(a: std.mem.Allocator, d: sql.Dialect, staging: []const u8) ![]const u8 {
    return msgfmt.allocPrint(a, "DROP TABLE IF EXISTS {s}", .{try sql.quoteIdent(a, d, staging)});
}

/// Runs `stmts` in order on `conn`; on a failure a transaction left open by it is
/// rolled back when the caller closes the connection.
pub fn execAll(conn: sql.Conn, stmts: []const []const u8) !void {
    for (stmts) |s| try conn.exec(s);
}

test "staging name keeps the schema, prefixes the table, ends in the run's tag and fits 63 bytes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("public._basalt_stage_orders_00c0ffee", try stagingName(a, "public.orders", 0xc0ffee));
    try std.testing.expectEqualStrings("_basalt_stage_t_00000001", try stagingName(a, "t", 1));
    const long = try stagingName(a, "s." ++ "x" ** 80, 0xffffffff);
    try std.testing.expectEqual(@as(usize, 2 + 63), long.len);
    try std.testing.expect(std.mem.endsWith(u8, long, "x_ffffffff"));
    try std.testing.expect(!std.mem.eql(u8, try stagingName(a, "t", 1), try stagingName(a, "t", 2)));
}

test "the cutover clears and copies in one transaction, per dialect" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{ .{ .name = "id", .ty = types.Type.init(.int) }, .{ .name = "v", .ty = types.Type.init(.string) } } };
    const pg = try cutoverSql(a, .postgres, "public.t", "public._basalt_stage_t_00000001", schema, .truncate);
    try std.testing.expectEqualStrings("BEGIN", pg[0]);
    try std.testing.expectEqualStrings("TRUNCATE TABLE \"public\".\"t\"", pg[1]);
    try std.testing.expectEqualStrings("INSERT INTO \"public\".\"t\" (\"id\",\"v\") SELECT \"id\",\"v\" FROM \"public\".\"_basalt_stage_t_00000001\"", pg[2]);
    try std.testing.expectEqualStrings("COMMIT", pg[3]);
    const ms = try cutoverSql(a, .sqlserver, "dbo.t", "dbo._basalt_stage_t_00000001", schema, .delete);
    try std.testing.expectEqualStrings("DELETE FROM [dbo].[t]", ms[1]);
    try std.testing.expectEqualStrings("IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[t]') AND is_identity = 1 AND name IN (N'id',N'v')) SET IDENTITY_INSERT [dbo].[t] ON", ms[2]);
    try std.testing.expectEqualStrings("INSERT INTO [dbo].[t] ([id],[v]) SELECT [id],[v] FROM [dbo].[_basalt_stage_t_00000001]", ms[3]);
    try std.testing.expectEqualStrings("IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[t]') AND is_identity = 1 AND name IN (N'id',N'v')) SET IDENTITY_INSERT [dbo].[t] OFF", ms[4]);
    try std.testing.expectEqualStrings("COMMIT TRANSACTION", ms[5]);
    const mt = try cutoverSql(a, .sqlserver, "dbo.t", "dbo._basalt_stage_t_00000001", schema, .truncate);
    try std.testing.expectEqualStrings("TRUNCATE TABLE [dbo].[t]", mt[1]);
    try std.testing.expectEqualStrings("INSERT INTO [dbo].[t] WITH (TABLOCK) ([id],[v]) SELECT [id],[v] FROM [dbo].[_basalt_stage_t_00000001]", mt[3]);
    const my = try cutoverSql(a, .mysql, "t", "_basalt_stage_t_00000001", schema, .truncate);
    try std.testing.expectEqualStrings("START TRANSACTION", my[0]);
    try std.testing.expectEqualStrings("DELETE FROM `t`", my[1]);
}

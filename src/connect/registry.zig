//! The connector names a script can write — `CREATE CONNECTION ... TYPE x`,
//! `FROM x.table`, `LOAD INTO x` — and what each resolves to. The one place the
//! name → kind mapping lives; dispatch elsewhere switches on the kind, so a new
//! connector is a compile error at every site that must know about it.

const std = @import("std");
const sql = @import("sql.zig");

/// A SQL connector: the driver that speaks its wire protocol and the dialect the
/// engine renders for it.
pub const SqlKind = enum {
    postgres,
    mysql,
    sqlserver,

    pub fn parse(name: []const u8) ?SqlKind {
        return std.meta.stringToEnum(SqlKind, name);
    }

    pub fn dialect(self: SqlKind) sql.Dialect {
        return switch (self) {
            .postgres => .postgres,
            .mysql => .mysql,
            .sqlserver => .sqlserver,
        };
    }

    pub fn defaultPort(self: SqlKind) u16 {
        return switch (self) {
            .postgres => 5432,
            .mysql => 3306,
            .sqlserver => 1433,
        };
    }
};

/// Every connector name the runtime and analyzer dispatch on.
pub const Connector = enum {
    csv,
    stdout,
    request,
    buffer,
    http,
    unit,
    range,
    starrocks,
    postgres,
    mysql,
    sqlserver,

    pub fn parse(name: []const u8) ?Connector {
        return std.meta.stringToEnum(Connector, name);
    }

    pub fn sqlKind(self: Connector) ?SqlKind {
        return switch (self) {
            .postgres => .postgres,
            .mysql => .mysql,
            .sqlserver => .sqlserver,
            else => null,
        };
    }

    /// Sources the engine reads without a `CREATE CONNECTION`.
    pub fn isBuiltinSource(self: Connector) bool {
        return switch (self) {
            .csv, .request, .http, .buffer, .range, .unit => true,
            else => false,
        };
    }
};

test "every SqlKind is a Connector with the same name and the expected port" {
    inline for (std.meta.fields(SqlKind)) |f| {
        const k: SqlKind = @enumFromInt(f.value);
        try std.testing.expectEqual(k, SqlKind.parse(f.name).?);
        try std.testing.expectEqual(k, Connector.parse(f.name).?.sqlKind().?);
    }
    try std.testing.expectEqual(@as(u16, 5432), SqlKind.postgres.defaultPort());
    try std.testing.expectEqual(@as(u16, 3306), SqlKind.mysql.defaultPort());
    try std.testing.expectEqual(@as(u16, 1433), SqlKind.sqlserver.defaultPort());
    try std.testing.expect(SqlKind.parse("starrocks") == null);
    try std.testing.expect(Connector.parse("starrocks").?.sqlKind() == null);
    try std.testing.expect(Connector.parse("nope") == null);
}

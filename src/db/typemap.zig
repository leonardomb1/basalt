//! Per-dialect type map. One row per (dialect, kind): the column type a CREATE
//! TABLE declares, the type a pushed-down CAST names, the wire code the database
//! reports back for that declared column, and the wire codes the driver's reader
//! turns into the kind. `ddlType`, `castType` and `readKind` are generated from
//! the rows at comptime. `lossy` lists every (dialect, kind) whose declared column
//! does not read back as the kind it was declared for.
//!
//! Wire codes are the PostgreSQL type OID, the MySQL column type byte (with the
//! UNSIGNED flag and the binary charset folded in, as the reader uses them), or
//! the TDS type byte. StarRocks and Doris speak the MySQL protocol.

const std = @import("std");
const msgfmt = @import("../format/msgfmt.zig");
const types = @import("../lang/types.zig");

const TypeKind = types.TypeKind;

pub const Dialect = enum { postgres, mysql, sqlserver, starrocks, doris };

/// The dialect with the same tag as `d`, any enum spelling these five names.
pub fn dialectOf(d: anytype) Dialect {
    return switch (d) {
        inline else => |t| @field(Dialect, @tagName(t)),
    };
}

pub const Wire = struct {
    code: u32,
    binary: bool = false,
    unsigned: bool = false,

    pub fn pg(oid: i32) Wire {
        return .{ .code = @bitCast(oid) };
    }

    /// `flags & 0x20` is UNSIGNED_FLAG and charset 63 is `binary`; each counts
    /// only on the column types whose reading it changes.
    pub fn mysql(mtype: u8, flags: u16, charset: u16) Wire {
        return .{
            .code = mtype,
            .unsigned = mtype == 0x08 and flags & 0x0020 != 0,
            .binary = mtype >= 0xfb and mtype <= 0xfe and charset == 63,
        };
    }

    pub fn tds(t: u8) Wire {
        return .{ .code = t };
    }

    pub fn eql(a: Wire, b: Wire) bool {
        return a.code == b.code and a.binary == b.binary and a.unsigned == b.unsigned;
    }
};

/// A type spelling: a fixed name, or `DECIMAL(p,s)` from the type's own precision and scale.
pub const Spell = union(enum) {
    name: []const u8,
    decimal,

    fn render(self: Spell, arena: std.mem.Allocator, ty: types.Type) ![]const u8 {
        return switch (self) {
            .name => |s| s,
            .decimal => try msgfmt.allocPrint(arena, "DECIMAL({d},{d})", .{ ty.precision, ty.scale }),
        };
    }
};

pub const Row = struct {
    kind: TypeKind,
    ddl: Spell,
    key: ?Spell = null,
    cast: ?Spell,
    echo: Wire,
    wire: []const Wire = &.{},
};

fn n(comptime s: []const u8) Spell {
    return .{ .name = s };
}

fn codes(comptime cs: []const u32) []const Wire {
    comptime var out: [cs.len]Wire = undefined;
    inline for (cs, 0..) |c, i| out[i] = .{ .code = c };
    const final = out;
    return &final;
}

const pg_rows = [_]Row{
    .{ .kind = .bool, .ddl = n("BOOLEAN"), .cast = null, .echo = .{ .code = 16 }, .wire = codes(&.{16}) },
    .{ .kind = .int, .ddl = n("BIGINT"), .cast = n("BIGINT"), .echo = .{ .code = 20 }, .wire = codes(&.{ 20, 21, 23 }) },
    .{ .kind = .float, .ddl = n("DOUBLE PRECISION"), .cast = n("DOUBLE PRECISION"), .echo = .{ .code = 701 }, .wire = codes(&.{ 700, 701 }) },
    .{ .kind = .decimal, .ddl = .decimal, .cast = .decimal, .echo = .{ .code = 1700 }, .wire = codes(&.{1700}) },
    .{ .kind = .string, .ddl = n("TEXT"), .key = n("VARCHAR(255)"), .cast = n("TEXT"), .echo = .{ .code = 25 } },
    .{ .kind = .bytes, .ddl = n("BYTEA"), .cast = null, .echo = .{ .code = 17 }, .wire = codes(&.{17}) },
    .{ .kind = .date, .ddl = n("DATE"), .cast = n("DATE"), .echo = .{ .code = 1082 }, .wire = codes(&.{1082}) },
    .{ .kind = .time, .ddl = n("TIME"), .cast = n("TIME"), .echo = .{ .code = 1083 }, .wire = codes(&.{1083}) },
    .{ .kind = .timestamp, .ddl = n("TIMESTAMP"), .cast = n("TIMESTAMP"), .echo = .{ .code = 1114 }, .wire = codes(&.{ 1114, 1184 }) },
    .{ .kind = .array, .ddl = n("TEXT"), .cast = null, .echo = .{ .code = 25 } },
    .{ .kind = .@"struct", .ddl = n("TEXT"), .cast = null, .echo = .{ .code = 25 } },
};

const my_int = codes(&.{ 0x08, 0x01, 0x02, 0x03, 0x09, 0x0d, 0x10 });
const my_float = codes(&.{ 0x04, 0x05 });
const my_decimal: []const Wire = &.{ .{ .code = 0x00 }, .{ .code = 0xf6 }, .{ .code = 0x08, .unsigned = true } };
const my_date = codes(&.{0x0a});
const my_timestamp = codes(&.{ 0x07, 0x0c });
const my_bytes: []const Wire = &.{
    .{ .code = 0xfb, .binary = true }, .{ .code = 0xfc, .binary = true },
    .{ .code = 0xfd, .binary = true }, .{ .code = 0xfe, .binary = true },
};

const mysql_rows = [_]Row{
    .{ .kind = .bool, .ddl = n("TINYINT(1)"), .cast = null, .echo = .{ .code = 0x01 } },
    .{ .kind = .int, .ddl = n("BIGINT"), .cast = n("SIGNED"), .echo = .{ .code = 0x08 }, .wire = my_int },
    .{ .kind = .float, .ddl = n("DOUBLE"), .cast = n("DOUBLE"), .echo = .{ .code = 0x05 }, .wire = my_float },
    .{ .kind = .decimal, .ddl = .decimal, .cast = .decimal, .echo = .{ .code = 0xf6 }, .wire = my_decimal },
    .{ .kind = .string, .ddl = n("VARCHAR(255)"), .cast = n("CHAR"), .echo = .{ .code = 0xfd } },
    .{ .kind = .bytes, .ddl = n("BLOB"), .cast = null, .echo = .{ .code = 0xfc, .binary = true }, .wire = my_bytes },
    .{ .kind = .date, .ddl = n("DATE"), .cast = n("DATE"), .echo = .{ .code = 0x0a }, .wire = my_date },
    .{ .kind = .time, .ddl = n("TIME"), .cast = n("TIME"), .echo = .{ .code = 0x0b } },
    .{ .kind = .timestamp, .ddl = n("DATETIME"), .cast = n("DATETIME"), .echo = .{ .code = 0x0c }, .wire = my_timestamp },
    .{ .kind = .array, .ddl = n("TEXT"), .cast = null, .echo = .{ .code = 0xfc } },
    .{ .kind = .@"struct", .ddl = n("TEXT"), .cast = null, .echo = .{ .code = 0xfc } },
};

const sqlserver_rows = [_]Row{
    .{ .kind = .bool, .ddl = n("BIT"), .cast = null, .echo = .{ .code = 0x68 }, .wire = codes(&.{ 0x32, 0x68 }) },
    .{ .kind = .int, .ddl = n("BIGINT"), .cast = n("BIGINT"), .echo = .{ .code = 0x26 }, .wire = codes(&.{ 0x38, 0x34, 0x30, 0x7F, 0x26 }) },
    .{ .kind = .float, .ddl = n("FLOAT"), .cast = n("FLOAT"), .echo = .{ .code = 0x6D }, .wire = codes(&.{ 0x3B, 0x3E, 0x6D }) },
    .{ .kind = .decimal, .ddl = .decimal, .cast = .decimal, .echo = .{ .code = 0x6A }, .wire = codes(&.{ 0x6E, 0x3C, 0x7A, 0x6A, 0x6C }) },
    .{ .kind = .string, .ddl = n("NVARCHAR(4000)"), .key = n("NVARCHAR(255)"), .cast = n("VARCHAR(MAX)"), .echo = .{ .code = 0xE7 }, .wire = codes(&.{ 0x24, 0xA7, 0xAF, 0xE7, 0xEF, 0xA5, 0xAD, 0xF1, 0x22, 0x23, 0x63 }) },
    .{ .kind = .bytes, .ddl = n("VARBINARY(MAX)"), .cast = null, .echo = .{ .code = 0xA5 } },
    .{ .kind = .date, .ddl = n("DATE"), .cast = n("DATE"), .echo = .{ .code = 0x28 }, .wire = codes(&.{0x28}) },
    .{ .kind = .time, .ddl = n("TIME"), .cast = n("TIME"), .echo = .{ .code = 0x29 }, .wire = codes(&.{0x29}) },
    .{ .kind = .timestamp, .ddl = n("DATETIME2"), .cast = n("DATETIME2"), .echo = .{ .code = 0x2A }, .wire = codes(&.{ 0x3D, 0x3A, 0x6F, 0x2A, 0x2B }) },
    .{ .kind = .array, .ddl = n("TEXT"), .cast = null, .echo = .{ .code = 0x23 } },
    .{ .kind = .@"struct", .ddl = n("TEXT"), .cast = null, .echo = .{ .code = 0x23 } },
};

const starrocks_rows = [_]Row{
    .{ .kind = .bool, .ddl = n("BOOLEAN"), .cast = null, .echo = .{ .code = 0x01 } },
    .{ .kind = .int, .ddl = n("BIGINT"), .cast = n("SIGNED"), .echo = .{ .code = 0x08 }, .wire = my_int },
    .{ .kind = .float, .ddl = n("DOUBLE"), .cast = n("DOUBLE"), .echo = .{ .code = 0x05 }, .wire = my_float },
    .{ .kind = .decimal, .ddl = .decimal, .cast = .decimal, .echo = .{ .code = 0xf6 }, .wire = my_decimal },
    .{ .kind = .string, .ddl = n("VARCHAR(65533)"), .cast = n("CHAR"), .echo = .{ .code = 0xfd } },
    .{ .kind = .bytes, .ddl = n("STRING"), .cast = null, .echo = .{ .code = 0xfd }, .wire = my_bytes },
    .{ .kind = .date, .ddl = n("DATE"), .cast = n("DATE"), .echo = .{ .code = 0x0a }, .wire = my_date },
    .{ .kind = .time, .ddl = n("VARCHAR(32)"), .cast = n("TIME"), .echo = .{ .code = 0xfd } },
    .{ .kind = .timestamp, .ddl = n("DATETIME"), .cast = n("DATETIME"), .echo = .{ .code = 0x0c }, .wire = my_timestamp },
    .{ .kind = .array, .ddl = n("STRING"), .cast = null, .echo = .{ .code = 0xfd } },
    .{ .kind = .@"struct", .ddl = n("JSON"), .cast = null, .echo = .{ .code = 0xf5 } },
};

const doris_rows = [_]Row{
    .{ .kind = .bool, .ddl = n("BOOLEAN"), .cast = null, .echo = .{ .code = 0x01 } },
    .{ .kind = .int, .ddl = n("BIGINT"), .cast = n("SIGNED"), .echo = .{ .code = 0x08 }, .wire = my_int },
    .{ .kind = .float, .ddl = n("DOUBLE"), .cast = n("DOUBLE"), .echo = .{ .code = 0x05 }, .wire = my_float },
    .{ .kind = .decimal, .ddl = .decimal, .cast = .decimal, .echo = .{ .code = 0xf6 }, .wire = my_decimal },
    .{ .kind = .string, .ddl = n("STRING"), .key = n("VARCHAR(65533)"), .cast = n("CHAR"), .echo = .{ .code = 0xfd } },
    .{ .kind = .bytes, .ddl = n("STRING"), .cast = null, .echo = .{ .code = 0xfd }, .wire = my_bytes },
    .{ .kind = .date, .ddl = n("DATE"), .cast = n("DATE"), .echo = .{ .code = 0x0a }, .wire = my_date },
    .{ .kind = .time, .ddl = n("VARCHAR(32)"), .cast = n("TIME"), .echo = .{ .code = 0xfd } },
    .{ .kind = .timestamp, .ddl = n("DATETIME(6)"), .cast = n("DATETIME"), .echo = .{ .code = 0x0c }, .wire = my_timestamp },
    .{ .kind = .array, .ddl = n("STRING"), .cast = null, .echo = .{ .code = 0xfd } },
    .{ .kind = .@"struct", .ddl = n("JSON"), .cast = null, .echo = .{ .code = 0xf5 } },
};

pub const rows = std.EnumArray(Dialect, []const Row).init(.{
    .postgres = &pg_rows,
    .mysql = &mysql_rows,
    .sqlserver = &sqlserver_rows,
    .starrocks = &starrocks_rows,
    .doris = &doris_rows,
});

/// The kinds each dialect's declared column reads back as something else.
pub const lossy = std.EnumArray(Dialect, []const TypeKind).init(.{
    .postgres = &.{ .array, .@"struct" },
    .mysql = &.{ .bool, .time, .array, .@"struct" },
    .sqlserver = &.{ .bytes, .array, .@"struct" },
    .starrocks = &.{ .bool, .bytes, .time, .array, .@"struct" },
    .doris = &.{ .bool, .bytes, .time, .array, .@"struct" },
});

/// The kind a wire code reads as when no row lists it: text on PostgreSQL and the
/// MySQL family, refused (null) on SQL Server.
fn fallback(d: Dialect) ?TypeKind {
    return switch (d) {
        .postgres, .mysql, .starrocks, .doris => .string,
        .sqlserver => null,
    };
}

const index = blk: {
    @setEvalBranchQuota(100_000);
    var out = std.EnumArray(Dialect, std.EnumArray(TypeKind, Row)).initUndefined();
    for (std.enums.values(Dialect)) |d| {
        var seen = std.EnumSet(TypeKind).initEmpty();
        var by_kind = std.EnumArray(TypeKind, Row).initUndefined();
        const rs = rows.get(d);
        for (rs, 0..) |r, i| {
            if (seen.contains(r.kind)) @compileError("typemap: kind " ++ @tagName(r.kind) ++ " listed twice for " ++ @tagName(d));
            seen.insert(r.kind);
            by_kind.set(r.kind, r);
            for (r.wire) |w| for (rs[i + 1 ..]) |r2| for (r2.wire) |w2| if (w.eql(w2))
                @compileError("typemap: a wire code is listed for two kinds on " ++ @tagName(d));
        }
        if (seen.count() != std.enums.values(TypeKind).len) @compileError("typemap: " ++ @tagName(d) ++ " misses a kind");
        out.set(d, by_kind);
    }
    break :blk out;
};

pub fn row(d: Dialect, k: TypeKind) Row {
    return index.get(d).get(k);
}

/// The column type a CREATE TABLE gets for an engine type. `is_key` shrinks
/// unbounded text to something indexable where the database demands it.
pub fn ddlType(d: Dialect, arena: std.mem.Allocator, ty: types.Type, is_key: bool) ![]const u8 {
    const r = row(d, ty.kind);
    const s = if (is_key) r.key orelse r.ddl else r.ddl;
    return s.render(arena, ty);
}

/// The type a pushed-down `CAST` uses, or null when the predicate stays in the engine.
pub fn castType(d: Dialect, arena: std.mem.Allocator, ty: types.Type) !?[]const u8 {
    const s = row(d, ty.kind).cast orelse return null;
    return try s.render(arena, ty);
}

/// The kind the driver reads a column reported as `w`, or null when SQL Server's
/// reader refuses the type. Precision and scale stay with the driver.
pub fn readKind(d: Dialect, w: Wire) ?TypeKind {
    for (rows.get(d)) |r| for (r.wire) |x| if (x.eql(w)) return r.kind;
    return fallback(d);
}

pub fn isLossy(d: Dialect, k: TypeKind) bool {
    return std.mem.indexOfScalar(TypeKind, lossy.get(d), k) != null;
}

/// The column type SQL Server's INSERT BULK declares for every column.
pub const sqlserver_bulk_column = "nvarchar(4000)";

const testing = std.testing;

/// sql.zig:428 `Dialect.ddlType` at 7f890a1, verbatim but for the dialect parameter.
fn pinnedDdl(self: Dialect, arena: std.mem.Allocator, ty: types.Type, is_key: bool) ![]const u8 {
    return switch (ty.kind) {
        .bool => switch (self) {
            .sqlserver => "BIT",
            .mysql => "TINYINT(1)",
            .postgres, .starrocks, .doris => "BOOLEAN",
        },
        .int => "BIGINT",
        .float => switch (self) {
            .postgres => "DOUBLE PRECISION",
            .mysql, .starrocks, .doris => "DOUBLE",
            .sqlserver => "FLOAT",
        },
        .decimal => try msgfmt.allocPrint(arena, "DECIMAL({d},{d})", .{ ty.precision, ty.scale }),
        .string => switch (self) {
            .postgres => if (is_key) "VARCHAR(255)" else "TEXT",
            .doris => if (is_key) "VARCHAR(65533)" else "STRING",
            .mysql => "VARCHAR(255)",
            .sqlserver => if (is_key) "NVARCHAR(255)" else "NVARCHAR(4000)",
            .starrocks => "VARCHAR(65533)",
        },
        .bytes => switch (self) {
            .postgres => "BYTEA",
            .mysql => "BLOB",
            .sqlserver => "VARBINARY(MAX)",
            .starrocks, .doris => "STRING",
        },
        .date => "DATE",
        .time => switch (self) {
            .postgres, .mysql, .sqlserver => "TIME",
            .starrocks, .doris => "VARCHAR(32)",
        },
        .timestamp => switch (self) {
            .postgres => "TIMESTAMP",
            .mysql, .starrocks => "DATETIME",
            .doris => "DATETIME(6)",
            .sqlserver => "DATETIME2",
        },
        .array => switch (self) {
            .postgres, .mysql, .sqlserver => "TEXT",
            .starrocks, .doris => "STRING",
        },
        .@"struct" => switch (self) {
            .postgres, .mysql, .sqlserver => "TEXT",
            .starrocks, .doris => "JSON",
        },
    };
}

/// sql.zig:480 `Dialect.castType` at 7f890a1, verbatim but for the dialect parameter.
fn pinnedCast(self: Dialect, arena: std.mem.Allocator, ty: types.Type) !?[]const u8 {
    return switch (ty.kind) {
        .int => switch (self) {
            .postgres, .sqlserver => "BIGINT",
            .mysql, .starrocks, .doris => "SIGNED",
        },
        .float => switch (self) {
            .postgres => "DOUBLE PRECISION",
            .mysql, .starrocks, .doris => "DOUBLE",
            .sqlserver => "FLOAT",
        },
        .decimal => try msgfmt.allocPrint(arena, "DECIMAL({d},{d})", .{ ty.precision, ty.scale }),
        .string => switch (self) {
            .postgres => "TEXT",
            .mysql, .starrocks, .doris => "CHAR",
            .sqlserver => "VARCHAR(MAX)",
        },
        .date => "DATE",
        .time => "TIME",
        .timestamp => switch (self) {
            .postgres => "TIMESTAMP",
            .mysql, .starrocks, .doris => "DATETIME",
            .sqlserver => "DATETIME2",
        },
        .bool, .bytes, .array, .@"struct" => null,
    };
}

/// postgres.zig:413 `pgType` at 7f890a1, kind only, with D9's `bytea` and `time`.
fn pinnedPg(oid: i32) TypeKind {
    return switch (oid) {
        16 => .bool,
        17 => .bytes,
        1083 => .time,
        20, 21, 23 => .int,
        700, 701 => .float,
        1700 => .decimal,
        1082 => .date,
        1114, 1184 => .timestamp,
        else => .string,
    };
}

/// mysql.zig:627 `engineTypeFor` at 7f890a1, kind only.
fn pinnedMysql(mtype: u8, flags: u16, charset: u16) TypeKind {
    const unsigned = flags & 0x0020 != 0;
    const binary = charset == 63;
    return switch (mtype) {
        0x08 => if (unsigned) .decimal else .int,
        0x01, 0x02, 0x03, 0x09, 0x0d => .int,
        0x04, 0x05 => .float,
        0x00, 0xf6 => .decimal,
        0x0a => .date,
        0x07, 0x0c => .timestamp,
        0x10 => .int,
        0xfb, 0xfc, 0xfd, 0xfe => if (binary) .bytes else .string,
        else => .string,
    };
}

/// tds.zig:836 `parseTypeInfo` at 7f890a1, kind only; null is UnsupportedTdsType.
fn pinnedTds(t: u8) ?TypeKind {
    return switch (t) {
        0x38, 0x34, 0x30, 0x7F, 0x26 => .int,
        0x32, 0x68 => .bool,
        0x3B, 0x3E, 0x6D => .float,
        0x3D, 0x3A, 0x6F, 0x2A, 0x2B => .timestamp,
        0x6E, 0x3C, 0x7A, 0x6A, 0x6C => .decimal,
        0x28 => .date,
        0x29 => .time,
        0x24, 0xA7, 0xAF, 0xE7, 0xEF, 0xA5, 0xAD, 0xF1, 0x22, 0x23, 0x63 => .string,
        else => null,
    };
}

fn sampleTypes() [15]types.Type {
    var out: [15]types.Type = undefined;
    var i: usize = 0;
    for (std.enums.values(TypeKind)) |k| {
        out[i] = types.Type.init(k);
        i += 1;
    }
    out[i] = types.Type.decimal(10, 2);
    out[i + 1] = types.Type.decimal(38, 6);
    out[i + 2] = types.Type.decimal(1, 0);
    out[i + 3] = types.Type.decimal(18, 18).asNullable();
    return out;
}

test "ddlType and castType equal sql.zig's for every dialect, kind and key flag" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (std.enums.values(Dialect)) |d| {
        for (sampleTypes()) |ty| {
            for ([_]bool{ false, true }) |is_key| {
                try testing.expectEqualStrings(try pinnedDdl(d, a, ty, is_key), try ddlType(d, a, ty, is_key));
            }
            const want = try pinnedCast(d, a, ty);
            const got = try castType(d, a, ty);
            try testing.expectEqual(want == null, got == null);
            if (want) |w| try testing.expectEqualStrings(w, got.?);
        }
    }
}

test "ddlType spells a decimal from the type's own precision and scale" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (std.enums.values(Dialect)) |d| {
        try testing.expectEqualStrings("DECIMAL(10,2)", try ddlType(d, a, types.Type.decimal(10, 2), false));
        try testing.expectEqualStrings("DECIMAL(10,2)", try ddlType(d, a, types.Type.decimal(10, 2), true));
        try testing.expectEqualStrings("DECIMAL(38,0)", (try castType(d, a, types.Type.decimal(38, 0))).?);
    }
}

test "key spellings shrink text only where the dialect demands it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const str = types.Type.init(.string);
    try testing.expectEqualStrings("VARCHAR(255)", try ddlType(.postgres, a, str, true));
    try testing.expectEqualStrings("TEXT", try ddlType(.postgres, a, str, false));
    try testing.expectEqualStrings("NVARCHAR(255)", try ddlType(.sqlserver, a, str, true));
    try testing.expectEqualStrings("NVARCHAR(4000)", try ddlType(.sqlserver, a, str, false));
    try testing.expectEqualStrings("VARCHAR(65533)", try ddlType(.doris, a, str, true));
    try testing.expectEqualStrings("STRING", try ddlType(.doris, a, str, false));
    try testing.expectEqualStrings("VARCHAR(255)", try ddlType(.mysql, a, str, true));
    try testing.expectEqualStrings("VARCHAR(65533)", try ddlType(.starrocks, a, str, true));
    for (std.enums.values(Dialect)) |d| {
        for (std.enums.values(TypeKind)) |k| {
            if (k == .string) continue;
            try testing.expect(row(d, k).key == null);
        }
    }
}

test "readKind equals pgType's kind for every OID" {
    var oid: i32 = -2;
    while (oid < 70_000) : (oid += 1) {
        try testing.expectEqual(@as(?TypeKind, pinnedPg(oid)), readKind(.postgres, .pg(oid)));
    }
    for ([_]i32{ std.math.minInt(i32), std.math.maxInt(i32), -1 }) |o| {
        try testing.expectEqual(@as(?TypeKind, pinnedPg(o)), readKind(.postgres, .pg(o)));
    }
}

test "readKind equals engineTypeFor's kind for every column type, flag and charset" {
    const flag_set = [_]u16{ 0, 0x0020, 0x0001, 0x0021, 0xFFFF, 0xFFDF };
    const charsets = [_]u16{ 0, 8, 33, 45, 63, 255, 0xFFFF };
    for ([_]Dialect{ .mysql, .starrocks, .doris }) |d| {
        for (0..256) |m| {
            const mtype: u8 = @intCast(m);
            for (flag_set) |f| for (charsets) |c| {
                try testing.expectEqual(@as(?TypeKind, pinnedMysql(mtype, f, c)), readKind(d, .mysql(mtype, f, c)));
            };
        }
    }
}

test "readKind equals parseTypeInfo's kind for every TDS type byte" {
    for (0..256) |t| {
        const b: u8 = @intCast(t);
        try testing.expectEqual(pinnedTds(b), readKind(.sqlserver, .tds(b)));
    }
}

test "a declared column reads back as its kind or is listed lossy" {
    for (std.enums.values(Dialect)) |d| {
        for (std.enums.values(TypeKind)) |k| {
            const r = row(d, k);
            const back = readKind(d, r.echo);
            try testing.expect(back != null);
            try testing.expectEqual(back.? != k, isLossy(d, k));
        }
    }
}

test "lossy pairs read back as the kind each driver produces today" {
    const Case = struct { d: Dialect, k: TypeKind, back: TypeKind };
    const cases = [_]Case{
        .{ .d = .mysql, .k = .bool, .back = .int },
        .{ .d = .mysql, .k = .time, .back = .string },
        .{ .d = .sqlserver, .k = .bytes, .back = .string },
        .{ .d = .starrocks, .k = .bool, .back = .int },
        .{ .d = .starrocks, .k = .bytes, .back = .string },
        .{ .d = .doris, .k = .time, .back = .string },
    };
    for (cases) |c| try testing.expectEqual(@as(?TypeKind, c.back), readKind(c.d, row(c.d, c.k).echo));
    for (std.enums.values(Dialect)) |d| {
        try testing.expectEqual(@as(?TypeKind, .string), readKind(d, row(d, .array).echo));
        try testing.expectEqual(@as(?TypeKind, .string), readKind(d, row(d, .@"struct").echo));
    }
}

test "a PostgreSQL bytea and time read back as their kinds; a MySQL-protocol TIME stays text" {
    try testing.expectEqual(@as(?TypeKind, .bytes), readKind(.postgres, .pg(17)));
    try testing.expectEqual(@as(?TypeKind, .time), readKind(.postgres, .pg(1083)));
    try testing.expectEqual(@as(?TypeKind, .string), readKind(.postgres, .pg(1266)));
    for ([_]TypeKind{ .bytes, .time }) |k| {
        try testing.expect(!isLossy(.postgres, k));
        try testing.expectEqual(@as(?TypeKind, k), readKind(.postgres, row(.postgres, k).echo));
    }
    for ([_]Dialect{ .mysql, .starrocks, .doris }) |d| {
        try testing.expectEqual(@as(?TypeKind, .string), readKind(d, .mysql(0x0b, 0, 63)));
        try testing.expect(isLossy(d, .time));
    }
}

test "Wire.mysql folds UNSIGNED and binary only where the reader uses them" {
    try testing.expect(Wire.mysql(0x08, 0x20, 0).unsigned);
    try testing.expect(!Wire.mysql(0x03, 0x20, 0).unsigned);
    try testing.expect(Wire.mysql(0xfc, 0, 63).binary);
    try testing.expect(!Wire.mysql(0x0f, 0, 63).binary);
    try testing.expect(!Wire.mysql(0xfd, 0, 33).binary);
    try testing.expectEqual(@as(?TypeKind, .decimal), readKind(.mysql, .mysql(0x08, 0x20, 0)));
    try testing.expectEqual(@as(?TypeKind, .int), readKind(.mysql, .mysql(0x08, 0, 63)));
    try testing.expectEqual(@as(?TypeKind, .bytes), readKind(.mysql, .mysql(0xfd, 0, 63)));
    try testing.expectEqual(@as(?TypeKind, .string), readKind(.mysql, .mysql(0x0f, 0, 63)));
}

test "dialectOf maps sql.Dialect tag for tag" {
    const sql = @import("sql.zig");
    try testing.expectEqual(std.enums.values(sql.Dialect).len, std.enums.values(Dialect).len);
    for (std.enums.values(sql.Dialect)) |d| {
        try testing.expectEqualStrings(@tagName(d), @tagName(dialectOf(d)));
    }
}

test "the INSERT BULK column type is tds.zig's" {
    try testing.expectEqualStrings("nvarchar(4000)", sqlserver_bulk_column);
}

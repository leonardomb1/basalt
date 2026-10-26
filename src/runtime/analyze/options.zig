//! Connection and sink options judged without connecting, the one rule `check` and the
//! run both ask before anything executes: the keys an SFTP, SMB or FTP connection
//! takes, the literal values the run would refuse, and what a StarRocks or Doris load
//! needs. A value computed by `env()` or `secret()` is left to the run, which words
//! its problem with the same helpers.

const Diag = @import("../analyze.zig").Diag;
const analyze = @import("../analyze.zig");
const Error = @import("../analyze.zig").Error;
const ast = @import("../../lang/ast.zig");
const env_mod = @import("../env.zig");
const fail = @import("../analyze.zig").fail;
const registry = @import("../../connect/registry.zig");
const schema = @import("../connect/options_schema.zig");
const http_client = @import("../../net/http_client.zig");
const httpAttrUsed = @import("../connect.zig").httpAttrUsed;
const sql = @import("../../db/sql.zig");
const textconv = @import("../../exec/textconv.zig");
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const streamload = @import("../../db/streamload.zig");

const sftp_keys = [_][]const u8{ "host", "port", "user", "password", "key_file", "key_passphrase", "known_hosts", "host_key" };
const smb_keys = [_][]const u8{ "host", "port", "user", "password", "domain", "share", "auth", "realm", "kdc", "spn" };
const ftp_keys = [_][]const u8{ "host", "port", "user", "password" };

const stream_int_keys = [_][]const u8{ "port", "fe_port", "buckets", "replication_num" };
const stream_str_keys = [_][]const u8{ "host", "fe_host", "be_url", "load_url", "database", "user", "password", "label_prefix", "load_size", "compression" };

pub const int_cfg_msg = "invalid integer config value";
pub const int_cfg_kind_msg = "config value must be an integer";
pub const bool_cfg_msg = "config value must be a bool";
pub const literal_cfg_msg = "config value must be a literal or env()/secret()";

const sql_keys = [_][]const u8{ "host", "port", "database", "user", "password", "tls" };
const sqlserver_keys = sql_keys ++ [_][]const u8{ "auth", "domain", "realm", "kdc", "spn", "client_id", "resource", "token" };
const stream_keys = sql_keys ++ [_][]const u8{ "fe_host", "fe_port", "be_url", "load_url", "buckets", "replication_num", "auto_create", "label_prefix", "load_size", "compression" };

fn fileKeys(connector: []const u8) ?[]const []const u8 {
    if (std.mem.eql(u8, connector, "sftp")) return &sftp_keys;
    if (std.mem.eql(u8, connector, "smb")) return &smb_keys;
    if (std.mem.eql(u8, connector, "ftp")) return &ftp_keys;
    return null;
}

/// The options a connector reads: a file connection's, or a database's as
/// `parseDbConfig` and `resolveStreamLoadConfig` read them.
fn connectionKeys(connector: []const u8) ?[]const []const u8 {
    if (fileKeys(connector)) |keys| return keys;
    const conn = registry.Connector.parse(connector) orelse return null;
    if (conn.sqlRead() == null) return null;
    if (conn.streamLoad()) return &stream_keys;
    if (std.mem.eql(u8, connector, "sqlserver")) return &sqlserver_keys;
    return &sql_keys;
}

pub fn unknownOptionMsg(arena: std.mem.Allocator, c: ast.Connection, key: []const u8) Error![]const u8 {
    const keys = connectionKeys(c.connector) orelse &.{};
    return msgfmt.allocPrint(arena, "{s} connection `{s}`: unknown option `{s}` ({s})", .{ c.connector, c.name, key, try std.mem.join(arena, ", ", keys) });
}

pub fn portRangeMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return rangeMsg(arena, connector, "port");
}

pub fn rangeMsg(arena: std.mem.Allocator, connector: []const u8, key: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "{s} connection `{s}` is out of range", .{ connector, key });
}

pub fn needsHostMsg(arena: std.mem.Allocator, c: ast.Connection) Error![]const u8 {
    return msgfmt.allocPrint(arena, "{s} connection `{s}` needs a `host`", .{ c.connector, c.name });
}

pub fn smbAuthMsg(arena: std.mem.Allocator, name: []const u8, auth: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "smb connection `{s}`: `auth` is kerberos or ntlm, not `{s}`", .{ name, auth });
}

pub fn smbRealmMsg(arena: std.mem.Allocator, name: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "smb connection `{s}`: Kerberos needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`", .{name});
}

pub fn smbAuthValid(auth: []const u8) bool {
    return std.ascii.eqlIgnoreCase(auth, "kerberos") or std.ascii.eqlIgnoreCase(auth, "ntlm");
}

pub fn loadSizeMsg(arena: std.mem.Allocator, text: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "`load_size` must be a positive size like '128MB', not '{s}'", .{text});
}

pub fn loadCompressionMsg(arena: std.mem.Allocator, text: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "`compression` must be 'none' or 'gzip', not '{s}'", .{text});
}

pub fn needsDatabaseMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "{s} connection needs a `database`", .{connector});
}

pub fn unsupportedSinkMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "unsupported sink connector `{s}`", .{connector});
}

pub fn unsupportedSourceMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "unsupported source connector `{s}`", .{connector});
}

pub fn unknownConnectionMsg(arena: std.mem.Allocator, name: []const u8, where: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "unknown connection `{s}` in {s}", .{ name, where });
}

/// The text a literal option value reads as, or null for one only the run evaluates.
fn literalText(arena: std.mem.Allocator, e: *const ast.Expr) Error!?[]const u8 {
    return switch (e.*) {
        .str_lit => |s| s,
        .int_lit => |i| try msgfmt.allocPrint(arena, "{d}", .{i}),
        .bool_lit => |b| if (b) "true" else "false",
        else => null,
    };
}

fn isEnvCall(e: *const ast.Expr) bool {
    if (e.* != .call) return false;
    const c = e.call;
    return (std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and c.args.len == 1 and c.args[0].* == .str_lit;
}

/// Why the run would refuse a connection's declaration before any statement runs:
/// an unknown connector type, a SQL connection's literal `tls` and `auth`, an SFTP,
/// SMB or FTP connection's keys, port and literal values. Null when it would not.
pub fn connectionProblem(arena: std.mem.Allocator, c: ast.Connection) Error!?[]const u8 {
    if (!analyze.isConnectionType(c.connector))
        return try msgfmt.allocPrint(arena, "unknown connection type `{s}` (one of {s})", .{ c.connector, analyze.connection_types });
    const conn = registry.Connector.parse(c.connector).?;
    if (conn.sqlRead() != null) {
        for (c.config) |attr| {
            if (!containsKey(connectionKeys(c.connector).?, attr.key)) return try unknownOptionMsg(arena, c, attr.key);
            if (!std.mem.eql(u8, attr.key, "port") and !std.mem.eql(u8, attr.key, "fe_port")) continue;
            switch (schema.cfgIntIn(u16, attr.value)) {
                .ok => {},
                .problem => |p| return p.fixedMsg() orelse try rangeMsg(arena, c.connector, attr.key),
            }
        }
        if (sqlOptionProblem(c)) |why| return why;
        return try sqlLiteralProblem(arena, c);
    }
    if (conn == .http) return httpProblem(arena, c);
    const keys = fileKeys(c.connector) orelse return null;
    var host: ?[]const u8 = null;
    var host_known = false;
    var kerberos = false;
    var realm = false;
    var user_at: ?bool = false;
    for (c.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            switch (schema.cfgIntIn(u16, attr.value)) {
                .ok => {},
                .problem => |p| return p.fixedMsg() orelse try portRangeMsg(arena, c.connector),
            }
            continue;
        }
        const text = try literalText(arena, attr.value);
        if (text == null and !isEnvCall(attr.value)) return literal_cfg_msg;
        for (keys) |known| {
            if (std.mem.eql(u8, k, known)) break;
        } else return try unknownOptionMsg(arena, c, k);
        if (std.mem.eql(u8, k, "host")) {
            host = text;
            host_known = true;
        } else if (std.mem.eql(u8, k, "user")) {
            user_at = if (text) |u| std.mem.indexOfScalar(u8, u, '@') != null else null;
        } else if (std.mem.eql(u8, k, "realm")) {
            realm = true;
        } else if (std.mem.eql(u8, k, "auth")) {
            const a = text orelse continue;
            if (!smbAuthValid(a)) return try smbAuthMsg(arena, c.name, a);
            kerberos = std.ascii.eqlIgnoreCase(a, "kerberos");
        }
    }
    if (!host_known or (host != null and host.?.len == 0)) return try needsHostMsg(arena, c);
    if (kerberos and !realm and user_at == false) return try smbRealmMsg(arena, c.name);
    return null;
}

/// A SQL connection's literal `tls` and `auth`, judged as the run judges them at
/// connect time; a value computed by an expression is left to the run.
pub fn sqlOptionProblem(c: ast.Connection) ?[]const u8 {
    const conn = registry.Connector.parse(c.connector) orelse return null;
    if (conn.sqlRead() == null) return null;
    var tls: ?[]const u8 = null;
    var auth: ?[]const u8 = null;
    for (c.config) |a| if (a.value.* == .str_lit) {
        if (std.mem.eql(u8, a.key, "tls")) tls = a.value.str_lit;
        if (std.mem.eql(u8, a.key, "auth")) auth = a.value.str_lit;
    };
    if (tls) |v| if (std.meta.stringToEnum(sql.TlsMode, v) == null) return env_mod.tls_values_msg;
    if (auth) |v| if (std.meta.stringToEnum(env_mod.DbAuth, v) == null) return env_mod.auth_values_msg;
    if (auth != null and tls != null and std.mem.eql(u8, auth.?, "ntlm") and std.mem.eql(u8, tls.?, "off"))
        return env_mod.ntlm_needs_tls_msg;
    return null;
}

pub fn httpConnMsg(arena: std.mem.Allocator, name: []const u8, why: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "http connection `{s}`: {s}", .{ name, why });
}

/// An http connection whose options are all literals, judged by the run's own
/// `connFromKvs`; one reading `env()` or `secret()` is left to the run.
fn httpProblem(arena: std.mem.Allocator, c: ast.Connection) Error!?[]const u8 {
    var auth: []const u8 = "";
    for (c.config) |a| if (std.mem.eql(u8, a.key, "auth")) {
        auth = (try literalText(arena, a.value)) orelse return null;
    };
    var kvs = std.array_list.Managed(http_client.KV).init(arena);
    for (c.config) |a| if (httpAttrUsed(c, auth, a.key))
        try kvs.append(.{ .key = a.key, .value = (try literalText(arena, a.value)) orelse return null });
    var why: []const u8 = "";
    _ = http_client.connFromKvs(arena, kvs.items, &why) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory;
        return try httpConnMsg(arena, c.name, why);
    };
    return null;
}

/// A database connection's `host` and Kerberos `realm` as `resolveDbConfig` demands
/// them: no host option at all, or Kerberos with neither a realm nor `me@REALM`.
fn sqlLiteralProblem(arena: std.mem.Allocator, c: ast.Connection) Error!?[]const u8 {
    var host = false;
    var kerberos = false;
    var realm = false;
    var plain_user = false;
    for (c.config) |a| {
        if (std.mem.eql(u8, a.key, "host") or std.mem.eql(u8, a.key, "fe_host"))
            host = a.value.* != .str_lit or a.value.str_lit.len > 0;
        if (std.mem.eql(u8, a.key, "realm")) realm = true;
        if (a.value.* != .str_lit) continue;
        if (std.mem.eql(u8, a.key, "auth")) kerberos = std.mem.eql(u8, a.value.str_lit, "kerberos");
        if (std.mem.eql(u8, a.key, "user")) plain_user = std.mem.indexOfScalar(u8, a.value.str_lit, '@') == null;
    }
    if (!host) return try needsHostMsg(arena, c);
    if (kerberos and !realm and plain_user) return try sqlRealmMsg(arena, c.name);
    return null;
}

pub fn sqlRealmMsg(arena: std.mem.Allocator, name: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "connection `{s}`: `auth = 'kerberos'` needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`", .{name});
}

/// What a `LOAD INTO conn.table` is refused for by the connection it names, before
/// it opens: a connector that takes no load, and a StarRocks or Doris connection's
/// literal options as `resolveStreamLoadConfig` reads them.
pub fn checkSinkConnection(arena: std.mem.Allocator, conn: ast.Connection, diag: *Diag) Error!void {
    const flavor = streamload.Flavor.of(conn.connector) orelse {
        const c = registry.Connector.parse(conn.connector);
        if (c == null or c.?.sqlKind() == null) return fail(diag, "{s}", .{try unsupportedSinkMsg(arena, conn.connector)});
        return;
    };
    var database = false;
    for (conn.config) |attr| {
        const k = attr.key;
        if (containsKey(&stream_int_keys, k)) {
            const problem: ?schema.Problem = if (std.mem.endsWith(u8, k, "port")) switch (schema.cfgIntIn(u16, attr.value)) {
                .ok => null,
                .problem => |p| p,
            } else switch (schema.cfgIntIn(u32, attr.value)) {
                .ok => null,
                .problem => |p| p,
            };
            if (problem) |p| return fail(diag, "{s}", .{p.fixedMsg() orelse try rangeMsg(arena, conn.connector, k)});
            continue;
        }
        if (std.mem.eql(u8, k, "auto_create")) {
            if (schema.cfgBool(attr.value) == .problem) return fail(diag, "{s}", .{bool_cfg_msg});
            continue;
        }
        if (!containsKey(&stream_str_keys, k)) continue;
        const text = try literalText(arena, attr.value);
        if (text == null and !isEnvCall(attr.value)) return fail(diag, "{s}", .{literal_cfg_msg});
        if (std.mem.eql(u8, k, "database")) database = text == null or text.?.len > 0;
        const t = text orelse continue;
        if (std.mem.eql(u8, k, "load_size") and (env_mod.parseByteSize(t) orelse 0) == 0)
            return fail(diag, "{s}", .{try loadSizeMsg(arena, t)});
        if (std.mem.eql(u8, k, "compression") and std.meta.stringToEnum(streamload.Compression, t) == null)
            return fail(diag, "{s}", .{try loadCompressionMsg(arena, t)});
    }
    if (!database) return fail(diag, "{s}", .{try needsDatabaseMsg(arena, flavor.name())});
}

pub const BuildCap = union(enum) { bytes: usize, problem: []const u8 };

/// A join's `max_build` as the run caps its build side: a positive byte count, or a
/// size like '512MB'.
pub fn maxBuild(arena: std.mem.Allocator, h: ast.Hint) Error!BuildCap {
    const txt: []const u8 = switch (h.value) {
        .str, .ident => |v| v,
        .int => |v| return if (v > 0) .{ .bytes = @intCast(v) } else .{ .problem = "max_build must be positive" },
        .flag => "",
    };
    if (env_mod.parseByteSize(txt)) |v| if (std.math.cast(usize, v)) |cap| return .{ .bytes = cap };
    return .{ .problem = try msgfmt.allocPrint(arena, "bad max_build `{s}` — use e.g. '512MB' or '8GB'", .{txt}) };
}

pub const bare_upsert_source_msg = "a bare `UPSERT` infers the primary key from the source, which needs a SQL table read — this query's source can't be introspected; name the key with `UPSERT ON (col)`";
pub const bare_upsert_query_msg = "a bare `UPSERT` needs a table read (`FROM conn.schema.table`) to infer its key; a `QUERY(...)` has no single table to introspect — name the key with `UPSERT ON (col)`";

/// A bare `UPSERT` (no `ON (...)`) takes its key from the table it reads, so one
/// over a source that is no SQL table, and joins nothing that could be, is refused
/// before it runs; the rest is decided once the source opens.
pub fn checkBareUpsert(stages: []const ast.Stage, connections: *const std.StringHashMap(ast.Connection), diag: *Diag) Error!void {
    const w = stages[stages.len - 1].node.write;
    if (w.mode != .upsert or w.mode.upsert.keys.len > 0) return;
    if (stages[0].node != .read) return;
    const rd = stages[0].node.read;
    const sql_read = if (connections.get(rd.connector)) |c|
        (if (registry.Connector.parse(c.connector)) |k| k.sqlRead() != null else false)
    else
        false;
    if (sql_read) {
        if (rd.form == .query) return fail(diag, "{s}", .{bare_upsert_query_msg});
        return;
    }
    for (stages) |st| if (st.node == .join) return;
    return fail(diag, "{s}", .{bare_upsert_source_msg});
}

fn containsKey(keys: []const []const u8, k: []const u8) bool {
    for (keys) |x| if (std.mem.eql(u8, x, k)) return true;
    return false;
}

test "check refuses a SQL connection's out-of-range port and a stream load's non-bool auto_create" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const pos = ast.Pos{ .line = 0, .col = 0 };
    var host = ast.Expr{ .str_lit = "h" };
    var db = ast.Expr{ .str_lit = "d" };
    for ([_]ast.Expr{ .{ .int_lit = 70000 }, .{ .int_lit = -1 }, .{ .str_lit = "65536" } }) |p| {
        var v = p;
        const c = ast.Connection{ .name = "pg", .connector = "postgres", .pos = pos, .config = &.{
            .{ .key = "host", .value = &host, .pos = pos },
            .{ .key = "port", .value = &v, .pos = pos },
        } };
        try std.testing.expectEqualStrings("postgres connection `port` is out of range", (try connectionProblem(a, c)).?);
    }
    var bad = ast.Expr{ .str_lit = "x" };
    const c = ast.Connection{ .name = "pg", .connector = "mysql", .pos = pos, .config = &.{
        .{ .key = "host", .value = &host, .pos = pos },
        .{ .key = "port", .value = &bad, .pos = pos },
    } };
    try std.testing.expectEqualStrings(int_cfg_msg, (try connectionProblem(a, c)).?);

    var yes = ast.Expr{ .str_lit = "yes" };
    var upper = ast.Expr{ .str_lit = "TRUE" };
    var diag = Diag{};
    const refused = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "database", .value = &db, .pos = pos },
        .{ .key = "auto_create", .value = &yes, .pos = pos },
    } };
    try std.testing.expectError(error.AnalyzeFailed, checkSinkConnection(a, refused, &diag));
    try std.testing.expectEqualStrings(bool_cfg_msg, diag.msg);
    const taken = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "database", .value = &db, .pos = pos },
        .{ .key = "auto_create", .value = &upper, .pos = pos },
    } };
    try checkSinkConnection(a, taken, &diag);
}

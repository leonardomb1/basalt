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
const sql = @import("../../db/sql.zig");
const std = @import("std");
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

fn fileKeys(connector: []const u8) ?[]const []const u8 {
    if (std.mem.eql(u8, connector, "sftp")) return &sftp_keys;
    if (std.mem.eql(u8, connector, "smb")) return &smb_keys;
    if (std.mem.eql(u8, connector, "ftp")) return &ftp_keys;
    return null;
}

pub fn unknownOptionMsg(arena: std.mem.Allocator, c: ast.Connection, key: []const u8) Error![]const u8 {
    const keys = fileKeys(c.connector) orelse &.{};
    return std.fmt.allocPrint(arena, "{s} connection `{s}`: unknown option `{s}` ({s})", .{ c.connector, c.name, key, try std.mem.join(arena, ", ", keys) });
}

pub fn portRangeMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "{s} connection `port` is out of range", .{connector});
}

pub fn needsHostMsg(arena: std.mem.Allocator, c: ast.Connection) Error![]const u8 {
    return std.fmt.allocPrint(arena, "{s} connection `{s}` needs a `host`", .{ c.connector, c.name });
}

pub fn smbAuthMsg(arena: std.mem.Allocator, name: []const u8, auth: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "smb connection `{s}`: `auth` is kerberos or ntlm, not `{s}`", .{ name, auth });
}

pub fn smbRealmMsg(arena: std.mem.Allocator, name: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "smb connection `{s}`: Kerberos needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`", .{name});
}

pub fn smbAuthValid(auth: []const u8) bool {
    return std.ascii.eqlIgnoreCase(auth, "kerberos") or std.ascii.eqlIgnoreCase(auth, "ntlm");
}

pub fn loadSizeMsg(arena: std.mem.Allocator, text: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "`load_size` must be a positive size like '128MB', not '{s}'", .{text});
}

pub fn loadCompressionMsg(arena: std.mem.Allocator, text: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "`compression` must be 'none' or 'gzip', not '{s}'", .{text});
}

pub fn needsDatabaseMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "{s} connection needs a `database`", .{connector});
}

pub fn unsupportedSinkMsg(arena: std.mem.Allocator, connector: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "unsupported sink connector `{s}`", .{connector});
}

pub fn unknownConnectionMsg(arena: std.mem.Allocator, name: []const u8, where: []const u8) Error![]const u8 {
    return std.fmt.allocPrint(arena, "unknown connection `{s}` in {s}", .{ name, where });
}

/// The text a literal option value reads as, or null for one only the run evaluates.
fn literalText(arena: std.mem.Allocator, e: *const ast.Expr) Error!?[]const u8 {
    return switch (e.*) {
        .str_lit => |s| s,
        .int_lit => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
        .bool_lit => |b| if (b) "true" else "false",
        else => null,
    };
}

fn isEnvCall(e: *const ast.Expr) bool {
    if (e.* != .call) return false;
    const c = e.call;
    return (std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and c.args.len == 1 and c.args[0].* == .str_lit;
}

/// An integer option as the run's `evalCfgInt` reads it, its refusal or null.
fn intProblem(e: *const ast.Expr) ?[]const u8 {
    return switch (e.*) {
        .int_lit => null,
        .str_lit => |s| if (std.fmt.parseInt(i64, s, 10)) |_| null else |_| int_cfg_msg,
        else => int_cfg_kind_msg,
    };
}

fn intValue(e: *const ast.Expr) ?i64 {
    return switch (e.*) {
        .int_lit => |i| i,
        .str_lit => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
}

/// Why the run would refuse a connection's declaration before any statement runs:
/// an unknown connector type, a SQL connection's literal `tls` and `auth`, an SFTP,
/// SMB or FTP connection's keys, port and literal values. Null when it would not.
pub fn connectionProblem(arena: std.mem.Allocator, c: ast.Connection) Error!?[]const u8 {
    if (!analyze.isConnectionType(c.connector))
        return try std.fmt.allocPrint(arena, "unknown connection type `{s}` (one of {s})", .{ c.connector, analyze.connection_types });
    const conn = registry.Connector.parse(c.connector).?;
    if (conn.sqlRead() != null) return sqlOptionProblem(c);
    const keys = fileKeys(c.connector) orelse return null;
    var host: ?[]const u8 = null;
    var host_known = false;
    var kerberos = false;
    var realm = false;
    var user_at: ?bool = false;
    for (c.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            if (intProblem(attr.value)) |why| return why;
            if (std.math.cast(u16, intValue(attr.value).?) == null) return try portRangeMsg(arena, c.connector);
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
            if (intProblem(attr.value)) |why| return fail(diag, "{s}", .{why});
            continue;
        }
        if (std.mem.eql(u8, k, "auto_create")) {
            if (attr.value.* != .bool_lit and attr.value.* != .str_lit) return fail(diag, "{s}", .{bool_cfg_msg});
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

fn containsKey(keys: []const []const u8, k: []const u8) bool {
    for (keys) |x| if (std.mem.eql(u8, x, k)) return true;
    return false;
}

//! A database connection's settings, evaluated from its options and the environment,
//! and how each kind connects (SQL Server's AAD, NTLM and Kerberos included).

pub const DbAuth = @import("../env.zig").DbAuth;
const DbConfig = @import("../env.zig").DbConfig;
const Env = @import("../env.zig").Env;
const aad = @import("../../db/aad.zig");
const ast = @import("../../lang/ast.zig");
const eqlAny = @import("../env.zig").eqlAny;
const evalCfgInt = @import("register.zig").evalCfgInt;
const evalCfgStr = @import("register.zig").evalCfgStr;
const krb5 = @import("../../net/krb5.zig");
const ntlm = @import("../../net/ntlm.zig");
const planErr = @import("../env.zig").planErr;
const sql = @import("../../db/sql.zig");
const ssrp = @import("../../db/ssrp.zig");
const std = @import("std");
const tds = @import("../../db/tds.zig");
const LitCfg = @import("testing_util.zig").LitCfg;
pub const SqlKind = @import("../connect.zig").SqlKind;
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;

/// A connect failure's error name, with the server's or identity provider's own words
/// when they refused the login.
pub fn connectWhy(arena: std.mem.Allocator, connector: []const u8, e: anyerror) ![]const u8 {
    if (e == error.LoginFailed and std.mem.eql(u8, connector, "sqlserver") and tds.lastError().len > 0)
        return std.fmt.allocPrint(arena, "{s}: {s}", .{ @errorName(e), tds.lastError() });
    if (e == error.AadTokenFailed and aad.lastError().len > 0)
        return std.fmt.allocPrint(arena, "{s}: {s}", .{ @errorName(e), aad.lastError() });
    return @errorName(e);
}

/// Azure AD (ROPC token, FEDAUTH) for `auth = aad`, NTLMv2 or Kerberos for
/// `ntlm` / `kerberos`, else a SQL login.
pub fn tdsConnect(gpa: std.mem.Allocator, cfg_in: DbConfig) !*tds.Conn {
    var cfg = cfg_in;
    const hi = ssrp.splitHostInstance(cfg.host);
    if (hi.instance) |inst| {
        cfg.host = hi.host;
        if (!cfg.port_explicit) cfg.port = try ssrp.resolveInstancePort(gpa, hi.host, inst);
    }
    if (cfg.auth == .sql) return tds.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
    const mode: sql.TlsMode = if (cfg.tls == .off) .require else cfg.tls;
    if (cfg.auth == .ntlm) return tds.Conn.connectNtlm(gpa, cfg.host, cfg.port, ntlmCredential(cfg), cfg.database, mode);
    if (cfg.auth == .kerberos) {
        var realm_buf: [256]u8 = undefined;
        var spn_buf: [300]u8 = undefined;
        const cred = krb5.credential(cfg.user, cfg.password, cfg.realm, cfg.kdc, &realm_buf) orelse return error.LoginFailed;
        const spn = if (cfg.spn.len > 0) cfg.spn else try std.fmt.bufPrint(&spn_buf, "MSSQLSvc/{s}:{d}", .{ cfg.host, cfg.port });
        return tds.Conn.connectKerberos(gpa, cfg.host, cfg.port, cred, spn, cfg.database, mode);
    }
    if (cfg.token.len > 0) return tds.Conn.connectAad(gpa, cfg.host, cfg.port, cfg.token, cfg.database, mode);
    const client_id = if (cfg.client_id.len > 0) cfg.client_id else aad.ado_client_id;
    var rbuf: ?[]u8 = null;
    defer if (rbuf) |b| gpa.free(b);
    const resource = if (cfg.resource.len > 0) cfg.resource else if (std.mem.endsWith(u8, cfg.host, ".dynamics.com")) blk: {
        rbuf = try std.fmt.allocPrint(gpa, "https://{s}", .{cfg.host});
        break :blk rbuf.?;
    } else aad.sql_resource;
    const token = try aad.passwordToken(gpa, client_id, cfg.user, cfg.password, resource);
    defer gpa.free(token);
    return tds.Conn.connectAad(gpa, cfg.host, cfg.port, token, cfg.database, mode);
}

/// `user = 'DOMAIN\me'` carries the domain inline; an explicit `domain` wins,
/// but the prefix is stripped from the user either way.
fn ntlmCredential(cfg: DbConfig) ntlm.Credential {
    var domain = cfg.domain;
    var user = cfg.user;
    if (std.mem.indexOfScalar(u8, user, '\\')) |i| {
        if (domain.len == 0) domain = user[0..i];
        user = user[i + 1 ..];
    }
    return .{ .domain = domain, .user = user, .password = cfg.password };
}

/// `f` fetches each value. Generic because a lenient offline fetcher once existed;
/// the seam is kept so one can return. `fe_host`/`fe_port` are StarRocks spellings.
pub fn parseDbConfig(conn: ast.Connection, default_port: u16, f: anytype) anyerror!DbConfig {
    var cfg = DbConfig{ .port = default_port };
    for (conn.config) |attr| {
        const k = attr.key;
        if (eqlAny(k, &.{ "port", "fe_port" })) {
            if (try f.port(attr.value)) |p| {
                cfg.port = p;
                cfg.port_explicit = true;
            }
            continue;
        }
        if (!eqlAny(k, &.{ "host", "fe_host", "user", "password", "database", "tls", "auth", "domain", "realm", "kdc", "spn", "client_id", "resource", "token" })) continue;
        const v = (try f.str(attr.value)) orelse continue;
        if (eqlAny(k, &.{ "host", "fe_host" })) {
            cfg.host = v;
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = v;
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = v;
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = v;
        } else if (std.mem.eql(u8, k, "tls")) {
            cfg.tls = try f.tls(v);
        } else if (std.mem.eql(u8, k, "auth")) {
            cfg.auth = try f.auth(v);
        } else if (std.mem.eql(u8, k, "domain")) {
            cfg.domain = v;
        } else if (std.mem.eql(u8, k, "realm")) {
            cfg.realm = v;
        } else if (std.mem.eql(u8, k, "kdc")) {
            cfg.kdc = v;
        } else if (std.mem.eql(u8, k, "spn")) {
            cfg.spn = v;
        } else if (std.mem.eql(u8, k, "client_id")) {
            cfg.client_id = v;
        } else if (std.mem.eql(u8, k, "resource")) {
            cfg.resource = v;
        } else if (std.mem.eql(u8, k, "token")) {
            cfg.token = v;
        }
    }
    return cfg;
}

const EnvCfg = struct {
    env: *Env,
    pub fn str(self: EnvCfg, e: *const ast.Expr) !?[]const u8 {
        return try evalCfgStr(self.env, e);
    }
    pub fn port(self: EnvCfg, e: *const ast.Expr) !?u16 {
        const p: u16 = @intCast(try evalCfgInt(self.env, e));
        return p;
    }
    pub fn tls(self: EnvCfg, v: []const u8) !sql.TlsMode {
        return std.meta.stringToEnum(sql.TlsMode, v) orelse
            planErr(self.env.diag, "connection `tls` must be \"off\", \"require\" or \"insecure\"");
    }
    pub fn auth(self: EnvCfg, v: []const u8) !DbAuth {
        return std.meta.stringToEnum(DbAuth, v) orelse
            planErr(self.env.diag, "connection `auth` must be \"sql\", \"aad\", \"ntlm\" or \"kerberos\"");
    }
};

pub fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
    const cfg = try parseDbConfig(conn, default_port, EnvCfg{ .env = env });
    if (cfg.host.len == 0) return planErr(env.diag, "connection needs a `host`");
    if (cfg.auth == .kerberos and cfg.realm.len == 0 and std.mem.indexOfScalar(u8, cfg.user, '@') == null)
        return planErr(env.diag, "connection `auth = 'kerberos'` needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`");
    if (cfg.auth == .ntlm and cfg.tls == .off) return planErr(env.diag, "connection `auth = 'ntlm'` requires an encrypted channel: set `tls = 'require'`, or `tls = 'insecure'` for a self-signed server certificate");
    return cfg;
}

test "a starrocks connection reads over MySQL: FE host and port, default 9030" {
    var host = ast.Expr{ .str_lit = "fe.internal" };
    var port = ast.Expr{ .int_lit = 9031 };
    var user = ast.Expr{ .str_lit = "etl" };
    const pos = ast.Pos{ .line = 0, .col = 0 };
    const conn = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "fe_host", .value = &host, .pos = pos },
        .{ .key = "user", .value = &user, .pos = pos },
    } };
    const info = sqlConnInfo(conn).?;
    try std.testing.expectEqual(SqlKind.mysql, info.kind);
    try std.testing.expectEqual(sql.Dialect.starrocks, info.dialect);

    const cfg = try parseDbConfig(conn, info.port, LitCfg{});
    try std.testing.expectEqualStrings("fe.internal", cfg.host);
    try std.testing.expectEqualStrings("etl", cfg.user);
    try std.testing.expectEqual(@as(u16, 9030), cfg.port);

    const explicit = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "host", .value = &host, .pos = pos },
        .{ .key = "fe_port", .value = &port, .pos = pos },
    } };
    try std.testing.expectEqual(@as(u16, 9031), (try parseDbConfig(explicit, info.port, LitCfg{})).port);
}

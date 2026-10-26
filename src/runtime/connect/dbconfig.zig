//! A database connection's settings, evaluated from its options and the environment,
//! and how each kind connects (SQL Server's AAD, NTLM and Kerberos included).

pub const DbAuth = @import("../env.zig").DbAuth;
const DbConfig = @import("../env.zig").DbConfig;
const Env = @import("../env.zig").Env;
const aad = @import("../../db/sqlserver/aad.zig");
const ast = @import("../../lang/ast.zig");
const driver = @import("../../connect/driver.zig");
const env_mod = @import("../env.zig");
const RunCfg = @import("register.zig").RunCfg;
const krb5 = @import("../../net/auth/krb5.zig");
const ntlm = @import("../../net/auth/ntlm.zig");
const options = @import("../analyze/options.zig");
const planErr = @import("../env.zig").planErr;
const schema = @import("options_schema.zig");
const sql = @import("../../db/sql.zig");
const ssrp = @import("../../db/sqlserver/ssrp.zig");
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const tds = @import("../../db/sqlserver/tds.zig");
pub const SqlKind = @import("../connect.zig").SqlKind;
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;

/// A connect failure's error name, with the server's or identity provider's own words
/// when they refused the login.
pub fn connectWhy(arena: std.mem.Allocator, connector: []const u8, e: anyerror) ![]const u8 {
    if (e == error.LoginFailed and std.mem.eql(u8, connector, "sqlserver"))
        return driver.connectorNoteAlloc(arena, "", e, tds.lastError());
    if (e == error.AadTokenFailed) return driver.connectorNoteAlloc(arena, "", e, aad.lastError());
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
        rbuf = try msgfmt.allocPrint(gpa, "https://{s}", .{cfg.host});
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

/// A SQL connection's options; `fe_host`/`fe_port` are StarRocks spellings of
/// `host`/`port`, and any port is refused as `port`.
const DbCfg = struct {
    run: RunCfg,

    pub fn value(self: DbCfg, comptime F: type, _: schema.Opt, e: *const ast.Expr) !?F {
        if (F == u16) return try self.run.int(u16, e, "port");
        const v = try self.run.text(e);
        if (F == []const u8) return v;
        if (F == sql.TlsMode) return std.meta.stringToEnum(sql.TlsMode, v) orelse planErr(self.run.env.diag, env_mod.tls_values_msg);
        if (F == DbAuth) return std.meta.stringToEnum(DbAuth, v) orelse planErr(self.run.env.diag, env_mod.auth_values_msg);
        @compileError("no reader for " ++ @typeName(F));
    }
};

pub fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
    var cfg = DbConfig{ .port = default_port };
    try schema.bind(DbConfig, &schema.db_read, .ignore, &schema.db_aliases, &cfg, conn.config, DbCfg{ .run = .{ .env = env, .connector = conn.connector } });
    if (cfg.host.len == 0) return planErr(env.diag, try options.needsHostMsg(env.arena, conn));
    if (cfg.auth == .kerberos and cfg.realm.len == 0 and std.mem.indexOfScalar(u8, cfg.user, '@') == null)
        return planErr(env.diag, try options.sqlRealmMsg(env.arena, conn.name));
    if (cfg.auth == .ntlm and cfg.tls == .off) return planErr(env.diag, env_mod.ntlm_needs_tls_msg);
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

    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const env = try @import("testing_util.zig").testEnv(ar.allocator());
    const cfg = try resolveDbConfig(env, conn, info.port);
    try std.testing.expectEqualStrings("fe.internal", cfg.host);
    try std.testing.expectEqualStrings("etl", cfg.user);
    try std.testing.expectEqual(@as(u16, 9030), cfg.port);

    const explicit = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "host", .value = &host, .pos = pos },
        .{ .key = "fe_port", .value = &port, .pos = pos },
    } };
    try std.testing.expectEqual(@as(u16, 9031), (try resolveDbConfig(env, explicit, info.port)).port);
}

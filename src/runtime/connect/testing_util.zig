//! Test helpers shared by the tests of connect.zig's parts, and the option readers
//! as they were before `options_schema.bind`, kept as the oracle the new ones are
//! checked against.

pub const DbAuth = @import("../connect.zig").DbAuth;
const DbConfig = @import("../env.zig").DbConfig;
const Env = @import("../env.zig").Env;
const ast = @import("../../lang/ast.zig");
const env_mod = @import("../env.zig");
const eqlAny = @import("../env.zig").eqlAny;
const ftp = @import("../../store/ftp.zig");
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const options = @import("../analyze/options.zig");
const planErr = @import("../env.zig").planErr;
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");
const sql = @import("../../db/sql.zig");
const std = @import("std");
const streamload = @import("../../db/streamload.zig");

pub const LitCfg = struct {
    pub fn str(_: LitCfg, e: *const ast.Expr) !?[]const u8 {
        return if (e.* == .str_lit) e.str_lit else null;
    }
    pub fn port(_: LitCfg, e: *const ast.Expr) !?u16 {
        return if (e.* == .int_lit) @intCast(e.int_lit) else null;
    }
    pub fn tls(_: LitCfg, _: []const u8) !sql.TlsMode {
        return .off;
    }
    pub fn auth(_: LitCfg, _: []const u8) !DbAuth {
        return .sql;
    }
};

/// An Env on `a` with nothing declared, for tests that read options.
pub fn testEnv(a: std.mem.Allocator) !*Env {
    const e = try a.create(Env);
    const params = try a.create(std.StringHashMap(@import("../../exec/value.zig").Value));
    params.* = .init(a);
    const bindings = try a.create(std.StringHashMap(ast.Pipeline));
    bindings.* = .init(a);
    const connections = try a.create(std.StringHashMap(ast.Connection));
    connections.* = .init(a);
    const sources = try a.create(std.array_list.Managed(@import("../../connect/driver.zig").Source));
    sources.* = .init(a);
    const diag = try a.create(env_mod.Diag);
    diag.* = .{};
    const log = try a.create(obs.Logger);
    log.* = obs.Logger.init(0, .text, .err);
    const params_expr = try a.create(std.StringHashMap(*const ast.Expr));
    params_expr.* = .init(a);
    const errctx = try a.create(op.ErrCtx);
    errctx.* = .{};
    const rows = try a.create(obs.RowCounter);
    rows.* = obs.RowCounter.init(0);
    const json = try a.create(std.StringHashMap(std.json.Value));
    json.* = .init(a);
    const fns = try a.create(std.StringHashMap(ast.FnDecl));
    fns.* = .init(a);
    e.* = .{ .arena = a, .gpa = a, .params = params, .bindings = bindings, .connections = connections, .sources = sources, .diag = diag, .log = log, .params_expr = params_expr, .errctx = errctx, .rows_read = rows, .json_params = json, .fns = fns };
    return e;
}

pub const old = struct {
    pub fn sftpConn(env: *Env, conn: ast.Connection) !sftp.Conn {
        var c = sftp.Conn{ .host = "" };
        for (conn.config) |attr| {
            const k = attr.key;
            if (std.mem.eql(u8, k, "port")) {
                c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, try options.portRangeMsg(env.arena, "sftp"));
                continue;
            }
            const v = optCfgStr(env, attr.value) catch |e| return e;
            if (std.mem.eql(u8, k, "host")) {
                c.host = v orelse "";
            } else if (std.mem.eql(u8, k, "user")) {
                c.user = v;
            } else if (std.mem.eql(u8, k, "password")) {
                c.password = v;
            } else if (std.mem.eql(u8, k, "key_file")) {
                c.key_file = v;
            } else if (std.mem.eql(u8, k, "key_passphrase")) {
                c.key_passphrase = v;
            } else if (std.mem.eql(u8, k, "known_hosts")) {
                c.known_hosts = v;
            } else if (std.mem.eql(u8, k, "host_key")) {
                c.host_key = v;
            } else return planErr(env.diag, try options.unknownOptionMsg(env.arena, conn, k));
        }
        if (c.host.len == 0) return planErr(env.diag, try options.needsHostMsg(env.arena, conn));
        return c;
    }

    pub fn smbConn(env: *Env, conn: ast.Connection) !smb.Conn {
        var c = smb.Conn{ .host = "" };
        for (conn.config) |attr| {
            const k = attr.key;
            if (std.mem.eql(u8, k, "port")) {
                c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, try options.portRangeMsg(env.arena, "smb"));
                continue;
            }
            const v = optCfgStr(env, attr.value) catch |e| return e;
            if (std.mem.eql(u8, k, "host")) {
                c.host = v orelse "";
            } else if (std.mem.eql(u8, k, "user")) {
                c.user = v;
            } else if (std.mem.eql(u8, k, "password")) {
                c.password = v;
            } else if (std.mem.eql(u8, k, "domain")) {
                c.domain = v;
            } else if (std.mem.eql(u8, k, "share")) {
                c.share = if (v) |sh| std.mem.trim(u8, sh, "/\\") else null;
            } else if (std.mem.eql(u8, k, "realm")) {
                c.realm = if (v) |r| try std.ascii.allocUpperString(env.arena, r) else null;
            } else if (std.mem.eql(u8, k, "kdc")) {
                c.kdc = v;
            } else if (std.mem.eql(u8, k, "spn")) {
                c.spn = v;
            } else if (std.mem.eql(u8, k, "auth")) {
                const a = v orelse "";
                if (!options.smbAuthValid(a)) return planErr(env.diag, try options.smbAuthMsg(env.arena, conn.name, a));
                c.auth = if (std.ascii.eqlIgnoreCase(a, "kerberos")) .kerberos else .ntlm;
            } else return planErr(env.diag, try options.unknownOptionMsg(env.arena, conn, k));
        }
        if (c.host.len == 0) return planErr(env.diag, try options.needsHostMsg(env.arena, conn));
        if (c.auth == .kerberos and c.realm == null and std.mem.indexOfScalar(u8, c.user orelse "", '@') == null)
            return planErr(env.diag, try options.smbRealmMsg(env.arena, conn.name));
        return c;
    }

    pub fn ftpConn(env: *Env, conn: ast.Connection) !ftp.Conn {
        var c = ftp.Conn{ .host = "" };
        for (conn.config) |attr| {
            const k = attr.key;
            if (std.mem.eql(u8, k, "port")) {
                c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, try options.portRangeMsg(env.arena, "ftp"));
                continue;
            }
            const v = optCfgStr(env, attr.value) catch |e| return e;
            if (std.mem.eql(u8, k, "host")) {
                c.host = v orelse "";
            } else if (std.mem.eql(u8, k, "user")) {
                c.user = v;
            } else if (std.mem.eql(u8, k, "password")) {
                c.password = v;
            } else return planErr(env.diag, try options.unknownOptionMsg(env.arena, conn, k));
        }
        if (c.host.len == 0) return planErr(env.diag, try options.needsHostMsg(env.arena, conn));
        return c;
    }

    fn optCfgStr(env: *Env, expr: *const ast.Expr) !?[]const u8 {
        if (expr.* == .call) {
            const c = expr.call;
            if ((std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and c.args.len == 1 and c.args[0].* == .str_lit)
                return std.process.getEnvVarOwned(env.arena, c.args[0].str_lit) catch null;
        }
        return try evalCfgStr(env, expr);
    }

    pub fn evalCfgStr(env: *Env, expr: *const ast.Expr) ![]const u8 {
        switch (expr.*) {
            .str_lit => |s| return s,
            .int_lit => |i| return std.fmt.allocPrint(env.arena, "{d}", .{i}),
            .bool_lit => |b| return if (b) "true" else "false",
            .call => |c| {
                if ((std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and
                    c.args.len == 1 and c.args[0].* == .str_lit)
                {
                    const name = c.args[0].str_lit;
                    return std.process.getEnvVarOwned(env.arena, name) catch
                        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "env var `{s}` is not set", .{name}));
                }
                return planErr(env.diag, options.literal_cfg_msg);
            },
            else => return planErr(env.diag, options.literal_cfg_msg),
        }
    }

    pub fn evalCfgInt(env: *Env, expr: *const ast.Expr) !i64 {
        return switch (expr.*) {
            .int_lit => |i| i,
            .str_lit => |s| std.fmt.parseInt(i64, s, 10) catch return planErr(env.diag, options.int_cfg_msg),
            else => planErr(env.diag, options.int_cfg_kind_msg),
        };
    }

    pub fn evalCfgIntIn(comptime T: type, env: *Env, expr: *const ast.Expr, connector: []const u8, key: []const u8) !T {
        return std.math.cast(T, try evalCfgInt(env, expr)) orelse planErr(env.diag, try options.rangeMsg(env.arena, connector, key));
    }

    pub fn evalCfgBool(env: *Env, expr: *const ast.Expr) !bool {
        return switch (expr.*) {
            .bool_lit => |b| b,
            .str_lit => |s| cfgBoolText(s) orelse planErr(env.diag, options.bool_cfg_msg),
            else => planErr(env.diag, options.bool_cfg_msg),
        };
    }

    /// A bool option written as text: `true` or `false` in any case, else null.
    fn cfgBoolText(s: []const u8) ?bool {
        if (std.ascii.eqlIgnoreCase(s, "true")) return true;
        if (std.ascii.eqlIgnoreCase(s, "false")) return false;
        return null;
    }

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
        connector: []const u8,
        pub fn str(self: EnvCfg, e: *const ast.Expr) !?[]const u8 {
            return try evalCfgStr(self.env, e);
        }
        pub fn port(self: EnvCfg, e: *const ast.Expr) !?u16 {
            return try evalCfgIntIn(u16, self.env, e, self.connector, "port");
        }
        pub fn tls(self: EnvCfg, v: []const u8) !sql.TlsMode {
            return std.meta.stringToEnum(sql.TlsMode, v) orelse
                planErr(self.env.diag, env_mod.tls_values_msg);
        }
        pub fn auth(self: EnvCfg, v: []const u8) !DbAuth {
            return std.meta.stringToEnum(DbAuth, v) orelse
                planErr(self.env.diag, env_mod.auth_values_msg);
        }
    };

    pub fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
        const cfg = try parseDbConfig(conn, default_port, EnvCfg{ .env = env, .connector = conn.connector });
        if (cfg.host.len == 0) return planErr(env.diag, try options.needsHostMsg(env.arena, conn));
        if (cfg.auth == .kerberos and cfg.realm.len == 0 and std.mem.indexOfScalar(u8, cfg.user, '@') == null)
            return planErr(env.diag, try options.sqlRealmMsg(env.arena, conn.name));
        if (cfg.auth == .ntlm and cfg.tls == .off) return planErr(env.diag, env_mod.ntlm_needs_tls_msg);
        return cfg;
    }

    pub fn resolveStreamLoadConfig(env: *Env, conn: ast.Connection, flavor: streamload.Flavor) !streamload.Config {
        var cfg = streamload.Config{ .flavor = flavor, .database = "", .errctx = env.errctx, .space = env.space };
        for (conn.config) |attr| {
            const k = attr.key;
            if (eqlAny(k, &.{ "host", "fe_host" })) {
                cfg.fe_host = try evalCfgStr(env, attr.value);
            } else if (eqlAny(k, &.{ "port", "fe_port" })) {
                cfg.fe_port = try evalCfgIntIn(u16, env, attr.value, conn.connector, k);
            } else if (eqlAny(k, &.{ "be_url", "load_url" })) {
                cfg.load_url = try evalCfgStr(env, attr.value);
            } else if (std.mem.eql(u8, k, "database")) {
                cfg.database = try evalCfgStr(env, attr.value);
            } else if (std.mem.eql(u8, k, "user")) {
                cfg.user = try evalCfgStr(env, attr.value);
            } else if (std.mem.eql(u8, k, "password")) {
                cfg.password = try evalCfgStr(env, attr.value);
            } else if (std.mem.eql(u8, k, "tls")) {
                cfg.tls = std.meta.stringToEnum(sql.TlsMode, try evalCfgStr(env, attr.value)) orelse return planErr(env.diag, env_mod.tls_values_msg);
            } else if (std.mem.eql(u8, k, "buckets")) {
                cfg.buckets = try evalCfgIntIn(u32, env, attr.value, conn.connector, k);
            } else if (std.mem.eql(u8, k, "replication_num")) {
                cfg.replication_num = try evalCfgIntIn(u32, env, attr.value, conn.connector, k);
            } else if (std.mem.eql(u8, k, "auto_create")) {
                cfg.auto_create = try evalCfgBool(env, attr.value);
            } else if (std.mem.eql(u8, k, "label_prefix")) {
                cfg.label_prefix = try evalCfgStr(env, attr.value);
            } else if (std.mem.eql(u8, k, "load_size")) {
                const txt = try evalCfgStr(env, attr.value);
                cfg.load_size = env_mod.parseByteSize(txt) orelse 0;
                if (cfg.load_size == 0) return planErr(env.diag, try options.loadSizeMsg(env.arena, txt));
            } else if (std.mem.eql(u8, k, "compression")) {
                const txt = try evalCfgStr(env, attr.value);
                cfg.compression = std.meta.stringToEnum(streamload.Compression, txt) orelse
                    return planErr(env.diag, try options.loadCompressionMsg(env.arena, txt));
            }
        }
        if (cfg.database.len == 0) return planErr(env.diag, try options.needsDatabaseMsg(env.arena, flavor.name()));
        if (env.sink_label_prefix) |lp| cfg.label_prefix = lp;
        return cfg;
    }
};

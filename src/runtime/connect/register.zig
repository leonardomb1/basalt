//! SFTP, SMB and FTP connections registered from their options, and how option values
//! (literals, env(), secrets) are evaluated.

const Env = @import("../env.zig").Env;
const ast = @import("../../lang/ast.zig");
const ftp = @import("../../store/ftp.zig");
const planErr = @import("../env.zig").planErr;
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");
const std = @import("std");

/// A password is optional (a key logs in without one); an unknown option is an
/// error naming the known ones.
pub fn registerSftp(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "sftp")) return;
    var c = sftp.Conn{ .host = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, "sftp connection `port` is out of range");
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
        } else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sftp connection `{s}`: unknown option `{s}` (host, port, user, password, key_file, key_passphrase, known_hosts, host_key)", .{ conn.name, k }));
    }
    if (c.host.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sftp connection `{s}` needs a `host`", .{conn.name}));
    try sftp.register(conn.name, c);
}

/// With a `share`, paths under the name are inside it; without one the first path
/// part names the share. An unknown option is an error naming the known ones.
pub fn registerSmb(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "smb")) return;
    var c = smb.Conn{ .host = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, "smb connection `port` is out of range");
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
            c.auth = if (std.ascii.eqlIgnoreCase(a, "kerberos")) .kerberos else if (std.ascii.eqlIgnoreCase(a, "ntlm")) .ntlm else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}`: `auth` is kerberos or ntlm, not `{s}`", .{ conn.name, a }));
        } else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}`: unknown option `{s}` (host, port, user, password, domain, share, auth, realm, kdc, spn)", .{ conn.name, k }));
    }
    if (c.host.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}` needs a `host`", .{conn.name}));
    if (c.auth == .kerberos and c.realm == null and std.mem.indexOfScalar(u8, c.user orelse "", '@') == null)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}`: Kerberos needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`", .{conn.name}));
    try smb.register(conn.name, c);
}

/// Without a `user` (or `NAME_USER`) the login is anonymous; an unknown option is
/// an error naming the known ones.
pub fn registerFtp(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "ftp")) return;
    var c = ftp.Conn{ .host = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, "ftp connection `port` is out of range");
            continue;
        }
        const v = optCfgStr(env, attr.value) catch |e| return e;
        if (std.mem.eql(u8, k, "host")) {
            c.host = v orelse "";
        } else if (std.mem.eql(u8, k, "user")) {
            c.user = v;
        } else if (std.mem.eql(u8, k, "password")) {
            c.password = v;
        } else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "ftp connection `{s}`: unknown option `{s}` (host, port, user, password)", .{ conn.name, k }));
    }
    if (c.host.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "ftp connection `{s}` needs a `host`", .{conn.name}));
    try ftp.register(conn.name, c);
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
            return planErr(env.diag, "config value must be a literal or env()/secret()");
        },
        else => return planErr(env.diag, "config value must be a literal or env()/secret()"),
    }
}

pub fn evalCfgInt(env: *Env, expr: *const ast.Expr) !i64 {
    return switch (expr.*) {
        .int_lit => |i| i,
        .str_lit => |s| std.fmt.parseInt(i64, s, 10) catch return planErr(env.diag, "invalid integer config value"),
        else => planErr(env.diag, "config value must be an integer"),
    };
}

pub fn evalCfgBool(env: *Env, expr: *const ast.Expr) !bool {
    return switch (expr.*) {
        .bool_lit => |b| b,
        .str_lit => |s| std.mem.eql(u8, s, "true"),
        else => planErr(env.diag, "config value must be a bool"),
    };
}

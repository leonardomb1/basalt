//! SFTP, SMB and FTP connections registered from their options, and how option values
//! (literals, env(), secrets) are evaluated.

const Env = @import("../env.zig").Env;
const ast = @import("../../lang/ast.zig");
const ftp = @import("../../store/ftp.zig");
const options = @import("../analyze/options.zig");
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

/// An integer option that must fit `T`, refused as out of range for `key` otherwise.
pub fn evalCfgIntIn(comptime T: type, env: *Env, expr: *const ast.Expr, connector: []const u8, key: []const u8) !T {
    return std.math.cast(T, try evalCfgInt(env, expr)) orelse planErr(env.diag, try options.rangeMsg(env.arena, connector, key));
}

pub fn evalCfgBool(env: *Env, expr: *const ast.Expr) !bool {
    return switch (expr.*) {
        .bool_lit => |b| b,
        .str_lit => |s| options.cfgBoolText(s) orelse planErr(env.diag, options.bool_cfg_msg),
        else => planErr(env.diag, options.bool_cfg_msg),
    };
}

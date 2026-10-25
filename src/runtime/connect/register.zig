//! SFTP, SMB and FTP connections registered from their options, and `RunCfg`, how
//! the run reads an option's value (literals, env(), secrets) through
//! `options_schema`, each refusal becoming the plan error `check` gives.

const Env = @import("../env.zig").Env;
const ast = @import("../../lang/ast.zig");
const ftp = @import("../../store/ftp.zig");
const options = @import("../analyze/options.zig");
const planErr = @import("../env.zig").planErr;
const schema = @import("options_schema.zig");
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");
const std = @import("std");

pub const RunCfg = struct {
    env: *Env,
    connector: []const u8,

    fn refuse(self: RunCfg, p: schema.Problem, key: []const u8) error{ PlanFailed, OutOfMemory } {
        const msg = schema.message(self.env.arena, p, self.connector, key) catch return error.OutOfMemory;
        return planErr(self.env.diag, msg);
    }

    /// A text value; an env()/secret() whose variable is unset is refused.
    pub fn text(self: RunCfg, e: *const ast.Expr) ![]const u8 {
        return switch (try schema.cfgText(self.env.arena, e)) {
            .ok => |t| t,
            .unset => |name| planErr(self.env.diag, try schema.unsetEnvMsg(self.env.arena, name)),
            .problem => |p| self.refuse(p, ""),
        };
    }

    /// A text value, null when its env()/secret() variable is unset.
    pub fn optText(self: RunCfg, e: *const ast.Expr) !?[]const u8 {
        return switch (try schema.cfgText(self.env.arena, e)) {
            .ok => |t| t,
            .unset => null,
            .problem => |p| self.refuse(p, ""),
        };
    }

    /// An integer option that must fit `T`, refused as out of range for `key` otherwise.
    pub fn int(self: RunCfg, comptime T: type, e: *const ast.Expr, key: []const u8) !T {
        return switch (schema.cfgIntIn(T, e)) {
            .ok => |v| v,
            .problem => |p| self.refuse(p, key),
        };
    }

    pub fn flag(self: RunCfg, e: *const ast.Expr) !bool {
        return switch (schema.cfgBool(e)) {
            .ok => |b| b,
            .problem => |p| self.refuse(p, ""),
        };
    }
};

/// A file connection's options: an unset env() leaves a login part null, an
/// unknown key is refused after its value is read, naming the known ones.
const FileCfg = struct {
    run: RunCfg,
    conn: ast.Connection,

    pub fn unknown(self: FileCfg, key: []const u8, e: *const ast.Expr) !void {
        _ = try self.run.optText(e);
        return planErr(self.run.env.diag, try options.unknownOptionMsg(self.run.env.arena, self.conn, key));
    }

    pub fn value(self: FileCfg, comptime F: type, opt: schema.Opt, e: *const ast.Expr) !?F {
        if (F == u16) return try self.run.int(u16, e, opt.key);
        const v = try self.run.optText(e);
        if (F == []const u8) return v orelse "";
        if (F == ?smb.Auth) {
            const a = v orelse "";
            if (!options.smbAuthValid(a)) return planErr(self.run.env.diag, try options.smbAuthMsg(self.run.env.arena, self.conn.name, a));
            return if (std.ascii.eqlIgnoreCase(a, "kerberos")) .kerberos else .ntlm;
        }
        if (F != ?[]const u8) @compileError("no reader for " ++ @typeName(F));
        const t = v orelse return @as(F, null);
        if (std.mem.eql(u8, opt.key, "share")) return std.mem.trim(u8, t, "/\\");
        if (std.mem.eql(u8, opt.key, "realm")) return try std.ascii.allocUpperString(self.run.env.arena, t);
        return t;
    }
};

fn fileConn(comptime C: type, comptime opts: []const schema.Opt, env: *Env, conn: ast.Connection) !C {
    var c = C{ .host = "" };
    try schema.bind(C, opts, .refuse, &.{}, &c, conn.config, FileCfg{ .run = .{ .env = env, .connector = conn.connector }, .conn = conn });
    if (c.host.len == 0) return planErr(env.diag, try options.needsHostMsg(env.arena, conn));
    return c;
}

pub fn sftpConn(env: *Env, conn: ast.Connection) !sftp.Conn {
    return fileConn(sftp.Conn, &schema.sftp, env, conn);
}

/// With `auth = 'kerberos'` the realm comes from `realm` or a `me@REALM` user.
pub fn smbConn(env: *Env, conn: ast.Connection) !smb.Conn {
    const c = try fileConn(smb.Conn, &schema.smb, env, conn);
    if (c.auth == .kerberos and c.realm == null and std.mem.indexOfScalar(u8, c.user orelse "", '@') == null)
        return planErr(env.diag, try options.smbRealmMsg(env.arena, conn.name));
    return c;
}

pub fn ftpConn(env: *Env, conn: ast.Connection) !ftp.Conn {
    return fileConn(ftp.Conn, &schema.ftp, env, conn);
}

/// A password is optional (a key logs in without one).
pub fn registerSftp(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "sftp")) return;
    try sftp.register(conn.name, try sftpConn(env, conn));
}

/// With a `share`, paths under the name are inside it; without one the first path
/// part names the share.
pub fn registerSmb(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "smb")) return;
    try smb.register(conn.name, try smbConn(env, conn));
}

/// Without a `user` (or `NAME_USER`) the login is anonymous.
pub fn registerFtp(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "ftp")) return;
    try ftp.register(conn.name, try ftpConn(env, conn));
}

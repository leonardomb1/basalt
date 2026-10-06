//! Test helpers shared by the tests of connect.zig's parts.

pub const DbAuth = @import("../connect.zig").DbAuth;
const ast = @import("../../lang/ast.zig");
const sql = @import("../../db/sql.zig");
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

//! Root module: re-exports every subsystem so tests and the CLI can reach them.

pub const types = @import("lang/types.zig");
pub const token = @import("lang/token.zig");
pub const lexer = @import("lang/lexer.zig");
pub const ast = @import("lang/ast.zig");
pub const parser = @import("lang/parser.zig");
pub const value = @import("exec/value.zig");
pub const column = @import("exec/column.zig");
pub const batch = @import("exec/batch.zig");
pub const eval = @import("exec/eval.zig");
pub const op = @import("exec/op.zig");
pub const driver = @import("connect/driver.zig");
pub const csv = @import("connect/csv.zig");
pub const starrocks = @import("connect/starrocks.zig");
pub const tds = @import("connect/tds.zig");
pub const request = @import("connect/request.zig");
pub const http = @import("server/http.zig");
pub const runtime = @import("runtime/run.zig");
pub const cli = @import("cli/cli.zig");

test {
    @import("std").testing.refAllDeclsRecursive(@This());
}

//! Root module: re-exports every subsystem so tests and the CLI can reach them.

pub const types = @import("lang/types.zig");
pub const token = @import("lang/token.zig");
pub const lexer = @import("lang/lexer.zig");
pub const ast = @import("lang/ast.zig");
pub const parser = @import("lang/parser.zig");
pub const expand = @import("lang/expand.zig");
pub const value = @import("exec/value.zig");
pub const column = @import("exec/column.zig");
pub const batch = @import("exec/batch.zig");
pub const eval = @import("exec/eval.zig");
pub const op = @import("exec/op.zig");
pub const simd = @import("exec/simd.zig");
pub const driver = @import("connect/driver.zig");
pub const sql = @import("connect/sql.zig");
pub const csv = @import("connect/csv.zig");
pub const table = @import("connect/table.zig");
pub const starrocks = @import("connect/starrocks.zig");
pub const tds = @import("connect/tds.zig");
pub const mysql = @import("connect/mysql.zig");
pub const postgres = @import("connect/postgres.zig");
pub const request = @import("connect/request.zig");
pub const httpsrc = @import("connect/http.zig");
pub const aad = @import("connect/aad.zig");
pub const split = @import("connect/split.zig");
pub const http = @import("server/http.zig");
pub const runtime = @import("runtime/run.zig");
pub const parallel = @import("runtime/parallel.zig");
pub const obs = @import("runtime/obs.zig");
pub const analyze = @import("runtime/analyze.zig");
pub const cli = @import("cli/cli.zig");

test {
    @import("std").testing.refAllDeclsRecursive(@This());
}

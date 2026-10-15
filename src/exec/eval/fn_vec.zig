//! The vector kernels of the built-in functions that have one.

const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;
const Num = @import("vec.zig").Num;
const Str = @import("vec.zig").Str;
const Type = @import("../../lang/types.zig").Type;
const Vec = @import("vec.zig").Vec;
const VecError = @import("vec.zig").VecError;
const asNum = @import("vec.zig").asNum;
const asStr = @import("vec.zig").asStr;
const ast = @import("../../lang/ast.zig");
const caseMapInto = @import("strings.zig").caseMapInto;
const charCount = @import("strings.zig").charCount;
const column = @import("../column.zig");
const eq = @import("support.zig").eq;
const evalVec = @import("vec.zig").evalVec;
const isAscii = @import("strings.zig").isAscii;
const isIntNum = @import("vec.zig").isIntNum;
const likeMatch = @import("strings.zig").likeMatch;
const mkCol = @import("vec.zig").mkCol;
const numI = @import("vec.zig").numI;
const numValid = @import("vec.zig").numValid;
const std = @import("std");
const strArg = @import("vec.zig").strArg;
const strAt = @import("vec.zig").strAt;
const substrChars = @import("strings.zig").substrChars;
const trim = @import("support.zig").trim;

pub const vectorized = struct {
    pub fn now(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        _ = arena;
        _ = c;
        _ = batch;
        return .{ .scalar = .{ .timestamp = std.time.microTimestamp() } };
    }

    pub fn today(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        _ = arena;
        _ = c;
        _ = batch;
        return .{ .scalar = .{ .date = @intCast(@divFloor(std.time.microTimestamp(), 86_400_000_000)) } };
    }

    pub fn coalesce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len == 0) return error.Unsupported;
        const parts = try arena.alloc(Str, c.args.len);
        for (c.args, parts) |a, *sp| sp.* = try strArg(arena, a, batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        rows: while (i < n) : (i += 1) {
            for (parts) |sp| {
                if (strAt(sp, i)) |sv| {
                    try out.push(sv);
                    continue :rows;
                }
            }
            try out.pushNull();
            bm.setValid(i, false);
            any = true;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }
};

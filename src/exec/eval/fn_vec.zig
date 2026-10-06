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

    pub fn upperLower(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const up = eq(c.name, "upper");
        var scratch = std.array_list.Managed(u8).init(arena);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i) orelse {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            };
            if (isAscii(sv)) {
                const o = try out.pushMutable(sv);
                for (o) |*ch| ch.* = if (up) std.ascii.toUpper(ch.*) else std.ascii.toLower(ch.*);
            } else {
                scratch.clearRetainingCapacity();
                try caseMapInto(&scratch, sv, up);
                try out.push(scratch.items);
            }
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    pub fn trimSpace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i) orelse {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            };
            try out.push(trim(sv));
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    pub fn strlen(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 1) return error.Unsupported;
        const v = try evalVec(arena, c.args[0], batch);
        const s = asStr(v) orelse return error.Unsupported;
        const is_bytes = switch (v) {
            .col => |col| col.ty.kind == .bytes,
            .scalar => |sc| sc == .bytes,
        };
        const chars = eq(c.name, "length") and !is_bytes;
        const out = try arena.alloc(i64, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (strAt(s, i)) |sv| {
                out[i] = @intCast(if (chars) charCount(sv) else sv.len);
            } else {
                out[i] = 0;
                bm.setValid(i, false);
                any = true;
            }
        }
        return mkCol(Type.init(.int).withNull(any), n, bm, .{ .i64 = out });
    }

    pub fn strPredicate(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const name = c.name;
        const n = batch.len;
        if (c.args.len < 2) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const p = try strArg(arena, c.args[1], batch);
        const out = try arena.alloc(bool, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const pv = strAt(p, i);
            if (sv == null or pv == null) {
                out[i] = false;
                bm.setValid(i, false);
                any = true;
                continue;
            }
            out[i] = if (eq(name, "starts_with"))
                std.mem.startsWith(u8, sv.?, pv.?)
            else if (eq(name, "ends_with"))
                std.mem.endsWith(u8, sv.?, pv.?)
            else if (eq(name, "contains"))
                std.mem.indexOf(u8, sv.?, pv.?) != null
            else
                likeMatch(sv.?, pv.?);
        }
        return mkCol(Type.init(.bool).withNull(any), n, bm, .{ .b = out });
    }

    pub fn concat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
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
                if (strAt(sp, i) == null) {
                    try out.pushNull();
                    bm.setValid(i, false);
                    any = true;
                    continue :rows;
                }
            }
            for (parts) |sp| try out.append(strAt(sp, i).?);
            try out.endRow();
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
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

    pub fn substr(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 2) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const start = (try asNum(arena, try evalVec(arena, c.args[1], batch), n)) orelse return error.Unsupported;
        if (!isIntNum(start)) return error.Unsupported;
        var len_num: ?Num = null;
        if (c.args.len > 2) {
            len_num = (try asNum(arena, try evalVec(arena, c.args[2], batch), n)) orelse return error.Unsupported;
            if (!isIntNum(len_num.?)) return error.Unsupported;
        }
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const start_ok = numValid(start, i);
            const len_ok = if (len_num) |l| numValid(l, i) else true;
            if (sv == null or !start_ok or !len_ok) {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            }
            try out.push(try substrChars(arena, sv.?, numI(start, i), if (len_num) |l| numI(l, i) else null));
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    pub fn replace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 3) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const f = try strArg(arena, c.args[1], batch);
        const t = try strArg(arena, c.args[2], batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const fv = strAt(f, i);
            const tv = strAt(t, i);
            if (sv == null or fv == null or tv == null) {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            }
            if (fv.?.len == 0) {
                try out.push(sv.?);
                continue;
            }
            const o = try arena.alloc(u8, std.mem.replacementSize(u8, sv.?, fv.?, tv.?));
            _ = std.mem.replace(u8, sv.?, fv.?, tv.?, o);
            try out.push(o);
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }
};

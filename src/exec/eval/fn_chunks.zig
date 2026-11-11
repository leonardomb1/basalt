//! `chunks(text, size [, overlap])`: a text cut into pieces of at most `size`
//! characters, for embedding, as a LIST of STRUCT(no, start, text): `no` counts
//! the chunks from 1 and `start` is a chunk's first character's position from 1,
//! so `substr(text, start, length(chunk))` gives the chunk back.
//!
//! The rule, small so that it never changes (a chunk's text is what its embedding
//! and its hash were taken of): a chunk is an exact substring, never trimmed. It
//! ends after the last ASCII whitespace in its `size` characters, when one lies in
//! their second half, else at `size` characters, or at the text's end. The next
//! chunk starts `overlap` characters before that end, moved forward to just past
//! the first whitespace from there when that lands inside a word, and always at
//! least one character past the previous start. Characters are UTF-8 code points;
//! an empty text has no chunks, a NULL text gives NULL.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const Batch = @import("../batch.zig").Batch;
const Bitmap = column.Bitmap;
const Column = column.Column;
const Type = @import("../../lang/types.zig").Type;
const Value = @import("../value.zig").Value;
const eval = @import("../eval.zig");
const EvalError = eval.EvalError;
const TypeCtx = eval.TypeCtx;
const TypeError = eval.TypeError;
const vec = @import("vec.zig");
const Vec = vec.Vec;
const VecError = vec.VecError;
const evalRow = @import("row.zig").evalRow;
const charWidth = @import("strings.zig").charWidth;
const failWith = @import("support.zig").failWith;
const max_str_bytes = @import("functions.zig").max_str_bytes;

const chunk_fields = [_]Type.Field{
    .{ .name = "no", .ty = Type.init(.int) },
    .{ .name = "start", .ty = Type.init(.int) },
    .{ .name = "text", .ty = Type.init(.string) },
};
const chunk_type = Type{ .kind = .@"struct", .fields = &chunk_fields };
const list_type = Type{ .kind = .array, .elem = &chunk_type, .nullable = true };

/// The bytes a row's chunks may hold in all, past which the row fails: a large
/// overlap repeats most of the text in every chunk.
const max_chunk_bytes = 64 * max_str_bytes;

pub fn typing(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
    if (c.args.len != 2 and c.args.len != 3) return self.err("`chunks` takes (text, size) or (text, size, overlap)", .{});
    _ = try self.wantText(c, 0);
    _ = try self.wantInt(c, 1, "size");
    if (c.args.len == 3) _ = try self.wantInt(c, 2, "overlap");
    return list_type;
}

/// The chunks of every row, built as one list column.
const Out = struct {
    nos: std.array_list.Managed(i64),
    starts: std.array_list.Managed(i64),
    texts: column.BytesAppender,
    offsets: std.array_list.Managed(i32),

    fn init(arena: std.mem.Allocator, rows: usize) !Out {
        var offsets = std.array_list.Managed(i32).init(arena);
        try offsets.ensureTotalCapacity(rows + 1);
        offsets.appendAssumeCapacity(0);
        return .{
            .nos = .init(arena),
            .starts = .init(arena),
            .texts = try column.BytesAppender.init(arena, 0),
            .offsets = offsets,
        };
    }

    fn endRow(self: *Out) !void {
        try self.offsets.append(@intCast(try column.checkSpan(self.nos.items.len)));
    }

    fn finish(self: *Out, arena: std.mem.Allocator, bm: Bitmap) !Column {
        const m = self.nos.items.len;
        const full = try Bitmap.initFull(arena, m);
        const kids = try arena.alloc(Column, 3);
        kids[0] = Column.derived(Type.init(.int), m, full, .{ .i64 = try self.nos.toOwnedSlice() });
        kids[1] = Column.derived(Type.init(.int), m, full, .{ .i64 = try self.starts.toOwnedSlice() });
        kids[2] = Column.derived(Type.init(.string), m, full, .{ .bytes = try self.texts.finish() });
        const st = try arena.create(column.Nested);
        st.* = .{ .ty = chunk_type, .body = .{ .fields = kids } };
        const offsets = try self.offsets.toOwnedSlice();
        const list = try arena.create(column.Nested);
        list.* = .{ .ty = list_type, .body = .{ .list = .{ .offsets = offsets, .child = Column.derived(chunk_type, m, full, .{ .nested = st }) } } };
        return Column.derived(list_type, offsets.len - 1, bm, .{ .nested = list });
    }
};

fn isSpace(b: u8) bool {
    return switch (b) {
        ' ', '\t', '\n', '\r', 0x0b, 0x0c => true,
        else => false,
    };
}

/// `text`'s chunks appended to `out`, as the file's rule says.
fn cut(arena: std.mem.Allocator, out: *Out, text: []const u8, size: i64, overlap: i64) EvalError!void {
    if (size <= 0) return failWith(error.CastFailed, "chunks: size must be positive, got {d}", .{size});
    if (overlap < 0 or overlap >= size) return failWith(error.CastFailed, "chunks: overlap must be from 0 to below size ({d}), got {d}", .{ size, overlap });
    var at = std.array_list.Managed(usize).init(arena);
    var i: usize = 0;
    while (i < text.len) : (i += charWidth(text, i)) try at.append(i);
    const n = at.items.len;
    try at.append(text.len);
    const bo = at.items;
    const sz: usize = @intCast(size);
    const ov: usize = @intCast(overlap);
    var held: usize = 0;
    var pos: usize = 0;
    var no: i64 = 1;
    while (pos < n) : (no += 1) {
        const end = @min(pos + sz, n);
        var stop = end;
        if (end < n) {
            var k = end;
            while (k > pos + sz / 2) {
                k -= 1;
                if (isSpace(text[bo[k]])) {
                    stop = k + 1;
                    break;
                }
            }
        }
        const piece = text[bo[pos]..bo[stop]];
        held += piece.len;
        if (held > max_chunk_bytes) return failWith(error.CastFailed, "chunks: the chunks pass {d} bytes; lower the overlap", .{max_chunk_bytes});
        try out.nos.append(no);
        try out.starts.append(@intCast(pos + 1));
        try out.texts.push(piece);
        if (stop == n) break;
        var next = stop -| ov;
        if (next > 0 and !isSpace(text[bo[next - 1]])) {
            var k = next;
            while (k < stop) : (k += 1) if (isSpace(text[bo[k]])) {
                next = k + 1;
                break;
            };
        }
        pos = @max(next, pos + 1);
    }
}

fn overlapOf(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!?i64 {
    if (c.args.len < 3) return 0;
    const v = try evalRow(arena, c.args[2], batch, row);
    return if (v == .int) v.int else null;
}

pub fn rowFn(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
    const tv = try evalRow(arena, c.args[0], batch, row);
    const sv = try evalRow(arena, c.args[1], batch, row);
    const ov = try overlapOf(arena, c, batch, row);
    if (tv != .string or sv != .int or ov == null) return .null;
    var out = try Out.init(arena, 1);
    try cut(arena, &out, tv.string, sv.int, ov.?);
    try out.endRow();
    const col = try out.finish(arena, try Bitmap.initFull(arena, 1));
    return .{ .nested = .{ .node = col.data.nested, .row = 0 } };
}

/// An INT argument per row: a column's value, or one value for every row.
fn intAt(v: Vec, row: usize) ?i64 {
    const x = switch (v) {
        .col => |col| col.getValue(row),
        .scalar => |s| s,
    };
    return if (x == .int) x.int else null;
}

pub fn vecFn(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    const texts = try vec.strArg(arena, c.args[0], batch);
    const sizes = try vec.evalVec(arena, c.args[1], batch);
    const overlaps: Vec = if (c.args.len == 3) try vec.evalVec(arena, c.args[2], batch) else .{ .scalar = .{ .int = 0 } };
    const n = batch.len;
    var out = try Out.init(arena, n);
    var bm = try Bitmap.initFull(arena, n);
    for (0..n) |row| {
        const t = vec.strAt(texts, row);
        const s = intAt(sizes, row);
        const o = intAt(overlaps, row);
        if (t != null and s != null and o != null) {
            try cut(arena, &out, t.?, s.?, o.?);
        } else bm.setValid(row, false);
        try out.endRow();
    }
    return .{ .col = try out.finish(arena, bm) };
}

test "every chunk is its text's substring at its start, of at most size characters, each starting past the last and none leaving a gap, for random texts, sizes and overlaps" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(42);
    const r = prng.random();
    const alphabet = [_][]const u8{ "a", "b", "c", " ", " ", "\n", "é", "ção", "—", "x" };
    for (0..400) |_| {
        var text = std.array_list.Managed(u8).init(a);
        for (0..r.uintLessThan(usize, 200)) |_| try text.appendSlice(alphabet[r.uintLessThan(usize, alphabet.len)]);
        const size: i64 = @intCast(1 + r.uintLessThan(usize, 40));
        const overlap: i64 = @intCast(r.uintLessThan(usize, @intCast(size)));
        var out = try Out.init(a, 1);
        try cut(a, &out, text.items, size, overlap);
        var chars = std.array_list.Managed(usize).init(a);
        var i: usize = 0;
        while (i < text.items.len) : (i += charWidth(text.items, i)) try chars.append(i);
        const n = chars.items.len;
        try chars.append(text.items.len);
        const pieces = try out.texts.finish();
        var reach: usize = 0;
        for (out.starts.items, out.nos.items, 0..) |start, no, k| {
            try std.testing.expectEqual(@as(i64, @intCast(k + 1)), no);
            const s: usize = @intCast(start - 1);
            const piece = pieces.at(k);
            try std.testing.expect(std.mem.startsWith(u8, text.items[chars.items[s]..], piece));
            const len = try std.unicode.utf8CountCodepoints(piece);
            try std.testing.expect(len >= 1 and len <= size);
            if (k > 0) try std.testing.expect(start > out.starts.items[k - 1]);
            try std.testing.expect(s <= reach);
            reach = @max(reach, s + len);
        }
        try std.testing.expectEqual(n, reach);
        try std.testing.expectEqual(n == 0, out.nos.items.len == 0);
    }
}

test "chunks' kernel gives each row the chunks its row form does" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const types = @import("../../lang/types.zig");
    var sb = column.Builder.init(a, Type.init(.string).asNullable());
    for ([_]?[]const u8{ "The quick brown fox jumps over the lazy dog.", null, "", "supercalifragilistic", "ação é útil na região sul" }) |w| try sb.append(if (w) |x| .{ .string = x } else .null);
    var cols = [_]Column{try sb.finish()};
    const schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = cols[0].ty }} };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 5 };
    try @import("testing_util.zig").expectVecMatchesRows(a, batch, &.{ "chunks(s, 7, 2)", "chunks(s, 12)", "cardinality(chunks(s, 5, 4))" }, false);
}

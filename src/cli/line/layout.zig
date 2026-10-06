//! How an entry's text lays out in terminal rows: logical lines, wrapped rows, and
//! the columns a cursor moves through.

const std = @import("std");
const testBuffer = @import("testing_util.zig").testBuffer;

pub const Row = struct { start: usize, end: usize, head: bool };

pub fn charLen(text: []const u8, i: usize) usize {
    var j = i + 1;
    while (j < text.len and text[j] & 0xC0 == 0x80) j += 1;
    return j - i;
}

/// A tab is drawn as two spaces: a real one would jump to the terminal's own tab
/// stop and the cursor arithmetic would no longer know where anything is.
fn charCols(c: u8) usize {
    return if (c == '\t') 2 else 1;
}

/// Break `text` into terminal rows `width` columns wide, wrapping long lines; `head`
/// marks a line's first row, the one with a prompt. Every byte belongs to exactly
/// one row except the newlines, which sit between them.
pub fn layoutRows(gpa: std.mem.Allocator, text: []const u8, width: usize) ![]Row {
    const w = @max(width, 2);
    var rows = std.array_list.Managed(Row).init(gpa);
    errdefer rows.deinit();
    var line_start: usize = 0;
    while (true) {
        const nl = std.mem.indexOfScalarPos(u8, text, line_start, '\n') orelse text.len;
        var seg = line_start;
        var cols: usize = 0;
        var head = true;
        var i = line_start;
        while (i < nl) {
            const cw = charCols(text[i]);
            if (cols + cw > w) {
                try rows.append(.{ .start = seg, .end = i, .head = head });
                head = false;
                seg = i;
                cols = 0;
            }
            cols += cw;
            i += charLen(text, i);
        }
        try rows.append(.{ .start = seg, .end = nl, .head = head });
        if (nl == text.len) break;
        line_start = nl + 1;
    }
    return rows.toOwnedSlice();
}

/// The row the cursor is on. A cursor between two halves of a wrapped line
/// belongs to the start of the second, which is where the next character goes.
pub fn rowOf(rows: []const Row, cursor: usize) usize {
    for (rows, 0..) |r, i| {
        if (cursor < r.start or cursor > r.end) continue;
        if (cursor == r.end and i + 1 < rows.len and rows[i + 1].start == r.end) continue;
        return i;
    }
    return rows.len - 1;
}

pub fn colsBetween(text: []const u8, from: usize, to: usize) usize {
    var n: usize = 0;
    var i = from;
    while (i < to) : (i += charLen(text, i)) n += charCols(text[i]);
    return n;
}

pub fn byteAtCol(text: []const u8, row: Row, col: usize) usize {
    var n: usize = 0;
    var i = row.start;
    while (i < row.end) {
        const cw = charCols(text[i]);
        if (n + cw > col) break;
        n += cw;
        i += charLen(text, i);
    }
    return i;
}

pub fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

test "layoutRows: logical lines, wrapped rows, and which row a cursor on the seam belongs to" {
    const gpa = std.testing.allocator;
    const rows = try layoutRows(gpa, "SELECT 1\nFROM abcdefghij\n", 8);
    defer gpa.free(rows);
    try std.testing.expectEqual(@as(usize, 4), rows.len);
    try std.testing.expectEqual(Row{ .start = 0, .end = 8, .head = true }, rows[0]);
    try std.testing.expectEqual(Row{ .start = 9, .end = 17, .head = true }, rows[1]);
    try std.testing.expectEqual(Row{ .start = 17, .end = 24, .head = false }, rows[2]);
    try std.testing.expectEqual(Row{ .start = 25, .end = 25, .head = true }, rows[3]);
    try std.testing.expectEqual(@as(usize, 0), rowOf(rows, 8));
    try std.testing.expectEqual(@as(usize, 2), rowOf(rows, 17));
    try std.testing.expectEqual(@as(usize, 3), rowOf(rows, 25));
}

test "Buffer: Up and Down keep their column across a short row and report the edge" {
    const gpa = std.testing.allocator;
    var b = try testBuffer("SELECT id,\n  x\nFROM orders", 8);
    defer b.deinit();
    const rows = try layoutRows(gpa, b.bytes(), 80);
    defer gpa.free(rows);
    var goal: ?usize = null;
    try std.testing.expect(b.moveVertical(rows, true, false, &goal));
    try std.testing.expectEqual(@as(usize, 14), b.cursor);
    try std.testing.expect(b.moveVertical(rows, true, false, &goal));
    try std.testing.expectEqual(@as(usize, 23), b.cursor);
    try std.testing.expect(!b.moveVertical(rows, true, false, &goal));
    try std.testing.expect(b.moveVertical(rows, false, true, &goal));
    try std.testing.expect(b.selection() != null);
}

//! The entry being edited: text, cursor and selection, undo and redo, and the
//! editing operations a code editor has (word travel, line moves, brackets, comments).

const Nav = @import("keys.zig").Nav;
const Row = @import("layout.zig").Row;
const byteAtCol = @import("layout.zig").byteAtCol;
const charLen = @import("layout.zig").charLen;
const colsBetween = @import("layout.zig").colsBetween;
const isWordByte = @import("layout.zig").isWordByte;
const rowOf = @import("layout.zig").rowOf;
const std = @import("std");
const testBuffer = @import("testing_util.zig").testBuffer;

pub const Buffer = struct {
    gpa: std.mem.Allocator,
    text: std.array_list.Managed(u8),
    cursor: usize = 0,
    anchor: ?usize = null,
    undo_stack: std.array_list.Managed(Snapshot),
    redo_stack: std.array_list.Managed(Snapshot),
    typing: bool = false,

    const Snapshot = struct { text: []u8, cursor: usize };
    const max_undo = 200;

    pub fn init(gpa: std.mem.Allocator) Buffer {
        return .{
            .gpa = gpa,
            .text = std.array_list.Managed(u8).init(gpa),
            .undo_stack = std.array_list.Managed(Snapshot).init(gpa),
            .redo_stack = std.array_list.Managed(Snapshot).init(gpa),
        };
    }

    pub fn deinit(self: *Buffer) void {
        self.text.deinit();
        dropSnapshots(self.gpa, &self.undo_stack);
        self.undo_stack.deinit();
        dropSnapshots(self.gpa, &self.redo_stack);
        self.redo_stack.deinit();
    }

    fn dropSnapshots(gpa: std.mem.Allocator, stack: *std.array_list.Managed(Snapshot)) void {
        for (stack.items) |s| gpa.free(s.text);
        stack.clearRetainingCapacity();
    }

    pub fn bytes(self: *const Buffer) []const u8 {
        return self.text.items;
    }

    /// Replace everything (history recall). Not an undoable edit: it starts over.
    pub fn set(self: *Buffer, s: []const u8) !void {
        self.text.clearRetainingCapacity();
        try self.text.appendSlice(s);
        self.cursor = s.len;
        self.anchor = null;
        self.typing = false;
        dropSnapshots(self.gpa, &self.undo_stack);
        dropSnapshots(self.gpa, &self.redo_stack);
    }

    pub const Range = struct { start: usize, end: usize };

    pub fn selection(self: *const Buffer) ?Range {
        const a = self.anchor orelse return null;
        if (a == self.cursor) return null;
        return .{ .start = @min(a, self.cursor), .end = @max(a, self.cursor) };
    }

    fn checkpoint(self: *Buffer, typing: bool) !void {
        defer self.typing = typing;
        if (typing and self.typing) return;
        const copy = try self.gpa.dupe(u8, self.text.items);
        errdefer self.gpa.free(copy);
        try self.undo_stack.append(.{ .text = copy, .cursor = self.cursor });
        if (self.undo_stack.items.len > max_undo) self.gpa.free(self.undo_stack.orderedRemove(0).text);
        dropSnapshots(self.gpa, &self.redo_stack);
    }

    fn restore(self: *Buffer, from: *std.array_list.Managed(Snapshot), to: *std.array_list.Managed(Snapshot)) !void {
        const snap = from.pop() orelse return;
        defer self.gpa.free(snap.text);
        const now = try self.gpa.dupe(u8, self.text.items);
        errdefer self.gpa.free(now);
        try to.append(.{ .text = now, .cursor = self.cursor });
        self.text.clearRetainingCapacity();
        try self.text.appendSlice(snap.text);
        self.cursor = snap.cursor;
        self.anchor = null;
        self.typing = false;
    }

    pub fn undo(self: *Buffer) !void {
        try self.restore(&self.undo_stack, &self.redo_stack);
    }

    pub fn redo(self: *Buffer) !void {
        try self.restore(&self.redo_stack, &self.undo_stack);
    }

    fn removeRange(self: *Buffer, r: Range) void {
        self.text.replaceRange(r.start, r.end - r.start, &.{}) catch unreachable;
        self.cursor = r.start;
        self.anchor = null;
    }

    /// Type or paste `s`, over the selection if there is one. Typing over a selection
    /// starts an undo run: the new word comes back out as one step, the selection with the next.
    pub fn insert(self: *Buffer, s: []const u8) !void {
        const plain = s.len == 1 and s[0] != '\n' and s[0] != ' ';
        const sel = self.selection();
        try self.checkpoint(plain and sel == null);
        if (sel) |r| self.removeRange(r);
        try self.text.insertSlice(self.cursor, s);
        self.cursor += s.len;
        self.anchor = null;
        self.typing = plain;
    }

    /// A new line, at the line's start: an indent that followed the cursor down read
    /// as the cursor refusing to go home. Between `(` and `)` the new line steps in two
    /// past the opener's line, and the closer moves to a line of its own.
    pub fn newline(self: *Buffer) !void {
        const t = self.text.items;
        const prev: u8 = if (self.cursor > 0) t[self.cursor - 1] else 0;
        const next: u8 = if (self.cursor < t.len) t[self.cursor] else 0;
        const opened = prev == '(' or prev == '[' or prev == '{';
        const ls = self.lineStart(self.cursor);
        var ind = ls;
        while (opened and ind < self.cursor and (t[ind] == ' ' or t[ind] == '\t')) ind += 1;
        var buf: [128]u8 = undefined;
        const n = @min(ind - ls, buf.len - 4);
        buf[0] = '\n';
        @memcpy(buf[1 .. 1 + n], t[ls .. ls + n]);
        var len = 1 + n;
        if (opened) {
            buf[len] = ' ';
            buf[len + 1] = ' ';
            len += 2;
        }
        const closes = opened and (next == ')' or next == ']' or next == '}');
        if (closes) {
            const tail = try std.mem.concat(self.gpa, u8, &.{ buf[0..len], buf[0 .. 1 + n] });
            defer self.gpa.free(tail);
            try self.insert(tail);
            self.cursor -= 1 + n;
            return;
        }
        try self.insert(buf[0..len]);
    }

    /// Delete the selection, or else what `to` would travel over from the cursor.
    pub fn delete(self: *Buffer, to: Nav.To) !void {
        if (self.selection()) |r| {
            try self.checkpoint(false);
            return self.removeRange(r);
        }
        const dest = self.target(to);
        if (dest == self.cursor) return;
        try self.checkpoint(false);
        self.removeRange(.{ .start = @min(dest, self.cursor), .end = @max(dest, self.cursor) });
    }

    /// ^U: the current line's text, leaving the line itself.
    pub fn killLine(self: *Buffer) !void {
        const r = Range{ .start = self.lineStart(self.cursor), .end = self.lineEnd(self.cursor) };
        if (r.start == r.end) return;
        try self.checkpoint(false);
        self.removeRange(r);
    }

    pub fn selectAll(self: *Buffer) void {
        self.anchor = 0;
        self.cursor = self.text.items.len;
        self.typing = false;
    }

    /// Remove and return the selection (gpa-owned), for cut. Null: nothing selected.
    pub fn cut(self: *Buffer) !?[]u8 {
        const r = self.selection() orelse return null;
        const copy = try self.gpa.dupe(u8, self.text.items[r.start..r.end]);
        errdefer self.gpa.free(copy);
        try self.checkpoint(false);
        self.removeRange(r);
        return copy;
    }

    fn lineStart(self: *const Buffer, at: usize) usize {
        return if (std.mem.lastIndexOfScalar(u8, self.text.items[0..at], '\n')) |p| p + 1 else 0;
    }

    fn lineEnd(self: *const Buffer, at: usize) usize {
        return std.mem.indexOfScalarPos(u8, self.text.items, at, '\n') orelse self.text.items.len;
    }

    /// Where a horizontal move lands. Home toggles, as editors do, between the
    /// first non-blank of the line and its true start.
    fn target(self: *const Buffer, to: Nav.To) usize {
        const t = self.text.items;
        const c = self.cursor;
        switch (to) {
            .left => {
                if (c == 0) return 0;
                var i = c - 1;
                while (i > 0 and t[i] & 0xC0 == 0x80) i -= 1;
                return i;
            },
            .right => return if (c >= t.len) t.len else c + charLen(t, c),
            .word_left => {
                var i = c;
                while (i > 0 and !isWordByte(t[i - 1])) i -= 1;
                while (i > 0 and isWordByte(t[i - 1])) i -= 1;
                return i;
            },
            .word_right => {
                var i = c;
                while (i < t.len and !isWordByte(t[i])) i += 1;
                while (i < t.len and isWordByte(t[i])) i += 1;
                return i;
            },
            .home => {
                const ls = self.lineStart(c);
                var first = ls;
                while (first < t.len and (t[first] == ' ' or t[first] == '\t')) first += 1;
                return if (c == first) ls else first;
            },
            .end => return self.lineEnd(c),
            .buf_home => return 0,
            .buf_end => return t.len,
            .up, .down => return c,
        }
    }

    /// The logical lines the selection touches (or the cursor's), without the last
    /// newline. A selection ending at the very start of a line does not include that line.
    fn lineBlock(self: *const Buffer) Range {
        const sel = self.selection() orelse Range{ .start = self.cursor, .end = self.cursor };
        const last = if (sel.end > sel.start and self.text.items[sel.end - 1] == '\n') sel.end - 1 else sel.end;
        return .{ .start = self.lineStart(sel.start), .end = self.lineEnd(last) };
    }

    /// Replace `r` with `s`, keeping the selection over the new text when there was
    /// one; the cursor lands at `cursor` inside it (or the end).
    fn replaceBlock(self: *Buffer, r: Range, s: []const u8, cursor: ?usize, keep_sel: bool) !void {
        try self.checkpoint(false);
        try self.text.replaceRange(r.start, r.end - r.start, s);
        self.anchor = if (keep_sel) r.start else null;
        self.cursor = r.start + (cursor orelse s.len);
    }

    pub fn moveLines(self: *Buffer, down: bool) !void {
        const t = self.text.items;
        const blk = self.lineBlock();
        const had_sel = self.selection() != null;
        const off = self.cursor - blk.start;
        const anchor_off = if (self.anchor) |a| a - blk.start else off;
        if (down) {
            if (blk.end >= t.len) return;
            const nb = Range{ .start = blk.end + 1, .end = self.lineEnd(blk.end + 1) };
            const joined = try std.mem.concat(self.gpa, u8, &.{ t[nb.start..nb.end], "\n", t[blk.start..blk.end] });
            defer self.gpa.free(joined);
            const shift = nb.end - nb.start + 1;
            try self.replaceBlock(.{ .start = blk.start, .end = nb.end }, joined, shift + off, false);
            if (had_sel) self.anchor = blk.start + shift + anchor_off;
        } else {
            if (blk.start == 0) return;
            const pb = Range{ .start = self.lineStart(blk.start - 1), .end = blk.start - 1 };
            const joined = try std.mem.concat(self.gpa, u8, &.{ t[blk.start..blk.end], "\n", t[pb.start..pb.end] });
            defer self.gpa.free(joined);
            try self.replaceBlock(.{ .start = pb.start, .end = blk.end }, joined, off, false);
            if (had_sel) self.anchor = pb.start + anchor_off;
        }
    }

    pub fn duplicateLines(self: *Buffer, down: bool) !void {
        const blk = self.lineBlock();
        const off = self.cursor - blk.start;
        const lines = try self.gpa.dupe(u8, self.text.items[blk.start..blk.end]);
        defer self.gpa.free(lines);
        const joined = try std.mem.concat(self.gpa, u8, &.{ lines, "\n", lines });
        defer self.gpa.free(joined);
        try self.replaceBlock(blk, joined, if (down) lines.len + 1 + off else off, false);
    }

    pub fn indentLines(self: *Buffer, out: bool) !void {
        const blk = self.lineBlock();
        const t = self.text.items;
        var buf = std.array_list.Managed(u8).init(self.gpa);
        defer buf.deinit();
        var it = std.mem.splitScalar(u8, t[blk.start..blk.end], '\n');
        var first = true;
        var cursor_shift: isize = 0;
        while (it.next()) |ln| {
            if (!first) try buf.append('\n');
            first = false;
            if (out) {
                var strip: usize = 0;
                while (strip < 2 and strip < ln.len and ln[strip] == ' ') strip += 1;
                try buf.appendSlice(ln[strip..]);
                cursor_shift -= @intCast(strip);
            } else {
                try buf.appendSlice("  ");
                try buf.appendSlice(ln);
                cursor_shift += 2;
            }
        }
        const had_sel = self.selection() != null;
        const new_cursor: usize = @intCast(@max(0, @as(isize, @intCast(self.cursor)) + cursor_shift) - @as(isize, @intCast(blk.start)));
        try self.replaceBlock(blk, buf.items, @min(new_cursor, buf.items.len), had_sel);
        if (had_sel) self.cursor = blk.start + buf.items.len;
    }

    /// Comment the lines out with `-- `, or back in when they all are.
    pub fn toggleComment(self: *Buffer) !void {
        const blk = self.lineBlock();
        const t = self.text.items;
        var all = true;
        var it = std.mem.splitScalar(u8, t[blk.start..blk.end], '\n');
        while (it.next()) |ln| {
            const s = std.mem.trimLeft(u8, ln, " \t");
            if (s.len > 0 and !std.mem.startsWith(u8, s, "--")) all = false;
        }
        var buf = std.array_list.Managed(u8).init(self.gpa);
        defer buf.deinit();
        it = std.mem.splitScalar(u8, t[blk.start..blk.end], '\n');
        var first = true;
        while (it.next()) |ln| {
            if (!first) try buf.append('\n');
            first = false;
            const ind = ln.len - std.mem.trimLeft(u8, ln, " \t").len;
            const body = ln[ind..];
            try buf.appendSlice(ln[0..ind]);
            if (all) {
                if (std.mem.startsWith(u8, body, "-- ")) try buf.appendSlice(body[3..]) else if (std.mem.startsWith(u8, body, "--")) try buf.appendSlice(body[2..]) else try buf.appendSlice(body);
            } else {
                try buf.appendSlice("-- ");
                try buf.appendSlice(body);
            }
        }
        const had_sel = self.selection() != null;
        try self.replaceBlock(blk, buf.items, null, had_sel);
        if (had_sel) self.cursor = blk.start + buf.items.len;
    }

    fn closerOf(c: u8) ?u8 {
        return switch (c) {
            '(' => ')',
            '[' => ']',
            '{' => '}',
            '\'' => '\'',
            '"' => '"',
            else => null,
        };
    }

    /// A typed character, with an editor's reflexes: an opener over a selection wraps
    /// it; an opener before a blank is paired with its closer; a closer that is already
    /// next is stepped over; `)` on an empty line takes the indent out first.
    pub fn typeChar(self: *Buffer, c: u8) !void {
        const t = self.text.items;
        if (self.selection()) |r| if (closerOf(c)) |cl| {
            const inner = try self.gpa.dupe(u8, t[r.start..r.end]);
            defer self.gpa.free(inner);
            const wrapped = try std.mem.concat(self.gpa, u8, &.{ &[_]u8{c}, inner, &[_]u8{cl} });
            defer self.gpa.free(wrapped);
            try self.replaceBlock(r, wrapped, null, false);
            self.anchor = r.start + 1;
            self.cursor = r.start + 1 + inner.len;
            return;
        };
        const next: u8 = if (self.cursor < t.len) t[self.cursor] else 0;
        const prev: u8 = if (self.cursor > 0) t[self.cursor - 1] else 0;
        if ((c == ')' or c == ']' or c == '}' or c == '\'' or c == '"') and next == c) {
            self.move(.{ .to = .right });
            return;
        }
        if (c == ')' or c == ']' or c == '}') {
            const ls = self.lineStart(self.cursor);
            if (std.mem.trim(u8, t[ls..self.cursor], " ").len == 0 and self.cursor - ls >= 2) {
                try self.checkpoint(false);
                self.removeRange(.{ .start = self.cursor - 2, .end = self.cursor });
            }
        }
        if (closerOf(c)) |cl| {
            const quote = c == '\'' or c == '"';
            const before_ok = !quote or !isWordByte(prev);
            const after_ok = next == 0 or next == ' ' or next == '\n' or next == ')' or next == ']' or next == '}' or next == ',' or next == ';';
            if (before_ok and after_ok) {
                try self.insert(&[_]u8{ c, cl });
                self.cursor -= 1;
                return;
            }
        }
        try self.insert(&[_]u8{c});
    }

    /// The bracket paired with the one at or just before the cursor, as two byte
    /// offsets for the repaint to underline. Strings are not looked into.
    pub fn bracketPair(self: *const Buffer) ?[2]usize {
        const t = self.text.items;
        const at: usize = if (self.cursor < t.len and isBracket(t[self.cursor])) self.cursor else if (self.cursor > 0 and isBracket(t[self.cursor - 1])) self.cursor - 1 else return null;
        const c = t[at];
        const open = c == '(' or c == '[' or c == '{';
        const mate: u8 = switch (c) {
            '(' => ')',
            ')' => '(',
            '[' => ']',
            ']' => '[',
            '{' => '}',
            else => '{',
        };
        var depth: usize = 0;
        var i = at;
        while (true) {
            if (t[i] == c) depth += 1 else if (t[i] == mate) {
                depth -= 1;
                if (depth == 0) return .{ @min(at, i), @max(at, i) };
            }
            if (open) {
                i += 1;
                if (i >= t.len) return null;
            } else {
                if (i == 0) return null;
                i -= 1;
            }
        }
    }

    fn isBracket(c: u8) bool {
        return c == '(' or c == ')' or c == '[' or c == ']' or c == '{' or c == '}';
    }

    /// With `select` the anchor stays put and the cursor drags the selection; without
    /// it a selection collapses, Left or Right to its near edge rather than past it.
    pub fn move(self: *Buffer, nav: Nav) void {
        self.typing = false;
        if (nav.select) {
            if (self.anchor == null) self.anchor = self.cursor;
        } else {
            const sel = self.selection();
            self.anchor = null;
            if (sel) |r| switch (nav.to) {
                .left => {
                    self.cursor = r.start;
                    return;
                },
                .right => {
                    self.cursor = r.end;
                    return;
                },
                else => {},
            };
        }
        self.cursor = self.target(nav.to);
    }

    /// Up and Down go by terminal row, keeping to `goal_col` across short rows.
    /// False when there is no row that way: the caller turns to the history.
    pub fn moveVertical(self: *Buffer, rows: []const Row, down: bool, select: bool, goal_col: *?usize) bool {
        const r = rowOf(rows, self.cursor);
        if ((down and r + 1 >= rows.len) or (!down and r == 0)) return false;
        self.typing = false;
        if (select) {
            if (self.anchor == null) self.anchor = self.cursor;
        } else self.anchor = null;
        const col = goal_col.* orelse colsBetween(self.text.items, rows[r].start, self.cursor);
        goal_col.* = col;
        self.cursor = byteAtCol(self.text.items, rows[if (down) r + 1 else r - 1], col);
        return true;
    }
};

test "Buffer: Shift extends a selection, typing replaces it, Right collapses to its far edge" {
    var b = try testBuffer("SELECT id FROM t", 7);
    defer b.deinit();
    b.move(.{ .to = .word_right, .select = true });
    try std.testing.expectEqual(Buffer.Range{ .start = 7, .end = 9 }, b.selection().?);
    try b.insert("name");
    try std.testing.expectEqualStrings("SELECT name FROM t", b.bytes());
    try std.testing.expect(b.selection() == null);

    b.move(.{ .to = .buf_home, .select = true });
    try std.testing.expectEqual(Buffer.Range{ .start = 0, .end = 11 }, b.selection().?);
    b.move(.{ .to = .right });
    try std.testing.expectEqual(@as(usize, 11), b.cursor);
    try std.testing.expect(b.selection() == null);
}

test "Buffer: cut returns the selection; select all; a multi-byte character moves as one" {
    var b = try testBuffer("a\xc3\xa7\xc3\xa3o;", 0);
    defer b.deinit();
    b.move(.{ .to = .right });
    b.move(.{ .to = .right });
    try std.testing.expectEqual(@as(usize, 3), b.cursor);
    b.move(.{ .to = .left, .select = true });
    const got = (try b.cut()).?;
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("\xc3\xa7", got);
    try std.testing.expectEqualStrings("a\xc3\xa3o;", b.bytes());
    b.selectAll();
    try std.testing.expectEqual(Buffer.Range{ .start = 0, .end = 5 }, b.selection().?);
}

test "Buffer: auto-close, skip-over, wrap a selection, and dedent a closer" {
    var b = try testBuffer("", 0);
    defer b.deinit();
    try b.typeChar('(');
    try std.testing.expectEqualStrings("()", b.bytes());
    try std.testing.expectEqual(@as(usize, 1), b.cursor);
    try b.typeChar('x');
    try b.typeChar(')');
    try std.testing.expectEqualStrings("(x)", b.bytes());
    try std.testing.expectEqual(@as(usize, 3), b.cursor);
    try b.set("it");
    try b.typeChar('\'');
    try std.testing.expectEqualStrings("it'", b.bytes());
    try b.set("SELECT a FROM t");
    b.cursor = 7;
    b.move(.{ .to = .word_right, .select = true });
    try b.typeChar('(');
    try std.testing.expectEqualStrings("SELECT (a) FROM t", b.bytes());
    try std.testing.expectEqual(Buffer.Range{ .start = 8, .end = 9 }, b.selection().?);
    try b.set("f(\n  ");
    try b.typeChar(')');
    try std.testing.expectEqualStrings("f(\n)", b.bytes());
}

test "Buffer: word travel stops at punctuation, Home toggles indent and line start" {
    var b = try testBuffer("  FROM erp.dbo.SC5010", 21);
    defer b.deinit();
    b.move(.{ .to = .word_left });
    try std.testing.expectEqual(@as(usize, 15), b.cursor);
    b.move(.{ .to = .word_left });
    try std.testing.expectEqual(@as(usize, 11), b.cursor);
    b.move(.{ .to = .home });
    try std.testing.expectEqual(@as(usize, 2), b.cursor);
    b.move(.{ .to = .home });
    try std.testing.expectEqual(@as(usize, 0), b.cursor);
    try b.delete(.word_right);
    try std.testing.expectEqualStrings(" erp.dbo.SC5010", b.bytes());
}

test "Buffer: newline starts at the margin; undo takes back a run of typing at once, redo returns it" {
    var b = try testBuffer("  WHERE x", 9);
    defer b.deinit();
    try b.newline();
    try std.testing.expectEqualStrings("  WHERE x\n", b.bytes());
    for ("AND") |c| try b.insert(&.{c});
    try std.testing.expectEqualStrings("  WHERE x\nAND", b.bytes());
    try b.undo();
    try std.testing.expectEqualStrings("  WHERE x\n", b.bytes());
    try b.undo();
    try std.testing.expectEqualStrings("  WHERE x", b.bytes());
    try b.redo();
    try b.redo();
    try std.testing.expectEqualStrings("  WHERE x\nAND", b.bytes());
}

test "Buffer: a word typed over a selection is one undo step, the selection the next" {
    var b = try testBuffer("SELECT id FROM t", 7);
    defer b.deinit();
    b.move(.{ .to = .word_right, .select = true });
    for ("name") |c| try b.insert(&.{c});
    try std.testing.expectEqualStrings("SELECT name FROM t", b.bytes());
    try b.undo();
    try std.testing.expectEqualStrings("SELECT id FROM t", b.bytes());
}

test "Buffer: Enter after an opener indents past the opener's line and moves the closer to its own line" {
    var b = try testBuffer("  WITH t AS ()", 13);
    defer b.deinit();
    try b.newline();
    try std.testing.expectEqualStrings("  WITH t AS (\n    \n  )", b.bytes());
    try std.testing.expectEqual(@as(usize, 18), b.cursor);
}

test "Buffer: lines move, duplicate, indent, dedent and comment as a block" {
    var b = try testBuffer("a\nb\nc", 2);
    defer b.deinit();
    try b.moveLines(true);
    try std.testing.expectEqualStrings("a\nc\nb", b.bytes());
    try std.testing.expectEqual(@as(usize, 4), b.cursor);
    try b.moveLines(false);
    try std.testing.expectEqualStrings("a\nb\nc", b.bytes());
    try b.duplicateLines(true);
    try std.testing.expectEqualStrings("a\nb\nb\nc", b.bytes());
    try std.testing.expectEqual(@as(usize, 4), b.cursor);
    b.anchor = 0;
    b.cursor = 3;
    try b.indentLines(false);
    try std.testing.expectEqualStrings("  a\n  b\nb\nc", b.bytes());
    try b.indentLines(true);
    try std.testing.expectEqualStrings("a\nb\nb\nc", b.bytes());
    try b.toggleComment();
    try std.testing.expectEqualStrings("-- a\n-- b\nb\nc", b.bytes());
    try b.toggleComment();
    try std.testing.expectEqualStrings("a\nb\nb\nc", b.bytes());
}

test "Buffer: the bracket under or before the cursor finds its mate" {
    var b = try testBuffer("f(a, (b))", 9);
    defer b.deinit();
    try std.testing.expectEqual([2]usize{ 1, 8 }, b.bracketPair().?);
    b.cursor = 5;
    try std.testing.expectEqual([2]usize{ 5, 7 }, b.bracketPair().?);
    b.cursor = 3;
    try std.testing.expect(b.bracketPair() == null);
}

//! The REPL's entry editor: a small multi-line text editor in the terminal, with
//! the habits of a code editor rather than of a shell prompt. Enter opens a new
//! line until the statement is complete and runs it once it is; arrows travel the
//! whole entry, Shift extends a selection, typing replaces it; there is undo, a
//! clipboard that reaches the system's, and a history of whole entries.
//!
//! No dependency, TTY only (the piped path never touches this). Raw mode is
//! entered per `readEntry` and the terminal is always restored before returning,
//! so query execution and error printing run under normal cooked-mode rules.
//!
//! What a terminal cannot give: the mouse (claiming it would take away the
//! terminal's own selection), multiple cursors, and any run key it keeps for
//! itself — Windows Terminal owns Alt+Enter and sends Ctrl+Enter as plain Enter.
//! So an unfinished entry runs with Ctrl+J, which every terminal delivers, and
//! with Ctrl+Enter where xterm's modifyOtherKeys is spoken.

const std = @import("std");
const hilite = @import("hilite.zig");

pub const Result = union(enum) { line: []u8, eof, interrupt };

pub const Nav = struct {
    to: To,
    /// Shift was held: extend the selection instead of dropping it.
    select: bool = false,

    pub const To = enum { left, right, up, down, home, end, word_left, word_right, buf_home, buf_end };
};

pub const Key = union(enum) {
    char: u8,
    enter,
    alt_enter,
    ctrl_enter,
    tab,
    backspace,
    delete,
    nav: Nav,
    page_up,
    page_down,
    escape, // a bare Esc: drop the selection
    shift_tab,
    line_up, // Alt-Up: move the line
    line_down,
    dup_up, // Shift-Alt-Up: duplicate the line
    dup_down,
    comment, // Ctrl-/ (0x1f)
    kill_end, // ^K
    kill_line, // ^U
    word_back, // ^W, Alt-Backspace, Ctrl-Backspace
    word_fwd_kill, // Alt-d, Ctrl-Delete
    select_all, // ^A
    undo, // ^Z
    redo, // ^Y
    cut, // ^X
    paste, // ^V, when the terminal passes it through
    paste_begin, // ESC [ 200 ~ : a bracketed paste follows
    clear, // ^L
    search, // ^R: search the history
    interrupt, // ^C: copy when something is selected, else drop the entry
    eof_or_delete, // ^D: EOF on an empty entry, delete-at-cursor otherwise
    none, // unrecognized escape — ignore
};

/// Decode one keypress from `fd` (one byte, plus the tail of an ESC sequence).
/// Split out so it is testable over a pipe.
/// A control byte (`^A` = 1 …) as a key; null where it means nothing here.
fn controlKey(c: u8) ?Key {
    return switch (c) {
        '\r' => .enter,
        '\n' => .ctrl_enter,
        '\t' => .tab,
        0x7f => .backspace,
        0x08 => .word_back,
        0x01 => .select_all,
        0x05 => .{ .nav = .{ .to = .end } },
        0x0b => .kill_end,
        0x15 => .kill_line,
        0x17 => .word_back,
        0x18 => .cut,
        0x16 => .paste,
        0x19 => .redo,
        0x1a => .undo,
        0x0c => .clear,
        0x12 => .search,
        0x03 => .interrupt,
        0x04 => .eof_or_delete,
        0x1f => .comment,
        else => null,
    };
}

/// Is there a byte to read on `fd` within `ms`? A lone Esc is followed by nothing;
/// the Esc that opens a key sequence is followed at once.
fn pending(fd: std.posix.fd_t, ms: i32) bool {
    var fds = [_]std.posix.pollfd{.{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 }};
    const n = std.posix.poll(&fds, ms) catch return true;
    return n > 0;
}

/// A key the terminal reported as a code plus modifiers — the xterm
/// `CSI 27;mod;code~` and kitty `CSI code;mod u` forms, which are how Ctrl+Enter
/// arrives when it arrives at all. `mask`: shift 1, alt 2, ctrl 4.
fn modifiedKey(code: u32, mask: u32) Key {
    const ctrl = mask & 4 != 0;
    const alt = mask & 2 != 0;
    // Ctrl+Enter, Shift+Enter (Jupyter's) and Alt+Enter all run; the plain key is Enter.
    if (code == 13 or code == 10) return if (ctrl or mask & 1 != 0) .ctrl_enter else if (alt) .alt_enter else .enter;
    if (code == 27) return .none;
    if (code > 0x7f) return .none;
    const c: u8 = @intCast(code);
    if (alt) return switch (c) {
        'b' => .{ .nav = .{ .to = .word_left } },
        'f' => .{ .nav = .{ .to = .word_right } },
        'd' => .word_fwd_kill,
        0x7f, 0x08 => .word_back,
        else => .none,
    };
    if (ctrl) return if (std.ascii.isAlphabetic(c)) (controlKey(std.ascii.toLower(c) & 0x1f) orelse .none) else .none;
    return controlKey(c) orelse if (c >= 0x20) Key{ .char = c } else .none;
}

pub fn readKey(fd: std.posix.fd_t) !Key {
    var b: [1]u8 = undefined;
    if (try std.posix.read(fd, &b) == 0) return .eof_or_delete;
    if (controlKey(b[0])) |k| return k;
    switch (b[0]) {
        0x1b => {
            if (!pending(fd, 40)) return .escape;
            if (try std.posix.read(fd, &b) == 0) return .none;
            switch (b[0]) {
                '[', 'O' => {},
                '\r', '\n' => return .alt_enter,
                'b' => return .{ .nav = .{ .to = .word_left } },
                'f' => return .{ .nav = .{ .to = .word_right } },
                'd' => return .word_fwd_kill,
                0x7f, 0x08 => return .word_back,
                else => return .none,
            }
            // A control sequence is parameters (`0-9 ; :` and the private markers)
            // and then one final byte. Read all of it whatever it turns out to be:
            // `ESC [ 1 ; 5 C` cut short at the `;` left `5C` to be typed into the line.
            var first: u32 = 0;
            var modifier: u32 = 0;
            var third: u32 = 0;
            var field: usize = 0;
            while (true) {
                if (try std.posix.read(fd, &b) == 0) return .none;
                switch (b[0]) {
                    '0'...'9' => {
                        const d = b[0] - '0';
                        if (field == 0) first = first *| 10 +| d else if (field == 1) modifier = modifier *| 10 +| d else if (field == 2) third = third *| 10 +| d;
                    },
                    ';', ':' => field += 1,
                    0x20...0x2f, '<', '=', '>', '?' => {},
                    else => break,
                }
            }
            // xterm's modifier parameter is 1 + a bitmask: shift 1, alt 2, ctrl 4.
            const mask = if (modifier > 1) modifier - 1 else 0;
            const shift = mask & 1 != 0;
            const alt = mask & 2 != 0;
            const word = mask & 0b110 != 0;
            return switch (b[0]) {
                'u' => modifiedKey(first, mask),
                'Z' => .shift_tab,
                'A' => if (alt) (if (shift) Key.dup_up else Key.line_up) else .{ .nav = .{ .to = .up, .select = shift } },
                'B' => if (alt) (if (shift) Key.dup_down else Key.line_down) else .{ .nav = .{ .to = .down, .select = shift } },
                'C' => .{ .nav = .{ .to = if (word) .word_right else .right, .select = shift } },
                'D' => .{ .nav = .{ .to = if (word) .word_left else .left, .select = shift } },
                'H' => .{ .nav = .{ .to = if (word) .buf_home else .home, .select = shift } },
                'F' => .{ .nav = .{ .to = if (word) .buf_end else .end, .select = shift } },
                '~' => switch (first) {
                    27 => modifiedKey(third, mask),
                    1, 7 => .{ .nav = .{ .to = if (word) .buf_home else .home, .select = shift } },
                    4, 8 => .{ .nav = .{ .to = if (word) .buf_end else .end, .select = shift } },
                    3 => if (word) .word_fwd_kill else .delete,
                    5 => .page_up,
                    6 => .page_down,
                    200 => .paste_begin,
                    else => .none,
                },
                else => .none,
            };
        },
        else => return if (b[0] >= 0x20) .{ .char = b[0] } else .none,
    }
}

/// One terminal row of the entry: the bytes `start..end` of the text. `head` marks
/// the first row of a logical line — the one that carries a prompt — as opposed to
/// the continuation of a line too long for the terminal.
pub const Row = struct { start: usize, end: usize, head: bool };

fn charLen(text: []const u8, i: usize) usize {
    var j = i + 1;
    while (j < text.len and text[j] & 0xC0 == 0x80) j += 1;
    return j - i;
}

/// A tab is drawn as two spaces: a real one would jump to the terminal's own tab
/// stop and the cursor arithmetic would no longer know where anything is.
fn charCols(c: u8) usize {
    return if (c == '\t') 2 else 1;
}

/// Break `text` into terminal rows `width` columns wide: one run per logical line,
/// wrapped where it is too long. Every byte belongs to exactly one row except the
/// newlines, which sit between them.
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

fn colsBetween(text: []const u8, from: usize, to: usize) usize {
    var n: usize = 0;
    var i = from;
    while (i < to) : (i += charLen(text, i)) n += charCols(text[i]);
    return n;
}

/// The byte in `row` that sits `col` columns in, or the row's end if it is shorter.
fn byteAtCol(text: []const u8, row: Row, col: usize) usize {
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

fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

/// The text being edited, its cursor and its selection — everything about editing
/// that does not need a terminal, so all of it is testable. Offsets are bytes and
/// always sit on a UTF-8 character boundary.
pub const Buffer = struct {
    gpa: std.mem.Allocator,
    text: std.array_list.Managed(u8),
    cursor: usize = 0,
    /// The fixed end of the selection; the cursor is the moving one. Null: none.
    anchor: ?usize = null,
    undo_stack: std.array_list.Managed(Snapshot),
    redo_stack: std.array_list.Managed(Snapshot),
    /// The last edit was plain typing, so the next keystroke joins its undo step
    /// instead of making a new one — undo takes back a run of typing, not a letter.
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

    /// Type or paste `s`, over the selection if there is one.
    pub fn insert(self: *Buffer, s: []const u8) !void {
        const plain = s.len == 1 and s[0] != '\n' and s[0] != ' ';
        const sel = self.selection();
        try self.checkpoint(plain and sel == null);
        if (sel) |r| self.removeRange(r);
        try self.text.insertSlice(self.cursor, s);
        self.cursor += s.len;
        self.anchor = null;
        // Typing over a selection starts a run too: the word that replaced it
        // comes back out as one undo step, and the selection with the next.
        self.typing = plain;
    }

    /// A new line, at the line's start — a prompt, not a code editor: an indent
    /// that followed the cursor down read as the cursor refusing to go home.
    /// Between `(` and `)` the new line steps in two past the opener's line, and
    /// the closer moves to a line of its own at that line's indent.
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

    /// The logical lines the selection touches (or the cursor's), as `start..end`
    /// where `end` is the last line's end, without its newline.
    fn lineBlock(self: *const Buffer) Range {
        const sel = self.selection() orelse Range{ .start = self.cursor, .end = self.cursor };
        // A selection ending at the very start of a line does not include that line.
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

    /// Alt-Up / Alt-Down: the current lines swap places with the neighbour.
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

    /// Shift-Alt-Up / Shift-Alt-Down: a copy of the current lines above or below.
    pub fn duplicateLines(self: *Buffer, down: bool) !void {
        const blk = self.lineBlock();
        const off = self.cursor - blk.start;
        const lines = try self.gpa.dupe(u8, self.text.items[blk.start..blk.end]);
        defer self.gpa.free(lines);
        const joined = try std.mem.concat(self.gpa, u8, &.{ lines, "\n", lines });
        defer self.gpa.free(joined);
        try self.replaceBlock(blk, joined, if (down) lines.len + 1 + off else off, false);
    }

    /// Tab / Shift-Tab over lines: two spaces in or out at the start of each.
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

    /// Ctrl-/: comment the lines out with `-- `, or back in when they all are.
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

    /// A typed character, with an editor's reflexes: an opener over a selection
    /// wraps it; an opener before a blank is paired with its closer; a closer that
    /// is already the next character is stepped over; `)` on an empty line takes
    /// the indent out first.
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

    /// The bracket paired with the one at or just before the cursor, as the two
    /// byte offsets, for the repaint to underline. Strings are not looked into.
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

    /// Travel. With `select` the anchor stays put and the cursor drags the
    /// selection; without it a selection collapses — and Left or Right collapse to
    /// its near edge rather than stepping past it.
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

pub const Editor = struct {
    gpa: std.mem.Allocator,
    in_fd: std.posix.fd_t,
    out: std.fs.File,
    hist: std.array_list.Managed([]u8),
    hist_path: ?[]u8 = null,
    /// What cut and copy last took, for ^V where the terminal passes it through.
    clipboard: ?[]u8 = null,
    /// The terminal row the cursor was left on by the last repaint, counted from
    /// the first row drawn; and which row of the entry that first one was — an
    /// entry taller than the terminal shows a window around the cursor.
    cursor_row: usize = 0,
    top: usize = 0,
    /// Colour the entry (off under `NO_COLOR`).
    color: bool = true,

    const max_history = 500;
    /// `» ` on the first line and, once there are more, each line's number in
    /// that same two-column place — the gutter grows only past nine lines, so the
    /// text never jumps as an entry gains a second line. Wrapped rows are blank there.
    const prompt_first = "\xc2\xbb";

    fn gutterWidth(text: []const u8) usize {
        const lines = std.mem.count(u8, text, "\n") + 1;
        var digits: usize = 1;
        var n = lines;
        while (n >= 10) : (n /= 10) digits += 1;
        return @max(2, digits + 1);
    }
    /// A newline inside a history entry, on disk: the file stays one entry per line.
    const hist_newline = 0x1f;

    pub const Options = struct {
        /// Is this text a whole entry, so that Enter should run it rather than
        /// open another line?
        complete: *const fn ([]const u8) bool,
        /// Tab: what the word at `cursor` could become. `items` are owned by
        /// `arena`; the editor replaces `text[start..cursor]` with one of them.
        suggest: ?*const fn (ctx: *anyopaque, arena: std.mem.Allocator, text: []const u8, cursor: usize) anyerror!Suggestions = null,
        suggest_ctx: *anyopaque = undefined,
    };

    pub const Suggestions = struct { start: usize = 0, items: []const []const u8 = &.{} };

    /// A completion in progress: the choices, which is lit, and where the word
    /// they replace begins. Any key but Tab dismisses it.
    const Menu = struct {
        arena: std.heap.ArenaAllocator,
        items: []const []const u8,
        start: usize,
        index: ?usize = null,
    };

    pub fn init(gpa: std.mem.Allocator) Editor {
        var self = Editor{
            .gpa = gpa,
            .in_fd = std.fs.File.stdin().handle,
            .out = std.fs.File.stderr(),
            .hist = std.array_list.Managed([]u8).init(gpa),
            .color = !std.process.hasEnvVarConstant("NO_COLOR"),
        };
        self.loadHistory();
        return self;
    }

    pub fn deinit(self: *Editor) void {
        for (self.hist.items) |h| self.gpa.free(h);
        self.hist.deinit();
        if (self.hist_path) |p| self.gpa.free(p);
        if (self.clipboard) |c| self.gpa.free(c);
    }

    /// `$HOME/.basalt_history`, last `max_history` entries. No HOME → in-memory only.
    fn loadHistory(self: *Editor) void {
        const home = std.process.getEnvVarOwned(self.gpa, "HOME") catch return;
        defer self.gpa.free(home);
        self.hist_path = std.fs.path.join(self.gpa, &.{ home, ".basalt_history" }) catch return;
        const text = std.fs.cwd().readFileAlloc(self.gpa, self.hist_path.?, 1 << 20) catch return;
        defer self.gpa.free(text);
        var it = std.mem.splitScalar(u8, text, '\n');
        while (it.next()) |ln| {
            if (ln.len == 0) continue;
            const copy = self.gpa.dupe(u8, ln) catch return;
            std.mem.replaceScalar(u8, copy, hist_newline, '\n');
            self.hist.append(copy) catch {
                self.gpa.free(copy);
                return;
            };
        }
        // Keep the tail; the file itself is rewritten from this trimmed set on add.
        while (self.hist.items.len > max_history) self.gpa.free(self.hist.orderedRemove(0));
    }

    /// Record an executed entry, all of its lines as one: skip blanks and immediate
    /// duplicates, append to the history file (best-effort).
    pub fn remember(self: *Editor, entry: []const u8) void {
        const t = std.mem.trim(u8, entry, " \t\r\n");
        if (t.len == 0) return;
        if (self.hist.items.len > 0 and std.mem.eql(u8, self.hist.items[self.hist.items.len - 1], t)) return;
        const copy = self.gpa.dupe(u8, t) catch return;
        self.hist.append(copy) catch {
            self.gpa.free(copy);
            return;
        };
        if (self.hist.items.len > max_history) self.gpa.free(self.hist.orderedRemove(0));
        const path = self.hist_path orelse return;
        const f = std.fs.cwd().createFile(path, .{ .truncate = false }) catch return;
        defer f.close();
        f.seekFromEnd(0) catch return;
        const flat = self.gpa.dupe(u8, t) catch return;
        defer self.gpa.free(flat);
        std.mem.replaceScalar(u8, flat, '\n', hist_newline);
        f.writeAll(flat) catch return;
        f.writeAll("\n") catch return;
    }

    fn write(self: *Editor, s: []const u8) void {
        self.out.writeAll(s) catch {};
    }

    /// Keep a copy for ^V and hand it to the system clipboard. OSC 52 is how a
    /// program inside a terminal reaches it; a terminal that does not know the
    /// sequence ignores it, and the internal copy still works.
    fn toClipboard(self: *Editor, s: []const u8) void {
        if (self.clipboard) |c| self.gpa.free(c);
        self.clipboard = self.gpa.dupe(u8, s) catch null;
        const enc = std.base64.standard.Encoder;
        const b64 = self.gpa.alloc(u8, enc.calcSize(s.len)) catch return;
        defer self.gpa.free(b64);
        self.write("\x1b]52;c;");
        self.write(enc.encode(b64, s));
        self.write("\x07");
    }

    fn textWidth(size: TermSize, gutter: usize) usize {
        return if (size.cols > gutter + 1) size.cols - gutter - 1 else 2;
    }

    /// Repaint the whole entry and park the cursor. It starts from the first row
    /// drawn last time and clears everything below, so rows a shorter entry no
    /// longer needs do not linger.
    fn redraw(self: *Editor, buf: *const Buffer) void {
        self.redrawWith(buf, null);
    }

    fn redrawWith(self: *Editor, buf: *const Buffer, menu: ?*const Menu) void {
        const size = termSize(self.out);
        const text = buf.bytes();
        const gutter = gutterWidth(text);
        const styles = self.gpa.alloc(hilite.Style, text.len) catch return;
        defer self.gpa.free(styles);
        hilite.scan(text, styles);
        const pair = buf.bracketPair();
        const rows = layoutRows(self.gpa, text, textWidth(size, gutter)) catch return;
        defer self.gpa.free(rows);
        const cur = rowOf(rows, buf.cursor);
        const room = if (size.rows > 1) size.rows - 1 else 1;
        if (cur < self.top) self.top = cur;
        if (cur >= self.top + room) self.top = cur + 1 - room;
        const last = @min(rows.len, self.top + room);

        var out = std.array_list.Managed(u8).init(self.gpa);
        defer out.deinit();
        const w = out.writer();
        if (self.cursor_row > 0) w.print("\x1b[{d}A", .{self.cursor_row}) catch return;
        w.writeAll("\r\x1b[J") catch return;
        const sel = buf.selection();
        var line_no: usize = 0;
        for (rows[0..last], 0..) |r, ri| {
            if (r.head) line_no += 1;
            if (ri < self.top) continue;
            if (ri > self.top) w.writeAll("\r\n") catch return;
            if (!r.head) {
                w.writeByteNTimes(' ', gutter) catch return;
            } else if (r.start == 0) {
                w.writeByteNTimes(' ', gutter - 2) catch return;
                w.writeAll(prompt_first ++ " ") catch return;
            } else {
                if (self.color) w.writeAll("\x1b[2m") catch return;
                var nbuf: [24]u8 = undefined;
                const num = std.fmt.bufPrint(&nbuf, "{d}", .{line_no}) catch return;
                w.writeByteNTimes(' ', gutter - 1 - num.len) catch return;
                w.writeAll(num) catch return;
                w.writeAll(" ") catch return;
                if (self.color) w.writeAll("\x1b[22m") catch return;
            }
            var i = r.start;
            var lit = false;
            var style: hilite.Style = .plain;
            while (i < r.end) {
                const in_sel = if (sel) |s| i >= s.start and i < s.end else false;
                if (in_sel != lit) {
                    w.writeAll(if (in_sel) "\x1b[7m" else "\x1b[27m") catch return;
                    lit = in_sel;
                }
                if (self.color and styles[i] != style) {
                    style = styles[i];
                    w.writeAll(hilite.sgr(style)) catch return;
                }
                const n = charLen(text, i);
                const mate = if (pair) |p| (i == p[0] or i == p[1]) else false;
                if (mate) w.writeAll("\x1b[4m") catch return;
                w.writeAll(if (text[i] == '\t') "  " else text[i .. i + n]) catch return;
                if (mate) w.writeAll("\x1b[24m") catch return;
                i += n;
            }
            if (self.color and style != .plain) w.writeAll(hilite.sgr_reset) catch return;
            // A selected line break shows as one lit cell past the row's end.
            const nl_sel = if (sel) |s| r.end < text.len and text[r.end] == '\n' and r.end >= s.start and r.end < s.end else false;
            if (nl_sel and !lit) w.writeAll("\x1b[7m") catch return;
            if (nl_sel) w.writeAll(" ") catch return;
            if (lit or nl_sel) w.writeAll("\x1b[27m") catch return;
        }
        // The choices, one row under the entry: as many as fit, the lit one in
        // reverse video, `…` when there are more.
        var extra: usize = 0;
        if (menu) |m| {
            w.writeAll("\r\n") catch return;
            var used: usize = 0;
            for (m.items, 0..) |it, i| {
                const need = colsBetween(it, 0, it.len) + 2;
                if (used + need + 1 > size.cols) {
                    w.writeAll("…") catch return;
                    break;
                }
                if (m.index == i) w.writeAll("\x1b[7m") catch return else if (self.color) w.writeAll("\x1b[2m") catch return;
                w.writeAll(it) catch return;
                w.writeAll("\x1b[0m  ") catch return;
                used += need;
            }
            extra = 1;
        }
        if (last - 1 + extra > cur) w.print("\x1b[{d}A", .{last - 1 + extra - cur}) catch return;
        w.print("\x1b[{d}G", .{gutter + colsBetween(text, rows[cur].start, buf.cursor) + 1}) catch return;
        self.write(out.items);
        self.cursor_row = cur - self.top;
    }

    /// Tab. With a menu up, the next choice replaces the word; otherwise the
    /// provider is asked, a lone answer is taken, and several fill in what they
    /// share and come up as a menu.
    fn completeWord(self: *Editor, buf: *Buffer, opts: Options, menu: *?Menu) !void {
        if (menu.*) |*m| {
            const i = if (m.index) |i| (i + 1) % m.items.len else 0;
            m.index = i;
            try self.replaceWord(buf, m.start, m.items[i]);
            return;
        }
        const suggest = opts.suggest orelse return buf.insert("  ");
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        errdefer arena.deinit();
        const s = try suggest(opts.suggest_ctx, arena.allocator(), buf.bytes(), buf.cursor);
        if (s.items.len == 0) {
            arena.deinit();
            return;
        }
        if (s.items.len == 1) {
            try self.replaceWord(buf, s.start, s.items[0]);
            arena.deinit();
            return;
        }
        var n = s.items[0].len;
        for (s.items[1..]) |it| {
            var i: usize = 0;
            while (i < n and i < it.len and std.ascii.toLower(it[i]) == std.ascii.toLower(s.items[0][i])) i += 1;
            n = i;
        }
        if (n > buf.cursor - s.start) try self.replaceWord(buf, s.start, s.items[0][0..n]);
        menu.* = .{ .arena = arena, .items = s.items, .start = s.start };
    }

    /// ^R: type to narrow, ^R again for an older match, Enter or an arrow keeps
    /// the match in the entry, Esc or ^C puts the entry back as it was. The match
    /// is shown in the entry itself, the query on the row under it.
    fn searchHistory(self: *Editor, buf: *Buffer) !void {
        const original = try self.gpa.dupe(u8, buf.bytes());
        defer self.gpa.free(original);
        var query = std.array_list.Managed(u8).init(self.gpa);
        defer query.deinit();
        var from: usize = self.hist.items.len;
        var found: ?usize = null;
        while (true) {
            var status = std.array_list.Managed(u8).init(self.gpa);
            defer status.deinit();
            try status.writer().print("(reverse-i-search) '{s}'{s}", .{ query.items, if (found == null and query.items.len > 0) "  — no match" else "" });
            const items = [_][]const u8{status.items};
            const m = Menu{ .arena = std.heap.ArenaAllocator.init(self.gpa), .items = &items, .start = 0 };
            self.redrawWith(buf, &m);
            switch (try readKey(self.in_fd)) {
                .char => |c| {
                    try query.append(c);
                    from = self.hist.items.len;
                },
                .backspace => {
                    if (query.items.len == 0) continue;
                    query.shrinkRetainingCapacity(query.items.len - 1);
                    from = self.hist.items.len;
                },
                .search => if (found) |f| {
                    from = f;
                } else continue,
                .escape, .interrupt => {
                    try buf.set(original);
                    return;
                },
                .enter, .nav, .tab => return,
                else => continue,
            }
            found = historyMatch(self.hist.items, query.items, from);
            if (found) |f| try buf.set(self.hist.items[f]);
        }
    }

    fn replaceWord(self: *Editor, buf: *Buffer, start: usize, with: []const u8) !void {
        _ = self;
        buf.anchor = start;
        try buf.insert(with);
    }

    /// Text arriving between the bracketed-paste markers: taken as typed, never as
    /// keys — a pasted newline is a new line, not a request to run half a script.
    fn readPaste(self: *Editor, buf: *Buffer) !void {
        var pasted = std.array_list.Managed(u8).init(self.gpa);
        defer pasted.deinit();
        const end = "\x1b[201~";
        var b: [1]u8 = undefined;
        var prev_cr = false;
        while (try std.posix.read(self.in_fd, &b) != 0) {
            // CRLF, CR and LF all mean one line break.
            if (b[0] == '\n' and prev_cr) {
                prev_cr = false;
                continue;
            }
            prev_cr = b[0] == '\r';
            try pasted.append(if (b[0] == '\r') '\n' else b[0]);
            if (std.mem.endsWith(u8, pasted.items, end)) {
                pasted.shrinkRetainingCapacity(pasted.items.len - end.len);
                break;
            }
        }
        try buf.insert(pasted.items);
    }

    /// Read one entry with editing. The returned `.line` is gpa-owned by the caller
    /// and may hold several lines.
    pub fn readEntry(self: *Editor, opts: Options) !Result {
        const orig = try std.posix.tcgetattr(self.in_fd);
        var raw = orig;
        raw.lflag.ECHO = false;
        raw.lflag.ICANON = false;
        raw.lflag.ISIG = false; // ^C arrives as a byte; execution re-arms normal signals
        raw.lflag.IEXTEN = false;
        raw.iflag.IXON = false;
        // Bytes as sent: with CR→LF translation on, a pasted CRLF reads as two
        // line feeds and every line of a Windows paste comes out double-spaced.
        raw.iflag.ICRNL = false;
        raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
        raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;
        // .NOW, not .FLUSH: flushing would eat the tail of a multi-line paste.
        try std.posix.tcsetattr(self.in_fd, .NOW, raw);
        defer std.posix.tcsetattr(self.in_fd, .NOW, orig) catch {};
        self.write("\x1b[?2004h");
        defer self.write("\x1b[?2004l");

        // Ask for Ctrl+Enter: xterm's modifyOtherKeys, level 1, which touches only
        // keys with no control code of their own. A terminal that does not know
        // it ignores it.
        self.write("\x1b[>4;1m");
        defer self.write("\x1b[>4;0m");

        var buf = Buffer.init(self.gpa);
        defer buf.deinit();
        var hist_pos: usize = self.hist.items.len;
        var stash: ?[]u8 = null; // the entry in progress while browsing history
        defer if (stash) |s| self.gpa.free(s);
        var goal_col: ?usize = null;
        var menu: ?Menu = null;
        defer if (menu) |*m| m.arena.deinit();

        self.cursor_row = 0;
        self.top = 0;
        self.redraw(&buf);
        while (true) {
            const key = try readKey(self.in_fd);
            if (!(key == .nav and (key.nav.to == .up or key.nav.to == .down))) goal_col = null;
            if (key != .tab) if (menu) |*m| {
                m.arena.deinit();
                menu = null;
            };
            switch (key) {
                .enter, .alt_enter, .ctrl_enter => {
                    const text = buf.bytes();
                    // Enter runs a finished entry only from its end; in the middle, or
                    // with no `;` yet, it is an editor's Enter — a new line. Ctrl+J and
                    // Ctrl+Enter run whatever is there, from anywhere.
                    const at_end = buf.cursor == text.len;
                    const meta = std.mem.startsWith(u8, std.mem.trimLeft(u8, text, " \t"), "\\") and std.mem.indexOfScalar(u8, text, '\n') == null;
                    const run = key != .enter or meta or (at_end and opts.complete(text));
                    if (run) {
                        buf.anchor = null;
                        buf.cursor = text.len;
                        self.redraw(&buf);
                        self.write("\r\n");
                        return .{ .line = try self.gpa.dupe(u8, text) };
                    }
                    try buf.newline();
                },
                .interrupt => {
                    if (buf.selection()) |r| {
                        self.toClipboard(buf.bytes()[r.start..r.end]);
                    } else {
                        buf.cursor = buf.bytes().len;
                        self.redraw(&buf);
                        self.write("^C\r\n");
                        return .interrupt;
                    }
                },
                .eof_or_delete => {
                    if (buf.bytes().len == 0) {
                        self.write("\r\n");
                        return .eof;
                    }
                    try buf.delete(.right);
                },
                .char => |c| try buf.typeChar(c),
                .tab => if (buf.selection() != null and std.mem.indexOfScalar(u8, buf.bytes()[buf.selection().?.start..buf.selection().?.end], '\n') != null) {
                    try buf.indentLines(false);
                } else if (buf.cursor > 0 and buf.bytes()[buf.cursor - 1] != ' ' and buf.bytes()[buf.cursor - 1] != '\n') {
                    try self.completeWord(&buf, opts, &menu);
                } else try buf.insert("  "),
                .shift_tab => try buf.indentLines(true),
                .escape => {
                    buf.anchor = null;
                    buf.typing = false;
                },
                .line_up => try buf.moveLines(false),
                .line_down => try buf.moveLines(true),
                .dup_up => try buf.duplicateLines(false),
                .dup_down => try buf.duplicateLines(true),
                .comment => try buf.toggleComment(),
                .backspace => try buf.delete(.left),
                .delete => try buf.delete(.right),
                .word_back => try buf.delete(.word_left),
                .word_fwd_kill => try buf.delete(.word_right),
                .kill_end => try buf.delete(.end),
                .kill_line => try buf.killLine(),
                .select_all => buf.selectAll(),
                .undo => try buf.undo(),
                .redo => try buf.redo(),
                .cut => if (try buf.cut()) |s| {
                    defer self.gpa.free(s);
                    self.toClipboard(s);
                },
                .paste => if (self.clipboard) |c| try buf.insert(c),
                .paste_begin => try self.readPaste(&buf),
                .clear => {
                    self.write("\x1b[2J\x1b[H");
                    self.cursor_row = 0;
                },
                .nav => |nav| switch (nav.to) {
                    .up, .down => {
                        const rows = try layoutRows(self.gpa, buf.bytes(), textWidth(termSize(self.out), gutterWidth(buf.bytes())));
                        defer self.gpa.free(rows);
                        const down = nav.to == .down;
                        if (!buf.moveVertical(rows, down, nav.select, &goal_col) and !nav.select) {
                            // Off the top or bottom of the entry: the history.
                            if (!down and hist_pos > 0) {
                                if (hist_pos == self.hist.items.len and stash == null) stash = try self.gpa.dupe(u8, buf.bytes());
                                hist_pos -= 1;
                                try buf.set(self.hist.items[hist_pos]);
                            } else if (down and hist_pos < self.hist.items.len) {
                                hist_pos += 1;
                                try buf.set(if (hist_pos == self.hist.items.len) (stash orelse "") else self.hist.items[hist_pos]);
                            }
                        }
                    },
                    else => buf.move(nav),
                },
                .search => try self.searchHistory(&buf),
                .page_up, .page_down, .none => {},
            }
            self.redrawWith(&buf, if (menu) |*m| m else null);
        }
    }
};

/// The newest history entry before `from` that contains `query` (any case), or
/// null. An empty query matches nothing: ^R alone shows the prompt, not a line.
pub fn historyMatch(hist: []const []const u8, query: []const u8, from: usize) ?usize {
    if (query.len == 0) return null;
    var i = @min(from, hist.len);
    while (i > 0) {
        i -= 1;
        if (std.ascii.indexOfIgnoreCase(hist[i], query) != null) return i;
    }
    return null;
}

pub const TermSize = struct { cols: usize = 80, rows: usize = 24 };

fn termSize(file: std.fs.File) TermSize {
    var ws: std.posix.winsize = undefined;
    const rc = std.posix.system.ioctl(file.handle, std.posix.T.IOCGWINSZ, @intFromPtr(&ws));
    if (std.posix.errno(rc) != .SUCCESS or ws.col == 0) return .{};
    return .{ .cols = ws.col, .rows = if (ws.row == 0) 24 else ws.row };
}

fn testBuffer(text: []const u8, cursor: usize) !Buffer {
    var b = Buffer.init(std.testing.allocator);
    try b.set(text);
    b.cursor = cursor;
    return b;
}

test "layoutRows: logical lines, wrapped rows, and which row a cursor on the seam belongs to" {
    const gpa = std.testing.allocator;
    const rows = try layoutRows(gpa, "SELECT 1\nFROM abcdefghij\n", 8);
    defer gpa.free(rows);
    // "SELECT 1" | "FROM abc" + "defghij" | ""
    try std.testing.expectEqual(@as(usize, 4), rows.len);
    try std.testing.expectEqual(Row{ .start = 0, .end = 8, .head = true }, rows[0]);
    try std.testing.expectEqual(Row{ .start = 9, .end = 17, .head = true }, rows[1]);
    try std.testing.expectEqual(Row{ .start = 17, .end = 24, .head = false }, rows[2]);
    try std.testing.expectEqual(Row{ .start = 25, .end = 25, .head = true }, rows[3]);
    // End of a logical line stays on it; the seam of a wrap is the next row's start.
    try std.testing.expectEqual(@as(usize, 0), rowOf(rows, 8));
    try std.testing.expectEqual(@as(usize, 2), rowOf(rows, 17));
    try std.testing.expectEqual(@as(usize, 3), rowOf(rows, 25));
}

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

test "Buffer: Up and Down keep their column across a short row and report the edge" {
    const gpa = std.testing.allocator;
    var b = try testBuffer("SELECT id,\n  x\nFROM orders", 8);
    defer b.deinit();
    const rows = try layoutRows(gpa, b.bytes(), 80);
    defer gpa.free(rows);
    var goal: ?usize = null;
    try std.testing.expect(b.moveVertical(rows, true, false, &goal));
    try std.testing.expectEqual(@as(usize, 14), b.cursor); // end of "  x", column 3
    try std.testing.expect(b.moveVertical(rows, true, false, &goal));
    try std.testing.expectEqual(@as(usize, 23), b.cursor); // column 8 again
    try std.testing.expect(!b.moveVertical(rows, true, false, &goal));
    try std.testing.expect(b.moveVertical(rows, false, true, &goal));
    try std.testing.expect(b.selection() != null);
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
    // `it's`: no pairing after a word character.
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

test "historyMatch: newest first, older on repeat, any case, nothing for an empty query" {
    const hist = [_][]const u8{ "SELECT 1;", "select id FROM orders;", "LOAD INTO x AS SELECT 2;" };
    try std.testing.expectEqual(@as(?usize, 2), historyMatch(&hist, "select", hist.len));
    try std.testing.expectEqual(@as(?usize, 1), historyMatch(&hist, "select", 2));
    try std.testing.expectEqual(@as(?usize, 1), historyMatch(&hist, "ORDERS", hist.len));
    try std.testing.expectEqual(@as(?usize, null), historyMatch(&hist, "", hist.len));
    try std.testing.expectEqual(@as(?usize, null), historyMatch(&hist, "nope", hist.len));
}

test "readKey: modified arrows carry Shift and word, and the editor's control keys decode" {
    const fds = try std.posix.pipe();
    defer std.posix.close(fds[0]);
    const w = fds[1];
    // Ctrl-Right, Shift-Left, Ctrl-Shift-Right, Shift-Up, Ctrl-Home, Shift-End,
    // Ctrl-Delete, Alt-b, Alt-Enter, ^A ^Z ^Y ^X, paste marker, an unknown
    // sequence, then a plain byte that must survive it.
    _ = try std.posix.write(w, "\x1b[1;5C\x1b[1;2D\x1b[1;6C\x1b[1;2A\x1b[1;5H\x1b[1;2F\x1b[3;5~\x1bb\x1b\r\x01\x1a\x19\x18\x1b[200~\x1b[?25;9zq\x1b[27;5;13~\x1b[13;5u\x1b[13;3u\x1b[98;3u\x1b[27u\x1b[13;2u\n\x1b[1;3A\x1b[1;4B\x1b[Z\x1f");
    std.posix.close(w);
    const expect = [_]Key{
        .{ .nav = .{ .to = .word_right } },
        .{ .nav = .{ .to = .left, .select = true } },
        .{ .nav = .{ .to = .word_right, .select = true } },
        .{ .nav = .{ .to = .up, .select = true } },
        .{ .nav = .{ .to = .buf_home } },
        .{ .nav = .{ .to = .end, .select = true } },
        .word_fwd_kill,
        .{ .nav = .{ .to = .word_left } },
        .alt_enter,
        .select_all,
        .undo,
        .redo,
        .cut,
        .paste_begin,
        .none,
        .{ .char = 'q' },
        // Ctrl+Enter in the xterm and kitty forms, Alt+Enter and Alt-b in kitty's, a bare Esc.
        .ctrl_enter,
        .ctrl_enter,
        .alt_enter,
        .{ .nav = .{ .to = .word_left } },
        .none,
        .ctrl_enter, // Shift+Enter, kitty form
        .ctrl_enter, // ^J
        .line_up,
        .dup_down,
        .shift_tab,
        .comment,
    };
    for (expect) |want| try std.testing.expectEqualDeep(want, try readKey(fds[0]));
}

test "readKey decodes plain escape sequences, controls, and bytes" {
    const fds = try std.posix.pipe();
    defer std.posix.close(fds[0]);
    const w = fds[1];
    _ = try std.posix.write(w, "a\x1b[A\x1b[B\x1b[C\x1b[D\x1b[3~\x1b[1~\x1bOF\x7f\r\x03\x05\x0b\x15\x1b[5~\x1b[6~\t");
    std.posix.close(w);
    const expect = [_]Key{
        .{ .char = 'a' },
        .{ .nav = .{ .to = .up } },
        .{ .nav = .{ .to = .down } },
        .{ .nav = .{ .to = .right } },
        .{ .nav = .{ .to = .left } },
        .delete,
        .{ .nav = .{ .to = .home } },
        .{ .nav = .{ .to = .end } },
        .backspace,
        .enter,
        .interrupt,
        .{ .nav = .{ .to = .end } },
        .kill_end,
        .kill_line,
        .page_up,
        .page_down,
        .tab,
    };
    for (expect) |want| try std.testing.expectEqualDeep(want, try readKey(fds[0]));
    try std.testing.expectEqual(Key.eof_or_delete, try readKey(fds[0]));
}

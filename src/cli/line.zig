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
//! terminal's own selection) and Ctrl+Enter, which arrives as plain Enter —
//! Alt+Enter runs an entry as it stands instead.

const std = @import("std");

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
    tab,
    backspace,
    delete,
    nav: Nav,
    page_up,
    page_down,
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
    interrupt, // ^C: copy when something is selected, else drop the entry
    eof_or_delete, // ^D: EOF on an empty entry, delete-at-cursor otherwise
    none, // unrecognized escape — ignore
};

/// Decode one keypress from `fd` (one byte, plus the tail of an ESC sequence).
/// Split out so it is testable over a pipe.
pub fn readKey(fd: std.posix.fd_t) !Key {
    var b: [1]u8 = undefined;
    if (try std.posix.read(fd, &b) == 0) return .eof_or_delete;
    switch (b[0]) {
        '\r', '\n' => return .enter,
        '\t' => return .tab,
        0x7f => return .backspace,
        0x08 => return .word_back,
        0x01 => return .select_all,
        0x05 => return .{ .nav = .{ .to = .end } },
        0x0b => return .kill_end,
        0x15 => return .kill_line,
        0x17 => return .word_back,
        0x18 => return .cut,
        0x16 => return .paste,
        0x19 => return .redo,
        0x1a => return .undo,
        0x0c => return .clear,
        0x03 => return .interrupt,
        0x04 => return .eof_or_delete,
        0x1b => {
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
            var field: usize = 0;
            while (true) {
                if (try std.posix.read(fd, &b) == 0) return .none;
                switch (b[0]) {
                    '0'...'9' => {
                        const d = b[0] - '0';
                        if (field == 0) first = first *| 10 +| d else if (field == 1) modifier = modifier *| 10 +| d;
                    },
                    ';', ':' => field += 1,
                    0x20...0x2f, '<', '=', '>', '?' => {},
                    else => break,
                }
            }
            // xterm's modifier parameter is 1 + a bitmask: shift 1, alt 2, ctrl 4.
            const mask = if (modifier > 1) modifier - 1 else 0;
            const shift = mask & 1 != 0;
            const word = mask & 0b110 != 0;
            return switch (b[0]) {
                'A' => .{ .nav = .{ .to = .up, .select = shift } },
                'B' => .{ .nav = .{ .to = .down, .select = shift } },
                'C' => .{ .nav = .{ .to = if (word) .word_right else .right, .select = shift } },
                'D' => .{ .nav = .{ .to = if (word) .word_left else .left, .select = shift } },
                'H' => .{ .nav = .{ .to = if (word) .buf_home else .home, .select = shift } },
                'F' => .{ .nav = .{ .to = if (word) .buf_end else .end, .select = shift } },
                '~' => switch (first) {
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
        else => {
            if (b[0] >= 0x20 or b[0] >= 0x80) return .{ .char = b[0] };
            return .none;
        },
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

    /// A new line that starts where the current one does: the indent carries over.
    pub fn newline(self: *Buffer) !void {
        const t = self.text.items;
        const ls = self.lineStart(self.cursor);
        var ind = ls;
        while (ind < self.cursor and (t[ind] == ' ' or t[ind] == '\t')) ind += 1;
        var buf: [128]u8 = undefined;
        const n = @min(ind - ls, buf.len - 1);
        buf[0] = '\n';
        @memcpy(buf[1 .. 1 + n], t[ls .. ls + n]);
        try self.insert(buf[0 .. 1 + n]);
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

    const max_history = 500;
    /// Both prompts are this wide and wrapped rows are indented to match, so every
    /// row of an entry has the same room.
    const gutter = 2;
    const prompt_first = "\xc2\xbb ";
    const prompt_more = "\xe2\x80\xa6 ";
    /// A newline inside a history entry, on disk: the file stays one entry per line.
    const hist_newline = 0x1f;

    pub const Options = struct {
        /// Is this text a whole entry, so that Enter should run it rather than
        /// open another line?
        complete: *const fn ([]const u8) bool,
    };

    pub fn init(gpa: std.mem.Allocator) Editor {
        var self = Editor{
            .gpa = gpa,
            .in_fd = std.fs.File.stdin().handle,
            .out = std.fs.File.stderr(),
            .hist = std.array_list.Managed([]u8).init(gpa),
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

    fn textWidth(size: TermSize) usize {
        return if (size.cols > gutter + 1) size.cols - gutter - 1 else 2;
    }

    /// Repaint the whole entry and park the cursor. It starts from the first row
    /// drawn last time and clears everything below, so rows a shorter entry no
    /// longer needs do not linger.
    fn redraw(self: *Editor, buf: *const Buffer) void {
        const size = termSize(self.out);
        const text = buf.bytes();
        const rows = layoutRows(self.gpa, text, textWidth(size)) catch return;
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
        for (rows[self.top..last], self.top..) |r, ri| {
            if (ri > self.top) w.writeAll("\r\n") catch return;
            w.writeAll(if (!r.head) "  " else if (r.start == 0) prompt_first else prompt_more) catch return;
            var i = r.start;
            var lit = false;
            while (i < r.end) {
                const in_sel = if (sel) |s| i >= s.start and i < s.end else false;
                if (in_sel != lit) {
                    w.writeAll(if (in_sel) "\x1b[7m" else "\x1b[27m") catch return;
                    lit = in_sel;
                }
                const n = charLen(text, i);
                w.writeAll(if (text[i] == '\t') "  " else text[i .. i + n]) catch return;
                i += n;
            }
            // A selected line break shows as one lit cell past the row's end.
            const nl_sel = if (sel) |s| r.end < text.len and text[r.end] == '\n' and r.end >= s.start and r.end < s.end else false;
            if (nl_sel and !lit) w.writeAll("\x1b[7m") catch return;
            if (nl_sel) w.writeAll(" ") catch return;
            if (lit or nl_sel) w.writeAll("\x1b[27m") catch return;
        }
        if (last - 1 > cur) w.print("\x1b[{d}A", .{last - 1 - cur}) catch return;
        w.print("\x1b[{d}G", .{gutter + colsBetween(text, rows[cur].start, buf.cursor) + 1}) catch return;
        self.write(out.items);
        self.cursor_row = cur - self.top;
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

        var buf = Buffer.init(self.gpa);
        defer buf.deinit();
        var hist_pos: usize = self.hist.items.len;
        var stash: ?[]u8 = null; // the entry in progress while browsing history
        defer if (stash) |s| self.gpa.free(s);
        var goal_col: ?usize = null;

        self.cursor_row = 0;
        self.top = 0;
        self.redraw(&buf);
        while (true) {
            const key = try readKey(self.in_fd);
            if (!(key == .nav and (key.nav.to == .up or key.nav.to == .down))) goal_col = null;
            switch (key) {
                .enter, .alt_enter => {
                    const text = buf.bytes();
                    // Enter on an empty last line runs the entry too: the way out
                    // when the statement has no `;` to end on.
                    const blank_tail = buf.cursor == text.len and text.len > 0 and text[text.len - 1] == '\n';
                    if (key == .alt_enter or blank_tail or opts.complete(text)) {
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
                .char => |c| try buf.insert(&.{c}),
                .tab => try buf.insert("  "),
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
                        const rows = try layoutRows(self.gpa, buf.bytes(), textWidth(termSize(self.out)));
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
                .page_up, .page_down, .none => {},
            }
            self.redraw(&buf);
        }
    }
};

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

test "Buffer: newline keeps the indent; undo takes back a run of typing at once, redo returns it" {
    var b = try testBuffer("  WHERE x", 9);
    defer b.deinit();
    try b.newline();
    try std.testing.expectEqualStrings("  WHERE x\n  ", b.bytes());
    for ("AND") |c| try b.insert(&.{c});
    try std.testing.expectEqualStrings("  WHERE x\n  AND", b.bytes());
    try b.undo();
    try std.testing.expectEqualStrings("  WHERE x\n  ", b.bytes());
    try b.undo();
    try std.testing.expectEqualStrings("  WHERE x", b.bytes());
    try b.redo();
    try b.redo();
    try std.testing.expectEqualStrings("  WHERE x\n  AND", b.bytes());
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

test "readKey: modified arrows carry Shift and word, and the editor's control keys decode" {
    const fds = try std.posix.pipe();
    defer std.posix.close(fds[0]);
    const w = fds[1];
    // Ctrl-Right, Shift-Left, Ctrl-Shift-Right, Shift-Up, Ctrl-Home, Shift-End,
    // Ctrl-Delete, Alt-b, Alt-Enter, ^A ^Z ^Y ^X, paste marker, an unknown
    // sequence, then a plain byte that must survive it.
    _ = try std.posix.write(w, "\x1b[1;5C\x1b[1;2D\x1b[1;6C\x1b[1;2A\x1b[1;5H\x1b[1;2F\x1b[3;5~\x1bb\x1b\r\x01\x1a\x19\x18\x1b[200~\x1b[?25;9zq");
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

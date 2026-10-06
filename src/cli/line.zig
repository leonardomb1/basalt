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
//!
//! `Buffer` is everything about editing that needs no terminal, so all of it is
//! testable; offsets are bytes and always sit on a UTF-8 boundary. Plain typing
//! joins one undo step, so undo takes back a run of typing, not a letter. `Editor`
//! adds the terminal: an entry taller than the screen is drawn as a window around
//! the cursor, and the prompt is `»` on the first line and each line's number in
//! that same two-column place after, the gutter growing only past nine lines so
//! text never jumps as an entry gains a second line. History lives in
//! `$HOME/.basalt_history` (in memory only without HOME), one entry per line with
//! an entry's own newlines stored as 0x1f.
//!
//! This file is the `Editor`: drawing, completion, history and the key loop. The
//! text being edited is `line/buffer.zig`, key decoding `line/keys.zig`, and how
//! text wraps into rows `line/layout.zig`.

const std = @import("std");
const hilite = @import("hilite.zig");

pub const Result = union(enum) { line: []u8, eof, interrupt };

pub const Nav = @import("line/keys.zig").Nav;
pub const Key = @import("line/keys.zig").Key;
pub const readKey = @import("line/keys.zig").readKey;
pub const Row = @import("line/layout.zig").Row;
const charLen = @import("line/layout.zig").charLen;
pub const layoutRows = @import("line/layout.zig").layoutRows;
pub const rowOf = @import("line/layout.zig").rowOf;
const colsBetween = @import("line/layout.zig").colsBetween;
pub const Buffer = @import("line/buffer.zig").Buffer;
const testBuffer = @import("line/testing_util.zig").testBuffer;

pub const Editor = struct {
    gpa: std.mem.Allocator,
    in_fd: std.posix.fd_t,
    out: std.fs.File,
    hist: std.array_list.Managed([]u8),
    hist_path: ?[]u8 = null,
    clipboard: ?[]u8 = null,
    cursor_row: usize = 0,
    top: usize = 0,
    color: bool = true,

    const max_history = 500;
    const prompt_first = "\xc2\xbb";

    fn gutterWidth(text: []const u8) usize {
        const lines = std.mem.count(u8, text, "\n") + 1;
        var digits: usize = 1;
        var n = lines;
        while (n >= 10) : (n /= 10) digits += 1;
        return @max(2, digits + 1);
    }
    const hist_newline = 0x1f;

    pub const Options = struct {
        complete: *const fn (ctx: *anyopaque, text: []const u8) bool,
        complete_ctx: *anyopaque = undefined,
        suggest: ?*const fn (ctx: *anyopaque, arena: std.mem.Allocator, text: []const u8, cursor: usize) anyerror!Suggestions = null,
        suggest_ctx: *anyopaque = undefined,
    };

    pub const Suggestions = struct { start: usize = 0, items: []const []const u8 = &.{} };

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

    /// Loads the last `max_history` entries; the file is rewritten from that set on the next add.
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

    /// Keep a copy for ^V and hand it to the system clipboard through OSC 52; a
    /// terminal that does not know the sequence ignores it, and the internal copy still works.
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
    /// drawn last time and clears everything below, so rows a shorter entry no longer
    /// needs do not linger.
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
            const hidden_above = ri == self.top and self.top > 0;
            const hidden_below = ri == last - 1 and last < rows.len;
            if (hidden_above or hidden_below) {
                if (self.color) w.writeAll("\x1b[2m") catch return;
                w.writeByteNTimes(' ', gutter - 2) catch return;
                w.writeAll(if (hidden_above) "\xe2\x86\x91 " else "\xe2\x86\x93 ") catch return;
                if (self.color) w.writeAll("\x1b[22m") catch return;
            } else if (!r.head) {
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
            const nl_sel = if (sel) |s| r.end < text.len and text[r.end] == '\n' and r.end >= s.start and r.end < s.end else false;
            if (nl_sel and !lit) w.writeAll("\x1b[7m") catch return;
            if (nl_sel) w.writeAll(" ") catch return;
            if (lit or nl_sel) w.writeAll("\x1b[27m") catch return;
        }
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

    /// Tab. With a menu up, the next choice replaces the word; otherwise the provider
    /// is asked, a lone answer is taken, and several fill in what they share and come up
    /// as a menu that any key but Tab dismisses.
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

    /// ^R: type to narrow, ^R again for an older match, Enter or an arrow keeps the
    /// match, Esc or ^C puts the entry back. The match shows in the entry, the query below.
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

    /// Text between the bracketed-paste markers is taken as typed, never as keys: a
    /// pasted newline is a new line, not a request to run half a script.
    fn readPaste(self: *Editor, buf: *Buffer) !void {
        const pasted = try readPasted(self.gpa, self.in_fd);
        defer self.gpa.free(pasted);
        try buf.insert(pasted);
    }

    /// Read one entry with editing; `.line` is gpa-owned and may hold several lines.
    /// Enter runs a complete entry only from its end; Ctrl+J and Ctrl+Enter (asked for via
    /// xterm modifyOtherKeys level 1) run whatever is there, from anywhere.
    pub fn readEntry(self: *Editor, opts: Options) !Result {
        const orig = try enterRaw(self.in_fd);
        defer std.posix.tcsetattr(self.in_fd, .NOW, orig) catch {};
        self.write("\x1b[?2004h");
        defer self.write("\x1b[?2004l");

        self.write("\x1b[>4;1m");
        defer self.write("\x1b[>4;0m");

        var buf = Buffer.init(self.gpa);
        defer buf.deinit();
        var hist_pos: usize = self.hist.items.len;
        var stash: ?[]u8 = null;
        defer if (stash) |s| self.gpa.free(s);
        var goal_col: ?usize = null;
        var menu: ?Menu = null;
        defer if (menu) |*m| m.arena.deinit();

        self.cursor_row = 0;
        self.top = 0;
        self.redraw(&buf);
        while (true) {
            const key = try readKey(self.in_fd);
            if (!(key == .page_up or key == .page_down or (key == .nav and (key.nav.to == .up or key.nav.to == .down)))) goal_col = null;
            if (key != .tab) if (menu) |*m| {
                m.arena.deinit();
                menu = null;
            };
            switch (key) {
                .enter, .alt_enter, .ctrl_enter => {
                    const text = buf.bytes();
                    const at_end = buf.cursor == text.len;
                    const meta = std.mem.startsWith(u8, std.mem.trimLeft(u8, text, " \t"), "\\") and std.mem.indexOfScalar(u8, text, '\n') == null;
                    const run = key != .enter or meta or (at_end and opts.complete(opts.complete_ctx, text));
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
                .page_up, .page_down => {
                    const size = termSize(self.out);
                    const rows = try layoutRows(self.gpa, buf.bytes(), textWidth(size, gutterWidth(buf.bytes())));
                    defer self.gpa.free(rows);
                    const page = if (size.rows > 2) size.rows - 2 else 1;
                    var n: usize = 0;
                    while (n < page and buf.moveVertical(rows, key == .page_down, false, &goal_col)) n += 1;
                },
                .none => {},
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

/// Put `fd` in raw mode and return the settings to restore. ICRNL is off, or a
/// pasted CRLF reads as two line feeds; `.NOW`, not `.FLUSH`, which would eat the
/// tail of a multi-line paste. ISIG is off: ^C arrives as a byte.
pub fn enterRaw(fd: std.posix.fd_t) !std.posix.termios {
    const orig = try std.posix.tcgetattr(fd);
    var raw = orig;
    raw.lflag.ECHO = false;
    raw.lflag.ICANON = false;
    raw.lflag.ISIG = false;
    raw.lflag.IEXTEN = false;
    raw.iflag.IXON = false;
    raw.iflag.ICRNL = false;
    raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
    raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;
    try std.posix.tcsetattr(fd, .NOW, raw);
    return orig;
}

/// The rest of a bracketed paste, after its `ESC [ 200 ~`: the text up to the
/// closing `ESC [ 201 ~`, every CRLF, CR or LF as `\n`. gpa-owned.
pub fn readPasted(gpa: std.mem.Allocator, fd: std.posix.fd_t) ![]u8 {
    var pasted = std.array_list.Managed(u8).init(gpa);
    defer pasted.deinit();
    const end = "\x1b[201~";
    var b: [1]u8 = undefined;
    var prev_cr = false;
    while (try std.posix.read(fd, &b) != 0) {
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
    return pasted.toOwnedSlice();
}

pub const TermSize = struct { cols: usize = 80, rows: usize = 24 };

pub fn termSize(file: std.fs.File) TermSize {
    var ws: std.posix.winsize = undefined;
    const rc = std.posix.system.ioctl(file.handle, std.posix.T.IOCGWINSZ, @intFromPtr(&ws));
    if (std.posix.errno(rc) != .SUCCESS or ws.col == 0) return .{};
    return .{ .cols = ws.col, .rows = if (ws.row == 0) 24 else ws.row };
}

test "historyMatch: newest first, older on repeat, any case, nothing for an empty query" {
    const hist = [_][]const u8{ "SELECT 1;", "select id FROM orders;", "LOAD INTO x AS SELECT 2;" };
    try std.testing.expectEqual(@as(?usize, 2), historyMatch(&hist, "select", hist.len));
    try std.testing.expectEqual(@as(?usize, 1), historyMatch(&hist, "select", 2));
    try std.testing.expectEqual(@as(?usize, 1), historyMatch(&hist, "ORDERS", hist.len));
    try std.testing.expectEqual(@as(?usize, null), historyMatch(&hist, "", hist.len));
    try std.testing.expectEqual(@as(?usize, null), historyMatch(&hist, "nope", hist.len));
}

test {
    _ = @import("line/buffer.zig");
    _ = @import("line/keys.zig");
    _ = @import("line/layout.zig");
    _ = @import("line/testing_util.zig");
}

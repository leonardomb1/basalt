//! Minimal raw-mode line editor for the REPL: arrow-key history, cursor
//! editing, and a persistent history file — no dependency, TTY only (the
//! piped path never touches this). Raw mode is entered per readLine and the
//! terminal is always restored before returning, so query execution and error
//! printing run under normal cooked-mode rules.

const std = @import("std");

pub const Result = union(enum) { line: []u8, eof, interrupt };

const Key = union(enum) {
    char: u8,
    enter,
    backspace,
    delete,
    up,
    down,
    left,
    right,
    home,
    end,
    kill_end, // ^K
    kill_line, // ^U
    word_back, // ^W
    clear, // ^L
    interrupt, // ^C
    eof_or_delete, // ^D: EOF on an empty line, delete-at-cursor otherwise
    none, // unrecognized escape — ignore
};

/// Decode one keypress from `fd` (one byte, plus the tail of an ESC sequence).
/// Split out so it is testable over a pipe.
fn readKey(fd: std.posix.fd_t) !Key {
    var b: [1]u8 = undefined;
    if (try std.posix.read(fd, &b) == 0) return .eof_or_delete;
    switch (b[0]) {
        '\r', '\n' => return .enter,
        0x7f, 0x08 => return .backspace,
        0x01 => return .home,
        0x05 => return .end,
        0x0b => return .kill_end,
        0x15 => return .kill_line,
        0x17 => return .word_back,
        0x0c => return .clear,
        0x03 => return .interrupt,
        0x04 => return .eof_or_delete,
        0x1b => {
            if (try std.posix.read(fd, &b) == 0) return .none;
            if (b[0] != '[' and b[0] != 'O') return .none;
            if (try std.posix.read(fd, &b) == 0) return .none;
            switch (b[0]) {
                'A' => return .up,
                'B' => return .down,
                'C' => return .right,
                'D' => return .left,
                'H' => return .home,
                'F' => return .end,
                '0'...'9' => {
                    // `ESC [ <digits> ~` — consume to the final byte.
                    const first = b[0];
                    while (true) {
                        if (try std.posix.read(fd, &b) == 0) return .none;
                        if (b[0] < '0' or b[0] > '9') break;
                    }
                    if (b[0] != '~') return .none;
                    return switch (first) {
                        '1', '7' => .home,
                        '4', '8' => .end,
                        '3' => .delete,
                        else => .none,
                    };
                },
                else => return .none,
            }
        },
        else => {
            if (b[0] >= 0x20 or b[0] >= 0x80) return .{ .char = b[0] };
            return .none;
        },
    }
}

pub const Editor = struct {
    gpa: std.mem.Allocator,
    in_fd: std.posix.fd_t,
    out: std.fs.File,
    hist: std.array_list.Managed([]u8),
    hist_path: ?[]u8 = null,

    const max_history = 500;

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
    }

    /// `$HOME/.basalt_history`, last `max_history` lines. No HOME → in-memory only.
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
            self.hist.append(copy) catch {
                self.gpa.free(copy);
                return;
            };
        }
        // Keep the tail; the file itself is rewritten from this trimmed set on add.
        while (self.hist.items.len > max_history) self.gpa.free(self.hist.orderedRemove(0));
    }

    /// Record an executed line: skip blanks and immediate duplicates, append to
    /// the history file (best-effort).
    pub fn remember(self: *Editor, line: []const u8) void {
        const t = std.mem.trim(u8, line, " \t\r\n");
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
        f.writeAll(t) catch return;
        f.writeAll("\n") catch return;
    }

    fn write(self: *Editor, bytes: []const u8) void {
        self.out.writeAll(bytes) catch {};
    }

    /// Repaint the whole line and park the cursor. Full-line redraw per key is
    /// plenty for a REPL.
    fn redraw(self: *Editor, prompt: []const u8, buf: []const u8, cursor: usize) void {
        self.write("\r\x1b[K");
        self.write(prompt);
        self.write(buf);
        if (cursor < buf.len) {
            var tmp: [16]u8 = undefined;
            const seq = std.fmt.bufPrint(&tmp, "\x1b[{d}D", .{buf.len - cursor}) catch return;
            self.write(seq);
        }
    }

    /// Read one line with editing. The returned `.line` is gpa-owned by the caller.
    pub fn readLine(self: *Editor, prompt: []const u8) !Result {
        const orig = try std.posix.tcgetattr(self.in_fd);
        var raw = orig;
        raw.lflag.ECHO = false;
        raw.lflag.ICANON = false;
        raw.lflag.ISIG = false; // ^C arrives as a byte; execution re-arms normal signals
        raw.lflag.IEXTEN = false;
        raw.iflag.IXON = false;
        raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
        raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;
        // .NOW, not .FLUSH: flushing would eat the tail of a multi-line paste.
        try std.posix.tcsetattr(self.in_fd, .NOW, raw);
        defer std.posix.tcsetattr(self.in_fd, .NOW, orig) catch {};

        var buf = std.array_list.Managed(u8).init(self.gpa);
        defer buf.deinit();
        var cursor: usize = 0;
        var hist_pos: usize = self.hist.items.len;
        var stash: ?[]u8 = null; // in-progress line while browsing history
        defer if (stash) |s| self.gpa.free(s);

        self.write(prompt);
        while (true) {
            switch (try readKey(self.in_fd)) {
                .enter => {
                    self.write("\r\n");
                    return .{ .line = try buf.toOwnedSlice() };
                },
                .interrupt => {
                    self.write("^C\r\n");
                    return .interrupt;
                },
                .eof_or_delete => {
                    if (buf.items.len == 0) {
                        self.write("\r\n");
                        return .eof;
                    }
                    if (cursor < buf.items.len) _ = buf.orderedRemove(cursor);
                },
                .char => |c| {
                    try buf.insert(cursor, c);
                    cursor += 1;
                },
                .backspace => if (cursor > 0) {
                    cursor -= 1;
                    _ = buf.orderedRemove(cursor);
                },
                .delete => if (cursor < buf.items.len) {
                    _ = buf.orderedRemove(cursor);
                },
                .left => cursor -|= 1,
                .right => cursor = @min(cursor + 1, buf.items.len),
                .home => cursor = 0,
                .end => cursor = buf.items.len,
                .kill_end => buf.shrinkRetainingCapacity(cursor),
                .kill_line => {
                    buf.clearRetainingCapacity();
                    cursor = 0;
                },
                .word_back => {
                    var i = cursor;
                    while (i > 0 and buf.items[i - 1] == ' ') i -= 1;
                    while (i > 0 and buf.items[i - 1] != ' ') i -= 1;
                    buf.replaceRange(i, cursor - i, &.{}) catch {};
                    cursor = i;
                },
                .clear => self.write("\x1b[2J\x1b[H"),
                .up => {
                    if (hist_pos > 0) {
                        if (hist_pos == self.hist.items.len and stash == null)
                            stash = try self.gpa.dupe(u8, buf.items);
                        hist_pos -= 1;
                        buf.clearRetainingCapacity();
                        try buf.appendSlice(self.hist.items[hist_pos]);
                        cursor = buf.items.len;
                    }
                },
                .down => {
                    if (hist_pos < self.hist.items.len) {
                        hist_pos += 1;
                        buf.clearRetainingCapacity();
                        if (hist_pos == self.hist.items.len) {
                            if (stash) |s| try buf.appendSlice(s);
                        } else {
                            try buf.appendSlice(self.hist.items[hist_pos]);
                        }
                        cursor = buf.items.len;
                    }
                },
                .none => {},
            }
            self.redraw(prompt, buf.items, cursor);
        }
    }
};

test "readKey decodes escape sequences, controls, and plain bytes" {
    const fds = try std.posix.pipe();
    defer std.posix.close(fds[0]);
    const w = fds[1];
    _ = try std.posix.write(w, "a\x1b[A\x1b[B\x1b[C\x1b[D\x1b[3~\x1b[1~\x1bOF\x7f\r\x03\x01\x05\x0b\x15");
    std.posix.close(w);

    const expect = [_]Key{
        .{ .char = 'a' }, .up,    .down,      .right,     .left, .delete, .home, .end,
        .backspace,       .enter, .interrupt, .home,      .end,  .kill_end, .kill_line,
    };
    for (expect) |want| {
        const got = try readKey(fds[0]);
        try std.testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
        if (want == .char) try std.testing.expectEqual(want.char, got.char);
    }
    try std.testing.expectEqual(Key.eof_or_delete, try readKey(fds[0]));
}

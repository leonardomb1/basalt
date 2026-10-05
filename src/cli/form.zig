//! The REPL's forms — what `\connect` asks with: a list to pick from, a set of
//! fields to fill, a yes or no. Each is drawn in place and redrawn on every key,
//! the way the entry is: arrows move between fields and along them, the fields
//! edit with the entry editor's own `Buffer` and keys (word moves, ^W, ^U, ^K,
//! undo, selection, a bracketed paste), and a finished step folds into one line
//! that says what was chosen.
//!
//! A widget is its state, `apply` (a key in, a step out) and `render` (rows into
//! a buffer, and where the cursor goes); `run` puts the terminal in raw mode and
//! drives one. Split that way so the tests drive keys and read rows without a TTY.
//! No row is drawn wider than the terminal — a wrapped row would throw off the
//! count the repaint moves back up by — so long values scroll around the cursor.

const std = @import("std");
const line = @import("line.zig");

const Buffer = line.Buffer;
const Key = line.Key;

pub const Colors = struct {
    dim: []const u8 = "",
    bold: []const u8 = "",
    mark: []const u8 = "",
    bad: []const u8 = "",
    rev: []const u8 = "",
    off: []const u8 = "",

    /// The banner's palette: its orange mark, dim and bold; nothing under NO_COLOR.
    pub fn init(on: bool) Colors {
        if (!on) return .{};
        return .{ .dim = "\x1b[2m", .bold = "\x1b[1m", .mark = "\x1b[38;5;208m", .bad = "\x1b[31m", .rev = "\x1b[7m", .off = "\x1b[0m" };
    }
};

pub const Step = enum { more, done, cancel };

/// Where the terminal cursor goes once the rows are drawn.
pub const Cursor = struct { rows: usize, row: usize, col: usize };

/// Glyphs: the active step, a finished one, the focused row, a masked byte.
const active = "\xe2\x97\x86"; // ◆
const finished = "\xe2\x97\x87"; // ◇
const pointer = "\xe2\x80\xba"; // ›
const dot = "\xe2\x80\xa2"; // •
const sep = " \xc2\xb7 "; // ·

/// Rows under construction: text counts toward the row's width and stops at the
/// edge; styles do not count and always go out, so a cut row still resets them.
const Rows = struct {
    out: *std.array_list.Managed(u8),
    limit: usize,
    used: usize = 0,
    count: usize = 1,

    fn style(self: *Rows, s: []const u8) !void {
        try self.out.appendSlice(s);
    }

    fn text(self: *Rows, s: []const u8) !void {
        var i: usize = 0;
        while (i < s.len) {
            const n = cpLen(s, i);
            if (self.used >= self.limit) return;
            try self.out.appendSlice(s[i .. i + n]);
            self.used += 1;
            i += n;
        }
    }

    fn pad(self: *Rows, to: usize) !void {
        while (self.used < to and self.used < self.limit) {
            try self.out.append(' ');
            self.used += 1;
        }
    }

    fn next(self: *Rows) !void {
        try self.out.appendSlice("\r\n");
        self.used = 0;
        self.count += 1;
    }
};

fn cpLen(s: []const u8, i: usize) usize {
    const n = std.unicode.utf8ByteSequenceLength(s[i]) catch 1;
    return @min(n, s.len - i);
}

/// Columns `s` takes: one per code point.
fn cols(s: []const u8) usize {
    var n: usize = 0;
    for (s) |b| {
        if (b & 0xc0 != 0x80) n += 1;
    }
    return n;
}

/// The byte where code point `k` of `s` starts (`s.len` past the end).
fn cpByte(s: []const u8, k: usize) usize {
    var seen: usize = 0;
    for (s, 0..) |b, i| {
        if (b & 0xc0 == 0x80) continue;
        if (seen == k) return i;
        seen += 1;
    }
    return s.len;
}

// --- a list to pick from ----------------------------------------------------------

pub const Item = struct { name: []const u8, detail: []const u8 = "" };

pub const Picker = struct {
    title: []const u8,
    items: []const Item,
    focus: usize = 0,
    /// Typed letters, to jump to the first name they begin.
    typed: [32]u8 = undefined,
    typed_len: usize = 0,

    pub fn apply(self: *Picker, key: Key) Step {
        switch (key) {
            .interrupt, .escape => return .cancel,
            .eof_or_delete => return .cancel,
            .enter, .ctrl_enter => return .done,
            .nav => |nav| {
                self.typed_len = 0;
                switch (nav.to) {
                    .up, .left => self.focus = if (self.focus == 0) self.items.len - 1 else self.focus - 1,
                    .down, .right => self.focus = (self.focus + 1) % self.items.len,
                    .home, .buf_home => self.focus = 0,
                    .end, .buf_end => self.focus = self.items.len - 1,
                    else => {},
                }
            },
            .tab => self.focus = (self.focus + 1) % self.items.len,
            .shift_tab => self.focus = if (self.focus == 0) self.items.len - 1 else self.focus - 1,
            .page_up => self.focus = 0,
            .page_down => self.focus = self.items.len - 1,
            .backspace => if (self.typed_len > 0) {
                self.typed_len -= 1;
            },
            .char => |c| {
                if (c >= '1' and c <= '9') {
                    const i: usize = c - '1';
                    if (i < self.items.len) self.focus = i;
                    self.typed_len = 0;
                } else if (self.typed_len < self.typed.len) {
                    self.typed[self.typed_len] = std.ascii.toLower(c);
                    self.typed_len += 1;
                    for (self.items, 0..) |it, i| if (std.ascii.startsWithIgnoreCase(it.name, self.typed[0..self.typed_len])) {
                        self.focus = i;
                        break;
                    };
                }
            },
            else => {},
        }
        return .more;
    }

    pub fn render(self: *const Picker, out: *std.array_list.Managed(u8), width: usize, c: Colors, final: bool) !Cursor {
        var r = Rows{ .out = out, .limit = width -| 1 };
        if (final) {
            try r.style(c.dim);
            try r.text(finished ++ " ");
            try r.style(c.off);
            try r.text(self.title);
            try r.text("  ");
            try r.style(c.mark);
            try r.text(self.items[self.focus].name);
            try r.style(c.off);
            return .{ .rows = 1, .row = 0, .col = r.used };
        }
        try r.style(c.mark);
        try r.text(active ++ " ");
        try r.style(c.off);
        try r.style(c.bold);
        try r.text(self.title);
        try r.style(c.off);
        var name_w: usize = 0;
        for (self.items) |it| name_w = @max(name_w, cols(it.name));
        for (self.items, 0..) |it, i| {
            try r.next();
            const on = i == self.focus;
            if (on) {
                try r.style(c.mark);
                try r.text(pointer ++ " ");
                try r.style(c.off);
                try r.style(c.bold);
                try r.text(it.name);
                try r.style(c.off);
            } else {
                try r.text("  ");
                try r.text(it.name);
            }
            try r.pad(2 + name_w + 2);
            if (!on) try r.style(c.dim);
            try r.text(it.detail);
            if (!on) try r.style(c.off);
        }
        try r.next();
        try r.style(c.dim);
        try r.text("  \xe2\x86\x91\xe2\x86\x93 move" ++ sep ++ "type to jump" ++ sep ++ "enter choose" ++ sep ++ "esc cancel");
        try r.style(c.off);
        return .{ .rows = r.count, .row = 1 + self.focus, .col = 0 };
    }
};

// --- fields to fill --------------------------------------------------------------

pub const Field = struct {
    label: []const u8,
    /// What the field is for, shown dim beside it while it has the focus.
    hint: []const u8 = "",
    /// What an empty field stands for, shown dim in it.
    placeholder: []const u8 = "",
    secret: bool = false,
    digits: bool = false,
    /// A fixed set to choose from with ←/→, in place of free text.
    choices: []const []const u8 = &.{},
    choice: usize = 0,
    hidden: bool = false,
    /// Set by `init`.
    buf: Buffer = undefined,

    pub fn init(gpa: std.mem.Allocator, f: Field) Field {
        var copy = f;
        copy.buf = Buffer.init(gpa);
        return copy;
    }

    /// What was typed, or chosen; empty when the field was left blank.
    pub fn value(self: *const Field) []const u8 {
        if (self.choices.len > 0) return self.choices[self.choice];
        return std.mem.trim(u8, self.buf.bytes(), " \t");
    }
};

pub const Form = struct {
    title: []const u8,
    fields: []Field,
    focus: usize = 0,
    /// Why the last submit was turned back, shown under the fields.
    problem: []const u8 = "",
    hooks: Hooks = .{},

    pub const Problem = struct { field: usize, why: []const u8 };
    pub const Hooks = struct {
        ctx: *anyopaque = undefined,
        /// After every key: placeholders or hidden fields that follow other answers.
        refresh: ?*const fn (ctx: *anyopaque, fields: []Field) void = null,
        /// On submit: a field to turn back to, and why.
        check: ?*const fn (ctx: *anyopaque, fields: []Field) ?Problem = null,
    };

    pub fn refresh(self: *Form) void {
        if (self.hooks.refresh) |f| f(self.hooks.ctx, self.fields);
        if (self.fields[self.focus].hidden) self.focus = self.step(self.focus, true) orelse self.step(self.focus, false) orelse 0;
    }

    /// The next (or previous) field that shows, from `from`.
    fn step(self: *const Form, from: usize, down: bool) ?usize {
        var i = from;
        while (true) {
            if (down) {
                if (i + 1 >= self.fields.len) return null;
                i += 1;
            } else {
                if (i == 0) return null;
                i -= 1;
            }
            if (!self.fields[i].hidden) return i;
        }
    }

    fn moveTo(self: *Form, to: ?usize) void {
        const t = to orelse return;
        self.fields[self.focus].buf.anchor = null;
        self.focus = t;
    }

    fn submit(self: *Form) Step {
        if (self.hooks.check) |f| if (f(self.hooks.ctx, self.fields)) |p| {
            self.problem = p.why;
            self.moveTo(p.field);
            return .more;
        };
        return .done;
    }

    /// Text from a paste: one line's worth, line breaks dropped.
    pub fn paste(self: *Form, text: []const u8) !void {
        const f = &self.fields[self.focus];
        if (f.choices.len > 0) return;
        for (text) |b| {
            if (b == '\n' or b == '\r' or b == '\t') continue;
            if (f.digits and !std.ascii.isDigit(b)) continue;
            try f.buf.insert(&.{b});
        }
        f.buf.typing = false;
    }

    pub fn apply(self: *Form, key: Key) !Step {
        const f = &self.fields[self.focus];
        const choosing = f.choices.len > 0;
        if (key != .none) self.problem = "";
        switch (key) {
            .interrupt => return .cancel,
            .escape => {
                if (f.buf.selection() != null) {
                    f.buf.anchor = null;
                } else return .cancel;
            },
            .eof_or_delete => if (choosing or f.buf.bytes().len == 0) return .cancel else try f.buf.delete(.right),
            .enter, .ctrl_enter, .alt_enter => if (self.step(self.focus, true)) |n| self.moveTo(n) else return self.submit(),
            .tab => self.moveTo(self.step(self.focus, true)),
            .shift_tab => self.moveTo(self.step(self.focus, false)),
            .page_up => self.moveTo(if (self.fields[0].hidden) self.step(0, true) else 0),
            .page_down => {
                var last = self.focus;
                while (self.step(last, true)) |n| last = n;
                self.moveTo(last);
            },
            .nav => |nav| switch (nav.to) {
                .up => self.moveTo(self.step(self.focus, false)),
                .down => self.moveTo(self.step(self.focus, true)),
                .left, .word_left => if (choosing) {
                    f.choice = if (f.choice == 0) f.choices.len - 1 else f.choice - 1;
                } else f.buf.move(nav),
                .right, .word_right => if (choosing) {
                    f.choice = (f.choice + 1) % f.choices.len;
                } else f.buf.move(nav),
                else => if (!choosing) f.buf.move(nav),
            },
            .char => |c| if (choosing) {
                if (c == ' ') {
                    f.choice = (f.choice + 1) % f.choices.len;
                } else for (f.choices, 0..) |ch, i| if (ch.len > 0 and std.ascii.toLower(ch[0]) == std.ascii.toLower(c)) {
                    f.choice = i;
                    break;
                };
            } else if (!f.digits or std.ascii.isDigit(c)) {
                // `insert`, not `typeChar`: a host or a password takes `(` as it is.
                try f.buf.insert(&.{c});
            },
            .backspace => if (!choosing) try f.buf.delete(.left),
            .delete => if (!choosing) try f.buf.delete(.right),
            .word_back => if (!choosing) try f.buf.delete(.word_left),
            .word_fwd_kill => if (!choosing) try f.buf.delete(.word_right),
            .kill_end => if (!choosing) try f.buf.delete(.end),
            .kill_line => if (!choosing) try f.buf.killLine(),
            .select_all => if (!choosing) f.buf.selectAll(),
            .undo => if (!choosing) try f.buf.undo(),
            .redo => if (!choosing) try f.buf.redo(),
            else => {},
        }
        return .more;
    }

    pub fn render(self: *const Form, out: *std.array_list.Managed(u8), width: usize, c: Colors, final: bool) !Cursor {
        var r = Rows{ .out = out, .limit = width -| 1 };
        try r.style(if (final) c.dim else c.mark);
        try r.text(if (final) finished ++ " " else active ++ " ");
        try r.style(c.off);
        if (!final) try r.style(c.bold);
        try r.text(self.title);
        try r.style(c.off);

        var label_w: usize = 0;
        for (self.fields) |f| if (!f.hidden) {
            label_w = @max(label_w, cols(f.label));
        };
        const value_col = 2 + label_w + 2;
        var cur = Cursor{ .rows = 0, .row = 0, .col = value_col };
        for (self.fields, 0..) |*f, i| {
            if (f.hidden) continue;
            try r.next();
            const on = !final and i == self.focus;
            if (on) {
                try r.style(c.mark);
                try r.text(pointer ++ " ");
                try r.style(c.off);
                try r.style(c.bold);
                try r.text(f.label);
                try r.style(c.off);
            } else {
                try r.text("  ");
                try r.style(c.dim);
                try r.text(f.label);
                try r.style(c.off);
            }
            try r.pad(value_col);
            if (on) cur.row = r.count - 1;
            if (f.choices.len > 0) {
                if (final or !on) {
                    try r.text(f.choices[f.choice]);
                } else for (f.choices, 0..) |ch, k| {
                    if (k > 0) {
                        try r.style(c.dim);
                        try r.text(" / ");
                        try r.style(c.off);
                    }
                    if (k == f.choice) {
                        cur.col = r.used;
                        try r.style(c.mark);
                        try r.style(c.bold);
                        try r.text(ch);
                    } else {
                        try r.style(c.dim);
                        try r.text(ch);
                    }
                    try r.style(c.off);
                }
                if (on and f.hint.len > 0) {
                    try r.text("  ");
                    try r.style(c.dim);
                    try r.text(f.hint);
                    try r.style(c.off);
                }
                continue;
            }
            const t = f.buf.bytes();
            if (t.len == 0) {
                if (on) cur.col = r.used;
                try r.style(c.dim);
                try r.text(f.placeholder);
                try r.style(c.off);
                if (on and f.hint.len > 0) {
                    try r.text(if (f.placeholder.len > 0) "  " else "");
                    try r.style(c.dim);
                    if (f.placeholder.len > 0) try r.text("(");
                    try r.text(f.hint);
                    if (f.placeholder.len > 0) try r.text(")");
                    try r.style(c.off);
                }
                continue;
            }
            // The value, scrolled so the cursor stays in sight.
            const room = r.limit -| value_col -| 1;
            const at = cols(t[0..f.buf.cursor]);
            const first = if (on and at >= room) at - room + 1 else 0;
            const sel = if (on) f.buf.selection() else null;
            var b = cpByte(t, first);
            var lit = false;
            while (b < t.len and r.used < r.limit) {
                const n = cpLen(t, b);
                const in_sel = if (sel) |s| b >= s.start and b < s.end else false;
                if (in_sel != lit) {
                    try r.style(if (in_sel) c.rev else c.off);
                    lit = in_sel;
                }
                try r.text(if (f.secret) dot else t[b .. b + n]);
                b += n;
            }
            if (lit) try r.style(c.off);
            if (on) {
                cur.col = value_col + (at - first);
                if (f.hint.len > 0 and r.used + 2 < r.limit) {
                    try r.text("  ");
                    try r.style(c.dim);
                    try r.text(f.hint);
                    try r.style(c.off);
                }
            }
        }
        if (!final) {
            if (self.problem.len > 0) {
                try r.next();
                try r.text("  ");
                try r.style(c.bad);
                try r.text(self.problem);
                try r.style(c.off);
            }
            try r.next();
            try r.style(c.dim);
            try r.text("  \xe2\x86\x91\xe2\x86\x93 fields" ++ sep ++ "\xe2\x86\x90\xe2\x86\x92 choices" ++ sep ++ "enter next" ++ sep ++ "esc cancel");
            try r.style(c.off);
        }
        cur.rows = r.count;
        return cur;
    }
};

// --- yes or no -------------------------------------------------------------------

pub const Confirm = struct {
    question: []const u8,
    yes: bool,

    pub fn apply(self: *Confirm, key: Key) Step {
        switch (key) {
            .interrupt, .escape, .eof_or_delete => return .cancel,
            .enter, .ctrl_enter => return .done,
            .nav => |nav| switch (nav.to) {
                .left, .right, .up, .down => self.yes = !self.yes,
                else => {},
            },
            .tab, .shift_tab => self.yes = !self.yes,
            .char => |c| switch (std.ascii.toLower(c)) {
                'y' => {
                    self.yes = true;
                    return .done;
                },
                'n' => {
                    self.yes = false;
                    return .done;
                },
                ' ' => self.yes = !self.yes,
                else => {},
            },
            else => {},
        }
        return .more;
    }

    pub fn render(self: *const Confirm, out: *std.array_list.Managed(u8), width: usize, c: Colors, final: bool) !Cursor {
        var r = Rows{ .out = out, .limit = width -| 1 };
        try r.style(if (final) c.dim else c.mark);
        try r.text(if (final) finished ++ " " else active ++ " ");
        try r.style(c.off);
        try r.text(self.question);
        try r.text("  ");
        if (final) {
            try r.style(c.mark);
            try r.text(if (self.yes) "yes" else "no");
            try r.style(c.off);
            return .{ .rows = 1, .row = 0, .col = r.used };
        }
        var col: usize = 0;
        for ([_]bool{ true, false }, 0..) |v, k| {
            if (k > 0) {
                try r.style(c.dim);
                try r.text(" / ");
                try r.style(c.off);
            }
            if (v == self.yes) {
                col = r.used;
                try r.style(c.mark);
                try r.style(c.bold);
            } else try r.style(c.dim);
            try r.text(if (v) "yes" else "no");
            try r.style(c.off);
        }
        return .{ .rows = 1, .row = 0, .col = col };
    }
};

// --- the terminal ----------------------------------------------------------------

pub const Term = struct {
    gpa: std.mem.Allocator,
    in_fd: std.posix.fd_t,
    w: *std.Io.Writer,
    colors: Colors,
    /// The rows the last repaint drew, and which of them the cursor was left on.
    rows: usize = 0,
    row: usize = 0,

    pub fn init(gpa: std.mem.Allocator, w: *std.Io.Writer) Term {
        return .{
            .gpa = gpa,
            .in_fd = std.fs.File.stdin().handle,
            .w = w,
            .colors = Colors.init(!std.process.hasEnvVarConstant("NO_COLOR")),
        };
    }

    fn paint(self: *Term, widget: anytype, final: bool) !void {
        var out = std.array_list.Managed(u8).init(self.gpa);
        defer out.deinit();
        const width = line.termSize(std.fs.File.stderr()).cols;
        const cur = try widget.render(&out, width, self.colors, final);
        if (self.row > 0) try self.w.print("\x1b[{d}A", .{self.row});
        try self.w.writeAll("\r\x1b[J");
        try self.w.writeAll(out.items);
        if (final) {
            try self.w.writeAll("\r\n");
            self.row = 0;
        } else {
            if (cur.rows - 1 > cur.row) try self.w.print("\x1b[{d}A", .{cur.rows - 1 - cur.row});
            try self.w.print("\r\x1b[{d}G", .{cur.col + 1});
            self.row = cur.row;
        }
        self.rows = cur.rows;
        try self.w.flush();
    }

    /// Drive `widget` to its end: true when it was finished, false when cancelled.
    /// The terminal is restored on every way out; a finished widget is left folded
    /// into its summary, a cancelled one erased.
    pub fn run(self: *Term, widget: anytype) !bool {
        const orig = try line.enterRaw(self.in_fd);
        defer std.posix.tcsetattr(self.in_fd, .NOW, orig) catch {};
        try self.w.writeAll("\x1b[?2004h");
        defer {
            self.w.writeAll("\x1b[?2004l") catch {};
            self.w.flush() catch {};
        }
        self.row = 0;
        const is_form = @TypeOf(widget) == *Form;
        while (true) {
            if (is_form) widget.refresh();
            try self.paint(widget, false);
            const key = try line.readKey(self.in_fd);
            if (key == .paste_begin) {
                const text = try line.readPasted(self.gpa, self.in_fd);
                defer self.gpa.free(text);
                if (is_form) try widget.paste(text);
                continue;
            }
            const s: Step = if (is_form) try widget.apply(key) else widget.apply(key);
            switch (s) {
                .more => {},
                .done => {
                    if (is_form) widget.refresh();
                    try self.paint(widget, true);
                    return true;
                },
                .cancel => {
                    if (self.row > 0) try self.w.print("\x1b[{d}A", .{self.row});
                    try self.w.writeAll("\r\x1b[J");
                    try self.w.flush();
                    return false;
                },
            }
        }
    }
};

// --- tests -----------------------------------------------------------------------

fn testRender(widget: anytype, width: usize, final: bool) ![]u8 {
    var out = std.array_list.Managed(u8).init(std.testing.allocator);
    defer out.deinit();
    _ = try widget.render(&out, width, .{}, final);
    return std.testing.allocator.dupe(u8, out.items);
}

fn testFields(gpa: std.mem.Allocator) [4]Field {
    return .{
        Field.init(gpa, .{ .label = "name", .hint = "how scripts refer to it" }),
        Field.init(gpa, .{ .label = "port", .placeholder = "1433", .digits = true }),
        Field.init(gpa, .{ .label = "tls", .choices = &.{ "off", "require", "insecure" }, .choice = 1 }),
        Field.init(gpa, .{ .label = "password", .secret = true }),
    };
}

test "form: arrows move between fields, typing fills them, Enter on the last submits" {
    const gpa = std.testing.allocator;
    var fields = testFields(gpa);
    defer for (&fields) |*f| f.buf.deinit();
    var form = Form{ .title = "new connection", .fields = &fields };
    for ("erp") |c| try std.testing.expectEqual(Step.more, try form.apply(.{ .char = c }));
    try std.testing.expectEqual(Step.more, try form.apply(.enter));
    try std.testing.expectEqual(@as(usize, 1), form.focus);
    // digits only in a number field
    for ("1a4") |c| _ = try form.apply(.{ .char = c });
    try std.testing.expectEqualStrings("14", fields[1].value());
    // up goes back, down comes again, and editing keys work where it lands
    _ = try form.apply(.{ .nav = .{ .to = .up } });
    try std.testing.expectEqual(@as(usize, 0), form.focus);
    _ = try form.apply(.{ .nav = .{ .to = .home } });
    _ = try form.apply(.{ .char = 'x' });
    try std.testing.expectEqualStrings("xerp", fields[0].value());
    _ = try form.apply(.word_back);
    try std.testing.expectEqualStrings("erp", fields[0].value());
    _ = try form.apply(.{ .nav = .{ .to = .down } });
    _ = try form.apply(.{ .nav = .{ .to = .down } });
    // a choice turns with ←/→ and its first letter
    _ = try form.apply(.{ .nav = .{ .to = .right } });
    try std.testing.expectEqualStrings("insecure", fields[2].value());
    _ = try form.apply(.{ .char = 'o' });
    try std.testing.expectEqualStrings("off", fields[2].value());
    _ = try form.apply(.enter);
    // a bracket goes in as typed, not paired
    _ = try form.apply(.{ .char = '(' });
    try std.testing.expectEqualStrings("(", fields[3].value());
    try std.testing.expectEqual(Step.done, try form.apply(.enter));
}

test "form: Esc and ^C cancel; Esc first drops a selection" {
    const gpa = std.testing.allocator;
    var fields = testFields(gpa);
    defer for (&fields) |*f| f.buf.deinit();
    var form = Form{ .title = "t", .fields = &fields };
    _ = try form.apply(.{ .char = 'a' });
    _ = try form.apply(.select_all);
    try std.testing.expectEqual(Step.more, try form.apply(.escape));
    try std.testing.expectEqual(Step.cancel, try form.apply(.escape));
    try std.testing.expectEqual(Step.cancel, try form.apply(.interrupt));
}

fn rejectBlankName(_: *anyopaque, fields: []Field) ?Form.Problem {
    if (fields[0].value().len == 0) return .{ .field = 0, .why = "a name is needed" };
    return null;
}

fn hidePort(_: *anyopaque, fields: []Field) void {
    fields[1].hidden = fields[2].choice == 0;
}

test "form: a check turns submit back to its field; a refresh hides fields from navigation" {
    const gpa = std.testing.allocator;
    var fields = testFields(gpa);
    defer for (&fields) |*f| f.buf.deinit();
    var form = Form{ .title = "t", .fields = &fields, .hooks = .{ .check = rejectBlankName, .refresh = hidePort } };
    form.refresh();
    _ = try form.apply(.page_down);
    try std.testing.expectEqual(Step.more, try form.apply(.enter));
    try std.testing.expectEqual(@as(usize, 0), form.focus);
    try std.testing.expectEqualStrings("a name is needed", form.problem);
    const shown = try testRender(&form, 80, false);
    defer gpa.free(shown);
    try std.testing.expect(std.mem.indexOf(u8, shown, "a name is needed") != null);
    // tls off hides the port: down from name lands on tls
    fields[2].choice = 0;
    form.refresh();
    _ = try form.apply(.{ .nav = .{ .to = .down } });
    try std.testing.expectEqual(@as(usize, 2), form.focus);
}

test "form: rows never pass the terminal's width, and a long value scrolls to the cursor" {
    const gpa = std.testing.allocator;
    var fields = testFields(gpa);
    defer for (&fields) |*f| f.buf.deinit();
    var form = Form{ .title = "a title rather longer than the terminal is wide", .fields = &fields };
    try form.paste("abcdefghijklmnopqrstuvwxyz0123456789\n");
    var out = std.array_list.Managed(u8).init(gpa);
    defer out.deinit();
    const cur = try form.render(&out, 30, .{}, false);
    var it = std.mem.splitSequence(u8, out.items, "\r\n");
    var rows: usize = 0;
    while (it.next()) |row| : (rows += 1) try std.testing.expect(cols(row) <= 29);
    try std.testing.expectEqual(cur.rows, rows);
    // the tail is in view, the cursor just past it, inside the row
    try std.testing.expect(std.mem.indexOf(u8, out.items, "789") != null);
    try std.testing.expect(cur.col < 29);
    try std.testing.expectEqual(@as(usize, 1), cur.row);
}

test "form: a secret shows as dots, and the finished form folds the hints away" {
    const gpa = std.testing.allocator;
    var fields = testFields(gpa);
    defer for (&fields) |*f| f.buf.deinit();
    var form = Form{ .title = "t", .fields = &fields };
    form.focus = 3;
    try form.paste("hunter2");
    const shown = try testRender(&form, 80, false);
    defer gpa.free(shown);
    try std.testing.expect(std.mem.indexOf(u8, shown, "hunter2") == null);
    try std.testing.expect(std.mem.indexOf(u8, shown, dot ** 7) != null);
    const done = try testRender(&form, 80, true);
    defer gpa.free(done);
    try std.testing.expect(std.mem.indexOf(u8, done, "esc cancel") == null);
    try std.testing.expect(std.mem.indexOf(u8, done, "hunter2") == null);
    try std.testing.expect(std.mem.indexOf(u8, done, "require") != null);
}

test "picker: arrows, digits and typed letters choose; it folds to one line" {
    const items = [_]Item{ .{ .name = "postgres" }, .{ .name = "sqlserver" }, .{ .name = "smb" } };
    var p = Picker{ .title = "type", .items = &items };
    _ = p.apply(.{ .nav = .{ .to = .up } });
    try std.testing.expectEqual(@as(usize, 2), p.focus);
    _ = p.apply(.{ .char = '1' });
    try std.testing.expectEqual(@as(usize, 0), p.focus);
    _ = p.apply(.{ .char = 's' });
    try std.testing.expectEqual(@as(usize, 1), p.focus);
    _ = p.apply(.{ .char = 'm' });
    try std.testing.expectEqual(@as(usize, 2), p.focus);
    try std.testing.expectEqual(Step.done, p.apply(.enter));
    const done = try testRender(&p, 80, true);
    defer std.testing.allocator.free(done);
    try std.testing.expectEqualStrings(finished ++ " type  smb", done);
    try std.testing.expectEqual(Step.cancel, p.apply(.escape));
}

test "confirm: y and n answer at once; arrows toggle" {
    var q = Confirm{ .question = "reach it now?", .yes = true };
    _ = q.apply(.{ .nav = .{ .to = .right } });
    try std.testing.expect(!q.yes);
    try std.testing.expectEqual(Step.done, q.apply(.{ .char = 'Y' }));
    try std.testing.expect(q.yes);
    try std.testing.expectEqual(Step.done, q.apply(.{ .char = 'n' }));
    try std.testing.expect(!q.yes);
}

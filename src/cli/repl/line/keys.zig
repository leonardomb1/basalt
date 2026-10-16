//! Keys from the terminal: control bytes, escape sequences with their Shift/Alt/Ctrl
//! modifiers, xterm's modifyOtherKeys, and a bare Esc told apart by a short wait.

const std = @import("std");

pub const Nav = struct {
    to: To,
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
    escape,
    shift_tab,
    line_up,
    line_down,
    dup_up,
    dup_down,
    comment,
    kill_end,
    kill_line,
    word_back,
    word_fwd_kill,
    select_all,
    undo,
    redo,
    cut,
    paste,
    paste_begin,
    clear,
    search,
    interrupt,
    eof_or_delete,
    none,
};

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

/// A key reported as code plus modifiers (xterm `CSI 27;mod;code~`, kitty
/// `CSI code;mod u`), which is how Ctrl+Enter arrives at all. `mask`: shift 1, alt 2,
/// ctrl 4. Ctrl+Enter and Shift+Enter (Jupyter's) run, as does Alt+Enter.
fn modifiedKey(code: u32, mask: u32) Key {
    const ctrl = mask & 4 != 0;
    const alt = mask & 2 != 0;
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

/// Decode one keypress from `fd` (one byte, plus the tail of an ESC sequence). A
/// control sequence is read to its final byte whatever it is: `ESC [ 1 ; 5 C` cut
/// short at the `;` once left `5C` to be typed into the line.
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

test "readKey: modified arrows carry Shift and word, and the editor's control keys decode" {
    const fds = try std.posix.pipe();
    defer std.posix.close(fds[0]);
    const w = fds[1];
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
        .ctrl_enter,
        .ctrl_enter,
        .alt_enter,
        .{ .nav = .{ .to = .word_left } },
        .none,
        .ctrl_enter,
        .ctrl_enter,
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

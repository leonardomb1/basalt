//! A file's raw records, the rows of the `lines`, `fixed`, `split` and `chunks`
//! table builtins: `n` (from 1), `offset` (the record's first byte in the decoded
//! stream, from 0) and `rec` (its bytes as BYTES, terminator left out). The file
//! is read through `Stream`, so every location and codec a format reader takes
//! works here too.
//!
//! One `Splitter` cuts every kind from a window it owns over the stream's reader;
//! a `Mode` supplies only where a record ends. The window grows only for a record
//! longer than it, up to `max_record`, so a file of any size reads in constant
//! memory, and a file without a line end fails naming where the record began
//! rather than becoming one giant row.

const std = @import("std");
const types = @import("../../lang/types.zig");
const column = @import("../../exec/column.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const eval = @import("../../exec/eval.zig");
const driver = @import("../../connect/driver.zig");
const stream = @import("../stream.zig");
const explainf = stream.explainf;

pub const max_record = 64 * 1024 * 1024;

pub const Mode = union(enum) {
    lines,
    fixed: struct { width: usize, skip_newlines: bool },
    split: []const u8,
    chunks: usize,
};

pub const fields = [_]types.Schema.Field{
    .{ .name = "n", .ty = .{ .kind = .int } },
    .{ .name = "offset", .ty = .{ .kind = .int } },
    .{ .name = "rec", .ty = .{ .kind = .bytes } },
};

pub const Rec = struct { offset: u64, bytes: []const u8 };

pub const Splitter = struct {
    gpa: std.mem.Allocator,
    reader: *std.Io.Reader,
    mode: Mode,
    name: []const u8,
    codec: stream.Codec,
    buf: []u8,
    lo: usize = 0,
    hi: usize = 0,
    scanned: usize = 0,
    pos: u64 = 0,
    count: u64 = 0,
    eof: bool = false,
    cap: usize = max_record,

    const Cut = struct { skip: usize, len: usize, adv: usize };

    pub const default_window = 1024 * 1024;

    /// A splitter over `reader` whose window starts at `window` bytes, or at the
    /// fixed record's size when that is larger.
    pub fn init(gpa: std.mem.Allocator, reader: *std.Io.Reader, mode: Mode, name: []const u8, codec: stream.Codec, window: usize) !Splitter {
        const least: usize = switch (mode) {
            .fixed => |f| f.width + 2,
            .chunks => |s| s,
            .lines, .split => 1,
        };
        return .{ .gpa = gpa, .reader = reader, .mode = mode, .name = name, .codec = codec, .buf = try gpa.alloc(u8, @max(window, least)) };
    }

    pub fn deinit(self: *Splitter) void {
        self.gpa.free(self.buf);
    }

    /// The next record, its bytes valid until the next call; null at the end.
    pub fn next(self: *Splitter) !?Rec {
        while (true) {
            const avail = self.buf[self.lo..self.hi];
            if (self.cut(avail)) |c| {
                const rec = Rec{ .offset = self.pos + c.skip, .bytes = avail[c.skip..][0..c.len] };
                self.lo += c.adv;
                self.pos += c.adv;
                self.scanned = 0;
                self.count += 1;
                return rec;
            }
            if (self.eof) return self.finish(avail);
            try self.fill();
        }
    }

    /// Where the record at the window's start ends, or null when more bytes are
    /// needed (or, at the end, none make one).
    fn cut(self: *Splitter, avail: []const u8) ?Cut {
        switch (self.mode) {
            .lines => {
                const c = self.delimited(avail, "\n") orelse return null;
                const cr = c.adv > c.len and c.len > 0 and avail[c.len - 1] == '\r';
                return .{ .skip = 0, .len = c.len - @intFromBool(cr), .adv = c.adv };
            },
            .split => |t| return self.delimited(avail, t),
            .chunks => |size| {
                if (avail.len < size and (!self.eof or avail.len == 0)) return null;
                const len = @min(size, avail.len);
                return .{ .skip = 0, .len = len, .adv = len };
            },
            .fixed => |f| {
                const lead = if (f.skip_newlines and self.count > 0) blk: {
                    if (avail.len == 1 and avail[0] == '\r' and !self.eof) return null;
                    break :blk eolLen(avail);
                } else 0;
                if (avail.len - lead < f.width) return null;
                return .{ .skip = lead, .len = f.width, .adv = lead + f.width };
            },
        }
    }

    fn delimited(self: *Splitter, avail: []const u8, term: []const u8) ?Cut {
        if (std.mem.indexOfPos(u8, avail, self.scanned, term)) |i| return .{ .skip = 0, .len = i, .adv = i + term.len };
        if (self.eof) return if (avail.len == 0) null else .{ .skip = 0, .len = avail.len, .adv = avail.len };
        self.scanned = (avail.len + 1) -| term.len;
        return null;
    }

    /// The end of the stream with `avail` left over: nothing, or a lone trailing
    /// line end, is the end; anything else is a fixed record cut short.
    fn finish(self: *Splitter, avail: []const u8) !?Rec {
        if (avail.len == eolLen(avail)) return null;
        const f = self.mode.fixed;
        const lead = if (f.skip_newlines and self.count > 0) eolLen(avail) else 0;
        return explainf(error.PartialRecord, "`{s}`: the last record, at byte {d}, is {d} bytes, short of the {d}-byte width", .{ self.name, self.pos + lead, avail.len - lead, f.width });
    }

    /// Moves the unread bytes to the window's start, grows a window they fill, and
    /// reads as much as fits.
    fn fill(self: *Splitter) !void {
        if (self.lo > 0) {
            std.mem.copyForwards(u8, self.buf[0 .. self.hi - self.lo], self.buf[self.lo..self.hi]);
            self.hi -= self.lo;
            self.lo = 0;
        }
        if (self.hi == self.buf.len) {
            const cap = self.cap + self.termLen();
            if (self.buf.len >= cap) return self.tooLong();
            self.buf = try self.gpa.realloc(self.buf, @min(self.buf.len * 2, cap));
        }
        const want = self.buf.len - self.hi;
        const got = self.reader.readSliceShort(self.buf[self.hi..]) catch return self.readFailed();
        self.hi += got;
        if (got < want) self.eof = true;
    }

    fn termLen(self: *const Splitter) usize {
        return switch (self.mode) {
            .split => |t| t.len,
            else => 2,
        };
    }

    fn tooLong(self: *const Splitter) anyerror {
        return switch (self.mode) {
            .split => |t| explainf(error.RecordTooLong, "`{s}`: the record at byte {d} runs past {d} MiB without the terminator '{s}' — is it the right terminator?", .{ self.name, self.pos, self.cap >> 20, t }),
            else => explainf(error.RecordTooLong, "`{s}`: the line at byte {d} runs past {d} MiB without a line end — is it a text file?", .{ self.name, self.pos, self.cap >> 20 }),
        };
    }

    fn readFailed(self: *const Splitter) anyerror {
        if (self.codec == .none) return explainf(error.ReadFailed, "`{s}`: the read failed after byte {d}", .{ self.name, self.pos + self.hi - self.lo });
        return explainf(error.ReadFailed, "`{s}`: the {s} stream is truncated or corrupt (it broke after byte {d})", .{ self.name, @tagName(self.codec), self.pos + self.hi - self.lo });
    }
};

/// The length of the `\n` or `\r\n` that `s` starts with, else 0.
fn eolLen(s: []const u8) usize {
    if (s.len > 0 and s[0] == '\n') return 1;
    if (s.len > 1 and s[0] == '\r' and s[1] == '\n') return 2;
    return 0;
}

pub const RecordSource = struct {
    stream: *stream.Stream,
    split: Splitter,
    schema_: types.Schema = .{ .fields = &fields },

    const batch_rows = 2048;
    const batch_bytes = 8 * 1024 * 1024;

    /// `path`'s records cut by `mode`; the stream and the source live in `arena`,
    /// the window in `gpa`.
    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8, mode: Mode) !*RecordSource {
        const s = try stream.Stream.open(arena, path);
        errdefer s.close();
        const self = try arena.create(RecordSource);
        self.* = .{ .stream = s, .split = try Splitter.init(gpa, s.reader, mode, path, s.codec, Splitter.default_window) };
        return self;
    }

    pub fn source(self: *RecordSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn schema(self: *RecordSource) types.Schema {
        return self.schema_;
    }

    /// Up to `batch_rows` records, fewer once their bytes pass `batch_bytes`.
    pub fn next(self: *RecordSource, arena: std.mem.Allocator) anyerror!?Batch {
        const ns = try arena.alloc(i64, batch_rows);
        const offs = try arena.alloc(i64, batch_rows);
        var recs = try column.BytesAppender.init(arena, batch_rows);
        var len: usize = 0;
        while (len < batch_rows and recs.values.items.len < batch_bytes) : (len += 1) {
            const r = (try self.split.next()) orelse break;
            ns[len] = @intCast(self.split.count);
            offs[len] = @intCast(r.offset);
            try recs.push(r.bytes);
        }
        if (len == 0) return null;
        const cols = try arena.alloc(column.Column, 3);
        cols[0] = .{ .ty = fields[0].ty, .len = len, .validity = try column.Bitmap.initFull(arena, len), .data = .{ .i64 = ns[0..len] } };
        cols[1] = .{ .ty = fields[1].ty, .len = len, .validity = try column.Bitmap.initFull(arena, len), .data = .{ .i64 = offs[0..len] } };
        cols[2] = .{ .ty = fields[2].ty, .len = len, .validity = try column.Bitmap.initFull(arena, len), .data = .{ .bytes = try recs.finish() } };
        return .{ .schema = &self.schema_, .columns = cols, .len = len };
    }

    pub fn close(self: *RecordSource) void {
        self.split.deinit();
        self.stream.close();
    }

    const vtable = driver.sourceVTable(RecordSource);
};

fn expectRecords(data: []const u8, mode: Mode, window: usize, want: []const []const u8, offsets: []const u64) !void {
    var r = std.Io.Reader.fixed(data);
    var s = try Splitter.init(std.testing.allocator, &r, mode, "t", .none, window);
    defer s.deinit();
    for (want, offsets) |w, o| {
        const rec = (try s.next()) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(w, rec.bytes);
        try std.testing.expectEqual(o, rec.offset);
    }
    try std.testing.expectEqual(@as(?Rec, null), try s.next());
}

test "lines: LF and CRLF end a line, a last line without one is a row, an empty file none" {
    for ([_]usize{ 1, 2, 3, 64 }) |w| {
        try expectRecords("a\r\nbb\n\nccc", .lines, w, &.{ "a", "bb", "", "ccc" }, &.{ 0, 3, 6, 7 });
        try expectRecords("a\nb\r\n", .lines, w, &.{ "a", "b" }, &.{ 0, 2 });
        try expectRecords("", .lines, w, &.{}, &.{});
        try expectRecords("\r", .lines, w, &.{"\r"}, &.{0});
    }
}

test "split: a multi-byte terminator found across every window boundary" {
    for ([_]usize{ 1, 2, 3, 4, 5, 64 }) |w| {
        try expectRecords("ab<!>c<!><!>dddd", .{ .split = "<!>" }, w, &.{ "ab", "c", "", "dddd" }, &.{ 0, 5, 9, 12 });
        try expectRecords("x~y~", .{ .split = "~" }, w, &.{ "x", "y" }, &.{ 0, 2 });
        try expectRecords("<<<", .{ .split = "<<" }, w, &.{ "", "<" }, &.{ 0, 2 });
    }
}

test "chunks: blocks of a size, the last one shorter" {
    for ([_]usize{ 1, 3, 64 }) |w| {
        try expectRecords("abcdefg", .{ .chunks = 3 }, w, &.{ "abc", "def", "g" }, &.{ 0, 3, 6 });
        try expectRecords("abcdef", .{ .chunks = 3 }, w, &.{ "abc", "def" }, &.{ 0, 3 });
        try expectRecords("", .{ .chunks = 3 }, w, &.{}, &.{});
    }
}

test "fixed: records back to back, line ends skipped between them on request" {
    for ([_]usize{ 1, 4, 64 }) |w| {
        try expectRecords("abcdef", .{ .fixed = .{ .width = 3, .skip_newlines = false } }, w, &.{ "abc", "def" }, &.{ 0, 3 });
        try expectRecords("abcdef\n", .{ .fixed = .{ .width = 3, .skip_newlines = false } }, w, &.{ "abc", "def" }, &.{ 0, 3 });
        try expectRecords("abc\r\ndef\nghi\r\n", .{ .fixed = .{ .width = 3, .skip_newlines = true } }, w, &.{ "abc", "def", "ghi" }, &.{ 0, 5, 9 });
        try expectRecords("ab\ncd\n", .{ .fixed = .{ .width = 3, .skip_newlines = false } }, w, &.{ "ab\n", "cd\n" }, &.{ 0, 3 });
        try expectRecords("", .{ .fixed = .{ .width = 3, .skip_newlines = true } }, w, &.{}, &.{});
    }
}

test "fixed: a trailing partial record fails naming its offset and length" {
    for ([_]struct { data: []const u8, skip: bool, whole: usize, msg: []const u8 }{
        .{ .data = "abcdefgh", .skip = false, .whole = 2, .msg = "`t`: the last record, at byte 6, is 2 bytes, short of the 3-byte width" },
        .{ .data = "abc\r\nde", .skip = true, .whole = 1, .msg = "`t`: the last record, at byte 5, is 2 bytes, short of the 3-byte width" },
    }) |c| {
        var r = std.Io.Reader.fixed(c.data);
        var s = try Splitter.init(std.testing.allocator, &r, .{ .fixed = .{ .width = 3, .skip_newlines = c.skip } }, "t", .none, 2);
        defer s.deinit();
        for (0..c.whole) |_| _ = (try s.next()).?;
        try std.testing.expectError(error.PartialRecord, s.next());
        try std.testing.expectEqualStrings(c.msg, eval.takeFailure(error.PartialRecord).?);
    }
}

test "lines and split: a record past the cap fails naming where it began" {
    var r = std.Io.Reader.fixed("ab\n" ++ "x" ** 40);
    var s = try Splitter.init(std.testing.allocator, &r, .lines, "t", .none, 2);
    defer s.deinit();
    s.cap = 16;
    try std.testing.expectEqualStrings("ab", (try s.next()).?.bytes);
    try std.testing.expectError(error.RecordTooLong, s.next());
    try std.testing.expect(std.mem.startsWith(u8, eval.takeFailure(error.RecordTooLong).?, "`t`: the line at byte 3 runs past"));
}

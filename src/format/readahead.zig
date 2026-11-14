//! A reader run on a thread of its own up to two blocks ahead of its consumer, so a
//! decoder and the parser reading its output run at once instead of in turn: a
//! gzipped CSV spent as long inflating as parsing, one after the other, on one
//! thread. The thread fills one block while the consumer reads the other; the
//! consumer's reads copy out of the block it holds.
//!
//! The inner reader is touched only by the thread from `start` until `stop`, and
//! the first error it raises ends the stream at the bytes read before it.

const std = @import("std");
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;

pub const block_len = 4 << 20;

pub const ReadAhead = struct {
    inner: *Reader,
    reader: Reader,
    blocks: [2][]u8,
    lens: [2]usize = .{ 0, 0 },
    full: [2]bool = .{ false, false },
    last: [2]bool = .{ false, false },
    err: ?anyerror = null,
    read_at: u1 = 0,
    off: usize = 0,
    holding: bool = false,
    stopping: bool = false,
    mtx: std.Thread.Mutex = .{},
    cv: std.Thread.Condition = .{},
    thread: ?std.Thread = null,

    const vtable: Reader.VTable = .{ .stream = streamFn };

    /// Reads `inner` ahead on a new thread; `self` must not move until `stop`.
    pub fn start(self: *ReadAhead, arena: std.mem.Allocator, inner: *Reader) !void {
        self.* = .{
            .inner = inner,
            .reader = .{ .vtable = &vtable, .buffer = try arena.alloc(u8, 64 << 10), .seek = 0, .end = 0 },
            .blocks = .{ try arena.alloc(u8, block_len), try arena.alloc(u8, block_len) },
        };
        self.thread = try std.Thread.spawn(.{}, fillLoop, .{self});
    }

    /// `start` again over `inner`, a stopped reader's blocks reused, so a folder of
    /// files costs one pair of blocks.
    pub fn restart(self: *ReadAhead, inner: *Reader) !void {
        std.debug.assert(self.thread == null);
        self.* = .{ .inner = inner, .reader = .{ .vtable = &vtable, .buffer = self.reader.buffer, .seek = 0, .end = 0 }, .blocks = self.blocks };
        self.thread = try std.Thread.spawn(.{}, fillLoop, .{self});
    }

    /// Ends the thread, at once if it is waiting and after its current read if not.
    pub fn stop(self: *ReadAhead) void {
        const t = self.thread orelse return;
        self.mtx.lock();
        self.stopping = true;
        self.cv.broadcast();
        self.mtx.unlock();
        t.join();
        self.thread = null;
    }

    fn fillLoop(self: *ReadAhead) void {
        var i: u1 = 0;
        while (true) {
            self.mtx.lock();
            while (self.full[i] and !self.stopping) self.cv.wait(&self.mtx);
            const quit = self.stopping;
            self.mtx.unlock();
            if (quit) return;
            const block = self.blocks[i];
            var n: usize = 0;
            var end = false;
            var failed: ?anyerror = null;
            while (n < block.len) {
                const k = self.inner.readSliceShort(block[n..]) catch |e| {
                    failed = e;
                    end = true;
                    break;
                };
                n += k;
                if (n < block.len) {
                    end = true;
                    break;
                }
            }
            self.mtx.lock();
            self.lens[i] = n;
            self.last[i] = end;
            if (failed) |e| self.err = e;
            self.full[i] = true;
            self.cv.broadcast();
            self.mtx.unlock();
            if (end) return;
            i ^= 1;
        }
    }

    fn streamFn(r: *Reader, w: *Writer, limit: std.Io.Limit) Reader.StreamError!usize {
        const self: *ReadAhead = @alignCast(@fieldParentPtr("reader", r));
        while (true) {
            if (self.holding) {
                const at = self.read_at;
                const rest = self.blocks[at][self.off..self.lens[at]];
                if (rest.len > 0) {
                    const dst = limit.slice(try w.writableSliceGreedy(1));
                    const k = @min(dst.len, rest.len);
                    @memcpy(dst[0..k], rest[0..k]);
                    self.off += k;
                    w.advance(k);
                    return k;
                }
                if (self.last[at]) return if (self.err != null) error.ReadFailed else error.EndOfStream;
                self.mtx.lock();
                self.full[at] = false;
                self.cv.broadcast();
                self.mtx.unlock();
                self.read_at ^= 1;
                self.holding = false;
                self.off = 0;
            }
            self.mtx.lock();
            while (!self.full[self.read_at]) self.cv.wait(&self.mtx);
            self.mtx.unlock();
            self.holding = true;
        }
    }
};

test "read ahead hands back the inner reader's bytes in order across many blocks, and its end" {
    const a = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(a);
    defer ar.deinit();
    const data = try ar.allocator().alloc(u8, 3 * block_len + 12345);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 2654435761 >> 7);
    var in: Reader = .fixed(data);
    var ra: ReadAhead = undefined;
    try ra.start(ar.allocator(), &in);
    defer ra.stop();
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    _ = try ra.reader.streamRemaining(&out.writer);
    try std.testing.expectEqualSlices(u8, data, out.written());
}

test "read ahead stops at once on a consumer that leaves early" {
    const a = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(a);
    defer ar.deinit();
    const data = try ar.allocator().alloc(u8, 5 * block_len);
    @memset(data, 7);
    var in: Reader = .fixed(data);
    var ra: ReadAhead = undefined;
    try ra.start(ar.allocator(), &in);
    var buf: [100]u8 = undefined;
    try ra.reader.readSliceAll(&buf);
    ra.stop();
}

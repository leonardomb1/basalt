//! Observability core: a logger and a run summary with two renderings each.
//!
//! Convention (matches git/npm/curl for humans, mongod/12-factor for machines):
//!   - stdout = data only (a sink, or the `--json` run summary).
//!   - stderr = logs + diagnostics, rendered by context: human text on a TTY,
//!     NDJSON (one object per line) when piped — so the same run is readable on a
//!     laptop and collector-friendly in Docker/k8s. `--log-format` forces either.
//! Every line and the summary carry the `run_id` for correlation.

const std = @import("std");
const driver = @import("../connect/driver.zig");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");

const Batch = batchmod.Batch;

pub const Format = enum { auto, text, json };

/// Log through `logger` when a handle is wired (connectors get one from the
/// runtime after open), else fall back to a raw stderr line — keeps standalone
/// and test use noisy enough to debug without a logger.
pub fn logOr(logger: ?*Logger, level: Level, comptime fmt: []const u8, args: anytype) void {
    if (logger) |lg| {
        lg.log(level, fmt, args);
    } else {
        std.debug.print(fmt ++ "\n", args);
    }
}

pub const Level = enum(u8) {
    err = 0,
    warn = 1,
    info = 2,
    debug = 3,

    pub fn label(self: Level) []const u8 {
        return switch (self) {
            .err => "error",
            .warn => "warn",
            .info => "info",
            .debug => "debug",
        };
    }

    pub fn parse(s: []const u8) ?Level {
        inline for (.{ .{ "error", Level.err }, .{ "warn", Level.warn }, .{ "info", Level.info }, .{ "debug", Level.debug } }) |p| {
            if (std.mem.eql(u8, s, p[0])) return p[1];
        }
        return null;
    }
};

/// Stderr logger. `json` is resolved once at init from the format + isatty so the
/// hot path is just a branch. Thread-safe (lanes log concurrently).
pub const Logger = struct {
    file: std.fs.File,
    json: bool,
    min: Level,
    run_id: u64,
    mutex: std.Thread.Mutex = .{},

    pub fn init(run_id: u64, format: Format, min: Level) Logger {
        const file = std.fs.File.stderr();
        const tty = std.posix.isatty(file.handle);
        const json = switch (format) {
            .auto => !tty,
            .text => false,
            .json => true,
        };
        return .{ .file = file, .json = json, .min = min, .run_id = run_id };
    }

    pub fn enabled(self: *Logger, level: Level) bool {
        return @intFromEnum(level) <= @intFromEnum(self.min);
    }

    /// Render the end-of-run summary to stderr in the logger's format (human block
    /// on a TTY, a structured `run_complete` line when piped).
    pub fn summary(self: *Logger, s: Summary) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        // Format into a stack buffer, then one positionless write(2). A fresh
        // File.Writer per call would pwrite at offset 0 and clobber prior lines when
        // stderr is a regular file (`2>run.log`) — see `log` below.
        var lbuf: [2048]u8 = undefined;
        var w = std.Io.Writer.fixed(&lbuf);
        if (self.json) {
            w.print("{{\"ts\":{d},\"level\":\"info\",\"run_id\":{d},\"event\":\"run_complete\",", .{ std.time.milliTimestamp(), s.run_id }) catch return;
            s.renderJsonFields(&w) catch return;
        } else {
            s.renderText(&w) catch return;
        }
        self.file.writeAll(w.buffered()) catch return;
    }

    pub fn log(self: *Logger, level: Level, comptime fmt: []const u8, args: anytype) void {
        if (!self.enabled(level)) return;
        var buf: [2048]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, fmt, args) catch return;
        self.mutex.lock();
        defer self.mutex.unlock();
        // Build the whole line in a stack buffer, then a single `file.writeAll`, which
        // uses write(2) — the fd offset advances, so successive lines append for both a
        // pipe and a regular file. A per-call buffered `File.Writer` (re-created here
        // each time, `.pos` resetting to 0) instead pwrites every line at offset 0, so
        // a `2>run.log` redirect kept only the last line. Lines too long for `lbuf` are
        // dropped (same as the old bufPrint cap).
        var lbuf: [4096]u8 = undefined;
        var w = std.Io.Writer.fixed(&lbuf);
        if (self.json) {
            w.print("{{\"ts\":{d},\"level\":\"{s}\",\"run_id\":{d},\"msg\":\"", .{ std.time.milliTimestamp(), level.label(), self.run_id }) catch return;
            writeEscaped(&w, msg) catch return;
            w.writeAll("\"}\n") catch return;
        } else {
            w.print("{s}: {s}\n", .{ level.label(), msg }) catch return;
        }
        self.file.writeAll(w.buffered()) catch return;
    }
};

/// End-of-run metrics. Rendered as a human block (stderr) or one JSON object
/// (stdout `--json`).
pub const Summary = struct {
    run_id: u64,
    source: []const u8 = "",
    sink: []const u8 = "",
    rows_read: u64 = 0,
    rows_written: u64 = 0,
    elapsed_ms: u64 = 0,
    threads: usize = 1,

    pub fn rate(self: Summary) u64 {
        if (self.elapsed_ms == 0) return self.rows_written;
        return self.rows_written * 1000 / self.elapsed_ms;
    }

    pub fn renderText(self: Summary, w: anytype) !void {
        try w.print("✓ {s} → {s}  ", .{ self.source, self.sink });
        if (self.rows_read != self.rows_written) {
            try w.print("read {d} → wrote {d}", .{ self.rows_read, self.rows_written });
        } else {
            try w.print("wrote {d}", .{self.rows_written});
        }
        try w.print("  ({d} rows/s, {d} ms", .{ self.rate(), self.elapsed_ms });
        if (self.threads > 1) try w.print(", {d} lanes", .{self.threads});
        try w.print(")  run={d}\n", .{self.run_id});
    }

    pub fn renderJson(self: Summary, w: anytype) !void {
        try w.print("{{\"status\":\"ok\",\"run_id\":{d},", .{self.run_id});
        try self.renderJsonFields(w);
    }

    /// The shared metric fields (and closing brace/newline) of both JSON
    /// renderings: the `--json` stdout summary and the NDJSON `run_complete`
    /// stderr line — only their envelope prefixes differ.
    fn renderJsonFields(self: Summary, w: anytype) !void {
        try w.print(
            "\"source\":\"{s}\",\"sink\":\"{s}\",\"rows_read\":{d},\"rows_written\":{d},\"elapsed_ms\":{d},\"rows_per_sec\":{d}}}\n",
            .{ self.source, self.sink, self.rows_read, self.rows_written, self.elapsed_ms, self.rate() },
        );
    }
};

/// Wraps a `driver.Source`, counting emitted rows into a shared atomic — so the
/// pipeline gets a "rows read" figure with no per-operator instrumentation.
pub const CountingSource = struct {
    inner: driver.Source,
    count: *std.atomic.Value(u64),

    pub fn source(self: *CountingSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.Source.VTable{ .schema = vtSchema, .next = vtNext, .close = vtClose };

    fn vtSchema(ptr: *anyopaque) types.Schema {
        const self: *CountingSource = @ptrCast(@alignCast(ptr));
        return self.inner.schema();
    }
    fn vtNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *CountingSource = @ptrCast(@alignCast(ptr));
        const b = try self.inner.next(arena);
        if (b) |bb| _ = self.count.fetchAdd(bb.len, .monotonic);
        return b;
    }
    fn vtClose(ptr: *anyopaque) void {
        const self: *CountingSource = @ptrCast(@alignCast(ptr));
        self.inner.close();
    }
};

fn writeEscaped(w: anytype, s: []const u8) !void {
    for (s) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => if (c < 0x20) try w.print("\\u{x:0>4}", .{c}) else try w.writeByte(c),
    };
}

test "level parse + summary rate" {
    try std.testing.expectEqual(Level.warn, Level.parse("warn").?);
    try std.testing.expect(Level.parse("nope") == null);
    const s = Summary{ .run_id = 1, .rows_written = 1000, .elapsed_ms = 500 };
    try std.testing.expectEqual(@as(u64, 2000), s.rate());
}

test "json log line escapes and is one line" {
    // Render to a buffer by mimicking the json branch.
    var buf: [256]u8 = undefined;
    var fbw = std.Io.Writer.fixed(&buf);
    const w = &fbw;
    try w.writeAll("{\"msg\":\"");
    try writeEscaped(w, "a\"b\nc\\d\t\r\x01");
    try w.writeAll("\"}");
    try std.testing.expectEqualStrings("{\"msg\":\"a\\\"b\\nc\\\\d\\t\\r\\u0001\"}", w.buffered());
}

test "summary rate: zero elapsed falls back to rows_written (no div-by-zero)" {
    const s = Summary{ .run_id = 1, .rows_written = 42, .elapsed_ms = 0 };
    try std.testing.expectEqual(@as(u64, 42), s.rate());
}

test "summary renderJson: one status-ok object with every metric field" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const s = Summary{ .run_id = 7, .source = "csv", .sink = "starrocks", .rows_read = 10, .rows_written = 8, .elapsed_ms = 2000 };
    try s.renderJson(&w);
    try std.testing.expectEqualStrings(
        "{\"status\":\"ok\",\"run_id\":7,\"source\":\"csv\",\"sink\":\"starrocks\",\"rows_read\":10,\"rows_written\":8,\"elapsed_ms\":2000,\"rows_per_sec\":4}\n",
        w.buffered(),
    );
}

test "summary renderText: read≠written shows both; lane count only when parallel" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const s = Summary{ .run_id = 7, .source = "csv", .sink = "starrocks", .rows_read = 10, .rows_written = 8, .elapsed_ms = 2000, .threads = 2 };
    try s.renderText(&w);
    try std.testing.expectEqualStrings("✓ csv → starrocks  read 10 → wrote 8  (4 rows/s, 2000 ms, 2 lanes)  run=7\n", w.buffered());

    var buf2: [256]u8 = undefined;
    var w2 = std.Io.Writer.fixed(&buf2);
    const eq = Summary{ .run_id = 7, .source = "csv", .sink = "csv", .rows_read = 8, .rows_written = 8, .elapsed_ms = 1000 };
    try eq.renderText(&w2);
    try std.testing.expectEqualStrings("✓ csv → csv  wrote 8  (8 rows/s, 1000 ms)  run=7\n", w2.buffered());
}

test "logger level gate: err/warn/info pass at min=info, debug is filtered" {
    var lg = Logger.init(1, .text, .info);
    try std.testing.expect(lg.enabled(.err));
    try std.testing.expect(lg.enabled(.warn));
    try std.testing.expect(lg.enabled(.info));
    try std.testing.expect(!lg.enabled(.debug));
}

const test_empty_schema = types.Schema{ .fields = &.{} };
var test_no_cols: [0]@import("../exec/column.zig").Column = .{};

/// A source emitting one zero-column batch per entry of `batches`, then EOF.
const FakeSource = struct {
    batches: []const usize,
    i: usize = 0,

    fn source(self: *FakeSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Source.VTable{ .schema = vtSchema, .next = vtNext, .close = vtClose };
    fn vtSchema(_: *anyopaque) types.Schema {
        return test_empty_schema;
    }
    fn vtNext(ptr: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
        const self: *FakeSource = @ptrCast(@alignCast(ptr));
        if (self.i >= self.batches.len) return null;
        const n = self.batches[self.i];
        self.i += 1;
        return Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = n };
    }
    fn vtClose(_: *anyopaque) void {}
};

test "CountingSource accumulates emitted rows across batches and forwards EOF" {
    var cnt = std.atomic.Value(u64).init(0);
    var fake = FakeSource{ .batches = &.{ 2, 3 } };
    var cs = CountingSource{ .inner = fake.source(), .count = &cnt };
    const src = cs.source();
    try std.testing.expectEqual(@as(usize, 0), src.schema().fields.len); // schema passthrough
    try std.testing.expectEqual(@as(usize, 2), ((try src.next(std.testing.allocator)) orelse unreachable).len);
    try std.testing.expectEqual(@as(usize, 3), ((try src.next(std.testing.allocator)) orelse unreachable).len);
    try std.testing.expect((try src.next(std.testing.allocator)) == null);
    try std.testing.expectEqual(@as(u64, 5), cnt.load(.monotonic)); // EOF adds nothing
}

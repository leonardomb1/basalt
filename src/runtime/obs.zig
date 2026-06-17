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
        var wbuf: [1024]u8 = undefined;
        var fw = self.file.writer(&wbuf);
        const w = &fw.interface;
        if (self.json) {
            w.print(
                "{{\"ts\":{d},\"level\":\"info\",\"run_id\":{d},\"event\":\"run_complete\",\"source\":\"{s}\",\"sink\":\"{s}\",\"rows_read\":{d},\"rows_written\":{d},\"elapsed_ms\":{d},\"rows_per_sec\":{d}}}\n",
                .{ std.time.milliTimestamp(), s.run_id, s.source, s.sink, s.rows_read, s.rows_written, s.elapsed_ms, s.rate() },
            ) catch return;
        } else {
            s.renderText(w) catch return;
        }
        w.flush() catch return;
    }

    pub fn log(self: *Logger, level: Level, comptime fmt: []const u8, args: anytype) void {
        if (!self.enabled(level)) return;
        var buf: [2048]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, fmt, args) catch return;
        self.mutex.lock();
        defer self.mutex.unlock();
        var wbuf: [1024]u8 = undefined;
        var fw = self.file.writer(&wbuf);
        const w = &fw.interface;
        if (self.json) {
            w.print("{{\"ts\":{d},\"level\":\"{s}\",\"run_id\":{d},\"msg\":\"", .{ std.time.milliTimestamp(), level.label(), self.run_id }) catch return;
            writeEscaped(w, msg) catch return;
            w.writeAll("\"}\n") catch return;
        } else {
            w.print("{s}: {s}\n", .{ level.label(), msg }) catch return;
        }
        w.flush() catch return;
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
        try w.print(
            "{{\"status\":\"ok\",\"run_id\":{d},\"source\":\"{s}\",\"sink\":\"{s}\",\"rows_read\":{d},\"rows_written\":{d},\"elapsed_ms\":{d},\"rows_per_sec\":{d}}}\n",
            .{ self.run_id, self.source, self.sink, self.rows_read, self.rows_written, self.elapsed_ms, self.rate() },
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
    try writeEscaped(w, "a\"b\nc");
    try w.writeAll("\"}");
    try std.testing.expectEqualStrings("{\"msg\":\"a\\\"b\\nc\"}", w.buffered());
}

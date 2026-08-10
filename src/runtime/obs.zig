//! Observability core: a logger and a run summary with two renderings each.
//!
//! Convention (matches git/npm/curl for humans, mongod/12-factor for machines):
//!   - stdout = data only (a sink, or the `--json` run summary).
//!   - stderr = logs + diagnostics, plain human text. `--log-format json` opts
//!     into NDJSON (one object per line) for collectors; nothing switches format
//!     on its own.
//! Every line and the summary carry the `run_id` for correlation.

const std = @import("std");
const driver = @import("../connect/driver.zig");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");

const Batch = batchmod.Batch;

/// Stderr log rendering. `auto` is the flag's default and an accepted alias; it
/// resolves to text, same as `text`.
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

/// Stderr logger. `json` is resolved once at init from the format so the hot path
/// is just a branch. Thread-safe (lanes log concurrently).
pub const Logger = struct {
    file: std.fs.File,
    json: bool,
    min: Level,
    run_id: u64,
    /// `-q`: silences `PRINT` too, which no log level does.
    quiet: bool = false,
    mutex: std.Thread.Mutex = .{},

    pub fn init(run_id: u64, format: Format, min: Level) Logger {
        const file = std.fs.File.stderr();
        return .{ .file = file, .json = format == .json, .min = min, .run_id = run_id };
    }

    pub fn enabled(self: *Logger, level: Level) bool {
        return @intFromEnum(level) <= @intFromEnum(self.min);
    }

    /// A line the script itself asked for (`PRINT`). It is output, not a
    /// diagnostic, so `--log-level` does not gate it and it carries no severity
    /// prefix — only `-q` silences it. Still stderr: stdout stays the data
    /// channel that `--format json` makes a parseable contract.
    pub fn script(self: *Logger, msg: []const u8) void {
        if (self.quiet) return;
        self.mutex.lock();
        defer self.mutex.unlock();
        var lbuf: [4096]u8 = undefined;
        var w = std.Io.Writer.fixed(&lbuf);
        if (self.json) {
            w.print("{{\"ts\":{d},\"level\":\"print\",\"run_id\":{d},\"msg\":\"", .{ std.time.milliTimestamp(), self.run_id }) catch return;
            writeEscaped(&w, msg) catch return;
            w.writeAll("\"}\n") catch return;
        } else {
            w.print("{s}\n", .{msg}) catch return;
        }
        self.file.writeAll(w.buffered()) catch return;
    }

    /// Render the end-of-run summary to stderr in the logger's format (human block
    /// by default, a structured `run_complete` line under `--log-format json`).
    pub fn summary(self: *Logger, s: Summary) void {
        self.mutex.lock();
        defer self.mutex.unlock();
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

    /// Throughput on rows **processed**, not rows emitted. Dividing the written count
    /// by the clock described how fast the answer was printed, not how fast the run
    /// worked: an aggregate folding 6,001,215 rows into 4 reported `11 rows/s`.
    ///
    /// `rows_read` is the volume that actually moved through the pipeline, and for a
    /// straight move it equals `rows_written`, so this only changes the shapes that
    /// reduce. It falls back to the written count when nothing was read — a sourceless
    /// run (`FROM BODY`) still has a meaningful rate.
    pub fn rate(self: Summary) u64 {
        const rows = if (self.rows_read > 0) self.rows_read else self.rows_written;
        if (self.elapsed_ms == 0) return rows;
        return rows * 1000 / self.elapsed_ms;
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
        "{\"status\":\"ok\",\"run_id\":7,\"source\":\"csv\",\"sink\":\"starrocks\",\"rows_read\":10,\"rows_written\":8,\"elapsed_ms\":2000,\"rows_per_sec\":5}\n",
        w.buffered(),
    );
}

test "summary renderText: read≠written shows both; lane count only when parallel" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const s = Summary{ .run_id = 7, .source = "csv", .sink = "starrocks", .rows_read = 10, .rows_written = 8, .elapsed_ms = 2000, .threads = 2 };
    try s.renderText(&w);
    try std.testing.expectEqualStrings("✓ csv → starrocks  read 10 → wrote 8  (5 rows/s, 2000 ms, 2 lanes)  run=7\n", w.buffered());

    var buf2: [256]u8 = undefined;
    var w2 = std.Io.Writer.fixed(&buf2);
    const eq = Summary{ .run_id = 7, .source = "csv", .sink = "csv", .rows_read = 8, .rows_written = 8, .elapsed_ms = 1000 };
    try eq.renderText(&w2);
    try std.testing.expectEqualStrings("✓ csv → csv  wrote 8  (8 rows/s, 1000 ms)  run=7\n", w2.buffered());
}

test "summary rate: an aggregate reports the rows it processed, not the rows it wrote" {
    // 6,001,215 rows folded to 4 in 350ms used to read as `11 rows/s` — the speed the
    // answer was written at. The run's throughput is the volume it moved.
    const agg = Summary{ .run_id = 1, .rows_read = 6_001_215, .rows_written = 4, .elapsed_ms = 350 };
    try std.testing.expectEqual(@as(u64, 17_146_328), agg.rate());

    // A straight move reads and writes the same rows, so nothing changes there.
    const move = Summary{ .run_id = 1, .rows_read = 1000, .rows_written = 1000, .elapsed_ms = 500 };
    try std.testing.expectEqual(@as(u64, 2000), move.rate());

    // Nothing read (a sourceless run) still reports a rate, from what it wrote.
    const sourceless = Summary{ .run_id = 1, .rows_read = 0, .rows_written = 50, .elapsed_ms = 100 };
    try std.testing.expectEqual(@as(u64, 500), sourceless.rate());

    // A sub-millisecond run divides by nothing; report the raw count.
    const instant = Summary{ .run_id = 1, .rows_read = 42, .rows_written = 1, .elapsed_ms = 0 };
    try std.testing.expectEqual(@as(u64, 42), instant.rate());
}

test "logger format: only explicit json is NDJSON; auto resolves to text" {
    try std.testing.expect(!Logger.init(1, .auto, .info).json);
    try std.testing.expect(!Logger.init(1, .text, .info).json);
    try std.testing.expect(Logger.init(1, .json, .info).json);
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
    try std.testing.expectEqual(@as(usize, 0), src.schema().fields.len);
    try std.testing.expectEqual(@as(usize, 2), ((try src.next(std.testing.allocator)) orelse unreachable).len);
    try std.testing.expectEqual(@as(usize, 3), ((try src.next(std.testing.allocator)) orelse unreachable).len);
    try std.testing.expect((try src.next(std.testing.allocator)) == null);
    try std.testing.expectEqual(@as(u64, 5), cnt.load(.monotonic));
}

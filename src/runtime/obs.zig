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
const Batch = @import("../exec/batch.zig").Batch;

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
    /// A `Progress` line currently occupies the terminal row. Guarded by `mutex`;
    /// every writer below erases it first, so a log line never lands mid-line.
    progress_drawn: bool = false,

    fn eraseProgress(self: *Logger) void {
        if (!self.progress_drawn) return;
        self.file.writeAll("\r\x1b[2K") catch {};
        self.progress_drawn = false;
    }

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
        self.eraseProgress();
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
        self.eraseProgress();
        self.file.writeAll(w.buffered()) catch return;
    }

    /// One finished `LOAD` of a run that has several, in the manner of `uv`'s
    /// ` + package` lines: printed as each completes, above the live progress line.
    /// Text only when `items` asked for it; under `--log-format json` it is an
    /// `info`-level `load_complete` / `load_failed` event instead.
    pub fn item(self: *Logger, it: Item) void {
        if (self.quiet) return;
        if (self.json and !self.enabled(.info)) return;
        self.mutex.lock();
        defer self.mutex.unlock();
        var lbuf: [1024]u8 = undefined;
        var w = std.Io.Writer.fixed(&lbuf);
        if (self.json) it.renderJson(&w, self.run_id) catch return else it.renderText(&w) catch return;
        self.eraseProgress();
        self.file.writeAll(w.buffered()) catch return;
    }

    pub fn log(self: *Logger, level: Level, comptime fmt: []const u8, args: anytype) void {
        if (!self.enabled(level)) return;
        // Wide enough for a debug line carrying a 300-column SELECT; a line that
        // still does not fit is cut, not dropped.
        var buf: [16384]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, fmt, args) catch buf[0..];
        self.mutex.lock();
        defer self.mutex.unlock();
        var lbuf: [17408]u8 = undefined;
        var w = std.Io.Writer.fixed(&lbuf);
        if (self.json) {
            w.print("{{\"ts\":{d},\"level\":\"{s}\",\"run_id\":{d},\"msg\":\"", .{ std.time.milliTimestamp(), level.label(), self.run_id }) catch return;
            writeEscaped(&w, msg) catch return;
            w.writeAll("\"}\n") catch return;
        } else {
            w.print("{s}: {s}\n", .{ level.label(), msg }) catch return;
        }
        self.eraseProgress();
        self.file.writeAll(w.buffered()) catch return;
    }
};

/// A live one-line status on stderr while a `LOAD` moves rows, in the manner of
/// `uv`/`cargo`: a spinner, what is moving where, rows so far, the rate and the
/// clock — and `[3/12]` in front when a `FOR EACH` is fanning out.
///
/// It is a courtesy for a person at a terminal and nothing else: the caller turns
/// it on only when stderr is a TTY, it never touches stdout, and it shares the
/// logger's mutex so a log line erases it rather than colliding with it. Nothing
/// is drawn for the first `quiet_ms`, so a short run never flickers.
pub const Progress = struct {
    logger: *Logger,
    rows: *std.atomic.Value(u64),
    thread: ?std.Thread = null,
    /// Set to stop: the ticker waits on it between frames, so `stop` returns at
    /// once instead of after the sleep it was in — which put 100 ms on every run.
    stop_flag: std.Thread.ResetEvent = .{},
    /// Below: guarded by `logger.mutex`.
    active: usize = 0,
    label_buf: [192]u8 = undefined,
    label_len: usize = 0,
    base_rows: u64 = 0,
    began_ms: i64 = 0,
    loop_depth: usize = 0,
    loop_total: usize = 0,
    loop_done: usize = 0,
    loop_began_ms: i64 = 0,
    frame: usize = 0,
    /// Colour the line (a cyan spinner, a green count, the rest dim), unless
    /// `NO_COLOR` asks for plain text.
    color: bool = true,

    const quiet_ms = 400;
    const tick_ns = 100 * std.time.ns_per_ms;
    const frames = [_][]const u8{ "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" };

    pub fn start(self: *Progress) void {
        self.color = !std.process.hasEnvVarConstant("NO_COLOR");
        self.thread = std.Thread.spawn(.{}, ticker, .{self}) catch null;
    }

    pub fn stop(self: *Progress) void {
        self.stop_flag.set();
        if (self.thread) |t| t.join();
        self.thread = null;
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        self.logger.eraseProgress();
    }

    /// A pipeline started writing `label` (`source → sink`). Calls nest under a
    /// parallel `FOR EACH`; the line names the most recent one.
    pub fn begin(self: *Progress, label: []const u8) void {
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        if (self.active == 0) {
            self.base_rows = self.rows.load(.monotonic);
            self.began_ms = std.time.milliTimestamp();
        }
        self.active += 1;
        self.label_len = @min(label.len, self.label_buf.len);
        @memcpy(self.label_buf[0..self.label_len], label[0..self.label_len]);
    }

    pub fn end(self: *Progress) void {
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        if (self.active > 0) self.active -= 1;
        // Inside a loop the line stays up between rows: three hundred quick loads
        // are one long wait, and a line that blinks per table reads as noise.
        if (self.active == 0 and self.loop_depth == 0) self.logger.eraseProgress();
    }

    /// A `FOR EACH` over `total` rows began. Only the outermost loop is counted;
    /// the result says whether this one is it, and is what `loopTick` takes.
    pub fn loopBegin(self: *Progress, total: usize) bool {
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        self.loop_depth += 1;
        if (self.loop_depth != 1) return false;
        self.loop_total = total;
        self.loop_done = 0;
        self.loop_began_ms = std.time.milliTimestamp();
        return true;
    }

    pub fn loopTick(self: *Progress, counted: bool) void {
        if (!counted) return;
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        self.loop_done += 1;
    }

    pub fn loopEnd(self: *Progress) void {
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        if (self.loop_depth > 0) self.loop_depth -= 1;
        if (self.loop_depth != 0) return;
        self.loop_total = 0;
        if (self.active == 0) self.logger.eraseProgress();
    }

    fn ticker(self: *Progress) void {
        while (true) {
            self.stop_flag.timedWait(tick_ns) catch {};
            if (self.stop_flag.isSet()) return;
            self.logger.mutex.lock();
            defer self.logger.mutex.unlock();
            const looping = self.loop_depth > 0;
            if (self.active == 0 and !(looping and self.label_len > 0)) continue;
            const now = std.time.milliTimestamp();
            // The wait that matters is the whole loop's, not the current row's.
            if (now - (if (looping) self.loop_began_ms else self.began_ms) < quiet_ms) continue;
            var buf: [512]u8 = undefined;
            var w = std.Io.Writer.fixed(&buf);
            self.frame +%= 1;
            render(&w, .{
                .spinner = frames[self.frame % frames.len],
                .label = self.label_buf[0..self.label_len],
                .rows = self.rows.load(.monotonic) -| self.base_rows,
                .elapsed_ms = @intCast(now - self.began_ms),
                .clock_ms = @intCast(now - (if (looping) self.loop_began_ms else self.began_ms)),
                .loop_done = self.loop_done,
                .loop_total = self.loop_total,
                .width = termWidth(self.logger.file),
                .color = self.color,
            }) catch continue;
            self.logger.file.writeAll("\r\x1b[2K") catch continue;
            self.logger.file.writeAll(w.buffered()) catch continue;
            self.logger.progress_drawn = true;
        }
    }

    pub const Line = struct {
        spinner: []const u8,
        label: []const u8,
        rows: u64,
        /// How long this statement has run — what the rate is over.
        elapsed_ms: u64,
        /// What the clock shows: the statement's time, or the whole loop's inside a
        /// `FOR EACH`, where the per-row time keeps snapping back to zero.
        clock_ms: ?u64 = null,
        loop_done: usize = 0,
        loop_total: usize = 0,
        width: usize = 80,
        color: bool = false,
    };

    /// One progress line, no newline, never wider than `width` columns — a wrapped
    /// line cannot be erased with a carriage return. The label gives way first.
    pub fn render(w: *std.Io.Writer, l: Line) !void {
        // Measured without its colour codes, which take no columns.
        var tail_buf: [96]u8 = undefined;
        var tw = std.Io.Writer.fixed(&tail_buf);
        try tw.writeAll("  ");
        try writeThousands(&tw, l.rows);
        try tw.writeAll(" rows  ");
        try writeRate(&tw, if (l.elapsed_ms == 0) l.rows else l.rows * 1000 / l.elapsed_ms);
        const secs = (l.clock_ms orelse l.elapsed_ms) / 1000;
        try tw.print(" rows/s  {d}:{d:0>2}", .{ secs / 60, secs % 60 });
        const tail = tw.buffered();
        var ctail_buf: [160]u8 = undefined;
        var cw = std.Io.Writer.fixed(&ctail_buf);
        if (l.color) {
            try cw.writeAll("  \x1b[32m");
            try writeThousands(&cw, l.rows);
            try cw.writeAll(" rows\x1b[0m  \x1b[2m");
            try cw.writeAll(tail[tail.len - (tail.len - std.mem.indexOf(u8, tail, "rows  ").? - 6) ..]);
            try cw.writeAll("\x1b[0m");
        }

        var head_buf: [32]u8 = undefined;
        var hw = std.Io.Writer.fixed(&head_buf);
        if (l.loop_total > 0) try hw.print("[{d}/{d}] ", .{ @min(l.loop_done + 1, l.loop_total), l.loop_total });
        const head = hw.buffered();

        const fixed = 2 + head.len + tail.len;
        const room = if (l.width > fixed + 1) l.width - fixed - 1 else 0;
        if (l.color) try w.print("\x1b[36m{s}\x1b[0m \x1b[1m{s}\x1b[0m", .{ l.spinner, head }) else try w.print("{s} {s}", .{ l.spinner, head });
        try writeFitted(w, l.label, room);
        try w.writeAll(if (l.color) cw.buffered() else tail);
    }
};

fn termWidth(file: std.fs.File) usize {
    var ws: std.posix.winsize = undefined;
    const rc = std.posix.system.ioctl(file.handle, std.posix.T.IOCGWINSZ, @intFromPtr(&ws));
    if (std.posix.errno(rc) != .SUCCESS or ws.col == 0) return 80;
    return ws.col;
}

fn columns(s: []const u8) usize {
    var n: usize = 0;
    for (s) |c| {
        if (c & 0xC0 != 0x80) n += 1;
    }
    return n;
}

/// `s` in at most `room` columns, the middle replaced by `…` when it does not fit:
/// a path or a qualified table says most at its two ends.
fn writeFitted(w: *std.Io.Writer, s: []const u8, room: usize) !void {
    if (columns(s) <= room) return w.writeAll(s);
    if (room < 4) return;
    const keep = room - 1;
    const left = keep / 2;
    const right = keep - left;
    var i: usize = 0;
    var seen: usize = 0;
    while (i < s.len and seen < left) : (i += 1) {
        if (i + 1 >= s.len or s[i + 1] & 0xC0 != 0x80) seen += 1;
    }
    var j: usize = s.len;
    seen = 0;
    while (j > i and seen < right) {
        j -= 1;
        if (s[j] & 0xC0 != 0x80) seen += 1;
    }
    try w.writeAll(s[0..i]);
    try w.writeAll("…");
    try w.writeAll(s[j..]);
}

fn writeThousands(w: anytype, n: u64) !void {
    var digits: [24]u8 = undefined;
    const s = std.fmt.bufPrint(&digits, "{d}", .{n}) catch unreachable;
    for (s, 0..) |c, i| {
        if (i != 0 and (s.len - i) % 3 == 0) try w.writeByte(',');
        try w.writeByte(c);
    }
}

/// `412ms`, `8.2s`, `3m 12s` — the precision a person reads at each scale.
fn writeDuration(w: anytype, ms: u64) !void {
    if (ms < 1000) return w.print("{d}ms", .{ms});
    if (ms < 60_000) return w.print("{d}.{d}s", .{ ms / 1000, ms % 1000 / 100 });
    return w.print("{d}m {d}s", .{ ms / 60_000, ms % 60_000 / 1000 });
}

fn writeRate(w: anytype, r: u64) !void {
    if (r >= 1_000_000) return w.print("{d}.{d}M", .{ r / 1_000_000, r % 1_000_000 / 100_000 });
    if (r >= 10_000) return w.print("{d}.{d}k", .{ r / 1000, r % 1000 / 100 });
    return writeThousands(w, r);
}

/// How many `LOAD`s a run finished and how many failed. Shared by pointer, so the
/// workers of a parallel `FOR EACH` count into the same two numbers.
pub const LoadTally = struct {
    ok: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    failed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Rows the finished loads wrote — not the run's `rows_written`, which also
    /// counts what a terminal SELECT printed.
    rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

pub const Item = struct {
    target: []const u8,
    rows: u64 = 0,
    elapsed_ms: u64 = 0,
    /// Set on a failed load: why.
    reason: ?[]const u8 = null,

    pub fn renderText(self: Item, w: *std.Io.Writer) !void {
        if (self.reason) |why| return w.print(" x {s}  {s}\n", .{ self.target, why });
        try w.print(" + {s}", .{self.target});
        var pad = columns(self.target);
        while (pad < 32) : (pad += 1) try w.writeByte(' ');
        var nbuf: [32]u8 = undefined;
        var nw = std.Io.Writer.fixed(&nbuf);
        try writeThousands(&nw, self.rows);
        try w.print("  {s: >13} rows  ", .{nw.buffered()});
        var dbuf: [16]u8 = undefined;
        var dw = std.Io.Writer.fixed(&dbuf);
        try writeDuration(&dw, self.elapsed_ms);
        try w.print("{s: >7}\n", .{dw.buffered()});
    }

    fn renderJson(self: Item, w: *std.Io.Writer, run_id: u64) !void {
        try w.print("{{\"ts\":{d},\"level\":\"info\",\"run_id\":{d},\"event\":\"{s}\",\"target\":\"", .{ std.time.milliTimestamp(), run_id, if (self.reason == null) "load_complete" else "load_failed" });
        try writeEscaped(w, self.target);
        try w.print("\",\"rows_written\":{d},\"elapsed_ms\":{d}", .{ self.rows, self.elapsed_ms });
        if (self.reason) |why| {
            try w.writeAll(",\"reason\":\"");
            try writeEscaped(w, why);
            try w.writeByte('"');
        }
        try w.writeAll("}\n");
    }
};

/// End-of-run metrics. Rendered as one sentence for a person (stderr) or one JSON
/// object for a program (stdout `--format json`, or the `run_complete` log line).
pub const Summary = struct {
    run_id: u64,
    source: []const u8 = "",
    sink: []const u8 = "",
    rows_read: u64 = 0,
    rows_written: u64 = 0,
    elapsed_ms: u64 = 0,
    threads: usize = 1,
    /// `LOAD`s finished and failed, and — when the run was exactly one — its target
    /// as the script spelled it.
    loads: u64 = 1,
    loads_failed: u64 = 0,
    target: []const u8 = "",
    /// Rows the loads wrote, for the sentence; null falls back to `rows_written`.
    rows_loaded: ?u64 = null,
    /// The run was one `LOAD` and nothing else, so `rows_read` is that load's own
    /// and worth saying when it differs. Beside other statements it is not.
    lone_load: bool = true,

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

    /// The run in one sentence, verb first, the way `uv` and `pip` close:
    /// `Loaded 20,000,000 rows into sr.bronze.orders in 8.2s (2.4M rows/s, 12 lanes)`.
    /// The run id is deliberately absent — it is for correlating machine logs, and
    /// both JSON renderings carry it.
    pub fn renderText(self: Summary, w: anytype) !void {
        const total = self.loads + self.loads_failed;
        const loaded = self.rows_loaded orelse self.rows_written;
        if (total > 1 or self.loads_failed > 0) {
            try w.writeAll("Loaded ");
            if (self.loads_failed > 0) try w.print("{d} of {d} targets, ", .{ self.loads, total }) else try w.print("{d} targets, ", .{self.loads});
            try writeThousands(w, loaded);
            try w.writeAll(" rows");
        } else {
            if (self.lone_load and self.rows_read != loaded and self.rows_read > 0) {
                try w.writeAll("Read ");
                try writeThousands(w, self.rows_read);
                try w.writeAll(" rows, loaded ");
                try writeThousands(w, loaded);
            } else {
                try w.writeAll("Loaded ");
                try writeThousands(w, loaded);
                try w.writeAll(" rows");
            }
            try w.print(" into {s}", .{if (self.target.len > 0) self.target else self.sink});
        }
        try w.writeAll(" in ");
        try writeDuration(w, self.elapsed_ms);
        try w.writeAll(" (");
        try writeRate(w, self.rate());
        try w.writeAll(" rows/s");
        if (self.threads > 1) try w.print(", {d} lanes", .{self.threads});
        try w.writeAll(")\n");
        if (self.loads_failed > 0) try w.print("{d} failed\n", .{self.loads_failed});
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
            "\"source\":\"{s}\",\"sink\":\"{s}\",\"rows_read\":{d},\"rows_written\":{d},\"elapsed_ms\":{d},\"rows_per_sec\":{d},\"loads\":{d},\"loads_failed\":{d}}}\n",
            .{ self.source, self.sink, self.rows_read, self.rows_written, self.elapsed_ms, self.rate(), self.loads, self.loads_failed },
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

test "progress line: counts, rate and clock, and a label that gives way to the width" {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try Progress.render(&w, .{ .spinner = "*", .label = "erp.orders → sr.bronze.orders", .rows = 1204112, .elapsed_ms = 24_900, .width = 120 });
    try std.testing.expectEqualStrings("* erp.orders → sr.bronze.orders  1,204,112 rows  48.3k rows/s  0:24", w.buffered());

    w = std.Io.Writer.fixed(&buf);
    try Progress.render(&w, .{ .spinner = "*", .label = "erp.dbo.a_very_long_table_name → az://lakeacct/bronze/a_very_long_table_name.parquet", .rows = 950, .elapsed_ms = 61_000, .loop_done = 2, .loop_total = 12, .width = 70 });
    const line = w.buffered();
    try std.testing.expect(columns(line) <= 69);
    try std.testing.expect(std.mem.startsWith(u8, line, "* [3/12] erp.dbo."));
    try std.testing.expect(std.mem.indexOf(u8, line, "…") != null);
    try std.testing.expect(std.mem.endsWith(u8, line, ".parquet  950 rows  15 rows/s  1:01"));

    // In colour the same text, dressed; stripped of its codes it is byte-identical.
    w = std.Io.Writer.fixed(&buf);
    try Progress.render(&w, .{ .spinner = "*", .label = "a → b", .rows = 12, .elapsed_ms = 2000, .loop_done = 0, .loop_total = 3, .width = 80, .color = true });
    var plain: [256]u8 = undefined;
    var n: usize = 0;
    var i: usize = 0;
    const c = w.buffered();
    while (i < c.len) : (i += 1) {
        if (c[i] == 0x1b) {
            while (c[i] != 'm') i += 1;
            continue;
        }
        plain[n] = c[i];
        n += 1;
    }
    try std.testing.expectEqualStrings("* [1/3] a → b  12 rows  6 rows/s  0:02", plain[0..n]);
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
        "{\"status\":\"ok\",\"run_id\":7,\"source\":\"csv\",\"sink\":\"starrocks\",\"rows_read\":10,\"rows_written\":8,\"elapsed_ms\":2000,\"rows_per_sec\":5,\"loads\":1,\"loads_failed\":0}\n",
        w.buffered(),
    );
}

test "summary sentence: one load, a load that reduces, and a run of several with failures" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try (Summary{ .run_id = 7, .sink = "starrocks", .target = "sr.bronze.orders", .rows_read = 20_000_000, .rows_written = 20_000_000, .elapsed_ms = 8200, .threads = 12 }).renderText(&w);
    try std.testing.expectEqualStrings("Loaded 20,000,000 rows into sr.bronze.orders in 8.2s (2.4M rows/s, 12 lanes)\n", w.buffered());

    w = std.Io.Writer.fixed(&buf);
    try (Summary{ .run_id = 7, .sink = "csv", .target = "agg_out.csv", .rows_read = 4_000_000, .rows_written = 7, .elapsed_ms = 1900 }).renderText(&w);
    try std.testing.expectEqualStrings("Read 4,000,000 rows, loaded 7 into agg_out.csv in 1.9s (2.1M rows/s)\n", w.buffered());

    // Beside a SELECT, the run's totals are not the load's: say only what it wrote.
    w = std.Io.Writer.fixed(&buf);
    try (Summary{ .run_id = 7, .sink = "parquet", .target = "/tmp/h.parquet", .rows_read = 291, .rows_written = 117, .rows_loaded = 112, .lone_load = false, .elapsed_ms = 771 }).renderText(&w);
    try std.testing.expectEqualStrings("Loaded 112 rows into /tmp/h.parquet in 771ms (377 rows/s)\n", w.buffered());

    w = std.Io.Writer.fixed(&buf);
    try (Summary{ .run_id = 7, .rows_read = 9_482_004, .rows_written = 9_482_004, .elapsed_ms = 192_000, .threads = 12, .loads = 11, .loads_failed = 1 }).renderText(&w);
    try std.testing.expectEqualStrings("Loaded 11 of 12 targets, 9,482,004 rows in 3m 12s (49.3k rows/s, 12 lanes)\n1 failed\n", w.buffered());
}

test "item lines: a finished load is aligned, a failed one says why" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try (Item{ .target = "sr.bronze.sc5010", .rows = 1_204_112, .elapsed_ms = 24_100 }).renderText(&w);
    try std.testing.expectEqualStrings(" + sr.bronze.sc5010                      1,204,112 rows    24.1s\n", w.buffered());
    w = std.Io.Writer.fixed(&buf);
    try (Item{ .target = "sr.bronze.sb1010", .reason = "connection reset by peer" }).renderText(&w);
    try std.testing.expectEqualStrings(" x sr.bronze.sb1010  connection reset by peer\n", w.buffered());
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

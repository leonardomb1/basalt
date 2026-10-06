//! Observability core: a logger and a run summary with two renderings each.
//!
//! Convention (matches git/npm/curl for humans, mongod/12-factor for machines):
//!   - stdout = data only (a sink, or the `--json` run summary).
//!   - stderr = logs + diagnostics, plain human text. `--log-format json` opts
//!     into NDJSON (one object per line) for collectors; nothing switches format
//!     on its own (`auto` resolves to text).
//! Every line and the summary carry the `run_id` for correlation.
//!
//! `Logger` is thread-safe, since lanes log concurrently. `PRINT` output is not a
//! diagnostic: no log level gates it and only `-q` silences it, but it stays on
//! stderr so stdout remains the data channel `--format json` makes a contract.
//!
//! `Progress` is a live one-line status for a person at a TTY: spinner, what is
//! moving where, rows, rate and clock, with `[3/12]` under a fanning-out
//! `FOR EACH`. It shares the logger's mutex and every log write erases it first,
//! so the two never collide; nothing is drawn for the first `quiet_ms`, and inside
//! a loop the line stays up between rows. Its `json` mode writes a `progress`
//! event once a second instead, and `hook` hands the event to a caller.
//!
//! `RowCounter` and `LoadTally` are shared by pointer across the workers of a
//! parallel `FOR EACH`: one `fetchAdd` credits every link of a counter chain, so
//! the run total and each worker's own count both see every row. The summary's
//! rate is over rows processed, not written: an aggregate folding six million
//! rows into four once reported `11 rows/s`.

const std = @import("std");
const driver = @import("../connect/driver.zig");
const types = @import("../lang/types.zig");
const Batch = @import("../exec/batch.zig").Batch;

pub const Format = enum { auto, text, json };

/// Logs through `logger` when the runtime wired one, else a raw stderr line, so
/// standalone and test use stays debuggable.
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

pub const Logger = struct {
    file: std.fs.File,
    json: bool,
    min: Level,
    run_id: u64,
    quiet: bool = false,
    mutex: std.Thread.Mutex = .{},
    progress_drawn: bool = false,

    /// Caller holds `mutex`.
    fn progressEvent(self: *Logger, ev: Progress.Event) void {
        var buf: [512]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        w.print("{{\"ts\":{d},\"level\":\"info\",\"run_id\":{d},\"event\":\"progress\",\"target\":", .{ std.time.milliTimestamp(), self.run_id }) catch return;
        std.json.Stringify.encodeJsonString(ev.target, .{}, &w) catch return;
        w.print(",\"rows\":{d},\"rows_per_sec\":{d},\"elapsed_ms\":{d}", .{ ev.rows, ev.rows_per_sec, ev.elapsed_ms }) catch return;
        if (ev.loop_total > 0) w.print(",\"loop_done\":{d},\"loop_total\":{d}", .{ ev.loop_done, ev.loop_total }) catch return;
        w.writeAll("}\n") catch return;
        self.file.writeAll(w.buffered()) catch {};
    }

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

    /// One finished `LOAD` of several, printed above the live progress line when
    /// `items` asked for it; a `load_complete` / `load_failed` event under JSON.
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

    /// Lines over the 16 KiB buffer are cut, not dropped.
    pub fn log(self: *Logger, level: Level, comptime fmt: []const u8, args: anytype) void {
        if (!self.enabled(level)) return;
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

pub const Progress = struct {
    logger: *Logger,
    rows: *RowCounter,
    thread: ?std.Thread = null,
    stop_flag: std.Thread.ResetEvent = .{},
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
    color: bool = true,
    mode: Mode = .line,
    hook: ?EventHook = null,
    last_event_ms: i64 = 0,

    pub const Mode = enum { line, json, hook };

    pub const Event = struct {
        target: []const u8,
        rows: u64,
        rows_per_sec: u64,
        elapsed_ms: u64,
        loop_done: usize = 0,
        loop_total: usize = 0,
    };

    pub const EventHook = struct {
        ctx: *anyopaque,
        f: *const fn (ctx: *anyopaque, ev: Event) void,
    };

    const quiet_ms = 400;
    const event_ms = 1000;
    const tick_ns = 100 * std.time.ns_per_ms;
    const frames = [_][]const u8{ "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" };

    pub fn start(self: *Progress) void {
        self.color = !std.process.hasEnvVarConstant("NO_COLOR");
        self.thread = std.Thread.spawn(.{}, ticker, .{self}) catch null;
    }

    /// Signals the ticker's wait rather than letting it finish a sleep, which once
    /// added 100 ms to every run.
    pub fn stop(self: *Progress) void {
        self.stop_flag.set();
        if (self.thread) |t| t.join();
        self.thread = null;
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        self.logger.eraseProgress();
    }

    /// Calls nest under a parallel `FOR EACH`; the line names the most recent one.
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
        if (self.active == 0 and self.loop_depth == 0) self.logger.eraseProgress();
    }

    /// Only the outermost loop is counted; the result says whether this is it, and
    /// is what `loopTick` takes.
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

    pub fn loopState(self: *Progress) struct { done: usize, total: usize } {
        self.logger.mutex.lock();
        defer self.logger.mutex.unlock();
        return .{ .done = self.loop_done, .total = self.loop_total };
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
            if (now - (if (looping) self.loop_began_ms else self.began_ms) < quiet_ms) continue;
            if (self.mode != .line) {
                if (now - self.last_event_ms < event_ms) continue;
                self.last_event_ms = now;
                const rows = self.rows.load(.monotonic) -| self.base_rows;
                const elapsed: u64 = @intCast(@max(1, now - self.began_ms));
                const ev = Event{
                    .target = self.label_buf[0..self.label_len],
                    .rows = rows,
                    .rows_per_sec = rows * 1000 / elapsed,
                    .elapsed_ms = elapsed,
                    .loop_done = self.loop_done,
                    .loop_total = self.loop_total,
                };
                if (self.hook) |h| h.f(h.ctx, ev) else self.logger.progressEvent(ev);
                continue;
            }
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
        elapsed_ms: u64,
        clock_ms: ?u64 = null,
        loop_done: usize = 0,
        loop_total: usize = 0,
        width: usize = 80,
        color: bool = false,
    };

    /// Never wider than `width` columns, since a wrapped line cannot be erased with a
    /// carriage return. The label gives way first; colour codes are not measured.
    pub fn render(w: *std.Io.Writer, l: Line) !void {
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

/// Elides the middle, since a path or a qualified table says most at its ends.
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

pub const RowCounter = struct {
    n: std.atomic.Value(u64) = .init(0),
    up: ?*RowCounter = null,

    pub fn init(v: u64) RowCounter {
        return .{ .n = .init(v) };
    }
    pub fn fetchAdd(self: *RowCounter, k: u64, comptime order: std.builtin.AtomicOrder) u64 {
        var c = self.up;
        while (c) |x| : (c = x.up) _ = x.n.fetchAdd(k, order);
        return self.n.fetchAdd(k, order);
    }
    pub fn load(self: *const RowCounter, comptime order: std.builtin.AtomicOrder) u64 {
        return self.n.load(order);
    }
};

pub const LoadTally = struct {
    ok: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    failed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    seq: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

pub const Item = struct {
    target: []const u8,
    rows: u64 = 0,
    elapsed_ms: u64 = 0,
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

pub const Summary = struct {
    run_id: u64,
    source: []const u8 = "",
    sink: []const u8 = "",
    rows_read: u64 = 0,
    rows_written: u64 = 0,
    elapsed_ms: u64 = 0,
    threads: usize = 1,
    loads: u64 = 1,
    loads_failed: u64 = 0,
    target: []const u8 = "",
    rows_loaded: ?u64 = null,
    lone_load: bool = true,
    pushdown: Pushdown = .{},

    pub const Pushdown = struct {
        row_groups: u64 = 0,
        row_groups_skipped: u64 = 0,
        columns_read: u64 = 0,
        columns_total: u64 = 0,
        sql_filtered_reads: u64 = 0,

        fn any(self: Pushdown) bool {
            return self.row_groups > 0 or self.columns_total > 0 or self.sql_filtered_reads > 0;
        }
    };

    /// Rows read per second, falling back to rows written for a sourceless run.
    pub fn rate(self: Summary) u64 {
        const rows = if (self.rows_read > 0) self.rows_read else self.rows_written;
        if (self.elapsed_ms == 0) return rows;
        return rows * 1000 / self.elapsed_ms;
    }

    /// `Loaded 20,000,000 rows into sr.bronze.orders in 8.2s (2.4M rows/s, 12 lanes)`.
    /// The run id is left out; both JSON renderings carry it.
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

    fn renderJsonFields(self: Summary, w: anytype) !void {
        try w.print(
            "\"source\":\"{s}\",\"sink\":\"{s}\",\"rows_read\":{d},\"rows_written\":{d},\"elapsed_ms\":{d},\"rows_per_sec\":{d},\"loads\":{d},\"loads_failed\":{d}",
            .{ self.source, self.sink, self.rows_read, self.rows_written, self.elapsed_ms, self.rate(), self.loads, self.loads_failed },
        );
        const p = self.pushdown;
        if (p.any()) try w.print(
            ",\"pushdown\":{{\"row_groups\":{d},\"row_groups_skipped\":{d},\"columns_read\":{d},\"columns_total\":{d},\"sql_filtered_reads\":{d}}}",
            .{ p.row_groups, p.row_groups_skipped, p.columns_read, p.columns_total, p.sql_filtered_reads },
        );
        try w.writeAll("}\n");
    }
};

pub const CountingSource = struct {
    inner: driver.Source,
    count: *RowCounter,

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

test "Level.parse" {
    try std.testing.expectEqual(Level.err, Level.parse("error").?);
    try std.testing.expectEqual(Level.warn, Level.parse("warn").?);
    try std.testing.expect(Level.parse("nope") == null);
}

test "writeEscaped: quotes, backslashes and control characters are escaped" {
    var buf: [256]u8 = undefined;
    var fbw = std.Io.Writer.fixed(&buf);
    const w = &fbw;
    try w.writeAll("{\"msg\":\"");
    try writeEscaped(w, "a\"b\nc\\d\t\r\x01");
    try w.writeAll("\"}");
    try std.testing.expectEqualStrings("{\"msg\":\"a\\\"b\\nc\\\\d\\t\\r\\u0001\"}", w.buffered());
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

test "summary renderJson: a pushdown object only when a source was spared something" {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const s = Summary{ .run_id = 7, .source = "parquet", .sink = "stdout", .rows_read = 10, .rows_written = 1, .elapsed_ms = 1, .pushdown = .{ .row_groups = 4, .row_groups_skipped = 3, .columns_read = 1, .columns_total = 5 } };
    try s.renderJson(&w);
    try std.testing.expect(std.mem.endsWith(u8, w.buffered(), ",\"pushdown\":{\"row_groups\":4,\"row_groups_skipped\":3,\"columns_read\":1,\"columns_total\":5,\"sql_filtered_reads\":0}}\n"));
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, w.buffered(), .{});
    parsed.deinit();
}

test "progress event: one NDJSON line with the target and the counts" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const f = try tmp.dir.createFile("log", .{ .read = true });
    defer f.close();
    var lg = Logger{ .file = f, .json = true, .min = .info, .run_id = 9 };
    lg.progressEvent(.{ .target = "a.csv → \"b\"", .rows = 1200, .rows_per_sec = 600, .elapsed_ms = 2000, .loop_done = 1, .loop_total = 3 });
    try f.seekTo(0);
    var rb: [512]u8 = undefined;
    const n = try f.readAll(&rb);
    const line = rb[0..n];
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, line, "\n"));
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, line, .{});
    defer parsed.deinit();
    const o = parsed.value.object;
    try std.testing.expectEqualStrings("progress", o.get("event").?.string);
    try std.testing.expectEqualStrings("a.csv → \"b\"", o.get("target").?.string);
    try std.testing.expectEqual(@as(i64, 1200), o.get("rows").?.integer);
    try std.testing.expectEqual(@as(i64, 3), o.get("loop_total").?.integer);
}

test "summary sentence: one load, a load that reduces, and a run of several with failures" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try (Summary{ .run_id = 7, .sink = "starrocks", .target = "sr.bronze.orders", .rows_read = 20_000_000, .rows_written = 20_000_000, .elapsed_ms = 8200, .threads = 12 }).renderText(&w);
    try std.testing.expectEqualStrings("Loaded 20,000,000 rows into sr.bronze.orders in 8.2s (2.4M rows/s, 12 lanes)\n", w.buffered());

    w = std.Io.Writer.fixed(&buf);
    try (Summary{ .run_id = 7, .sink = "csv", .target = "agg_out.csv", .rows_read = 4_000_000, .rows_written = 7, .elapsed_ms = 1900 }).renderText(&w);
    try std.testing.expectEqualStrings("Read 4,000,000 rows, loaded 7 into agg_out.csv in 1.9s (2.1M rows/s)\n", w.buffered());

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
    const agg = Summary{ .run_id = 1, .rows_read = 6_001_215, .rows_written = 4, .elapsed_ms = 350 };
    try std.testing.expectEqual(@as(u64, 17_146_328), agg.rate());

    const move = Summary{ .run_id = 1, .rows_read = 1000, .rows_written = 1000, .elapsed_ms = 500 };
    try std.testing.expectEqual(@as(u64, 2000), move.rate());

    const sourceless = Summary{ .run_id = 1, .rows_read = 0, .rows_written = 50, .elapsed_ms = 100 };
    try std.testing.expectEqual(@as(u64, 500), sourceless.rate());

    const instant = Summary{ .run_id = 1, .rows_read = 42, .rows_written = 1, .elapsed_ms = 0 };
    try std.testing.expectEqual(@as(u64, 42), instant.rate());

    const instant_sourceless = Summary{ .run_id = 1, .rows_read = 0, .rows_written = 42, .elapsed_ms = 0 };
    try std.testing.expectEqual(@as(u64, 42), instant_sourceless.rate());
}

fn testLogAll(dir: std.fs.Dir, name: []const u8, format: Format, buf: []u8) ![]const u8 {
    const f = try dir.createFile(name, .{ .read = true });
    defer f.close();
    var lg = Logger.init(1, format, .info);
    lg.file = f;
    lg.log(.err, "e{d}", .{1});
    lg.log(.warn, "w{d}", .{2});
    lg.log(.info, "i\"{d}\n", .{3});
    lg.log(.debug, "d{d}", .{4});
    try f.seekTo(0);
    const n = try f.readAll(buf);
    return buf[0..n];
}

test "Logger.log: min=info writes err, warn and info and drops debug; only explicit json is NDJSON" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [1024]u8 = undefined;

    try std.testing.expectEqualStrings("error: e1\nwarn: w2\ninfo: i\"3\n\n", try testLogAll(tmp.dir, "auto", .auto, &buf));
    try std.testing.expectEqualStrings("error: e1\nwarn: w2\ninfo: i\"3\n\n", try testLogAll(tmp.dir, "text", .text, &buf));

    const out = try testLogAll(tmp.dir, "json", .json, &buf);
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, out, "\n"));
    var it = std.mem.splitScalar(u8, out[0 .. out.len - 1], '\n');
    const want = [_][2][]const u8{ .{ "error", "e1" }, .{ "warn", "w2" }, .{ "info", "i\"3\n" } };
    for (want) |wl| {
        const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, it.next().?, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(wl[0], parsed.value.object.get("level").?.string);
        try std.testing.expectEqualStrings(wl[1], parsed.value.object.get("msg").?.string);
    }
    try std.testing.expect(it.next() == null);
}

const test_empty_schema = types.Schema{ .fields = &.{} };
var test_no_cols: [0]@import("../exec/column.zig").Column = .{};

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
    var cnt = RowCounter.init(0);
    var fake = FakeSource{ .batches = &.{ 2, 3 } };
    var cs = CountingSource{ .inner = fake.source(), .count = &cnt };
    const src = cs.source();
    try std.testing.expectEqual(@as(usize, 0), src.schema().fields.len);
    try std.testing.expectEqual(@as(usize, 2), ((try src.next(std.testing.allocator)) orelse unreachable).len);
    try std.testing.expectEqual(@as(usize, 3), ((try src.next(std.testing.allocator)) orelse unreachable).len);
    try std.testing.expect((try src.next(std.testing.allocator)) == null);
    try std.testing.expectEqual(@as(u64, 5), cnt.load(.monotonic));
}

//! DELETE and UPDATE of one file, rewritten whole and put in place of the old one in
//! one step (`format/output.zig`: a local file's `.part` renamed over it, an object's
//! one upload), so a reader sees the old file or the new one, never a mix. The engine
//! finds the rows to change as a query's stages find any rows, over the file read
//! serially, each row carrying its identity: in a CSV or NDJSON file the span of its
//! record's bytes, in any other format its place in the file (in a sheet, its row).
//! Sorted by that identity, they are merged into a second read of the file:
//!   - a CSV or NDJSON file is copied byte for byte, every byte outside a changed
//!     record as it stands (a byte order mark, line endings, quoting, blank lines,
//!     the text encoding); a deleted record goes with its line ending, an updated one
//!     is rendered as the writer renders a row, in the file's delimiter and encoding,
//!     and ends as it did, an NDJSON object changing only the keys set;
//!   - an Excel workbook keeps every part but the sheet as it was, and the sheet
//!     every row it keeps (`format/xlsx/edit.zig`);
//!   - any other format is read and written again as basalt writes it, its kept rows
//!     as read and its updated ones in their places.
//! Rows keep their order, and nothing is written when no row changes. A local file
//! whose size, modification time or inode changed since it was read is not replaced,
//! nor an object whose ETag did.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const column = @import("../../exec/column.zig");
const driver = @import("../../exec/driver.zig");
const op = @import("../../exec/op.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const csvfmt = @import("../../format/csv.zig");
const jsonread = @import("../../format/json/read.zig");
const jsonwrite = @import("../../format/json/write.zig");
const stream = @import("../../format/stream.zig");
const Output = @import("../../format/output.zig").Output;
const objstore = @import("../../store/objstore.zig");
const analyze = @import("../analyze.zig");
const plan = @import("../plan.zig");
const env_mod = @import("../env.zig");
const source = @import("../connect/source.zig");
const formats_open = @import("../connect/formats_open.zig");
const update_rules = @import("../analyze/update.zig");
const formats = @import("../../format/formats.zig");
const xlsxread = @import("../../format/xlsx/read.zig");
const xlsxedit = @import("../../format/xlsx/edit.zig");
const UnitPipeline = @import("../lanes/map.zig").UnitPipeline;

const Env = env_mod.Env;
const RunOptions = env_mod.RunOptions;
const Stats = env_mod.Stats;
const planErr = env_mod.planErr;
const planErrT = env_mod.planErrT;

pub const Op = enum { delete, update };

const row_name = "\x00row";

/// How the file's rows are told apart and its new bytes made.
const Kind = enum { csv, ndjson, sheet, whole };

/// `op` on the file `w.target`, its read `read` and the stages `between_in` (the
/// conditions, subqueries and FROM source) choosing the rows.
pub fn run(env: *Env, w: ast.Write, read: ast.Stage, between_in: []const ast.Stage, which: Op, opts: RunOptions, stats: *Stats) !void {
    const a = env.arena;
    const path = w.target;
    const verb: []const u8 = if (which == .delete) "DELETE FROM" else "UPDATE";
    const what = try msgfmt.allocPrint(a, "{s} {s}", .{ verb, path });
    if (try analyze.fileDmlProblem(a, w, read.hints)) |why| return planErr(env.diag, why);
    var fdiag = analyze.Diag{};
    const want = analyze.formatFromHints(read.hints, &fdiag) catch |e| return env_mod.aErr(env, &fdiag, e);
    const format = analyze.readFormat(path, want);
    if (try localProblem(a, path)) |why| return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: {s}", .{ what, why }));
    const between = try analyze.substFilterParams(a, between_in, env.params_expr);
    const t0 = std.time.milliTimestamp();
    const guard = try Guard.take(a, path);

    var kind: Kind = switch (formats.of(format).write.change) {
        .text => if (format == .csv) .csv else .ndjson,
        .sheet => .sheet,
        .whole, .refused => .whole,
    };
    var first = try openRead(env, read, format, &kind);
    const data = first.data;
    const dry = env.explain or opts.explain;
    if (dry and !opts.explain) {
        first.src.close();
        env.log.script(try msgfmt.allocPrint(a, "{s}: would read it and write it again, {s}", .{ what, switch (kind) {
            .csv, .ndjson => "copying every record it keeps byte for byte",
            .sheet => "copying its other sheets and every row it keeps as they are",
            .whole => "as basalt writes the format",
        } }));
        return;
    }

    const ids: []const []const u8 = if (kind == .whole or kind == .sheet) &.{row_name} else &.{ csvfmt.span_start, csvfmt.span_end };
    var stages = std.array_list.Managed(ast.Stage).init(a);
    for (between) |st| if (!(st.node == .select and st.node.select.len > 0 and st.node.select[0] == .star)) try stages.append(st);
    const at = if (between.len > 0) between[between.len - 1].pos else read.pos;
    if (which == .update) try stages.append(.{ .node = .{ .select = if (kind == .whole) try setProjection(env, w, data, ids) else try projection(env, w, data, ids) }, .hints = &.{}, .pos = at });
    if (which == .delete and kind == .whole) try stages.append(.{ .node = .{ .select = try idsOnly(a, ids) }, .hints = &.{}, .pos = at });
    if (kind == .sheet) if (try xlsxedit.problem(a, path, first.loc.?)) |why| {
        first.src.close();
        return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: {s}", .{ what, why }));
    };
    const key = try a.alloc(ast.SortKey, 1);
    key[0] = .{ .field = .{ .parts = try a.dupe([]const u8, &.{ids[0]}) }, .desc = false, .nulls_first = false };
    try stages.append(.{ .node = .{ .sort = .{ .keys = key } }, .hints = &.{}, .pos = at });

    if (kind == .whole) if (try plan.projectedColumns(env, stages.items)) |cols| {
        var keep = std.array_list.Managed([]const u8).init(a);
        for (cols) |c| if (data.indexOf(c) != null) try keep.append(c);
        if (keep.items.len == 0) try keep.append(data.fields[0].name);
        if (keep.items.len < data.fields.len) {
            first.src.close();
            const src = try source.openSourceProjected(env, read.node.read, read.hints, keep.items, &.{});
            first.src = (try Numbered.open(a, src)).source();
        }
    };
    const scan = try a.create(op.Scan);
    scan.* = .{ .src = first.src };
    var current: op.Op = .{ .scan = scan };
    var schema = first.src.schema();
    defer first.src.close();
    for (stages.items) |st| {
        const r = try plan.buildStage(env, st, current, schema);
        current = r.op;
        schema = r.schema;
    }
    defer current.release();
    var hits = Hits{ .op = current, .mem = std.heap.ArenaAllocator.init(env.gpa), .id = schema.indexOf(ids[0]).?, .end = if (ids.len > 1) schema.indexOf(ids[1]).? else null, .width = data.fields.len };
    defer hits.mem.deinit();

    if (dry) {
        var n: u64 = 0;
        while (try hits.peek()) |_| {
            n += 1;
            hits.i += 1;
        }
        env.log.script(try msgfmt.allocPrint(a, "{s}: would {s} {d} row{s}", .{ what, if (which == .delete) "delete" else "update", n, if (n == 1) "" else "s" }));
        return;
    }
    const n = if (try hits.peek() == null) 0 else switch (kind) {
        .csv, .ndjson => try rewriteText(env, w, path, kind, which, read, data, &hits, guard, what),
        .sheet => try rewriteXlsx(env, path, first, which, &hits, guard, what),
        .whole => try rewriteWhole(env, w, format, want, first.array, which, read, data, &hits, guard, what, opts),
    };
    stats.rows_out += n;
    const ms = std.time.milliTimestamp() - t0;
    env.log.script(try msgfmt.allocPrint(a, "{s} {d} row{s} {s} {s} in {d} ms", .{ if (which == .delete) "Deleted" else "Updated", n, if (n == 1) "" else "s", if (which == .delete) "from" else "of", path, ms }));
}

/// Why a rewrite of `path` cannot be put in place of it: a local symlink or a file
/// with other hard links is written in place (`store/part.zig`), over the bytes the
/// rewrite still reads.
fn localProblem(a: std.mem.Allocator, path: []const u8) !?[]const u8 {
    _ = a;
    if (objstore.isRemote(path)) return null;
    const st = std.posix.fstatat(std.fs.cwd().fd, path, std.posix.AT.SYMLINK_NOFOLLOW) catch return null;
    if (std.posix.S.ISLNK(st.mode)) return "it is a symbolic link, which a rewrite would replace with a file; name the file it points to";
    if (!std.posix.S.ISREG(st.mode)) return "it is not a regular file";
    if (st.nlink > 1) return "it has other hard links, which a rewrite put in its place would no longer share";
    return null;
}

/// A local file's size, modification time and inode when it was first read, or an
/// object's ETag: a rewrite is not put in its place when they changed, as another
/// writer did. An object's is checked by the store itself, the upload sent with
/// `If-Match`.
const Guard = struct {
    st: ?std.posix.Stat = null,
    etag: ?[]const u8 = null,
    path: []const u8,

    fn take(a: std.mem.Allocator, path: []const u8) !Guard {
        if (objstore.isRemote(path)) return .{ .path = path, .etag = objstore.etagOf(a, path) catch null };
        const st = std.posix.fstatat(std.fs.cwd().fd, path, 0) catch return .{ .path = path };
        return .{ .st = st, .path = path };
    }

    fn holds(self: Guard) bool {
        const was = self.st orelse return true;
        const now = std.posix.fstatat(std.fs.cwd().fd, self.path, 0) catch return false;
        return now.size == was.size and now.ino == was.ino and now.mtime().sec == was.mtime().sec and now.mtime().nsec == was.mtime().nsec;
    }
};

const Opened = struct { src: driver.Source, data: types.Schema, array: bool = false, loc: ?xlsxread.Located = null, first_row: u32 = 0, c0: u32 = 0 };

/// The file's first read, serial, each row with its identity last; `kind` turns to
/// `whole` for a JSON file that is one array rather than NDJSON.
fn openRead(env: *Env, read: ast.Stage, format: analyze.FileFormat, kind: *Kind) !Opened {
    const a = env.arena;
    const rd = read.node.read;
    const path = rd.form.path;
    switch (kind.*) {
        .csv => {
            var ddiag = analyze.Diag{};
            var d = analyze.dialectFromHints(read.hints, &ddiag) catch |e| return env_mod.aErr(env, &ddiag, e);
            d.text = rd.text_cols;
            const r = csvfmt.CsvReader.open(a, path, d) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not read `{s}` ({s})", .{ path, @errorName(e) }));
            const sp = try csvfmt.Spans.open(a, r);
            return .{ .src = sp.source(), .data = r.schema };
        },
        .ndjson => {
            const s = stream.Stream.open(a, path) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not read `{s}` ({s})", .{ path, @errorName(e) }));
            const r = jsonread.Reader.open(a, env.gpa, s.reader, analyze.jsonOptions(read.hints)) catch |e| {
                s.close();
                return planErrT(env.diag, e, try msgfmt.allocPrint(a, "`{s}` is not valid JSON or NDJSON in its first rows", .{path}));
            };
            if (try jsonread.Spans.open(a, r)) |sp| {
                const own = try a.create(Owned);
                own.* = .{ .inner = sp.source(), .stream = s };
                return .{ .src = own.source(), .data = r.schema };
            }
            const array = r.scan.array;
            r.close();
            s.close();
            kind.* = .whole;
            const src = try source.openSourceProjected(env, rd, read.hints, null, &.{});
            const num = try Numbered.open(a, src);
            return .{ .src = num.source(), .data = src.schema(), .array = array };
        },
        .sheet => {
            var odiag = analyze.Diag{};
            const o = analyze.xlsxOptions(read.hints, &odiag) catch |e| return env_mod.aErr(env, &odiag, e);
            if (o.range != null) return planErr(env.diag, try msgfmt.allocPrint(a, "`{s}`: a sheet read through `range` cannot have its rows changed; drop `range`", .{path}));
            const x = xlsxread.Reader.open(a, env.gpa, path, o) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not read Excel workbook `{s}` ({s})", .{ path, @errorName(e) }));
            const loc = xlsxread.locate(a, env.gpa, path, o.sheet) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not read Excel workbook `{s}` ({s})", .{ path, @errorName(e) }));
            const num = try Numbered.open(a, x.source());
            return .{ .src = num.source(), .data = x.schema, .loc = loc, .first_row = x.first_row, .c0 = x.c0 };
        },
        .whole => {},
    }
    _ = format;
    const src = try source.openSourceProjected(env, rd, read.hints, null, &.{});
    const num = try Numbered.open(a, src);
    return .{ .src = num.source(), .data = src.schema() };
}

/// An UPDATE's new values in a file written again whole: each column a SET names, in
/// the file's order, its expression cast to the column's type, then the row's place;
/// the rewrite takes the other columns from the row as it reads it again, so the
/// first read decodes only the columns the conditions and the SETs read.
fn setProjection(env: *Env, w: ast.Write, data: types.Schema, ids: []const []const u8) ![]ast.SelectItem {
    const a = env.arena;
    const u = w.update.?;
    var items = std.array_list.Managed(ast.SelectItem).init(a);
    for (data.fields) |f| for (u.sets) |set| if (std.ascii.eqlIgnoreCase(set.column, f.name)) {
        const e = @constCast(try analyze.dmlOwn(a, w, try analyze.substExpr(a, set.expr, env.params_expr)));
        try items.append(.{ .computed = .{ .name = f.name, .expr = try ast.mk(a, .{ .cast = .{ .e = e, .ty = f.ty } }) } });
    };
    for (ids) |id| try items.append(.{ .field = .{ .parts = try a.dupe([]const u8, &.{id}) } });
    return items.toOwnedSlice();
}

/// A DELETE's rows as the rewrite takes them, their places alone, so the first read
/// decodes only the columns the conditions read.
fn idsOnly(a: std.mem.Allocator, ids: []const []const u8) ![]ast.SelectItem {
    const items = try a.alloc(ast.SelectItem, ids.len);
    for (items, ids) |*it, id| it.* = .{ .field = .{ .parts = try a.dupe([]const u8, &.{id}) } };
    return items;
}

/// Where a file written again whole finds each column's new value in the UPDATE's
/// changed rows (`setProjection`): its place there, or null for one no SET names.
fn setPlaces(a: std.mem.Allocator, w: ast.Write, data: types.Schema) ![]const ?usize {
    const out = try a.alloc(?usize, data.fields.len);
    var k: usize = 0;
    for (out, data.fields) |*o, f| {
        o.* = null;
        for (w.update.?.sets) |set| if (std.ascii.eqlIgnoreCase(set.column, f.name)) {
            o.* = k;
            k += 1;
            break;
        };
    }
    return out;
}

/// The UPDATE's new rows in a text file or a sheet: every column of the file, a SET
/// one its expression, then the identity columns.
fn projection(env: *Env, w: ast.Write, data: types.Schema, ids: []const []const u8) ![]ast.SelectItem {
    const a = env.arena;
    const u = w.update.?;
    const items = try a.alloc(ast.SelectItem, data.fields.len + ids.len);
    for (items[0..data.fields.len], data.fields) |*it, f| {
        it.* = .{ .field = .{ .parts = try a.dupe([]const u8, &.{f.name}) } };
        for (u.sets) |set| if (std.ascii.eqlIgnoreCase(set.column, f.name)) {
            const e = @constCast(try analyze.dmlOwn(a, w, try analyze.substExpr(a, set.expr, env.params_expr)));
            it.* = .{ .computed = .{ .name = f.name, .expr = e } };
        };
    }
    for (items[data.fields.len..], ids) |*it, id| it.* = .{ .field = .{ .parts = try a.dupe([]const u8, &.{id}) } };
    return items;
}

/// The changed rows, sorted by identity, one at a time; a batch is held until its
/// last row is taken.
const Hits = struct {
    op: op.Op,
    mem: std.heap.ArenaAllocator,
    id: usize,
    end: ?usize,
    width: usize,
    b: ?Batch = null,
    i: usize = 0,
    done: bool = false,

    fn peek(self: *Hits) !?u64 {
        while (true) {
            if (self.b) |b| if (self.i < b.len) return @intCast(b.columns[self.id].getValue(self.i).int);
            if (self.done) return null;
            _ = self.mem.reset(.retain_capacity);
            self.b = try self.op.next(self.mem.allocator());
            self.i = 0;
            if (self.b == null) self.done = true;
        }
    }

    fn spanEnd(self: *const Hits) u64 {
        return @intCast(self.b.?.columns[self.end.?].getValue(self.i).int);
    }
};

/// Copies `path`'s decoded bytes into its rewrite, each changed record's span left
/// out or replaced; the rows changed.
fn rewriteText(env: *Env, w: ast.Write, path: []const u8, kind: Kind, which: Op, read: ast.Stage, data: types.Schema, hits: *Hits, guard: Guard, what: []const u8) !u64 {
    const a = env.arena;
    if (stream.splitCodec(path).codec == .zstd) return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: basalt writes no zstd, so it cannot write the file again", .{what}));
    var ddiag = analyze.Diag{};
    const dialect = analyze.dialectFromHints(read.hints, &ddiag) catch |e| return env_mod.aErr(env, &ddiag, e);
    const in = stream.Stream.open(a, path) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not read `{s}` ({s})", .{ path, @errorName(e) }));
    defer in.close();
    const out = Output.open(a, path, if (kind == .csv) "text/csv" else "application/x-ndjson", .truncate) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not write `{s}` ({s})", .{ path, @errorName(e) }));
    var ok = false;
    defer if (!ok) out.abort();
    if (guard.etag) |e| out.ifMatch(e);
    try out.compress(path);
    var cp = Copy{ .in = in.reader, .out = out.writer() };
    var n: u64 = 0;
    var last: ?u64 = null;
    var line = std.heap.ArenaAllocator.init(env.gpa);
    defer line.deinit();
    while (try hits.peek()) |start| {
        if (last) |l| if (start <= l)
            return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: a row matches more than one row of the FROM source, so it would take one of several values; give each key once", .{what}));
        last = start;
        const end = hits.spanEnd();
        try cp.copyTo(start);
        if (kind == .ndjson and which == .update) {
            _ = line.reset(.retain_capacity);
            const la = line.allocator();
            const orig = try cp.take(la, end);
            const row = try op.sliceBatch(la, hits.b.?, hits.i, 1);
            const one = Batch{ .schema = &data, .columns = row.columns[0..hits.width], .len = 1 };
            try cp.out.writeAll(try patchObject(la, orig, try jsonwrite.renderLines(la, data, one), w.update.?.sets, data));
            n += 1;
            hits.i += 1;
            continue;
        }
        const tail = try cp.skipTo(end);
        if (kind == .ndjson and which == .delete) try cp.skipLineEnd();
        if (which == .update) {
            _ = line.reset(.retain_capacity);
            const la = line.allocator();
            const row = try op.sliceBatch(la, hits.b.?, hits.i, 1);
            const cols = row.columns[0..hits.width];
            const one = Batch{ .schema = &data, .columns = cols, .len = 1 };
            var text = switch (kind) {
                .csv => blk: {
                    var aw = std.Io.Writer.Allocating.init(la);
                    try csvfmt.renderRows(&aw.writer, one, dialect.delim);
                    break :blk aw.written();
                },
                else => try jsonwrite.renderLines(la, data, one),
            };
            if (text.len > 0 and text[text.len - 1] == '\n') text = text[0 .. text.len - 1];
            if (kind == .csv) text = csvfmt.encodeField(la, dialect.encoding, text) catch
                return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: an updated row holds a character {s} cannot hold", .{ what, @tagName(dialect.encoding) }));
            try cp.out.writeAll(text);
            if (kind == .csv) try cp.out.writeAll(tail.ending());
        }
        n += 1;
        hits.i += 1;
    }
    if (n == 0) return 0;
    try cp.rest();
    if (!guard.holds()) return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: the file changed while it was rewritten, so the rewrite is not put in its place; run it again", .{what}));
    out.finish() catch |e| {
        const x = out.specific(e);
        const why = @import("../../exec/eval.zig").takeFailure(x) orelse @errorName(x);
        return planErrT(env.diag, x, try msgfmt.allocPrint(a, "{s}: could not write it ({s})", .{ what, why }));
    };
    ok = true;
    return n;
}

/// The last bytes of a skipped record, for the line ending its replacement takes.
const Tail = struct {
    b: [2]u8 = undefined,
    len: usize = 0,

    fn ending(self: *const Tail) []const u8 {
        const t = self.b[0..self.len];
        if (std.mem.endsWith(u8, t, "\r\n")) return "\r\n";
        if (std.mem.endsWith(u8, t, "\n")) return "\n";
        if (std.mem.endsWith(u8, t, "\r")) return "\r";
        return "";
    }
};

/// A decoded stream copied to the rewrite up to a byte, or passed over.
const Copy = struct {
    in: *std.Io.Reader,
    out: *std.Io.Writer,
    at: u64 = 0,

    fn copyTo(self: *Copy, pos: u64) !void {
        while (self.at < pos) {
            const k = self.in.stream(self.out, .limited64(pos - self.at)) catch |e| switch (e) {
                error.EndOfStream => return,
                else => |x| return x,
            };
            self.at += k;
        }
    }

    /// Passes over the bytes up to `pos` (or the end), keeping the last two.
    fn skipTo(self: *Copy, pos: u64) !Tail {
        var t = Tail{};
        if (pos <= self.at) return t;
        const n = pos - self.at;
        if (n > 2) {
            var left = n - 2;
            while (left > 0) {
                const k = self.in.discard(.limited64(left)) catch |e| switch (e) {
                    error.EndOfStream => break,
                    else => |x| return x,
                };
                left -= k;
                self.at += k;
            }
        }
        const want: usize = @intCast(@min(n, 2));
        t.len = try self.in.readSliceShort(t.b[0..want]);
        self.at += t.len;
        return t;
    }

    /// Passes over spaces and tabs and one line ending, as an NDJSON line holding a
    /// deleted object goes whole.
    fn skipLineEnd(self: *Copy) !void {
        while (true) {
            const c = self.in.peekByte() catch |e| switch (e) {
                error.EndOfStream => return,
                else => |x| return x,
            };
            switch (c) {
                ' ', '\t' => {},
                '\r' => {
                    self.in.toss(1);
                    self.at += 1;
                    const d = self.in.peekByte() catch return;
                    if (d == '\n') {
                        self.in.toss(1);
                        self.at += 1;
                    }
                    return;
                },
                '\n' => {
                    self.in.toss(1);
                    self.at += 1;
                    return;
                },
                else => return,
            }
            self.in.toss(1);
            self.at += 1;
        }
    }

    /// The bytes up to `pos`, taken rather than copied.
    fn take(self: *Copy, a: std.mem.Allocator, pos: u64) ![]const u8 {
        if (pos <= self.at) return "";
        const buf = try a.alloc(u8, @intCast(pos - self.at));
        const got = try self.in.readSliceShort(buf);
        self.at += got;
        return buf[0..got];
    }

    fn rest(self: *Copy) !void {
        self.at += try self.in.streamRemaining(self.out);
    }
};

/// Reads `path` again and writes it whole through its format's writer, each row the
/// changes leave in its place; the rows changed.
fn rewriteWhole(env: *Env, w: ast.Write, format: analyze.FileFormat, want: ?analyze.FileFormat, array: bool, which: Op, read: ast.Stage, data: types.Schema, hits: *Hits, guard: Guard, what: []const u8, opts: RunOptions) !u64 {
    const a = env.arena;
    const src = try source.openSourceProjected(env, read.node.read, read.hints, null, &.{});
    const num = try Numbered.open(a, src);
    defer num.close();
    var target = w;
    target.update = null;
    target.delete = null;
    target.mode = .overwrite;
    var wdiag = analyze.Diag{};
    const saved = .{ env.write_opts, env.fmt_out, env.csv_out };
    defer {
        env.write_opts = saved[0];
        env.fmt_out = saved[1];
        env.csv_out = saved[2];
    }
    env.fmt_out = want;
    env.write_opts = analyze.writeOptions(a, target, read.hints, &wdiag) catch |e| return env_mod.aErr(env, &wdiag, e);
    if (array) env.write_opts = .{ .json = .array };
    if (guard.etag) |e| @import("../../format/output.zig").expectVersion(w.target, e);
    defer @import("../../format/output.zig").forgetVersion();
    const sink = try formats_open.of(format).write(env, target, data, .truncate);
    var ok = false;
    defer if (!ok) sink.abort();

    const places: []const ?usize = if (which == .update) try setPlaces(a, w, data) else blk: {
        const none = try a.alloc(?usize, data.fields.len);
        @memset(none, null);
        break :blk none;
    };
    const piped = sink.unitRows() > 0 and opts.lanes() > 1;
    var up: UnitPipeline = undefined;
    if (piped) try up.start(a, env.gpa, sink, opts.lanes() - 1);
    defer if (piped) up.shutdown();
    var mem = std.heap.ArenaAllocator.init(env.gpa);
    defer mem.deinit();
    var n: u64 = 0;
    var last: ?u64 = null;
    const twice = "{s}: a row matches more than one row of the FROM source, so it would take one of several values; give each key once";
    while (try num.next(mem.allocator())) |b| {
        defer _ = mem.reset(.retain_capacity);
        const ma = mem.allocator();
        const first: u64 = num.n - b.len;
        const top: u64 = num.n;
        var out = b.columns[0..data.fields.len];
        var len = b.len;
        if (try hits.peek()) |h| if (h < top) {
            out = try ma.dupe(column.Column, out);
            const keep = try ma.alloc(bool, b.len);
            @memset(keep, true);
            const builders = try ma.alloc(column.Builder, places.len);
            for (builders, places, data.fields) |*bd, at, f| if (at != null) {
                bd.* = try column.Builder.initCapacity(ma, f.ty, b.len);
            };
            for (0..b.len) |r| {
                const ord = first + r;
                const hit = if (try hits.peek()) |x| x == ord else false;
                if (hit) {
                    if (last) |l| if (l == ord) return planErr(env.diag, try msgfmt.allocPrint(a, twice, .{what}));
                    last = ord;
                    n += 1;
                    keep[r] = false;
                }
                for (builders, places, out) |*bd, at, c| if (at) |i|
                    try bd.append(if (hit) hits.b.?.columns[i].getValue(hits.i) else c.getValue(r));
                if (hit) {
                    hits.i += 1;
                    if (try hits.peek()) |again| if (again == ord) return planErr(env.diag, try msgfmt.allocPrint(a, twice, .{what}));
                }
            }
            if (which == .update) {
                for (builders, places, out) |*bd, at, *o| if (at != null) {
                    o.* = try bd.finish();
                };
            } else {
                len = 0;
                for (keep) |k| len += @intFromBool(k);
                if (len == 0) continue;
                for (out) |*o| o.* = try column.gather(ma, o.*, keep, len);
            }
        };
        const batch = Batch{ .schema = &data, .columns = out, .len = len };
        if (piped) try up.submit(batch) else try sink.writeBatch(ma, batch);
    }
    if (piped and n > 0) try up.finish();
    if (n == 0) return 0;
    if (!guard.holds()) return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: the file changed while it was rewritten, so the rewrite is not put in its place; run it again", .{what}));
    sink.close() catch |e| return planErrT(env.diag, e, try msgfmt.allocPrint(a, "{s}: could not write it ({s})", .{ what, @errorName(e) }));
    ok = true;
    return n;
}

/// NDJSON object `orig` (leading spaces and all) with the values of the columns `sets`
/// name taken from `fresh`, the updated row as the writer renders it: each replaces
/// that key's value where `orig` has the key, or is added before its closing brace;
/// every other byte stays.
fn patchObject(a: std.mem.Allocator, orig: []const u8, fresh: []const u8, sets: []const ast.Update.Set, data: types.Schema) ![]const u8 {
    const jt = @import("../../exec/json.zig");
    const brace = std.mem.indexOfScalar(u8, orig, '{') orelse return fresh;
    const obj = orig[brace..];
    const Edit = struct { at: usize, end: usize, text: []const u8 };
    var edits = std.array_list.Managed(Edit).init(a);
    var added = std.array_list.Managed(u8).init(a);
    var has_members = false;
    var probe = jt.Members.root(obj);
    if (probe.next() != null) has_members = true;
    for (data.fields) |f| {
        const set = for (sets) |st| {
            if (std.ascii.eqlIgnoreCase(st.column, f.name)) break true;
        } else false;
        if (!set) continue;
        var nm = jt.Members.root(fresh);
        const value = while (nm.next()) |m| {
            if (std.mem.eql(u8, try jt.decodeString(a, m.key), f.name)) break m.value;
        } else continue;
        var om = jt.Members.root(obj);
        const old = while (om.next()) |m| {
            if (std.mem.eql(u8, try jt.decodeString(a, m.key), f.name)) break m.value;
        } else null;
        if (old) |v| {
            const at = @intFromPtr(v.ptr) - @intFromPtr(obj.ptr);
            try edits.append(.{ .at = at, .end = at + v.len, .text = value });
        } else {
            if (has_members or added.items.len > 0) try added.append(',');
            var aw = std.Io.Writer.Allocating.init(a);
            try @import("../../exec/jsontext.zig").key(&aw.writer, f.name);
            try added.appendSlice(aw.written());
            try added.appendSlice(value);
        }
    }
    std.mem.sort(Edit, edits.items, {}, struct {
        fn lt(_: void, x: Edit, y: Edit) bool {
            return x.at < y.at;
        }
    }.lt);
    const close = std.mem.lastIndexOfScalar(u8, obj, '}') orelse return fresh;
    var out = std.array_list.Managed(u8).init(a);
    try out.appendSlice(orig[0..brace]);
    var at: usize = 0;
    for (edits.items) |e| {
        try out.appendSlice(obj[at..e.at]);
        try out.appendSlice(e.text);
        at = e.end;
    }
    try out.appendSlice(obj[at..close]);
    try out.appendSlice(added.items);
    try out.appendSlice(obj[close..]);
    return out.items;
}

/// Writes the workbook at `path` again with its sheet's rows changed as the hits say
/// (`xlsx/edit.zig`); the rows changed.
fn rewriteXlsx(env: *Env, path: []const u8, first: Opened, which: Op, hits: *Hits, guard: Guard, what: []const u8) !u64 {
    const a = env.arena;
    var changes = std.array_list.Managed(xlsxedit.Change).init(a);
    while (try hits.peek()) |ord| {
        const row: u32 = first.first_row + @as(u32, @intCast(ord));
        if (changes.items.len > 0 and changes.items[changes.items.len - 1].row == row)
            return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: a row matches more than one row of the FROM source, so it would take one of several values; give each key once", .{what}));
        var c = xlsxedit.Change{ .row = row };
        if (which == .update) {
            const one = try op.sliceBatch(a, hits.b.?, hits.i, 1);
            const cols = try a.alloc(column.Column, hits.width);
            for (cols, one.columns[0..hits.width]) |*o, col| o.* = try column.copy(a, col);
            c.batch = .{ .schema = &first.data, .columns = cols, .len = 1 };
        }
        try changes.append(c);
        hits.i += 1;
    }
    if (changes.items.len == 0) return 0;
    const out = Output.open(a, path, @import("../../format/xlsx/write.zig").content_type, .truncate) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(a, "could not write `{s}` ({s})", .{ path, @errorName(e) }));
    var ok = false;
    defer if (!ok) out.abort();
    if (guard.etag) |e| out.ifMatch(e);
    xlsxedit.rewrite(a, path, .{ .loc = first.loc.?, .c0 = first.c0, .changes = changes.items }, out.raw()) catch |e| {
        const why = @import("../../exec/eval.zig").takeFailure(e) orelse @errorName(e);
        return planErrT(env.diag, e, try msgfmt.allocPrint(a, "{s}: {s}", .{ what, why }));
    };
    if (!guard.holds()) return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: the file changed while it was rewritten, so the rewrite is not put in its place; run it again", .{what}));
    out.finish() catch |e| {
        const x = out.specific(e);
        const why = @import("../../exec/eval.zig").takeFailure(x) orelse @errorName(x);
        return planErrT(env.diag, x, try msgfmt.allocPrint(a, "{s}: could not write it ({s})", .{ what, why }));
    };
    ok = true;
    return changes.items.len;
}

/// A source's rows numbered from 0 in the order read, the number a last column.
const Numbered = struct {
    inner: driver.Source,
    schema: types.Schema,
    n: u64 = 0,

    fn open(a: std.mem.Allocator, inner: driver.Source) !*Numbered {
        const s = inner.schema();
        const fields = try a.alloc(types.Schema.Field, s.fields.len + 1);
        @memcpy(fields[0..s.fields.len], s.fields);
        fields[s.fields.len] = .{ .name = row_name, .ty = types.Type.init(.int) };
        const self = try a.create(Numbered);
        self.* = .{ .inner = inner, .schema = .{ .fields = fields } };
        return self;
    }

    pub fn next(self: *Numbered, arena: std.mem.Allocator) !?Batch {
        const b = (try self.inner.next(arena)) orelse return null;
        var bd = try column.Builder.initCapacity(arena, types.Type.init(.int), b.len);
        for (0..b.len) |i| try bd.append(.{ .int = @intCast(self.n + i) });
        self.n += b.len;
        const cols = try arena.alloc(column.Column, b.columns.len + 1);
        @memcpy(cols[0..b.columns.len], b.columns);
        cols[b.columns.len] = try bd.finish();
        return .{ .schema = &self.schema, .columns = cols, .len = b.len };
    }

    pub fn close(self: *Numbered) void {
        self.inner.close();
    }

    fn source(self: *Numbered) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sourceVTable(Numbered);
};

/// A source owning the stream it reads, closed with it.
const Owned = struct {
    inner: driver.Source,
    stream: *stream.Stream,

    pub fn schema(self: *Owned) types.Schema {
        return self.inner.schema();
    }

    pub fn next(self: *Owned, arena: std.mem.Allocator) !?Batch {
        return self.inner.next(arena);
    }

    pub fn close(self: *Owned) void {
        self.inner.close();
        self.stream.close();
    }

    fn source(self: *Owned) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sourceVTable(Owned);
};

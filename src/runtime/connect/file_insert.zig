//! The sink of an INSERT into one existing file. A CSV or NDJSON file takes the rows
//! at its end, appended in place when local (`format/output.zig`'s append, cut back
//! to where the file ended when the write fails), or, remote, written whole as the
//! old bytes and then the new ones in one upload: a CSV's rows matched to its header
//! by name and written in its delimiter, encoding and line ending, each value the
//! type its column reads as from the file, refused when not exactly. Any other format
//! (Parquet, Avro, Arrow IPC, a JSON array) cannot be added to, so the file is
//! written again whole, its rows first, then the new ones, each matched to the file's
//! columns by name and cast to their types, a column it lacks refused and one the
//! rows lack NULL; the rewrite takes the old file's place in one step, unless the
//! file changed since it was read.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const column = @import("../../exec/column.zig");
const driver = @import("../../exec/driver.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const cast = @import("../../exec/eval/cast.zig");
const support = @import("../../exec/eval/support.zig");
const Value = @import("../../exec/value.zig").Value;
const csvfmt = @import("../../format/csv.zig");
const jsonwrite = @import("../../format/json/write.zig");
const stream = @import("../../format/stream.zig");
const formats = @import("../../format/formats.zig");
const analyze = @import("../analyze.zig");
const env_mod = @import("../env.zig");
const formats_open = @import("formats_open.zig");
const source = @import("source.zig");
const xlsxread = @import("../../format/xlsx/read.zig");
const xlsxedit = @import("../../format/xlsx/edit.zig");
const Output = @import("../../format/output.zig").Output;
const UnitPipeline = @import("../lanes/map.zig").UnitPipeline;

const Env = env_mod.Env;
const planErr = env_mod.planErr;
const planErrT = env_mod.planErrT;

/// The sink of INSERT `w` of rows shaped `schema` into the file `w.target`.
pub fn open(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    const a = env.arena;
    const path = w.target;
    const remote = stream.isUrl(path);
    if (!remote) std.fs.cwd().access(path, .{}) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(a, "INSERT INTO {s}: there is no such file; `INSERT` adds rows to an existing one, `LOAD INTO` writes a new one", .{path}));
    const format = analyze.readFormat(path, env.fmt_out);
    if (remote) if (@import("../../store/objstore.zig").etagOf(a, path) catch null) |e| @import("../../format/output.zig").expectVersion(path, e);
    defer @import("../../format/output.zig").forgetVersion();
    const failed = struct {
        fn f(e_: *Env, p: []const u8, e: anyerror) anyerror {
            if (e == error.OutOfMemory) return e;
            const why = @import("../../exec/eval.zig").takeFailure(e) orelse @errorName(e);
            return planErrT(e_.diag, e, msgfmt.allocPrint(e_.arena, "INSERT INTO {s}: {s}", .{ p, why }) catch "INSERT failed");
        }
    }.f;
    switch (formats.of(format).write.change) {
        .text => if (format == .csv) {
            const cw = csvfmt.CsvWriter.openInsert(a, path, schema, env.csv_out) catch |e| return failed(env, path, e);
            return Typed.wrap(env, cw.sink(), path, format, schema);
        } else if (!try isArray(a, path)) {
            const jw = jsonwrite.Writer.openInsert(a, path, schema) catch |e| return failed(env, path, e);
            return Typed.wrap(env, jw.sink(), path, format, schema);
        } else {
            const saved = env.write_opts;
            defer env.write_opts = saved;
            env.write_opts = .{ .json = .array };
            return Rewrite.open(env, w, format, schema);
        },
        .sheet => return XlsxInsert.open(env, w, schema),
        .whole, .refused => return Rewrite.open(env, w, format, schema),
    }
}

/// The rows of an INSERT into a CSV or NDJSON file, each value made the type its
/// column is read as from the file (as a read sniffs or samples it), so the file
/// reads back whole: 40.00 goes in as 40 where the column reads as INT. A value
/// that type cannot hold exactly, 40.5 there, is refused rather than rounded.
const Typed = struct {
    inner: driver.Sink,
    to: []const ?types.Type,
    schema: types.Schema,
    path: []const u8,

    fn wrap(env: *Env, inner: driver.Sink, path: []const u8, format: analyze.FileFormat, rows: types.Schema) !driver.Sink {
        errdefer inner.abort();
        const a = env.arena;
        const file = (try fileTypes(env, path, format)) orelse return inner;
        const to = try a.alloc(?types.Type, rows.fields.len);
        const fields = try a.dupe(types.Schema.Field, rows.fields);
        var any = false;
        for (to, fields) |*t, *f| {
            t.* = null;
            const i = file.indexOf(f.name) orelse continue;
            const want = file.fields[i].ty;
            if (want.asNullable().eql(f.ty.asNullable())) continue;
            t.* = want;
            f.ty = want.asNullable();
            any = true;
        }
        if (!any) return inner;
        const self = try a.create(Typed);
        self.* = .{ .inner = inner, .to = to, .schema = .{ .fields = fields }, .path = path };
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// The columns of the file at `path` as a read of it types them, in the INSERT's
    /// delimiter and encoding; null for a file holding no byte, which has none.
    fn fileTypes(env: *Env, path: []const u8, format: analyze.FileFormat) !?types.Schema {
        const a = env.arena;
        if (!stream.isUrl(path)) {
            const st = std.posix.fstatat(std.fs.cwd().fd, path, 0) catch return null;
            if (st.size == 0) return null;
        }
        const at: ast.Pos = .{ .line = 0, .col = 0 };
        var hints = std.array_list.Managed(ast.Hint).init(a);
        if (env.fmt_out) |f| try hints.append(.{ .key = "format", .value = .{ .str = formats.of(f).names[0] }, .pos = at });
        if (format == .csv) {
            try hints.append(.{ .key = "delimiter", .value = .{ .str = try a.dupe(u8, &.{env.csv_out.delim}) }, .pos = at });
            try hints.append(.{ .key = "encoding", .value = .{ .str = @tagName(env.csv_out.encoding) }, .pos = at });
        }
        const src = try source.openSourceProjected(env, .{ .connector = "csv", .form = .{ .path = path } }, hints.items, null, &.{});
        defer src.close();
        const got = src.schema();
        return .{ .fields = try a.dupe(types.Schema.Field, got.fields) };
    }

    pub fn writeBatch(self: *Typed, arena: std.mem.Allocator, b: Batch) !void {
        const cols = try arena.alloc(column.Column, b.columns.len);
        for (cols, b.columns, self.to, self.schema.fields) |*c, src, to, f| {
            const ty = to orelse {
                c.* = src;
                continue;
            };
            var bd = try column.Builder.initCapacity(arena, ty.asNullable(), b.len);
            for (0..b.len) |r| try bd.append(try self.exact(arena, src.getValue(r), ty, src.ty, f.name));
            c.* = try bd.finish();
        }
        return self.inner.writeBatch(arena, .{ .schema = &self.schema, .columns = cols, .len = b.len });
    }

    /// `v` as `to`, when it casts there and back to `from` unchanged.
    fn exact(self: *Typed, arena: std.mem.Allocator, v: Value, to: types.Type, from: types.Type, name: []const u8) !Value {
        if (v == .null) return .null;
        const fits = blk: {
            const w = cast.castValueTyped(arena, v, to) catch break :blk null;
            const back = cast.castValueTyped(arena, w, from) catch break :blk null;
            const o = support.compareValues(back, v) orelse break :blk null;
            break :blk if (o == .eq) w else null;
        };
        if (fits) |w| return w;
        const text = cast.castValue(arena, v, .string) catch Value{ .string = "?" };
        return support.failWith(error.CastFailed, "INSERT INTO {s}: column `{s}` reads as {s} from the file, and {s} is not one exactly; CAST it in the query, or write the rows to a new file with LOAD INTO", .{ self.path, name, try to.name(arena), support.clip(text.string) });
    }

    pub fn close(self: *Typed) !void {
        return self.inner.close();
    }

    pub fn abort(self: *Typed) void {
        self.inner.abort();
    }

    const vtable = driver.sinkVTable(Typed);
};

/// Whether the JSON file `path` is one top-level array rather than NDJSON: its name
/// says NDJSON (`.ndjson`, `.jsonl`), else its first byte past blanks is `[`.
fn isArray(a: std.mem.Allocator, path: []const u8) !bool {
    const name = stream.dataName(path);
    if (std.mem.endsWith(u8, name, ".ndjson") or std.mem.endsWith(u8, name, ".jsonl")) return false;
    const s = stream.Stream.open(a, path) catch return false;
    defer s.close();
    while (true) {
        const c = s.reader.peekByte() catch return false;
        switch (c) {
            ' ', '\t', '\r', '\n' => s.reader.toss(1),
            else => return c == '[',
        }
    }
}

/// An INSERT into one sheet of a workbook: the rows, matched to the sheet's columns by
/// name, held until the end, then added after its last row as the workbook is
/// written again with every other part as it was (`xlsx/edit.zig`).
const XlsxInsert = struct {
    arena: std.mem.Allocator,
    path: []const u8,
    loc: xlsxread.Located,
    c0: u32,
    file: types.Schema,
    order: []const ?usize,
    rows: std.array_list.Managed(Batch),
    guard: ?std.posix.Stat,
    etag: ?[]const u8,

    fn open(env: *Env, w: ast.Write, rows: types.Schema) !driver.Sink {
        const a = env.arena;
        const path = w.target;
        const sheet: ?[]const u8 = if (env.write_opts == .xlsx and env.write_opts.xlsx.len > 0) env.write_opts.xlsx else null;
        const x = xlsxread.Reader.open(a, env.gpa, path, .{ .sheet = sheet }) catch |e|
            return planErrT(env.diag, e, try msgfmt.allocPrint(a, "INSERT INTO {s}: could not read the workbook ({s}); INSERT adds rows to a sheet the workbook has, and `LOAD INTO` with that `sheet` writes a new one beside the others", .{ path, try formats_open.workbookFail(env, path, sheet, e) }));
        defer x.close();
        const loc = try xlsxread.locate(a, env.gpa, path, sheet);
        if (try xlsxedit.problem(a, path, loc, .insert)) |why| return planErr(env.diag, try msgfmt.allocPrint(a, "INSERT INTO {s}: {s}", .{ path, why }));
        const file = x.schema;
        const order = try a.alloc(?usize, file.fields.len);
        for (order, file.fields) |*o, f| o.* = for (rows.fields, 0..) |r, i| {
            if (std.mem.eql(u8, r.name, f.name)) break i;
        } else null;
        for (rows.fields) |r| if (for (file.fields) |f| {
            if (std.mem.eql(u8, f.name, r.name)) break false;
        } else true)
            return planErr(env.diag, try msgfmt.allocPrint(a, "INSERT INTO {s}: the rows have column `{s}`, which sheet `{s}` lacks", .{ path, r.name, loc.sheet_name }));
        const self = try a.create(XlsxInsert);
        self.* = .{
            .arena = a,
            .path = path,
            .loc = loc,
            .c0 = x.c0,
            .file = file,
            .order = order,
            .rows = std.array_list.Managed(Batch).init(a),
            .guard = if (stream.isUrl(path)) null else std.posix.fstatat(std.fs.cwd().fd, path, 0) catch null,
            .etag = @import("../../store/objstore.zig").etagOf(a, path) catch null,
        };
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn writeBatch(self: *XlsxInsert, arena: std.mem.Allocator, b: Batch) !void {
        _ = arena;
        const cols = try self.arena.alloc(column.Column, self.file.fields.len);
        for (cols, self.order) |*c, o| c.* = if (o) |i| try column.copy(self.arena, b.columns[i]) else blk: {
            var bd = try column.Builder.initCapacity(self.arena, types.Type.init(.string).asNullable(), b.len);
            for (0..b.len) |_| try bd.append(.null);
            break :blk try bd.finish();
        };
        try self.rows.append(.{ .schema = &self.file, .columns = cols, .len = b.len });
    }

    pub fn close(self: *XlsxInsert) !void {
        const out = try Output.open(self.arena, self.path, @import("../../format/xlsx/write.zig").content_type, .truncate);
        errdefer out.abort();
        if (self.etag) |e| out.ifMatch(e);
        try xlsxedit.rewrite(self.arena, self.path, .{ .loc = self.loc, .c0 = self.c0, .changes = &.{}, .appended = self.rows.items }, out.raw());
        if (self.guard) |was| {
            const now = std.posix.fstatat(std.fs.cwd().fd, self.path, 0) catch return error.FileChanged;
            if (now.size != was.size or now.ino != was.ino or now.mtime().sec != was.mtime().sec or now.mtime().nsec != was.mtime().nsec)
                return @import("../../exec/eval.zig").explain(error.FileChanged, "the file changed while it was written again, so the rewrite is not put in its place; run it again");
        }
        try out.finish();
    }

    pub fn abort(self: *XlsxInsert) void {
        _ = self;
    }

    const vtable = driver.sinkVTable(XlsxInsert);
};

/// A file written again whole: its own rows, their row groups encoded on the lanes
/// where the format encodes units (Parquet), then the INSERT's.
const Rewrite = struct {
    inner: driver.Sink,
    file: types.Schema,
    order: []const ?usize,
    guard: ?std.posix.Stat,
    path: []const u8,

    fn open(env: *Env, w: ast.Write, format: analyze.FileFormat, rows: types.Schema) !driver.Sink {
        const a = env.arena;
        const path = w.target;
        const guard: ?std.posix.Stat = if (stream.isUrl(path)) null else std.posix.fstatat(std.fs.cwd().fd, path, 0) catch null;
        var hints = std.array_list.Managed(ast.Hint).init(a);
        if (env.fmt_out) |f| try hints.append(.{ .key = "format", .value = .{ .str = formats.of(f).names[0] }, .pos = .{ .line = 0, .col = 0 } });
        const src = try source.openSourceProjected(env, .{ .connector = "csv", .form = .{ .path = path } }, hints.items, null, &.{});
        defer src.close();
        const file = src.schema();
        const order = try a.alloc(?usize, file.fields.len);
        for (order, file.fields) |*o, f| o.* = for (rows.fields, 0..) |r, i| {
            if (std.mem.eql(u8, r.name, f.name)) break i;
        } else null;
        for (rows.fields) |r| if (for (file.fields) |f| {
            if (std.mem.eql(u8, f.name, r.name)) break false;
        } else true)
            return planErr(env.diag, try msgfmt.allocPrint(a, "INSERT INTO {s}: the rows have column `{s}`, which the file lacks", .{ path, r.name }));
        var target = w;
        target.insert = null;
        target.mode = .overwrite;
        const sink = try formats_open.of(format).write(env, target, file, .truncate);
        errdefer sink.abort();
        var mem = std.heap.ArenaAllocator.init(env.gpa);
        defer mem.deinit();
        const lanes = env.pipe.threads;
        if (sink.unitRows() > 0 and lanes > 1) {
            var up: UnitPipeline = undefined;
            try up.start(a, env.gpa, sink, lanes - 1);
            defer up.shutdown();
            while (try src.next(mem.allocator())) |b| {
                try up.submit(.{ .schema = &file, .columns = b.columns, .len = b.len });
                _ = mem.reset(.retain_capacity);
            }
            try up.finish();
        } else while (try src.next(mem.allocator())) |b| {
            try sink.writeBatch(mem.allocator(), .{ .schema = &file, .columns = b.columns, .len = b.len });
            _ = mem.reset(.retain_capacity);
        }
        const self = try a.create(Rewrite);
        self.* = .{ .inner = sink, .file = file, .order = order, .guard = guard, .path = path };
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn writeBatch(self: *Rewrite, arena: std.mem.Allocator, b: Batch) !void {
        const cols = try arena.alloc(column.Column, self.file.fields.len);
        for (cols, self.file.fields, self.order) |*c, f, o| {
            var bd = try column.Builder.initCapacity(arena, f.ty.asNullable(), b.len);
            if (o) |i| {
                const src = b.columns[i];
                if (src.ty.eql(f.ty.asNullable()) or src.ty.eql(f.ty)) {
                    c.* = src;
                    continue;
                }
                for (0..b.len) |r| try bd.append(try cast.castValueTyped(arena, src.getValue(r), f.ty));
            } else for (0..b.len) |_| try bd.append(.null);
            c.* = try bd.finish();
        }
        return self.inner.writeBatch(arena, .{ .schema = &self.file, .columns = cols, .len = b.len });
    }

    pub fn close(self: *Rewrite) !void {
        if (self.guard) |was| {
            const now = std.posix.fstatat(std.fs.cwd().fd, self.path, 0) catch {
                self.inner.abort();
                return error.FileChanged;
            };
            if (now.size != was.size or now.ino != was.ino or now.mtime().sec != was.mtime().sec or now.mtime().nsec != was.mtime().nsec) {
                self.inner.abort();
                return @import("../../exec/eval.zig").explain(error.FileChanged, "the file changed while it was written again, so the rewrite is not put in its place; run it again");
            }
        }
        return self.inner.close();
    }

    pub fn abort(self: *Rewrite) void {
        self.inner.abort();
    }

    const vtable = driver.sinkVTable(Rewrite);
};

//! JSON and NDJSON as rows, for a file, stdin or a REST response alike. The input
//! is a run of JSON values (NDJSON, or values one after another), a top-level array
//! whose elements are the rows, or, with `items`, the array at that path of one
//! document. A row is an object; anything else is a row of NULLs, and a single
//! object is one row.
//!
//! Columns come from the first `sample` rows (`Infer`): their keys in the order first
//! met, each typed from the values it took there, NULLs aside: integers INT, any
//! float FLOAT, booleans BOOL, strings, nested objects and arrays, or a mix of kinds,
//! STRING, nested values as compact JSON text. A key never seen with a value is a
//! STRING, of unknown type when the sample read the whole input, so it unifies with
//! any type as a CSV column of NULLs does (`Type.nullColumn`); the first of several
//! files is not the whole input. Every row, sampled or later, is read into those
//! columns as `CAST` reads a value (`Rows`): a key missing from a row is NULL, a key
//! not among the columns is dropped, and a value its column cannot read is NULL
//! rather than a guess.
//!
//! The input streams: values are cut out of a buffer that holds about one batch of
//! rows (`Scan`), each validated and read in place by `exec/json.zig`, never built
//! into a tree. Only `items` reads its document whole, to find the path in it.

const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const column = @import("../../exec/column.zig");
const cell = @import("../../exec/cell.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const jt = @import("../../exec/json.zig");
const stream = @import("../stream.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");

pub const Error = error{ InvalidJson, OutOfMemory } || std.Io.Reader.ShortError;

pub const Shape = enum { auto, ndjson, json };

pub const Options = struct {
    shape: Shape = .auto,
    items: ?[]const u8 = null,
    sample: usize = 1024,
    batch_rows: usize = 2048,
    chunk: usize = 64 << 10,
    schema: ?types.Schema = null,
    /// Whether this input is all that is read, so a sample that reaches its end has
    /// seen every row; false for the first of several files.
    alone: bool = true,
};

pub const Infer = struct {
    names: std.ArrayListUnmanaged([]const u8) = .empty,
    samples: std.ArrayListUnmanaged(types.Sample) = .empty,
    index: std.StringHashMapUnmanaged(usize) = .empty,
    /// Whether the noted rows are the whole input, so a key never seen with a value
    /// is of unknown type (`Type.nullColumn`).
    whole: bool = false,

    /// Notes one row's keys and value kinds.
    pub fn note(self: *Infer, arena: std.mem.Allocator, row: []const u8) Error!void {
        if (row[0] != '{') return;
        var m = jt.Members.root(row);
        while (m.next()) |mem| {
            const key = try jt.decodeString(arena, mem.key);
            const gop = try self.index.getOrPut(arena, key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try arena.dupe(u8, key);
                gop.value_ptr.* = self.names.items.len;
                try self.names.append(arena, gop.key_ptr.*);
                try self.samples.append(arena, .none);
            }
            const s = &self.samples.items[gop.value_ptr.*];
            if (kindOf(mem.value)) |k| s.* = types.widenSample(s.*, k);
        }
    }

    /// A value's kind, null for JSON null; strings and nested values are text.
    fn kindOf(raw: []const u8) ?types.TypeKind {
        return switch (raw[0]) {
            'n' => null,
            't', 'f' => .bool,
            '"', '{', '[' => .string,
            else => switch (jt.number(raw)) {
                .int => .int,
                .float, .text => .float,
            },
        };
    }

    pub fn schema(self: *const Infer, arena: std.mem.Allocator) Error!types.Schema {
        const fields = try arena.alloc(types.Schema.Field, self.names.items.len);
        for (fields, self.names.items, self.samples.items) |*f, n, s| f.* = .{
            .name = n,
            .ty = if (s.kind()) |k| types.Type.init(k).asNullable() else types.Type.nullColumn(self.whole),
        };
        return .{ .fields = fields };
    }
};

pub const Rows = struct {
    schema: types.Schema,
    index: std.StringHashMapUnmanaged(usize) = .empty,
    builders: []column.Builder,
    fast: []Fast,
    vals: []?[]const u8,
    len: usize = 0,

    /// A column's shortcut, chosen once from its type: a plain number or a literal
    /// appended as `read` would append it, without the `Value` and cast between.
    const Fast = enum { none, int, float, bool };

    pub fn init(arena: std.mem.Allocator, schema: types.Schema, capacity: usize) Error!Rows {
        var r = Rows{
            .schema = schema,
            .builders = try arena.alloc(column.Builder, schema.fields.len),
            .fast = try arena.alloc(Fast, schema.fields.len),
            .vals = try arena.alloc(?[]const u8, schema.fields.len),
        };
        for (r.builders, r.fast, schema.fields, 0..) |*b, *fa, f, i| {
            b.* = try column.Builder.initCapacity(arena, f.ty, @max(capacity, 1));
            fa.* = switch (f.ty.kind) {
                .int => .int,
                .float => .float,
                .bool => .bool,
                else => .none,
            };
            try r.index.put(arena, f.name, i);
        }
        return r;
    }

    /// Appends one validated row: each column's key, the last of its name, read as its
    /// column's type, NULL when absent or unreadable. Rows mostly repeat one key order,
    /// so each key is first tried as the column after the last one matched.
    pub fn add(self: *Rows, arena: std.mem.Allocator, row: []const u8) Error!void {
        const vals = self.vals;
        const fields = self.schema.fields;
        @memset(vals, null);
        if (row[0] == '{') {
            var m = jt.Members.root(row);
            var at: usize = 0;
            while (m.next()) |mem| {
                const key = try jt.decodeString(arena, mem.key);
                const ci = if (at < fields.len and std.mem.eql(u8, fields[at].name, key)) at else self.index.get(key) orelse continue;
                vals[ci] = mem.value;
                at = ci + 1;
            }
        }
        for (self.builders, vals, fields, self.fast) |*b, raw, f, fa| {
            if (raw) |t| if (try appendFast(b, fa, t)) continue;
            try b.append(try read(arena, raw, f.ty));
        }
        self.len += 1;
    }

    /// Appends `raw` by its column's shortcut when it is the plain case the shortcut
    /// reads exactly as `read` would; false leaves it to `read`.
    fn appendFast(b: *column.Builder, fa: Fast, raw: []const u8) Error!bool {
        switch (fa) {
            .none => return false,
            .int => b.appendInt(cell.intFast(raw) orelse return false) catch return error.OutOfMemory,
            .float => {
                const x = cell.floatFast(raw) orelse return false;
                if (x == 0) return false;
                b.appendFloat(x) catch return error.OutOfMemory;
            },
            .bool => b.appendAs(.b, switch (raw[0]) {
                't' => true,
                'f' => false,
                else => return false,
            }) catch return error.OutOfMemory,
        }
        return true;
    }

    pub fn finish(self: *Rows, arena: std.mem.Allocator, schema: *const types.Schema) Error!Batch {
        const cols = try arena.alloc(column.Column, self.builders.len);
        for (self.builders, cols) |*b, *c| c.* = try b.finish();
        return .{ .schema = schema, .columns = cols, .len = self.len };
    }

    /// A JSON value as `ty`, as CAST reads it; a STRING column takes any value as text.
    fn read(arena: std.mem.Allocator, raw_in: ?[]const u8, ty: types.Type) Error!Value {
        const raw = raw_in orelse return .null;
        if (ty.kind == .string) return switch (try jt.cell(arena, raw)) {
            .null => .null,
            .text => |t| .{ .string = t },
        };
        const src: Value = switch (raw[0]) {
            'n' => return .null,
            't' => .{ .bool = true },
            'f' => .{ .bool = false },
            '"' => .{ .string = try jt.decodeString(arena, raw[1 .. raw.len - 1]) },
            '{', '[' => return .null,
            else => switch (jt.number(raw)) {
                .int => |x| .{ .int = x },
                .float => |x| .{ .float = x },
                .text => |t| .{ .string = t },
            },
        };
        return eval.castValueTyped(arena, src, ty) catch |e| switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            else => .null,
        };
    }
};

/// The columns of `rows`, each a validated row's text, from the first `sample`.
pub fn schemaOf(arena: std.mem.Allocator, rows: []const []const u8, sample: usize) Error!types.Schema {
    var infer = Infer{};
    for (rows[0..@min(rows.len, sample)]) |row| try infer.note(arena, row);
    return infer.schema(arena);
}

/// `rows`, each a validated row's text, read into `schema`'s columns as one batch.
pub fn batchOf(arena: std.mem.Allocator, schema: *const types.Schema, rows: []const []const u8) Error!Batch {
    var r = try Rows.init(arena, schema.*, rows.len);
    for (rows) |row| try r.add(arena, row);
    return r.finish(arena, schema);
}

/// The rows of one validated document: the array at `items`, or the document itself,
/// each element a row and an object a single row. Null when `items` is absent or
/// null, or what it names is neither an array nor an object.
pub fn rowsOf(arena: std.mem.Allocator, doc: []const u8, items: ?[]const u8) Error!?[]const []const u8 {
    const at = if (items) |p| (try field(arena, doc, p)) orelse return null else doc;
    const t = std.mem.trimLeft(u8, at, " \t\r\n");
    switch (t[0]) {
        '[' => {
            var out = std.array_list.Managed([]const u8).init(arena);
            var it = jt.Elements.root(t);
            while (it.next()) |e| try out.append(e);
            return out.items;
        },
        '{' => return try arena.dupe([]const u8, &.{t}),
        else => return null,
    }
}

/// The value at `path` (`a.b`, `a[0].b`) of a validated document, null when missing
/// or JSON null.
pub fn field(arena: std.mem.Allocator, doc: []const u8, path: []const u8) Error!?[]const u8 {
    const v = (try jt.path(arena, doc, path)) orelse return null;
    return if (std.mem.eql(u8, v, "null")) null else v;
}

pub const Scan = struct {
    in: ?*std.Io.Reader,
    gpa: std.mem.Allocator,
    buf: std.ArrayListUnmanaged(u8) = .empty,
    at: usize = 0,
    eof: bool = false,
    chunk: usize,
    array: bool = false,
    state: enum { start, first, more, done } = .start,
    mapped: bool = false,
    released: usize = 0,

    const release_every = 4 << 20;

    /// A scan of `text`, a slice of a file mapping read in place: nothing is copied,
    /// and the pages behind the scan are released as it goes (`stream.release`).
    pub fn ofMapped(gpa: std.mem.Allocator, text: []const u8) Scan {
        var sc = Scan{ .in = null, .gpa = gpa, .chunk = 0, .eof = true, .mapped = true };
        sc.buf.items = @constCast(text);
        return sc;
    }

    pub fn deinit(self: *Scan) void {
        if (!self.mapped) self.buf.deinit(self.gpa);
    }

    /// Reads at least `min` more bytes unless the stream ends first; false when nothing came.
    fn fill(self: *Scan, min: usize) Error!bool {
        if (self.eof) return false;
        if (self.at > 0 and self.at * 2 >= self.buf.items.len) {
            const rest = self.buf.items.len - self.at;
            std.mem.copyForwards(u8, self.buf.items[0..rest], self.buf.items[self.at..]);
            self.buf.items.len = rest;
            self.at = 0;
        }
        const want = @max(min, self.chunk);
        try self.buf.ensureUnusedCapacity(self.gpa, want);
        var got: usize = 0;
        while (got < want) {
            const dst = self.buf.unusedCapacitySlice();
            const ask = @min(dst.len, want - got);
            const n = try self.in.?.readSliceShort(dst[0..ask]);
            self.buf.items.len += n;
            got += n;
            if (n < ask) {
                self.eof = true;
                break;
            }
        }
        return got > 0;
    }

    /// The next byte after whitespace, reading more as needed; null at the end.
    fn peek(self: *Scan) Error!?u8 {
        while (true) {
            while (self.at < self.buf.items.len) : (self.at += 1) switch (self.buf.items[self.at]) {
                ' ', '\t', '\n', '\r' => {},
                else => return self.buf.items[self.at],
            };
            if (!try self.fill(0)) return null;
        }
    }

    /// Whether the input is one top-level array, judged by its first byte.
    pub fn detect(self: *Scan) Error!bool {
        return (try self.peek()) == '[';
    }

    /// The next whole value's text, or null at the end; in `array` mode the elements
    /// of the one top-level array. The text lives until the next call.
    pub fn next(self: *Scan) Error!?[]const u8 {
        if (self.array) {
            switch (self.state) {
                .start => {
                    if ((try self.peek()) != '[') return error.InvalidJson;
                    self.at += 1;
                    self.state = .first;
                },
                .done => return null,
                else => {},
            }
            const c = (try self.peek()) orelse return error.InvalidJson;
            if (c == ']') {
                self.at += 1;
                self.state = .done;
                if ((try self.peek()) != null) return error.InvalidJson;
                return null;
            }
            if (self.state == .more) {
                if (c != ',') return error.InvalidJson;
                self.at += 1;
            }
            self.state = .more;
        }
        _ = (try self.peek()) orelse {
            if (self.array) return error.InvalidJson;
            return null;
        };
        while (true) {
            const t = self.buf.items[self.at..];
            if (valueEnd(t, self.eof)) |end| {
                self.at += end;
                if (self.mapped and self.at - self.released >= release_every) {
                    stream.release(self.buf.items[self.released..self.at]);
                    self.released = self.at;
                }
                return t[0..end];
            }
            if (self.eof) return error.InvalidJson;
            _ = try self.fill(t.len);
        }
    }

    /// Just past the value at the start of `t`, or null when `t` ends inside it. A
    /// number or literal ends at a delimiter, or at the end of the input.
    fn valueEnd(t: []const u8, eof: bool) ?usize {
        switch (t[0]) {
            '"' => return stringEnd(t, 0),
            '{', '[' => {
                var depth: usize = 0;
                var j: usize = 0;
                while (std.mem.indexOfAnyPos(u8, t, j, "\"{}[]")) |k| {
                    j = k;
                    switch (t[j]) {
                        '"' => {
                            j = stringEnd(t, j) orelse return null;
                            continue;
                        },
                        '{', '[' => depth += 1,
                        else => {
                            depth -= 1;
                            if (depth == 0) return j + 1;
                        },
                    }
                    j += 1;
                }
                return null;
            },
            else => {
                const e = std.mem.indexOfAny(u8, t, " \t\r\n,]}") orelse return if (eof) t.len else null;
                return if (e == 0) 1 else e;
            },
        }
    }

    fn stringEnd(t: []const u8, i: usize) ?usize {
        var j = i + 1;
        while (std.mem.indexOfAnyPos(u8, t, j, "\"\\")) |k| {
            if (t[k] == '"') return k + 1;
            j = k + 2;
        }
        return null;
    }
};

pub const Reader = struct {
    arena: std.mem.Allocator,
    scan: Scan,
    opts: Options,
    schema: types.Schema,
    whole: ?[]const u8 = null,
    elems: ?jt.Elements = null,
    held: []const []const u8 = &.{},
    held_at: usize = 0,
    rows_seen: u64 = 0,
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, in: *std.Io.Reader, opts: Options) Error!*Reader {
        return start(arena, .{ .in = in, .gpa = gpa, .chunk = opts.chunk }, opts);
    }

    /// A reader of `text`, a slice of a file mapping, read in place (`Scan.ofMapped`).
    pub fn openMapped(arena: std.mem.Allocator, gpa: std.mem.Allocator, text: []const u8, opts: Options) Error!*Reader {
        return start(arena, Scan.ofMapped(gpa, text), opts);
    }

    fn start(arena: std.mem.Allocator, scan: Scan, opts: Options) Error!*Reader {
        const self = try arena.create(Reader);
        self.* = .{ .arena = arena, .scan = scan, .opts = opts, .schema = undefined };
        errdefer self.scan.deinit();
        if (opts.items) |path| {
            while (try self.scan.fill(0)) {}
            const doc = self.scan.buf.items;
            try jt.validate(arena, doc);
            if (std.mem.trim(u8, doc, " \t\r\n").len == 0) return error.InvalidJson;
            const at = (try jt.path(arena, doc, path)) orelse "[]";
            self.whole = at;
            if (jt.rootKind(at) == .array) self.elems = jt.Elements.root(at) else self.held = try arena.dupe([]const u8, &.{at});
        } else {
            self.scan.array = switch (opts.shape) {
                .ndjson => false,
                .json, .auto => try self.scan.detect(),
            };
        }
        if (opts.schema) |sch| {
            self.schema = sch;
            return self;
        }
        var infer = Infer{};
        var held = std.array_list.Managed([]const u8).init(arena);
        try held.appendSlice(self.held);
        while (held.items.len < opts.sample) {
            const row = (try self.nextRow(arena)) orelse {
                infer.whole = opts.alone;
                break;
            };
            try held.append(try arena.dupe(u8, row));
        }
        for (held.items) |row| try infer.note(arena, row);
        self.held = held.items;
        self.schema = try infer.schema(arena);
        return self;
    }

    pub fn next(self: *Reader, arena: std.mem.Allocator) Error!?Batch {
        if (self.done) return null;
        var rows = try Rows.init(arena, self.schema, self.opts.batch_rows);
        while (rows.len < self.opts.batch_rows) {
            if (self.held_at < self.held.len) {
                try rows.add(arena, self.held[self.held_at]);
                self.held_at += 1;
                continue;
            }
            const row = (try self.nextRow(arena)) orelse {
                self.done = true;
                break;
            };
            try rows.add(arena, row);
        }
        if (rows.len == 0) return null;
        return try rows.finish(arena, &self.schema);
    }

    /// The next row's text after the held ones, validated.
    fn nextRow(self: *Reader, arena: std.mem.Allocator) Error!?[]const u8 {
        const row = if (self.elems) |*e| e.next() else if (self.whole != null) null else try self.scan.next();
        const r = row orelse return null;
        self.rows_seen += 1;
        try jt.validate(arena, r);
        return r;
    }

    pub fn close(self: *Reader) void {
        self.scan.deinit();
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.schema;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.next(arena);
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        self.close();
    }

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

/// JSON files read as one table (`files.Files`); the first of several is not `alone`.
pub const Files = struct {
    const Table = @import("../files.zig").Files(Reader, Options);

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, files: []const []const u8, opts: Options) !*Table {
        var o = opts;
        o.alone = opts.alone and files.len <= 1;
        return Table.open(arena, gpa, files, o);
    }

    pub fn on(arena: std.mem.Allocator, gpa: std.mem.Allocator, s: *stream.Stream, rest: []const []const u8, opts: Options) !*Table {
        var o = opts;
        o.alone = opts.alone and rest.len == 0;
        return Table.on(arena, gpa, s, rest, o);
    }
};

const testing = std.testing;

fn readAll(a: std.mem.Allocator, text: []const u8, opts: Options) !struct { schema: types.Schema, rows: []const []const Value } {
    var in = std.Io.Reader.fixed(text);
    const r = try Reader.open(a, testing.allocator, &in, opts);
    defer r.close();
    var out = std.array_list.Managed([]const Value).init(a);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| {
            const row = try a.alloc(Value, b.columns.len);
            for (row, b.columns) |*v, c| v.* = c.getValue(i);
            try out.append(row);
        }
    }
    return .{ .schema = r.schema, .rows = out.items };
}

test "NDJSON, an array and a lone object read as the same rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const forms = [_][]const u8{
        "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n",
        "  [ {\"id\":1,\"name\":\"a\"} , {\"id\":2,\"name\":\"b\"} ]  ",
        "{\"id\":1,\"name\":\"a\"}{\"id\":2,\"name\":\"b\"}",
    };
    for (forms) |text| {
        const got = try readAll(a, text, .{});
        try testing.expectEqual(@as(usize, 2), got.rows.len);
        try testing.expectEqualStrings("id", got.schema.fields[0].name);
        try testing.expectEqual(types.TypeKind.int, got.schema.fields[0].ty.kind);
        try testing.expectEqual(@as(i64, 2), got.rows[1][0].int);
        try testing.expectEqualStrings("b", got.rows[1][1].string);
    }
    const one = try readAll(a, "{\"x\":true}", .{});
    try testing.expectEqual(@as(usize, 1), one.rows.len);
    try testing.expect(one.rows[0][0].bool);
    const empty = try readAll(a, "[]", .{});
    try testing.expectEqual(@as(usize, 0), empty.rows.len);
    try testing.expectEqual(@as(usize, 0), (try readAll(a, " \n", .{})).rows.len);
}

test "columns come from the sampled rows: keys in first-met order, types widened" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const got = try readAll(a,
        \\{"a":1,"b":null}
        \\{"a":2.5,"c":true,"b":"x"}
        \\{"a":3,"d":[1,2],"e":null,"c":1}
        \\{"z":"late"}
    , .{ .sample = 3 });
    const want = [_]struct { []const u8, types.TypeKind }{ .{ "a", .float }, .{ "b", .string }, .{ "c", .string }, .{ "d", .string }, .{ "e", .string } };
    try testing.expectEqual(want.len, got.schema.fields.len);
    for (want, got.schema.fields) |w, f| {
        try testing.expectEqualStrings(w[0], f.name);
        try testing.expectEqual(w[1], f.ty.kind);
    }
    try testing.expectEqual(@as(f64, 1), got.rows[0][0].float);
    try testing.expectEqualStrings("true", got.rows[1][2].string);
    try testing.expectEqualStrings("[1,2]", got.rows[2][3].string);
    try testing.expect(got.rows[3][0].isNull());
}

test "a key with no value is of unknown type only when the sample read the whole input" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const text = "{\"k\":1,\"n\":null}\n{\"k\":2}\n";
    const Case = struct { doc: []const u8, o: Options, unknown: bool };
    const cases = [_]Case{
        .{ .doc = text, .o = .{}, .unknown = true },
        .{ .doc = text, .o = .{ .sample = 3 }, .unknown = true },
        .{ .doc = text, .o = .{ .sample = 2 }, .unknown = false },
        .{ .doc = text, .o = .{ .alone = false }, .unknown = false },
        .{ .doc = text, .o = .{ .sample = 1 }, .unknown = false },
        .{ .doc = "[{\"k\":1,\"n\":null},{\"k\":2}]", .o = .{}, .unknown = true },
        .{ .doc = "{\"rows\":[{\"k\":1,\"n\":null},{\"k\":2}]}", .o = .{ .items = "rows" }, .unknown = true },
        .{ .doc = "{\"rows\":{\"k\":1,\"n\":null}}", .o = .{ .items = "rows" }, .unknown = true },
    };
    for (cases) |c| {
        const n = (try readAll(a, c.doc, c.o)).schema.fields[1].ty;
        try testing.expectEqual(types.TypeKind.string, n.kind);
        try testing.expect(n.nullable);
        try testing.expectEqual(c.unknown, n.unknown);
    }
    try testing.expect(!(try schemaOf(a, &.{"{\"n\":null}"}, 8)).fields[0].ty.unknown);
}

test "Infer on widenSample types every key as the lattice it replaced did" {
    const Orig = struct {
        fn oracleInferKinds(arena: std.mem.Allocator, rows: []const []const u8) ![]const types.TypeKind {
            const Kind = enum { none, boolean, int, float, text };
            var names = std.StringArrayHashMap(Kind).init(arena);
            for (rows) |row| {
                if (row[0] != '{') continue;
                var m = jt.Members.root(row);
                while (m.next()) |mem| {
                    const gop = try names.getOrPut(try jt.decodeString(arena, mem.key));
                    if (!gop.found_existing) gop.value_ptr.* = .none;
                    const b: Kind = switch (mem.value[0]) {
                        'n' => .none,
                        't', 'f' => .boolean,
                        '"', '{', '[' => .text,
                        else => switch (jt.number(mem.value)) {
                            .int => .int,
                            .float, .text => .float,
                        },
                    };
                    const x = gop.value_ptr.*;
                    gop.value_ptr.* = if (x == .none) b else if (b == .none or x == b) x else if ((x == .int and b == .float) or (x == .float and b == .int)) .float else .text;
                }
            }
            const out = try arena.alloc(types.TypeKind, names.count());
            for (names.values(), out) |k, *o| o.* = switch (k) {
                .boolean => .bool,
                .int => .int,
                .float => .float,
                .none, .text => .string,
            };
            return out;
        }
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x1f_e5);
    const r = prng.random();
    const vals = [_][]const u8{ "null", "true", "false", "1", "-7", "2.5", "1e3", "18446744073709551616", "\"s\"", "\"\"", "{\"x\":1}", "[1]", "[]" };
    const keys = [_][]const u8{ "a", "b", "c", "d" };
    var rows: [6][]const u8 = undefined;
    for (0..20_000) |_| {
        const nrows = 1 + r.uintLessThan(usize, rows.len);
        for (rows[0..nrows]) |*row| {
            var t = std.Io.Writer.Allocating.init(a);
            try t.writer.writeByte('{');
            for (0..r.uintLessThan(usize, keys.len + 1)) |i| {
                if (i > 0) try t.writer.writeByte(',');
                try t.writer.print("\"{s}\":{s}", .{ keys[r.uintLessThan(usize, keys.len)], vals[r.uintLessThan(usize, vals.len)] });
            }
            try t.writer.writeByte('}');
            row.* = t.written();
        }
        const want = try Orig.oracleInferKinds(a, rows[0..nrows]);
        const got = try schemaOf(a, rows[0..nrows], nrows);
        try testing.expectEqual(want.len, got.fields.len);
        for (want, got.fields) |k, f| {
            try testing.expectEqual(k, f.ty.kind);
            try testing.expect(f.ty.nullable and !f.ty.unknown);
        }
    }
}

test "a value its column cannot read is NULL; strings and escaped keys decode" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const got = try readAll(a,
        \\[{"ok":true,"n":1,"s\u0074":"a\nb"},{"ok":"maybe","n":1e300,"st":"x"},{"ok":false,"n":"9","st":2},{"ok":null,"n":{"x":1},"st":[1]}]
    , .{ .sample = 1 });
    try testing.expectEqualStrings("st", got.schema.fields[2].name);
    try testing.expectEqualStrings("a\nb", got.rows[0][2].string);
    try testing.expect(got.rows[1][0].isNull());
    try testing.expect(got.rows[1][1].isNull());
    try testing.expectEqual(@as(i64, 9), got.rows[2][1].int);
    try testing.expectEqualStrings("2", got.rows[2][2].string);
    try testing.expect(got.rows[3][1].isNull());
    try testing.expectEqualStrings("[1]", got.rows[3][2].string);
}

test "items picks the array at a path; a value larger than the read chunk is whole" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const got = try readAll(a, "{\"meta\":{\"n\":2},\"data\":{\"rows\":[{\"k\":1},{\"k\":2}]}}", .{ .items = "data.rows" });
    try testing.expectEqual(@as(usize, 2), got.rows.len);
    try testing.expectEqual(@as(i64, 2), got.rows[1][0].int);
    try testing.expectEqual(@as(usize, 0), (try readAll(a, "{\"data\":{}}", .{ .items = "data.rows" })).rows.len);

    var big = std.array_list.Managed(u8).init(a);
    for (0..3) |i| {
        try big.writer().print("{{\"i\":{d},\"pad\":\"", .{i});
        try big.appendNTimes('x', 5000);
        try big.appendSlice("\",\"tail\":{\"q\":[1,[2,\"]}\"]]}}\n");
    }
    const long = try readAll(a, big.items, .{ .chunk = 7, .batch_rows = 2, .sample = 1 });
    try testing.expectEqual(@as(usize, 3), long.rows.len);
    try testing.expectEqual(@as(i64, 2), long.rows[2][0].int);
    try testing.expectEqual(@as(usize, 5000), long.rows[2][1].string.len);
}

test "malformed or truncated input is an error, not rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_][]const u8{ "{\"a\":1", "[{\"a\":1},", "[{\"a\":1} {\"a\":2}]", "{\"a\":tru}", "[1] x" }) |bad| {
        try testing.expectError(error.InvalidJson, readAll(a, bad, .{}));
    }
}

test "a folder whose later file cannot be opened fails once and frees each reader once" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "1.jsonl", .data = "{\"k\":1}\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const files = [_][]const u8{ try std.fs.path.join(a, &.{ base, "1.jsonl" }), try std.fs.path.join(a, &.{ base, "gone.jsonl" }) };
    const f = try Files.open(a, std.testing.allocator, &files, .{});
    defer f.close();
    try std.testing.expect((try f.next(a)) != null);
    try std.testing.expectError(error.FileNotFound, f.next(a));
}

test "the column shortcuts append exactly what the general path does, whenever they take a value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x15_0a);
    const r = prng.random();
    const alphabet = "0123456789000-.eE+";
    const edges = [_][]const u8{ "0", "-0", "007", "9223372036854775807", "-9223372036854775808", "123456789012345678", "1234567890123456", "0.1", "-0.0", "1e5", "2.50", "true", "false", "null", "\"9\"" };
    var taken: usize = 0;
    for (0..200_000 + edges.len) |n| {
        const raw = if (n < edges.len) edges[n] else blk: {
            const t = try a.alloc(u8, 1 + r.uintLessThan(usize, 20));
            for (t) |*c| c.* = alphabet[r.uintLessThan(usize, alphabet.len)];
            break :blk t;
        };
        for ([_]Rows.Fast{ .int, .float, .bool }) |fa| {
            const ty = types.Type.init(switch (fa) {
                .int => .int,
                .float => .float,
                else => .bool,
            }).asNullable();
            var fast = column.Builder.init(a, ty);
            if (!try Rows.appendFast(&fast, fa, raw)) continue;
            taken += 1;
            var slow = column.Builder.init(a, ty);
            try slow.append(try Rows.read(a, raw, ty));
            const want = (try slow.finish()).getValue(0);
            const got = (try fast.finish()).getValue(0);
            const same = switch (want) {
                .float => |x| got == .float and @as(u64, @bitCast(x)) == @as(u64, @bitCast(got.float)),
                else => std.meta.eql(want, got),
            };
            if (!same) {
                std.debug.print("{s} `{s}`: general {any}, shortcut {any}\n", .{ @tagName(fa), raw, want, got });
                return error.TestUnexpectedResult;
            }
        }
    }
    try testing.expect(taken > 10_000);
}

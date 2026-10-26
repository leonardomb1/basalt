//! Decoding a block's objects into column builders. A file's plan (`schema.zig`) is
//! compiled once into a `Program`: a flat array of 16-byte `Op`s, each naming what
//! to decode and the builder it fills, laid out in the order the bytes come so a
//! row walks memory forward. A union's branches sit inline after its op, and the
//! common `["null", T]` is `T`'s op marked with the branch that is null, so it costs
//! one dispatch, not two. Each leaf op's decode and
//! append is a function `Machine.leaf` instantiates per `Code` at compile time, so
//! the per-value work is a varint or a copy and a typed append, with no `Value`
//! boxed between them, as arrow-avro decodes straight into Arrow builders.
//!
//! What has no typed column — arrays, maps, mixed unions, a record that recurses —
//! renders as JSON (`text`), and a column a later file holds in another type is
//! decoded as a `Value` and cast (`slow`); both share `value` with the JSON path.
//! A column the table does not keep is still decoded, its bytes checked, and
//! dropped.

const std = @import("std");
const Value = @import("../../exec/value.zig").Value;
const Decimal = @import("../../exec/value.zig").Decimal;
const column = @import("../../exec/column.zig");
const civil = @import("../../exec/civil.zig");
const decimal = @import("../../exec/decimal.zig");
const eval = @import("../../exec/eval.zig");
const format = @import("../../exec/eval/format.zig");
const jsontext = @import("../../exec/jsontext.zig");
const schema = @import("schema.zig");
const stream = @import("../stream.zig");
const types = @import("../../lang/types.zig");
const varint = @import("../varint.zig");

const Node = schema.Node;

pub const Error = error{ CorruptAvro, UnsupportedAvro, OutOfMemory };

pub const max_empty = 1 << 16;

pub inline fn corrupt(comptime fmt: []const u8, args: anytype) Error {
    return @errorCast(stream.explainf(error.CorruptAvro, fmt, args));
}

pub inline fn unsupported(comptime fmt: []const u8, args: anytype) Error {
    return @errorCast(stream.explainf(error.UnsupportedAvro, fmt, args));
}

/// A position in a decompressed block. A varint of one byte, the common case for
/// lengths, branches and small numbers, is read first; one with ten bytes of room
/// left is read unrolled with no bounds check per byte.
pub const Cursor = struct {
    buf: []const u8 = &.{},
    pos: usize = 0,

    fn short() Error {
        return corrupt("a value runs past the end of its block", .{});
    }

    pub inline fn long(c: *Cursor) Error!i64 {
        const p = c.pos;
        if (p < c.buf.len and c.buf[p] < 0x80) {
            c.pos = p + 1;
            return varint.unzigzag(c.buf[p]);
        }
        if (c.buf.len - p >= 10) return c.longUnrolled();
        return varint.unzigzag(varint.read(c.buf, &c.pos) catch return short());
    }

    fn longUnrolled(c: *Cursor) Error!i64 {
        const b = c.buf[c.pos..][0..10];
        var v: u64 = 0;
        inline for (0..10) |i| {
            v |= @as(u64, b[i] & 0x7F) << @as(u6, 7 * i);
            if (b[i] < 0x80) {
                c.pos += i + 1;
                return varint.unzigzag(v);
            }
        }
        return short();
    }

    pub fn int(c: *Cursor) Error!i32 {
        const v = try c.long();
        return std.math.cast(i32, v) orelse corrupt("an int holds {d}, past 32 bits", .{v});
    }

    pub fn take(c: *Cursor, n: usize) Error![]const u8 {
        if (n > c.buf.len - c.pos) return short();
        defer c.pos += n;
        return c.buf[c.pos..][0..n];
    }

    pub fn bytes(c: *Cursor) Error![]const u8 {
        const n = try c.long();
        if (n < 0) return corrupt("a negative length {d}", .{n});
        return c.take(std.math.cast(usize, n) orelse return short());
    }

    pub fn boolean(c: *Cursor) Error!bool {
        const b = (try c.take(1))[0];
        if (b > 1) return corrupt("a boolean holds byte {d}", .{b});
        return b == 1;
    }

    pub fn float(c: *Cursor) Error!f64 {
        return @as(f32, @bitCast(std.mem.readInt(u32, (try c.take(4))[0..4], .little)));
    }

    pub fn double(c: *Cursor) Error!f64 {
        return @bitCast(std.mem.readInt(u64, (try c.take(8))[0..8], .little));
    }

    /// The item count of an array or map block, its byte size skipped when negative.
    /// Each item of a type that takes bytes takes at least one, so a count past the
    /// bytes left is corrupt; one of a type that may take none is capped.
    pub fn blockCount(c: *Cursor, items: *const Node) Error!usize {
        var n = try c.long();
        if (n < 0) {
            n = -%n;
            _ = try c.long();
        }
        const k = std.math.cast(usize, n) orelse return short();
        if (k > (if (schema.zeroWidth(items)) max_empty else c.buf.len - c.pos)) return corrupt("an array or map claims {d} items its block cannot hold", .{k});
        return k;
    }
};

/// A two's complement big-endian integer as an unscaled decimal; one wider than
/// an i128 or with more digits than `precision` is corrupt.
pub fn unscaledOf(b: []const u8, precision: u8) Error!i128 {
    const v = decimal.bigEndianDecimal(b) orelse return corrupt("a decimal wider than its precision", .{});
    if (!decimal.withinPrecision(v, precision)) return corrupt("a decimal holds more digits than its precision {d}", .{precision});
    return v;
}

pub fn decimalOf(b: []const u8, precision: u8, scale: u8) Error!Value {
    return .{ .decimal = .{ .unscaled = try unscaledOf(b, precision), .scale = scale } };
}

/// What an op does: a leaf `Code` decodes one value into its builder's storage
/// (`storageOf`); `text`, `slow` and `choice` are the rest.
pub const Code = enum(u8) {
    null,
    boolean,
    int,
    date,
    time_millis,
    long,
    ts_millis,
    ts_nanos,
    float,
    double,
    bytes,
    fixed,
    @"enum",
    dec_bytes,
    dec_fixed,
    text,
    slow,
    choice,

    /// The builder storage a leaf fills; null for an op that is not a leaf.
    fn storageOf(comptime c: Code) ?column.Tag {
        return switch (c) {
            .boolean => .b,
            .date => .i32,
            .int, .time_millis, .long, .ts_millis, .ts_nanos => .i64,
            .float, .double => .f64,
            .bytes, .fixed, .@"enum" => .bytes,
            .dec_bytes, .dec_fixed => .dec,
            .null, .text, .slow, .choice => null,
        };
    }

    /// The leaf code a typed node decodes with, from its kind and logical type: the
    /// one mapping a compiled op and a decoded `Value` share.
    pub fn of(n: *const Node) Code {
        return switch (n.kind) {
            .null => .null,
            .boolean => .boolean,
            .int => switch (n.logical) {
                .date => .date,
                .time_millis => .time_millis,
                else => .int,
            },
            .long => switch (n.logical) {
                .timestamp_millis => .ts_millis,
                .timestamp_nanos => .ts_nanos,
                else => .long,
            },
            .float => .float,
            .double => .double,
            .string => .bytes,
            .bytes => if (n.logical == .decimal) .dec_bytes else .bytes,
            .fixed => if (n.logical == .decimal) .dec_fixed else .fixed,
            .@"enum" => .@"enum",
            .record, .array, .map, .@"union" => unreachable,
        };
    }
};

/// What a leaf decode reads besides its bytes, from a compiled op.
const OpArgs = struct {
    op: Op,
    nodes: []const *const Node,

    inline fn size(a: OpArgs) usize {
        return a.op.b;
    }

    inline fn symbols(a: OpArgs) []const []const u8 {
        return a.nodes[a.op.a].symbols;
    }

    inline fn dec(a: OpArgs) Dec {
        return a.op.decimal();
    }
};

/// What a leaf decode reads besides its bytes, from the schema node itself.
const NodeArgs = struct {
    n: *const Node,

    inline fn size(a: NodeArgs) usize {
        return a.n.size;
    }

    inline fn symbols(a: NodeArgs) []const []const u8 {
        return a.n.symbols;
    }

    inline fn dec(a: NodeArgs) Dec {
        return .{ .precision = a.n.precision, .scale = a.n.scale, .size = a.n.size };
    }
};

const Dec = struct { precision: u8, scale: u8, size: usize };

fn Read(comptime c: Code) type {
    return switch (Code.storageOf(c) orelse return void) {
        .b => bool,
        .i32 => i32,
        .i64 => i64,
        .f64 => f64,
        .bytes => []const u8,
        .dec => Decimal,
    };
}

/// One leaf value at the cursor in its builder storage's form, its logical type
/// applied; the typed program and `Machine.value` both decode through it.
inline fn decodeLeaf(cur: *Cursor, comptime c: Code, args: anytype) Error!Read(c) {
    return switch (c) {
        .null => {},
        .boolean => cur.boolean(),
        .int => try cur.int(),
        .date => cur.int(),
        .time_millis => @call(.always_inline, civil.Tick.toMicros, .{ .ms, try cur.int() }) catch unreachable,
        .long => cur.long(),
        .ts_millis => blk: {
            const x = try cur.long();
            break :blk @call(.always_inline, civil.Tick.toMicros, .{ .ms, x }) catch corrupt("a timestamp-millis {d} past the range basalt holds", .{x});
        },
        .ts_nanos => @call(.always_inline, civil.Tick.toMicros, .{ .ns, try cur.long() }) catch unreachable,
        .float => cur.float(),
        .double => cur.double(),
        .bytes => cur.bytes(),
        .fixed => cur.take(args.size()),
        .@"enum" => blk: {
            const syms = args.symbols();
            const i = try cur.long();
            if (i < 0 or i >= syms.len) return corrupt("an enum symbol {d} of {d}", .{ i, syms.len });
            break :blk syms[@intCast(i)];
        },
        .dec_bytes, .dec_fixed => blk: {
            const d = args.dec();
            const raw = if (c == .dec_bytes) try cur.bytes() else try cur.take(d.size);
            break :blk .{ .unscaled = try unscaledOf(raw, d.precision), .scale = d.scale };
        },
        .text, .slow, .choice => comptime unreachable,
    };
}

/// A leaf decoded in storage form as the `Value` of its column's kind `kind`.
fn valueOf(comptime c: Code, kind: types.TypeKind, x: Read(c)) Value {
    const t = comptime Code.storageOf(c);
    if (t == null) return .null;
    return switch (t.?) {
        .b => .{ .bool = x },
        .i32 => .{ .date = x },
        .i64 => switch (kind) {
            .time => .{ .time = x },
            .timestamp => .{ .timestamp = x },
            else => .{ .int = x },
        },
        .f64 => .{ .float = x },
        .bytes => if (kind == .string) .{ .string = x } else .{ .bytes = x },
        .dec => .{ .decimal = x },
    };
}

/// One step of a program. `col` is the builder a leaf, `text` or `slow` op fills
/// (`none` to decode and drop), and for `choice` the op just past its branches.
/// `a` and `b`: a decimal's precision, scale and fixed size packed in `a`; a fixed's
/// size in `b`; the node of an `enum`, `text` or `slow` op in `a`; a `choice`'s
/// first branch and branch count in `a` and `b`. `nulls` says which branch of a
/// `["null", T]` union around the op is null, in the byte padding leaves free.
pub const Op = extern struct {
    code: Code,
    nulls: Nulls = .none,
    col: u32,
    a: u32 = 0,
    b: u32 = 0,

    pub const none = std.math.maxInt(u32);

    pub const Nulls = enum(u8) { none, first, second };

    fn decimal(op: Op) Dec {
        return .{ .precision = @truncate(op.a >> 8), .scale = @truncate(op.a), .size = @as(u16, @truncate(op.a >> 16)) };
    }
};

/// A `choice`'s branch: the ops it runs, or for a `null` branch the builders it
/// sets NULL (`nulls` indexes `Program.cols`).
pub const Branch = struct { lo: u32, hi: u32, nulls_lo: u32 = 0, nulls_hi: u32 = 0 };

pub const Program = struct {
    ops: []const Op,
    nodes: []const *const Node,
    branches: []const Branch,
    cols: []const u32,
};

/// Where each of a file's own columns goes in the table: a builder, none, and
/// whether its type differs from the table's and is cast.
pub const Route = struct { to: ?usize = null, cast: bool = false };

/// A plan compiled against `routes`, one per column it numbered.
pub fn compile(arena: std.mem.Allocator, plan: schema.Plan, routes: []const Route) Error!Program {
    var c = Compiler{ .arena = arena, .own = plan.schema, .routes = routes };
    const step = plan.step;
    try c.emit(step);
    return .{ .ops = c.ops.items, .nodes = c.nodes.items, .branches = c.branches.items, .cols = c.cols.items };
}

const Compiler = struct {
    arena: std.mem.Allocator,
    own: types.Schema,
    routes: []const Route,
    ops: std.ArrayListUnmanaged(Op) = .empty,
    nodes: std.ArrayListUnmanaged(*const Node) = .empty,
    branches: std.ArrayListUnmanaged(Branch) = .empty,
    cols: std.ArrayListUnmanaged(u32) = .empty,

    fn node(self: *Compiler, n: *const Node) Error!u32 {
        try self.nodes.append(self.arena, n);
        return @intCast(self.nodes.items.len - 1);
    }

    fn colOf(self: *Compiler, own: usize) u32 {
        return if (self.routes[own].to) |to| @intCast(to) else Op.none;
    }

    fn push(self: *Compiler, op: Op) Error!void {
        try self.ops.append(self.arena, op);
    }

    fn emit(self: *Compiler, step: schema.Step) Error!void {
        switch (step) {
            .leaf => |t| try self.push(try self.leaf(t.node, t.col)),
            .text => |t| try self.push(.{ .code = .text, .col = self.colOf(t.col), .a = try self.node(t.node) }),
            .record => |steps| for (steps) |s| try self.emit(s),
            .choice => |ch| try self.choice(ch),
            .nulls => unreachable,
        }
    }

    /// The op for a typed leaf; `slow` when its column holds another type, as a
    /// union of INT and FLOAT branches does, or when the table casts the column.
    fn leaf(self: *Compiler, n: *const Node, own: usize) Error!Op {
        const col = self.colOf(own);
        if (self.routes[own].cast or schema.leafType(n).?.kind != self.own.fields[own].ty.kind)
            return .{ .code = .slow, .col = col, .a = try self.node(n) };
        const code = Code.of(n);
        const dec = (@as(u32, n.precision) << 8) | n.scale;
        return switch (code) {
            .dec_bytes => .{ .code = code, .col = col, .a = dec },
            .dec_fixed => .{ .code = code, .col = col, .a = dec | (@as(u32, @intCast(n.size)) << 16) },
            .fixed => .{ .code = code, .col = col, .b = @intCast(n.size) },
            .@"enum" => .{ .code = code, .col = col, .a = try self.node(n) },
            else => .{ .code = code, .col = col },
        };
    }

    fn choice(self: *Compiler, ch: schema.Step.Choice) Error!void {
        if (ch.branches.len == 2) for (ch.branches, 0..) |b, i| {
            const other = ch.branches[1 - i];
            if (b == .nulls and (other == .leaf or other == .text)) {
                try self.emit(other);
                self.ops.items[self.ops.items.len - 1].nulls = if (i == 0) .first else .second;
                return;
            }
        };
        const at = self.ops.items.len;
        try self.push(.{ .code = .choice, .col = 0, .a = @intCast(self.branches.items.len), .b = @intCast(ch.branches.len) });
        const first = self.branches.items.len;
        try self.branches.appendNTimes(self.arena, .{ .lo = 0, .hi = 0 }, ch.branches.len);
        for (ch.branches, 0..) |b, i| {
            var br = Branch{ .lo = @intCast(self.ops.items.len), .hi = 0 };
            if (b == .nulls) {
                br.nulls_lo = @intCast(self.cols.items.len);
                for (ch.lo..ch.hi) |own| if (self.routes[own].to) |to| try self.cols.append(self.arena, @intCast(to));
                br.nulls_hi = @intCast(self.cols.items.len);
            } else try self.emit(b);
            br.hi = @intCast(self.ops.items.len);
            self.branches.items[first + i] = br;
        }
        self.ops.items[at].col = @intCast(self.ops.items.len);
    }
};

/// Runs a program over a block's bytes into the builders of the batch at hand.
pub const Machine = struct {
    prog: Program,
    cur: Cursor = .{},
    out: []column.Builder = &.{},
    /// The table's column types and this file's column names, for a `slow` cast
    /// and its message.
    table: types.Schema,
    source_name: []const u8,
    text: std.Io.Writer.Allocating,

    pub fn init(gpa: std.mem.Allocator, prog: Program, table: types.Schema, source_name: []const u8) Machine {
        return .{ .prog = prog, .table = table, .source_name = source_name, .text = .init(gpa) };
    }

    pub fn deinit(m: *Machine) void {
        m.text.deinit();
    }

    /// Decodes one object: the whole program.
    pub fn row(m: *Machine, arena: std.mem.Allocator) Error!void {
        return m.run(arena, 0, @intCast(m.prog.ops.len));
    }

    fn run(m: *Machine, arena: std.mem.Allocator, lo: u32, hi: u32) Error!void {
        const ops = m.prog.ops;
        var ip = lo;
        while (ip < hi) {
            const op = ops[ip];
            switch (op.code) {
                .choice => {
                    const i = try m.cur.long();
                    if (i < 0 or i >= op.b) return corrupt("a union branch {d} of {d}", .{ i, op.b });
                    const br = m.prog.branches[op.a + @as(u32, @intCast(i))];
                    for (m.prog.cols[br.nulls_lo..br.nulls_hi]) |col| try m.out[col].appendNull();
                    try m.run(arena, br.lo, br.hi);
                    ip = op.col;
                },
                else => {
                    try m.one(arena, op);
                    ip += 1;
                },
            }
        }
    }

    /// One value op: a typed leaf, a JSON `text` or a cast `slow` one, its union's
    /// branch read first when it has a null one.
    inline fn one(m: *Machine, arena: std.mem.Allocator, op: Op) Error!void {
        if (op.nulls != .none) {
            const i = try m.cur.long();
            const null_at: i64 = if (op.nulls == .first) 0 else 1;
            if (i == null_at) {
                if (op.col != Op.none) try m.out[op.col].appendNull();
                return;
            }
            if (i != 1 - null_at) return corrupt("a union branch {d} of 2", .{i});
        }
        switch (op.code) {
            .text => {
                m.text.clearRetainingCapacity();
                try m.json(m.prog.nodes[op.a], &m.text.writer, 0);
                if (op.col != Op.none) try m.out[op.col].appendStr(m.text.written());
            },
            .slow => try m.slow(arena, op),
            .choice => unreachable,
            inline else => |c| try m.leaf(c, op),
        }
    }

    /// The decode and append of one leaf code, specialized per `c` at compile time.
    fn leaf(m: *Machine, comptime c: Code, op: Op) Error!void {
        const v = try decodeLeaf(&m.cur, c, OpArgs{ .op = op, .nodes = m.prog.nodes });
        if (op.col == Op.none) return;
        const b = &m.out[op.col];
        const t = comptime Code.storageOf(c);
        if (t == null) return b.appendNull();
        if (t.? == .bytes) try b.appendStr(v) else if (t.? == .dec) b.appendDecimalAssumeCapacity(v) else b.appendAsAssumeCapacity(t.?, v);
    }

    /// A value whose column the table types otherwise, decoded and cast to it.
    fn slow(m: *Machine, arena: std.mem.Allocator, op: Op) Error!void {
        const v = try m.value(m.prog.nodes[op.a]);
        if (op.col == Op.none) return;
        const ty = m.table.fields[op.col].ty;
        const cast = if (v == .null) v else eval.castValueTyped(arena, v, ty) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return unsupported("`{s}`: a value of column `{s}` does not fit the first file's {s}", .{ m.source_name, m.table.fields[op.col].name, try ty.name(arena) }),
        };
        try m.out[op.col].append(cast);
    }

    /// The value at the cursor as a `Value`, for JSON text and a cast.
    pub fn value(m: *Machine, n: *const Node) Error!Value {
        return switch (Code.of(n)) {
            .text, .slow, .choice => unreachable,
            inline else => |c| valueOf(c, schema.leafType(n).?.kind, try decodeLeaf(&m.cur, c, NodeArgs{ .n = n })),
        };
    }

    /// The value at the cursor as compact JSON, as basalt's JSON writer writes one.
    pub fn json(m: *Machine, n: *const Node, w: *std.Io.Writer, depth: usize) Error!void {
        if (depth > schema.max_depth) return corrupt("a value nests deeper than {d}", .{schema.max_depth});
        const c = &m.cur;
        switch (n.kind) {
            .record => {
                try put(w, "{");
                for (n.fields, 0..) |f, i| {
                    if (i > 0) try put(w, ",");
                    jsontext.key(w, f.name) catch return error.OutOfMemory;
                    try m.json(f.node, w, depth + 1);
                }
                try put(w, "}");
            },
            .array, .map => {
                try put(w, if (n.kind == .array) "[" else "{");
                var first = true;
                while (true) {
                    const k = try c.blockCount(n.items.?);
                    if (k == 0) break;
                    for (0..k) |_| {
                        if (!first) try put(w, ",");
                        first = false;
                        if (n.kind == .map) jsontext.key(w, try c.bytes()) catch return error.OutOfMemory;
                        try m.json(n.items.?, w, depth + 1);
                    }
                }
                try put(w, if (n.kind == .array) "]" else "}");
            },
            .@"union" => {
                const i = try c.long();
                if (i < 0 or i >= n.branches.len) return corrupt("a union branch {d} of {d}", .{ i, n.branches.len });
                try m.json(n.branches[@intCast(i)], w, depth + 1);
            },
            .int, .long => if (n.logical == .none) {
                const x = if (n.kind == .int) try c.int() else try c.long();
                format.writeInt(w, x) catch return error.OutOfMemory;
            } else try m.jsonValue(n, w),
            .string => if (n.logical == .none or n.logical == .uuid) (jsontext.string(w, try c.bytes()) catch return error.OutOfMemory) else try m.jsonValue(n, w),
            else => try m.jsonValue(n, w),
        }
    }

    /// A scalar as JSON by way of its `Value`: what `json` does not write directly.
    fn jsonValue(m: *Machine, n: *const Node, w: *std.Io.Writer) Error!void {
        jsontext.scalar(w, try m.value(n), jsontext.typed) catch return error.OutOfMemory;
    }
};

fn put(w: *std.Io.Writer, s: []const u8) Error!void {
    w.writeAll(s) catch return error.OutOfMemory;
}

const testing = std.testing;

test "an op is 16 bytes, four to a cache line" {
    try testing.expectEqual(@as(usize, 16), @sizeOf(Op));
}

test "the cursor reads a varint alike by its fast and careful paths, at every length" {
    var buf = std.array_list.Managed(u8).init(testing.allocator);
    defer buf.deinit();
    const vals = [_]i64{ 0, -1, 1, 63, -64, 64, 8191, -8192, 1 << 20, -(1 << 35), std.math.minInt(i64), std.math.maxInt(i64) };
    for (vals) |v| try varint.write(&buf, varint.zigzag(v));
    var fast = Cursor{ .buf = buf.items };
    for (vals) |v| try testing.expectEqual(v, try fast.long());
    try testing.expectEqual(buf.items.len, fast.pos);
    for (vals) |v| {
        var one = std.array_list.Managed(u8).init(testing.allocator);
        defer one.deinit();
        try varint.write(&one, varint.zigzag(v));
        var tight = Cursor{ .buf = one.items };
        try testing.expectEqual(v, try tight.long());
    }
    var long_run = Cursor{ .buf = &([_]u8{0x80} ** 11) };
    try testing.expectError(error.CorruptAvro, long_run.long());
    var cut = Cursor{ .buf = &[_]u8{ 0x80, 0x80 } };
    try testing.expectError(error.CorruptAvro, cut.long());
}

test "a nullable leaf compiles to one op marked nullable, a union of records to inline branches" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const root = try schema.parse(a,
        \\{"type":"record","name":"r","fields":[
        \\ {"name":"a","type":["null","long"]},
        \\ {"name":"b","type":["null",{"type":"record","name":"S","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}]},
        \\ {"name":"c","type":{"type":"int","logicalType":"date"}}
        \\]}
    );
    const p = try schema.plan(a, root);
    const routes = try a.alloc(Route, p.schema.fields.len);
    for (routes, 0..) |*r, i| r.* = .{ .to = i };
    routes[2].to = null;
    const prog = try compile(a, p, routes);
    const codes = [_]Code{ .long, .choice, .int, .bytes, .date };
    try testing.expectEqual(codes.len, prog.ops.len);
    for (codes, prog.ops) |c, op| try testing.expectEqual(c, op.code);
    try testing.expectEqual(Op.Nulls.first, prog.ops[0].nulls);
    try testing.expectEqual(Op.Nulls.none, prog.ops[2].nulls);
    try testing.expectEqual(@as(u32, 4), prog.ops[1].col);
    try testing.expectEqual(@as(u32, Op.none), prog.ops[3].col);
    try testing.expectEqual(@as(usize, 1), prog.cols.len);
    try testing.expectEqual(@as(u32, 1), prog.cols[0]);
}

fn valueBefore(c: *Cursor, n: *const Node) Error!Value {
    return switch (n.kind) {
        .null => .null,
        .boolean => .{ .bool = try c.boolean() },
        .int => blk: {
            const x = try c.int();
            break :blk switch (n.logical) {
                .date => .{ .date = x },
                .time_millis => .{ .time = @as(i64, x) * 1000 },
                else => .{ .int = x },
            };
        },
        .long => blk: {
            const x = try c.long();
            break :blk switch (n.logical) {
                .time_micros => .{ .time = x },
                .timestamp_millis => .{ .timestamp = std.math.mul(i64, x, 1000) catch return corrupt("a timestamp-millis {d} past the range basalt holds", .{x}) },
                .timestamp_micros => .{ .timestamp = x },
                .timestamp_nanos => .{ .timestamp = @divFloor(x, 1000) },
                else => .{ .int = x },
            };
        },
        .float => .{ .float = try c.float() },
        .double => .{ .float = try c.double() },
        .string => .{ .string = try c.bytes() },
        .@"enum" => blk: {
            const i = try c.long();
            if (i < 0 or i >= n.symbols.len) return corrupt("an enum symbol {d} of {d}", .{ i, n.symbols.len });
            break :blk .{ .string = n.symbols[@intCast(i)] };
        },
        .bytes, .fixed => blk: {
            const b = if (n.kind == .bytes) try c.bytes() else try c.take(n.size);
            break :blk if (n.logical == .decimal) try decimalOf(b, n.precision, n.scale) else .{ .bytes = b };
        },
        .record, .array, .map, .@"union" => unreachable,
    };
}

fn leafOpBefore(n: *const Node, col: u32, node_at: u32) Op {
    const dec = (@as(u32, n.precision) << 8) | n.scale;
    return switch (n.kind) {
        .null => .{ .code = .null, .col = col },
        .boolean => .{ .code = .boolean, .col = col },
        .int => .{ .code = switch (n.logical) {
            .date => .date,
            .time_millis => .time_millis,
            else => .int,
        }, .col = col },
        .long => .{ .code = switch (n.logical) {
            .timestamp_millis => .ts_millis,
            .timestamp_nanos => .ts_nanos,
            else => .long,
        }, .col = col },
        .float => .{ .code = .float, .col = col },
        .double => .{ .code = .double, .col = col },
        .string => .{ .code = .bytes, .col = col },
        .bytes => if (n.logical == .decimal) .{ .code = .dec_bytes, .col = col, .a = dec } else .{ .code = .bytes, .col = col },
        .fixed => if (n.logical == .decimal) .{ .code = .dec_fixed, .col = col, .a = dec | (@as(u32, @intCast(n.size)) << 16) } else .{ .code = .fixed, .col = col, .b = @intCast(n.size) },
        .@"enum" => .{ .code = .@"enum", .col = col, .a = node_at },
        .record, .array, .map, .@"union" => unreachable,
    };
}

fn readBefore(cur: *Cursor, comptime c: Code, op: Op, nodes: []const *const Node) Error!Read(c) {
    return switch (c) {
        .null => {},
        .boolean => cur.boolean(),
        .int => try cur.int(),
        .date => cur.int(),
        .time_millis => @as(i64, try cur.int()) * 1000,
        .long => cur.long(),
        .ts_millis => blk: {
            const x = try cur.long();
            break :blk std.math.mul(i64, x, 1000) catch corrupt("a timestamp-millis {d} past the range basalt holds", .{x});
        },
        .ts_nanos => @divFloor(try cur.long(), 1000),
        .float => cur.float(),
        .double => cur.double(),
        .bytes => cur.bytes(),
        .fixed => cur.take(op.b),
        .@"enum" => blk: {
            const syms = nodes[op.a].symbols;
            const i = try cur.long();
            if (i < 0 or i >= syms.len) return corrupt("an enum symbol {d} of {d}", .{ i, syms.len });
            break :blk syms[@intCast(i)];
        },
        .dec_bytes, .dec_fixed => blk: {
            const d = op.decimal();
            const raw = if (c == .dec_bytes) try cur.bytes() else try cur.take(d.size);
            break :blk .{ .unscaled = try unscaledOf(raw, d.precision), .scale = d.scale };
        },
        .text, .slow, .choice => comptime unreachable,
    };
}

/// Every scalar node the parser can give: each primitive under each logical type,
/// fixed sizes 0 to 17, decimals of several precisions and scales, and an enum.
fn scalarNodes(a: std.mem.Allocator) ![]const Node {
    var out = std.array_list.Managed(Node).init(a);
    const syms: []const []const u8 = &.{ "A", "B", "C" };
    for ([_]Node.Kind{ .null, .boolean, .int, .long, .float, .double, .bytes, .string }) |k|
        for (std.enums.values(schema.Logical)) |l| if (l != .decimal) try out.append(.{ .kind = k, .logical = l });
    try out.append(.{ .kind = .@"enum", .symbols = syms });
    try out.append(.{ .kind = .@"enum", .symbols = &.{} });
    for (0..18) |size| try out.append(.{ .kind = .fixed, .size = size });
    for ([_][2]u8{ .{ 1, 0 }, .{ 2, 1 }, .{ 9, 2 }, .{ 18, 4 }, .{ 19, 0 }, .{ 38, 10 }, .{ 38, 38 } }) |ps| {
        try out.append(.{ .kind = .bytes, .logical = .decimal, .precision = ps[0], .scale = ps[1] });
        for ([_]usize{ 1, 4, 8, 16, 17 }) |size| try out.append(.{ .kind = .fixed, .logical = .decimal, .precision = ps[0], .scale = ps[1], .size = size });
    }
    return out.items;
}

/// Random block bytes that a cursor reads as small and extreme varints, lengths and
/// raw bytes alike.
fn randomBlock(r: std.Random, a: std.mem.Allocator) ![]const u8 {
    var out = std.array_list.Managed(u8).init(a);
    const extremes = [_]i64{ 0, 1, -1, 2, 3, std.math.maxInt(i64) / 1000, std.math.maxInt(i64) / 1000 + 1, std.math.minInt(i64) / 1000 - 1, std.math.maxInt(i64), std.math.minInt(i64), std.math.maxInt(i32) + 1, -1001, 1999 };
    for (0..r.uintLessThan(usize, 4)) |_| switch (r.uintLessThan(u8, 3)) {
        0 => try varint.write(&out, varint.zigzag(extremes[r.uintLessThan(usize, extremes.len)])),
        1 => try out.append(r.uintLessThan(u8, 40)),
        else => for (0..r.uintLessThan(usize, 20)) |_| try out.append(r.int(u8)),
    };
    return out.items;
}

/// Equal as decoded, a float by its bits so a NaN equals itself.
fn sameDecoded(x: anytype, y: @TypeOf(x)) bool {
    const T = @TypeOf(x);
    if (T == f64) return @as(u64, @bitCast(x)) == @as(u64, @bitCast(y));
    if (T == Value) if (x == .float and y == .float) return sameDecoded(x.float, y.float);
    return std.meta.eql(x, y);
}

fn emptyMachine() Machine {
    return Machine.init(testing.allocator, .{ .ops = &.{}, .nodes = &.{}, .branches = &.{}, .cols = &.{} }, .{ .fields = &.{} }, "");
}

test "Machine.value through Code.of and decodeLeaf equals the per-kind decode it replaced on random blocks" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nodes = try scalarNodes(a);
    var prng = std.Random.DefaultPrng.init(0xa5_de);
    const r = prng.random();
    var m = emptyMachine();
    defer m.deinit();
    for (0..20_000) |_| {
        const buf = try randomBlock(r, a);
        for (nodes) |*n| {
            var old = Cursor{ .buf = buf };
            m.cur = .{ .buf = buf };
            const want = valueBefore(&old, n);
            const got = m.value(n);
            if (want) |v| {
                try testing.expect(sameDecoded(v, try got));
                try testing.expectEqual(old.pos, m.cur.pos);
            } else |e| try testing.expectError(e, got);
        }
    }
}

test "a compiled leaf op and its decode equal the per-kind op and read they replaced on random blocks" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nodes = try scalarNodes(a);
    var prng = std.Random.DefaultPrng.init(0xa5_0b);
    const r = prng.random();
    var ops = std.array_list.Managed(Op).init(a);
    var progs = std.array_list.Managed([]const *const Node).init(a);
    for (nodes) |*n| {
        const fields = try a.alloc(types.Schema.Field, 1);
        fields[0] = .{ .name = "c", .ty = schema.leafType(n).? };
        var comp = Compiler{ .arena = a, .own = .{ .fields = fields }, .routes = &.{.{ .to = 0 }} };
        const op = try comp.leaf(n, 0);
        try testing.expectEqual(leafOpBefore(n, 0, 0), op);
        try ops.append(op);
        try progs.append(comp.nodes.items);
    }
    for (0..20_000) |_| {
        const buf = try randomBlock(r, a);
        for (ops.items, progs.items) |op, pn| switch (op.code) {
            .text, .slow, .choice => unreachable,
            inline else => |c| {
                var old = Cursor{ .buf = buf };
                var new = Cursor{ .buf = buf };
                const want = readBefore(&old, c, op, pn);
                const got = decodeLeaf(&new, c, OpArgs{ .op = op, .nodes = pn });
                if (want) |v| {
                    try testing.expect(sameDecoded(v, try got));
                    try testing.expectEqual(old.pos, new.pos);
                } else |e| try testing.expectError(e, got);
            },
        };
    }
}

fn jsonValueBefore(arena: std.mem.Allocator, v: Value, w: *std.Io.Writer) !void {
    const celltext = @import("../../exec/celltext.zig");
    const jsonwrite = @import("../json/write.zig");
    switch (v) {
        .null => try w.writeAll("null"),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .int => |x| try format.writeInt(w, x),
        .float => |x| if (std.math.isFinite(x)) try celltext.writeFloat(w, x) else try w.writeAll("null"),
        .string => |s| {
            try w.writeAll("\"");
            try jsonwrite.writeEscaped(w, s);
            try w.writeAll("\"");
        },
        .bytes => |s| {
            try w.writeAll("\"");
            try std.base64.standard.Encoder.encodeWriter(w, s);
            try w.writeAll("\"");
        },
        else => {
            try w.writeAll("\"");
            try w.writeAll(try eval.valueToString(arena, v));
            try w.writeAll("\"");
        },
    }
}

test "jsontext.scalar in the typed style writes every nested scalar as Avro's own JSON writer did" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const imin = std.math.minInt(i64);
    const imax = std.math.maxInt(i64);
    const fixed = [_]Value{
        .null,
        .{ .bool = true },
        .{ .bool = false },
        .{ .int = 0 },
        .{ .int = imin },
        .{ .int = imax },
        .{ .float = 0 },
        .{ .float = -0.0 },
        .{ .float = 1.5 },
        .{ .float = 1e300 },
        .{ .float = 5e-324 },
        .{ .float = std.math.nan(f64) },
        .{ .float = std.math.inf(f64) },
        .{ .float = -std.math.inf(f64) },
        .{ .string = "" },
        .{ .string = "a\"b\\c\n\r\t\x01\x1f\x7f" },
        .{ .string = "caf\xc3\xa9 \xff\xfe" },
        .{ .string = "\xc3" },
        .{ .bytes = "" },
        .{ .bytes = "\x00\x01\xff" },
        .{ .decimal = .{ .unscaled = 0, .scale = 0 } },
        .{ .decimal = .{ .unscaled = -1, .scale = 2 } },
        .{ .decimal = .{ .unscaled = std.math.maxInt(i128), .scale = 38 } },
        .{ .decimal = .{ .unscaled = std.math.minInt(i128) + 1, .scale = 0 } },
        .{ .date = 0 },
        .{ .date = std.math.minInt(i32) },
        .{ .date = std.math.maxInt(i32) },
        .{ .date = -719_163 },
        .{ .time = 0 },
        .{ .time = 86_399_999_999 },
        .{ .timestamp = 0 },
        .{ .timestamp = imin },
        .{ .timestamp = imax },
        .{ .timestamp = -1 },
    };
    var prng = std.Random.DefaultPrng.init(0x15_0a);
    const r = prng.random();
    var values = std.array_list.Managed(Value).init(a);
    try values.appendSlice(&fixed);
    for (0..5_000) |_| {
        const s = try a.alloc(u8, r.uintLessThan(usize, 12));
        r.bytes(s);
        try values.append(switch (r.uintLessThan(u8, 8)) {
            0 => .{ .int = r.int(i64) },
            1 => .{ .float = @bitCast(r.int(u64)) },
            2 => .{ .string = s },
            3 => .{ .bytes = s },
            4 => .{ .decimal = .{ .unscaled = r.int(i64), .scale = r.uintAtMost(u8, 38) } },
            5 => .{ .date = r.int(i32) },
            6 => .{ .time = r.intRangeLessThan(i64, 0, 86_400_000_000) },
            else => .{ .timestamp = r.int(i64) },
        });
    }
    for (values.items) |v| {
        var want = std.Io.Writer.Allocating.init(a);
        var got = std.Io.Writer.Allocating.init(a);
        try jsonValueBefore(a, v, &want.writer);
        try jsontext.scalar(&got.writer, v, jsontext.typed);
        try testing.expectEqualStrings(want.written(), got.written());
    }
}

test "a timestamp-millis past i64 microseconds is a corrupt file on the typed and the cast path alike, as in Parquet and Arrow (D2)" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var buf = std.array_list.Managed(u8).init(a);
    const n = Node{ .kind = .long, .logical = .timestamp_millis };
    const ok = std.math.maxInt(i64) / 1000;
    for ([_]i64{ ok, ok + 1, std.math.minInt(i64) / 1000 - 1, std.math.minInt(i64) }) |x| {
        buf.clearRetainingCapacity();
        try varint.write(&buf, varint.zigzag(x));
        var cur = Cursor{ .buf = buf.items };
        const typed = decodeLeaf(&cur, .ts_millis, NodeArgs{ .n = &n });
        var m = emptyMachine();
        defer m.deinit();
        m.cur = .{ .buf = buf.items };
        const cast = m.value(&n);
        if (x == ok) {
            try testing.expectEqual(x * 1000, try typed);
            try testing.expectEqual(x * 1000, (try cast).timestamp);
        } else {
            try testing.expectError(error.CorruptAvro, typed);
            try testing.expectError(error.CorruptAvro, cast);
            try testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.CorruptAvro).?, "timestamp-millis") != null);
        }
    }
}

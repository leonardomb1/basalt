//! Row-group pruning: a group's min/max statistics against the query's bounds and
//! against the other side's keys of a join, and a file's min/max for a top-N
//! threshold.

const Error = @import("read.zig").Error;
const Leaf = @import("schema.zig").Leaf;
const PlainCursor = @import("encoding.zig").PlainCursor;
const Reader = @import("read.zig").Reader;
const Threshold = @import("../../exec/value.zig").Threshold;
const Value = @import("../../exec/value.zig").Value;
const basaltType = @import("logical.zig").basaltType;
const coerce = @import("logical.zig").coerce;
const column = @import("../../exec/column.zig");
const eval = @import("../../exec/eval.zig");
const parquet = @import("footer.zig");
const std = @import("std");
const temporalScale = @import("logical.zig").temporalScale;
const types = @import("../../lang/types.zig");
const collectLeaves = @import("schema.zig").collectLeaves;
const fx_logical = @import("testing_util.zig").fx_logical;
const testing = std.testing;

pub const Bound = struct {
    column: []const u8,
    op: Op,
    value: Value,

    pub const Op = enum { lt, le, gt, ge, eq };
};

pub fn groupMayMatch(
    schema: []const parquet.SchemaElement,
    leaves: []const Leaf,
    g: parquet.RowGroup,
    bounds: []const Bound,
) bool {
    for (bounds) |b| {
        const lf = findLeaf(leaves, b.column) orelse continue;
        if (lf.chunk_idx >= g.columns.len) continue;
        const meta = g.columns[lf.chunk_idx].meta orelse continue;
        const elem = schema[lf.schema_idx];

        const lo = statValue(elem, meta.ty, meta.stats.min) orelse continue;
        const hi = statValue(elem, meta.ty, meta.stats.max) orelse continue;

        const excluded = switch (b.op) {
            .lt => (cmp(lo, b.value) orelse .lt) != .lt,
            .le => (cmp(lo, b.value) orelse .eq) == .gt,
            .gt => (cmp(hi, b.value) orelse .gt) != .gt,
            .ge => (cmp(hi, b.value) orelse .eq) == .lt,
            .eq => (cmp(b.value, lo) orelse .eq) == .lt or (cmp(b.value, hi) orelse .eq) == .gt,
        };
        if (excluded) return false;
    }
    return true;
}

/// One join key's values on the other side of a join: the distinct values when
/// `values` lists them all, else only their `min`..`max`; `none` when that side
/// holds no key at all, so no row here can match.
pub const KeyBound = struct {
    column: []const u8,
    values: []const Value = &.{},
    min: ?Value = null,
    max: ?Value = null,
    none: bool = false,
};

/// Whether a row group could hold a row whose key equals one of the other side's.
/// A group is dropped only when its statistics are present, of the key's own kind,
/// and no key value (or, past the list, no part of the keys' range) falls within
/// its min/max. Text statistics are never read (`statValue`), so text keys skip
/// nothing: without the footer's column orders a writer's byte order is unknown.
pub fn groupMayHoldKeys(
    schema: []const parquet.SchemaElement,
    leaves: []const Leaf,
    g: parquet.RowGroup,
    keys: []const KeyBound,
) bool {
    for (keys) |k| {
        if (k.none) return false;
        const lf = findLeaf(leaves, k.column) orelse continue;
        if (lf.chunk_idx >= g.columns.len) continue;
        const meta = g.columns[lf.chunk_idx].meta orelse continue;
        const elem = schema[lf.schema_idx];
        const lo = statValue(elem, meta.ty, meta.stats.min) orelse continue;
        const hi = statValue(elem, meta.ty, meta.stats.max) orelse continue;
        if (k.values.len > 0) {
            for (k.values) |v| {
                if (!sameKind(lo, v) or !sameKind(hi, v)) break;
                const a = cmp(v, lo) orelse break;
                const b = cmp(v, hi) orelse break;
                if (a != .lt and b != .gt) break;
            } else return false;
            continue;
        }
        const mn = k.min orelse continue;
        const mx = k.max orelse continue;
        if (!sameKind(lo, mn) or !sameKind(hi, mx)) continue;
        if ((cmp(mx, lo) orelse continue) == .lt) return false;
        if ((cmp(mn, hi) orelse continue) == .gt) return false;
    }
    return true;
}

/// Whether a key value and a statistic order alike: both numbers, or both of one
/// kind; a NaN orders with nothing.
fn sameKind(stat: Value, v: Value) bool {
    if (v == .float and std.math.isNan(v.float)) return false;
    if (stat == .float and std.math.isNan(stat.float)) return false;
    if (isNumV(stat) and isNumV(v)) return true;
    return switch (v) {
        .string, .bytes, .date, .timestamp, .time => std.meta.activeTag(stat) == std.meta.activeTag(v),
        else => false,
    };
}

/// Whether a row group could hold a row entering the current top-N; only a
/// proven strict miss returns false.
pub fn groupBeatsThreshold(
    schema: []const parquet.SchemaElement,
    leaves: []const Leaf,
    g: parquet.RowGroup,
    t: Threshold,
) bool {
    if (!t.full or t.value == .null) return true;
    const lf = findLeaf(leaves, t.column) orelse return true;
    if (lf.chunk_idx >= g.columns.len) return true;
    const meta = g.columns[lf.chunk_idx].meta orelse return true;
    const elem = schema[lf.schema_idx];

    if (t.desc) {
        const hi = statValue(elem, meta.ty, meta.stats.max) orelse return true;
        return cmp(hi, t.value) == .gt;
    }
    const lo = statValue(elem, meta.ty, meta.stats.min) orelse return true;
    return cmp(lo, t.value) == .lt;
}

pub const MinMax = struct { min: Value, max: Value };

/// Folded from row-group statistics; null as soon as one group lacks them or the
/// type cannot be ordered (it once returned row group 0's value instead).
pub fn fileMinMax(rdr: *const Reader, name: []const u8) ?MinMax {
    if (rdr.md.row_groups.len == 0) return null;
    const lf = findLeaf(rdr.leaves, name) orelse return null;
    const elem = rdr.md.schema[lf.schema_idx];
    var lo: ?Value = null;
    var hi: ?Value = null;
    for (rdr.md.row_groups) |g| {
        if (lf.chunk_idx >= g.columns.len) return null;
        const meta = g.columns[lf.chunk_idx].meta orelse return null;
        switch (meta.ty) {
            .byte_array, .fixed_len_byte_array => return null,
            else => {},
        }
        const mn = statValue(elem, meta.ty, meta.stats.min) orelse return null;
        const mx = statValue(elem, meta.ty, meta.stats.max) orelse return null;
        if (lo == null) lo = mn else lo = if ((cmp(mn, lo.?) orelse return null) == .lt) mn else lo;
        if (hi == null) hi = mx else hi = if ((cmp(mx, hi.?) orelse return null) == .gt) mx else hi;
    }
    return .{ .min = lo orelse return null, .max = hi orelse return null };
}

pub fn wanted(want: ?[]const []const u8, name: []const u8) bool {
    const names = want orelse return true;
    for (names) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

/// A list column never matches: its chunk statistics describe the elements, not
/// the JSON text, so a bound on it proves nothing.
fn findLeaf(leaves: []const Leaf, name: []const u8) ?Leaf {
    for (leaves) |lf| {
        if (std.mem.eql(u8, lf.name, name)) return if (lf.list != null) null else lf;
    }
    return null;
}

pub fn leafType(e: parquet.SchemaElement, lf: Leaf) Error!types.Type {
    if (lf.list != null) {
        _ = try basaltType(e);
        return types.Type.init(.string).asNullable();
    }
    return (try basaltType(e)).asNullable();
}

fn statValue(elem: parquet.SchemaElement, phys: parquet.PhysicalType, raw: ?[]const u8) ?Value {
    const b = raw orelse return null;
    const ty = basaltType(elem) catch return null;
    var cur = PlainCursor.init(phys, elem.type_length orelse 0, b);
    const v = cur.next() catch return null;
    return coerce(ty, v, temporalScale(elem));
}

/// Null means unknown and the caller must keep the group. It once returned `.eq`
/// for unknown pairs, which pruned every group of a DECIMAL or date-vs-string range.
/// A date against an ISO string literal orders by parsing it (dialect §9).
pub fn cmp(a: Value, b: Value) ?std.math.Order {
    if (numOrder(a, b)) |o| return o;
    return switch (a) {
        .int => std.math.order(a.int, switch (b) {
            .int => |x| x,
            .float => |x| @as(i64, @intFromFloat(x)),
            else => return null,
        }),
        .float => std.math.order(a.float, switch (b) {
            .float => |x| x,
            .int => |x| @as(f64, @floatFromInt(x)),
            else => return null,
        }),
        .date => std.math.order(@as(i64, a.date), switch (b) {
            .date => |x| @as(i64, x),
            .int => |x| x,
            .string => |x| eval.parseIsoDate(x) orelse return null,
            else => return null,
        }),
        .time, .timestamp => blk: {
            const av = if (a == .time) a.time else a.timestamp;
            const bv = switch (b) {
                .time => |x| x,
                .timestamp => |x| x,
                .int => |x| x,
                .string => |x| if (a == .timestamp)
                    eval.parseIsoTimestamp(x) orelse return null
                else
                    return null,
                else => return null,
            };
            break :blk std.math.order(av, bv);
        },
        .string, .bytes => blk: {
            const sa = if (a == .string) a.string else a.bytes;
            const sb = switch (b) {
                .string => |x| x,
                .bytes => |x| x,
                else => return null,
            };
            break :blk std.mem.order(u8, sa, sb);
        },
        else => null,
    };
}

fn isNumV(v: Value) bool {
    return v == .int or v == .float or v == .decimal;
}

/// Numeric ordering via `eval.compareValues`, used only when both sides are
/// numeric, so pruning agrees with the filter that re-checks rows.
fn numOrder(a: Value, b: Value) ?std.math.Order {
    if (!isNumV(a) or !isNumV(b)) return null;
    return eval.compareValues(a, b);
}

test "cmp: an unorderable pair is unknown, never a fabricated equality" {
    const d: Value = .{ .date = 9131 };
    try std.testing.expectEqual(std.math.Order.lt, cmp(.{ .date = 9130 }, .{ .string = "1995-01-01" }).?);
    try std.testing.expectEqual(std.math.Order.eq, cmp(d, .{ .string = "1995-01-01" }).?);
    try std.testing.expectEqual(std.math.Order.gt, cmp(.{ .date = 9132 }, .{ .string = "1995-01-01" }).?);
    try std.testing.expect(cmp(d, .{ .string = "not-a-date" }) == null);
    try std.testing.expect(cmp(d, .{ .bool = true }) == null);
    try std.testing.expect(cmp(.{ .int = 5 }, .{ .string = "5" }) == null);
    try std.testing.expect(cmp(.{ .string = "a" }, .{ .int = 1 }) == null);
    try std.testing.expectEqual(std.math.Order.lt, cmp(.{ .timestamp = 0 }, .{ .string = "1970-01-02" }).?);
}

test "row groups are skipped only when statistics prove no row can match" {
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "id", .ty = .int64, .repetition = .optional },
    };
    const leaves = [_]Leaf{.{ .schema_idx = 1, .chunk_idx = 0, .name = "id", .max_def = 1, .max_rep = 0 }};

    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]parquet.ColumnChunk{.{ .meta = .{
        .ty = .int64,
        .stats = .{ .min = &lo, .max = &hi },
    } }};
    const g = parquet.RowGroup{ .columns = &chunks, .num_rows = 10 };

    const keep = [_]Bound{.{ .column = "id", .op = .lt, .value = .{ .int = 500 } }};
    const drop = [_]Bound{.{ .column = "id", .op = .lt, .value = .{ .int = 50 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &keep));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &drop));

    const eq_in = [_]Bound{.{ .column = "id", .op = .eq, .value = .{ .int = 150 } }};
    const eq_out = [_]Bound{.{ .column = "id", .op = .eq, .value = .{ .int = 999 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &eq_in));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &eq_out));

    const gt_keep = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 150 } }};
    const gt_drop = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 200 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &gt_keep));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &gt_drop));

    var bare = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = parquet.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(groupMayMatch(&schema, &leaves, g2, &drop));

    const other = [_]Bound{.{ .column = "nosuch", .op = .lt, .value = .{ .int = 0 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &other));
}

test "join keys skip a row group only when no key value can fall within its min/max" {
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "id", .ty = .int64, .repetition = .optional },
        .{ .name = "code", .ty = .byte_array, .repetition = .optional, .converted_type = 0 },
    };
    const leaves = [_]Leaf{
        .{ .schema_idx = 1, .chunk_idx = 0, .name = "id", .max_def = 1, .max_rep = 0 },
        .{ .schema_idx = 2, .chunk_idx = 1, .name = "code", .max_def = 1, .max_rep = 0 },
    };
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]parquet.ColumnChunk{
        .{ .meta = .{ .ty = .int64, .stats = .{ .min = &lo, .max = &hi } } },
        .{ .meta = .{ .ty = .byte_array, .stats = .{ .min = "b", .max = "d" } } },
    };
    const g = parquet.RowGroup{ .columns = &chunks, .num_rows = 10 };
    const hold = struct {
        fn f(gr: parquet.RowGroup, sch: []const parquet.SchemaElement, lv: []const Leaf, k: KeyBound) bool {
            return groupMayHoldKeys(sch, lv, gr, &.{k});
        }
    }.f;

    try testing.expect(!hold(g, &schema, &leaves, .{ .column = "id", .values = &.{ .{ .int = 5 }, .{ .int = 250 } } }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id", .values = &.{ .{ .int = 5 }, .{ .int = 150 } } }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id", .values = &.{ .{ .int = 5 }, .{ .int = 200 } } }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id", .values = &.{.{ .float = 150.5 }} }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id", .values = &.{ .{ .int = 5 }, .{ .string = "150" } } }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id", .values = &.{.{ .float = std.math.nan(f64) }} }));
    try testing.expect(!hold(g, &schema, &leaves, .{ .column = "id", .min = .{ .int = 300 }, .max = .{ .int = 900 } }));
    try testing.expect(!hold(g, &schema, &leaves, .{ .column = "id", .min = .{ .int = 1 }, .max = .{ .int = 99 } }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id", .min = .{ .int = 1 }, .max = .{ .int = 900 } }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "id" }));
    try testing.expect(!hold(g, &schema, &leaves, .{ .column = "id", .none = true }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "nosuch", .values = &.{.{ .int = 5 }} }));
    try testing.expect(hold(g, &schema, &leaves, .{ .column = "code", .values = &.{.{ .string = "z" }} }));

    var bare = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = parquet.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(hold(g2, &schema, &leaves, .{ .column = "id", .values = &.{.{ .int = 5 }} }));
}

test "a polars footer carries the LogicalType, and pruning uses converted units" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const md = try parquet.parseFile(a, fx_logical);
    const leaves = try collectLeaves(a, md.schema);
    const ts = md.schema[leaves[1].schema_idx];
    const utc = md.schema[leaves[2].schema_idx];
    try testing.expectEqual(@as(?i32, null), ts.converted_type);
    try testing.expectEqual(parquet.TimeUnit.micros, ts.logical_type.?.timestamp.unit);
    try testing.expect(!ts.logical_type.?.timestamp.adjusted_to_utc);
    try testing.expectEqual(parquet.TimeUnit.nanos, utc.logical_type.?.timestamp.unit);
    try testing.expect(utc.logical_type.?.timestamp.adjusted_to_utc);
    try testing.expectEqual(types.TypeKind.time, (try basaltType(md.schema[leaves[3].schema_idx])).kind);

    try testing.expectEqual(@as(usize, 2), md.row_groups.len);
    const jan1: i64 = 1_767_225_600_000_000;
    const ge_jan1 = [_]Bound{.{ .column = "ts", .op = .ge, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[0], &ge_jan1));
    try testing.expect(!groupMayMatch(md.schema, leaves, md.row_groups[1], &ge_jan1));
    const utc_lt = [_]Bound{.{ .column = "ts_utc", .op = .lt, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[0], &utc_lt));
    try testing.expect(!groupMayMatch(md.schema, leaves, md.row_groups[1], &utc_lt));
    const utc_gt = [_]Bound{.{ .column = "ts_utc", .op = .gt, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[1], &utc_gt));
}

test "top-N threshold skips only groups it can prove cannot contribute" {
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "v", .ty = .int64, .repetition = .optional },
    };
    const leaves = [_]Leaf{.{ .schema_idx = 1, .chunk_idx = 0, .name = "v", .max_def = 1, .max_rep = 0 }};
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64, .stats = .{ .min = &lo, .max = &hi } } }};
    const g = parquet.RowGroup{ .columns = &chunks, .num_rows = 10 };

    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 150 } }));
    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 200 } }));

    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = false, .full = true, .value = .{ .int = 50 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = false, .full = true, .value = .{ .int = 150 } }));

    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = false, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "nosuch", .desc = true, .full = true, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .null }));
    var bare = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = parquet.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g2, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 500 } }));
}

test "a nested column is typed string and no bound prunes on it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.parquet", .data = @embedFile("../testdata/lists_v1.parquet") });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const r = try Reader.open(a, try std.fs.path.join(a, &.{ dir, "l.parquet" }));
    defer r.close();
    try testing.expectEqual(types.TypeKind.string, r.schema.fields[r.schema.indexOf("xs").?].ty.kind);
    const names = [_][]const u8{ "id", "xs", "nest", "ds", "recs", "m" };
    try testing.expectEqual(names.len, r.schema.fields.len);
    for (names, r.schema.fields) |n, f| try testing.expectEqualStrings(n, f.name);
    try testing.expectEqual(types.TypeKind.string, r.schema.fields[4].ty.kind);
    const rb = [_]Bound{.{ .column = "recs", .op = .eq, .value = .{ .string = "zzz" } }};
    for (r.md.row_groups) |g| try testing.expect(groupMayMatch(r.md.schema, r.leaves, g, &rb));
    const b = [_]Bound{.{ .column = "xs", .op = .gt, .value = .{ .int = 100 } }};
    for (r.md.row_groups) |g| try testing.expect(groupMayMatch(r.md.schema, r.leaves, g, &b));
}

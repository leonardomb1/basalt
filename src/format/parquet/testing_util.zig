//! Test helpers shared by the tests of read.zig's parts.

pub const fx = @embedFile("../testdata/zstd.parquet");
const Entries = @import("chunk.zig").Entries;
const PlainCursor = @import("encoding.zig").PlainCursor;
const Reader = @import("read.zig").Reader;
const Value = @import("../../exec/value.zig").Value;
const decodeByteStreamSplit = @import("encoding.zig").decodeByteStreamSplit;
const decodeDeltaBinaryPacked = @import("encoding.zig").decodeDeltaBinaryPacked;
const decodeDeltaByteArray = @import("encoding.zig").decodeDeltaByteArray;
const decodeDeltaLengthByteArray = @import("encoding.zig").decodeDeltaLengthByteArray;
const decodeRleHybrid = @import("encoding.zig").decodeRleHybrid;
const eval = @import("../../exec/eval.zig");
const listColumn = @import("read.zig").listColumn;
const parquet = @import("footer.zig");
const std = @import("std");
const testing = std.testing;

pub fn logical(phys: parquet.PhysicalType, lt: parquet.LogicalType) parquet.SchemaElement {
    return .{ .ty = phys, .repetition = .optional, .logical_type = lt };
}

pub const fx_v2 = @embedFile("../testdata/v2delta.parquet");

pub const fx_logical = @embedFile("../testdata/logical_types.parquet");

pub fn fuzzKernels(_: void, input: []const u8) anyerror!void {
    if (input.len < 3) return;
    const width6: u6 = @truncate(input[0]);
    const count: usize = ((@as(usize, input[1]) << 4) | (input[2] & 0x0F)) & 0x1FF;
    const src = input[3..];
    var mem: [256 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    var arena = std.heap.ArenaAllocator.init(fba.allocator());
    defer arena.deinit();
    const a = arena.allocator();

    _ = decodeRleHybrid(a, src, width6 % 33, count) catch {};
    _ = decodeDeltaBinaryPacked(a, src, count) catch {};
    _ = decodeDeltaLengthByteArray(a, src, count) catch {};
    _ = decodeDeltaByteArray(a, src, count) catch {};
    _ = decodeByteStreamSplit(a, src, @max(1, @as(usize, width6 % 17)), count) catch {};

    inline for (.{ .boolean, .int32, .int64, .int96, .float, .double, .byte_array, .fixed_len_byte_array }) |pt| {
        var cur = PlainCursor.init(pt, @intCast(width6), src);
        var n: usize = 0;
        while (n < count) : (n += 1) {
            _ = cur.next() catch break;
        }
    }
}

pub const fuzzKernels_corpus = [_][]const u8{
    "\x03\x01\x00" ++ "\x03\x88\x01\x02\x03",
    "\x02\x00\x08" ++ "\x80\x01\x04\x05\x00\x01\x02\x03\x04",
};

pub fn readAllText(a: std.mem.Allocator, bytes: []const u8, name: []const u8) ![]const u8 {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = name, .data = bytes });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const r = try Reader.open(a, try std.fs.path.join(a, &.{ dir, name }));
    defer r.close();
    var out = std.Io.Writer.Allocating.init(a);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| {
            for (b.columns, r.schema.fields, 0..) |c, f, k| {
                if (k > 0) try out.writer.writeByte(' ');
                const v = c.getValue(i);
                try out.writer.print("{s}={s}", .{ f.name, if (v == .null) "null" else try eval.valueToString(a, v) });
            }
            try out.writer.writeByte('\n');
        }
    }
    return out.written();
}

pub fn entriesOf(a: std.mem.Allocator, vals: []const Value, reps: []const u32, defs: []const u32) !Entries {
    return .{ .vals = try listColumn(a, vals), .reps = reps, .defs = defs };
}

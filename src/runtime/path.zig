//! The path an output pipeline runs, decided once from its stages and its source's
//! kind: file lanes of one `LaneShape`, a SQL source's descents and key-range
//! splits, a union split branch by branch, or serial. The run dispatches on
//! `choose` (`run/output.zig`) and `EXPLAIN` names what it returns (`analyze`), so
//! the two cannot drift; they once disagreed in five shapes, EXPLAIN calling a
//! serial run parallel and the reverse. What only the run learns can still veto a
//! parallel path (a file's size, quoting or row groups, a table's key and rows, a
//! join build past `--op-memory`); the run then falls to the next path in the chain
//! and EXPLAIN names those as conditions. A window's input or an aggregating
//! binding read into memory first is a prelude ahead of the path, decided by
//! `windowInputAt` and `materializes` over the stages as they stand at that point.
//!
//! A map stage is a filter, select or explode (`analyze.isMap`) on a map path: a
//! file's map lanes, a SQL split and a union branch; the stages ahead of a lane
//! breaker or join stay filters and selects. The run takes the
//! plain SQL split off the built operators (`op.linearize`), which `Split.map`
//! states over the stages; `run/path_tests.zig` holds the two to one answer.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const connect = @import("connect.zig");
const lanes = @import("lanes.zig");
const isMap = @import("analyze/schema.zig").isMap;
const laneHints = @import("analyze/targets.zig").laneHints;
const op = @import("../exec/op.zig");
const pushdown = @import("pushdown.zig");

pub const Source = enum { parquet, csv, sql, other };

/// What kind of read heads `st`: a Parquet file or folder, a local CSV, NDJSON or
/// record file, a SQL connection's read, or anything else.
pub fn sourceOf(st: ast.Stage, connections: *const std.StringHashMap(ast.Connection)) Source {
    if (st.node != .read) return .other;
    const rd = st.node.read;
    if (connect.isParquetFileRead(rd) or connect.isFolderRead(rd)) return .parquet;
    if (connect.isLocalCsvRead(rd) or connect.isRecordRead(rd)) return .csv;
    const conn = connections.get(rd.connector) orelse return .other;
    return if (connect.sqlConnInfo(conn) != null) .sql else .other;
}

pub const Facts = struct {
    lanes: usize,
    sink: ast.Write,
    /// The kind of `stages[0]`, and of the head with every binding laid out.
    source: Source,
    head_source: Source,
};

pub const Split = union(enum) { none, map, agg: lanes.SinkShape, map_join: lanes.MapJoinShape };

/// A SQL source's chain: the aggregate, else the `ORDER BY … LIMIT`, sent to the
/// source; else the read cut into key ranges.
pub const Sql = struct { whole_agg: ?lanes.SinkShape, top_n: bool, split: Split };

pub const Path = union(enum) {
    serial,
    union_split,
    lanes: struct { source: Source, shape: lanes.LaneShape },
    sql: Sql,
};

/// The path for `stages` (its write last), `head` being them with the leading
/// binding chain laid out, which is what file lanes read.
pub fn choose(arena: std.mem.Allocator, stages: []const ast.Stage, head: []const ast.Stage, f: Facts) !Path {
    const middle = stages[1 .. stages.len - 1];
    if (stages[0].node == .union_ and f.lanes > 1 and !std.mem.eql(u8, f.sink.connector, "csv") and
        branchesAreReads(stages[0].node.union_) and streamsOnly(middle))
        return .union_split;
    if ((f.head_source == .parquet or f.head_source == .csv) and laneEligible(head, f.lanes)) {
        if (lanes.classifyLaneShape(head)) |shape| if (lanes.shapeLaneSafe(shape))
            return .{ .lanes = .{ .source = f.head_source, .shape = shape } };
    }
    if (f.source != .sql) return .serial;
    var why: []const u8 = "";
    return .{ .sql = .{
        .whole_agg = lanes.classifyWholeAgg(stages),
        .top_n = try pushdown.classifyTopN(arena, stages[0 .. stages.len - 1], &why) != null,
        .split = if (f.lanes < 2)
            .none
        else if (sqlAgg(stages)) |s|
            .{ .agg = s }
        else if (lanes.classifyMapJoinPipeline(stages)) |s|
            .{ .map_join = s }
        else if (streamsOnly(middle)) .map else .none,
    } };
}

/// At one lane, a file aggregate over joins whose head is filtered: its first join
/// still turns around when the filter leaves few rows (`run/output.zig`
/// `flipLaterJoin`), on one worker, and the plan stays serial otherwise.
pub fn oneLaneTurn(head: []const ast.Stage, f: Facts) ?lanes.SinkShape {
    if (f.lanes != 1 or !(f.head_source == .parquet or f.head_source == .csv)) return null;
    if (head.len < 2 or head[0].node != .read or !laneHints(head[0])) return null;
    const shape = lanes.classifyLaneShape(head) orelse return null;
    if (shape != .sink or shape.sink.joins.len == 0 or !lanes.shapeLaneSafe(shape)) return null;
    for (shape.sink.prefix) |st| if (st.node == .filter) return shape.sink;
    return null;
}

/// A SQL read's aggregate with no join ahead of it, which its key ranges can fold.
fn sqlAgg(stages: []const ast.Stage) ?lanes.SinkShape {
    const s = lanes.classifySinkPipeline(stages) orelse return null;
    return if (s.joins.len == 0) s else null;
}

/// Preconditions every lane path shares. A hinted read is left to the paths that honour
/// the hint: any hint used to keep a read serial while EXPLAIN called it morsel-parallel.
pub fn laneEligible(stages: []const ast.Stage, n: usize) bool {
    return n > 1 and stages.len >= 2 and stages[0].node == .read and laneHints(stages[0]);
}

/// Whether a binding of `stages` that a pipeline reads is aggregated first, in
/// parallel, into memory: built where it is read it ran on one thread.
pub fn materializes(stages: []const ast.Stage, n: usize) bool {
    if (n < 2 or stages.len < 2 or stages[0].node != .read) return false;
    for (stages) |st| {
        if (st.node == .aggregate) return true;
    }
    return false;
}

/// Where the window (or full sort) is that a file pipeline reads its input for
/// first, in parallel, into memory; null when it does not. Not when a filter
/// follows the window (`WHERE rn <= k` keeps only `k` rows a partition as they
/// stream) or a fusing `LIMIT` follows the sort.
pub fn windowInputAt(stages: []const ast.Stage, n: usize) ?usize {
    if (n < 2 or stages.len < 3 or !laneEligible(stages, n)) return null;
    const rd = stages[0].node.read;
    if (!connect.isLocalCsvRead(rd) and !connect.isParquetFileRead(rd) and !connect.isFolderRead(rd)) return null;
    var wi: usize = 1;
    while (wi < stages.len - 1 and (stages[wi].node == .filter or stages[wi].node == .select)) wi += 1;
    switch (stages[wi].node) {
        .window => |w| {
            if (w.top_k != null) return null;
            for (stages[wi + 1 ..]) |st| if (st.node == .filter) return null;
        },
        .sort => if (wi + 1 < stages.len and stages[wi + 1].node == .limit and op.TopN.fits(stages[wi + 1].node.limit)) return null,
        else => return null,
    }
    return wi;
}

fn branchesAreReads(u: ast.Union) bool {
    for (u.branches) |b| if (b.pipeline != null) return false;
    return true;
}

fn streamsOnly(stages: []const ast.Stage) bool {
    for (stages) |s| if (!isMap(s.node, true)) return false;
    return true;
}

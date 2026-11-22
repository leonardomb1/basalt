//! The path an output pipeline runs, decided once from its stages and its source's
//! kind: file lanes of one `LaneShape`, a SQL source's descents and key-range
//! splits, a union split branch by branch, or serial. The run dispatches on
//! `choose` (`run/output.zig`) and `EXPLAIN` names what it returns (`analyze`), so
//! the two cannot drift; they once disagreed in five shapes, EXPLAIN calling a
//! serial run parallel and the reverse. What only the run learns can still veto a
//! parallel path (a file's size, quoting or row groups, a table's key and rows, a
//! join build past `--op-memory`); the run then falls to the next path in the chain
//! and EXPLAIN names those as conditions. An aggregating binding read into memory
//! first is a prelude ahead of the path, decided by `materializes` over the stages
//! as they stand at that point.
//!
//! A map stage is a filter, select or explode (`analyze.isMap`) on a map path: a
//! file's map lanes, a SQL split and a union branch; the stages ahead of a lane
//! breaker or join stay filters and selects. The run takes the
//! plain SQL split off the built operators (`op.linearize`), which `Split.map`
//! states over the stages; `run/path_tests.zig` holds the two to one answer.

const std = @import("std");
const aggregates = @import("../lang/aggregates.zig");
const ast = @import("../lang/ast.zig");
const connect = @import("connect.zig");
const lanes = @import("lanes.zig");
const isMap = @import("analyze/schema.zig").isMap;
const laneHints = @import("analyze/targets.zig").laneHints;
const formatOfPath = @import("analyze/targets.zig").formatOfPath;
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
    if (footerCount(head) or foldsInInputOrder(head) or foldsInInputOrder(stages)) return .serial;
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
        else if (mapJoin(stages)) |s|
            .{ .map_join = s }
        else if (streamsOnly(middle)) .map else .none,
    } };
}

/// A Parquet file read whole into a Parquet file: no filter, hint or writer option,
/// at most one SELECT that only keeps, drops and renames columns. The run then copies
/// the column chunks as they stand where every kept column is one the writer would
/// write alike (`format/parquet/copy.zig`), else takes the path `choose` names.
pub fn chunkCopy(stages: []const ast.Stage, f: Facts) bool {
    if (stages.len < 2 or stages.len > 3 or stages[0].node != .read) return false;
    const rd = stages[0].node.read;
    if (stages[0].hints.len > 0 or rd.where.len > 0 or !connect.isParquetFileRead(rd)) return false;
    if (stages.len == 3) {
        if (stages[1].node != .select or stages[1].hints.len > 0) return false;
        for (stages[1].node.select) |it| switch (it) {
            .star, .star_except, .star_rename => {},
            .field => |q| if (q.parts.len != 1) return false,
            .computed => |c| if (c.expr.* != .field or c.expr.field.parts.len != 1) return false,
            .star_replace, .star_of => return false,
        };
    }
    const w = f.sink;
    if (!std.mem.eql(u8, w.connector, "csv") or w.split != null or w.changes != null) return false;
    if (w.mode != .default and w.mode != .overwrite) return false;
    if (stages[stages.len - 1].hints.len > 0) return false;
    return formatOfPath(w.target) == .parquet;
}

/// Whether an aggregate of `stages` answers by the order its rows arrive in
/// (`string_agg`, `array_agg`, `any_value`, `arg_max`, their ties). One lane reads the
/// input as it stands, spilling included, so the answer does not depend on `-j`. The
/// lanes' merge knows nothing of a grand total owed over no rows, so the first
/// aggregate, the one the lanes fold, may not owe one; a later one runs serially over
/// the merged groups, as a ROLLUP's sets over its pre-aggregate do.
fn foldsInInputOrder(stages: []const ast.Stage) bool {
    var first = true;
    for (stages) |st| if (st.node == .aggregate) {
        if (first and st.node.aggregate.on_empty.len > 0) return true;
        first = false;
        for (st.node.aggregate.aggs) |a| if (aggregates.spec(a.func).input_ordered) return true;
    };
    return false;
}

/// An ungrouped `COUNT(*)` alone straight off a local Parquet file, which the plan
/// answers from its footer (`plan.metaShortcut`); on lanes it decoded a column.
fn footerCount(head: []const ast.Stage) bool {
    if (head.len != 3 or head[0].node != .read or head[1].node != .aggregate) return false;
    const rd = head[0].node.read;
    if (!connect.isParquetFileRead(rd) or std.mem.indexOf(u8, rd.form.path, "://") != null) return false;
    const ag = head[1].node.aggregate;
    if (ag.by_rest != null or ag.by.len != 0 or ag.aggs.len == 0) return false;
    for (ag.aggs) |a| if (a.func != .count or a.arg != null or a.distinct) return false;
    return true;
}

/// At one lane, a file aggregate over joins whose head is filtered: its first join
/// still turns around when the filter leaves few rows (`run/output.zig`
/// `flipLaterJoin`), on one worker, and the plan stays serial otherwise.
pub fn oneLaneTurn(head: []const ast.Stage, f: Facts) ?lanes.SinkShape {
    if (f.lanes != 1 or !(f.head_source == .parquet or f.head_source == .csv) or foldsInInputOrder(head)) return null;
    if (head.len < 2 or head[0].node != .read or !laneHints(head[0])) return null;
    const shape = lanes.classifyLaneShape(head) orelse return null;
    if (shape != .sink or shape.sink.joins.len == 0 or !lanes.shapeLaneSafe(shape)) return null;
    for (shape.sink.prefix) |st| if (st.node == .filter) return shape.sink;
    return null;
}

/// A SQL read's aggregate with no join ahead of it, which its key ranges can fold.
fn sqlAgg(stages: []const ast.Stage) ?lanes.SinkShape {
    const shape = lanes.classifyLaneShape(stages) orelse return null;
    if (shape != .sink) return null;
    return if (shape.sink.joins.len == 0) shape.sink else null;
}

/// A SQL read's single map and join, which its key ranges can probe.
/// A SQL source's map+join: one join, its key ranges' lanes probing one index.
fn mapJoin(stages: []const ast.Stage) ?lanes.MapJoinShape {
    const shape = lanes.classifyLaneShape(stages) orelse return null;
    if (shape != .map_join) return null;
    for (shape.map_join.suffix) |st| if (st.node == .join) return null;
    return shape.map_join;
}

/// Preconditions every lane path shares. A hinted read is left to the paths that honour
/// the hint: any hint used to keep a read serial while EXPLAIN called it morsel-parallel.
pub fn laneEligible(stages: []const ast.Stage, n: usize) bool {
    return n > 1 and stages.len >= 2 and stages[0].node == .read and laneHints(stages[0]);
}

/// Whether a binding of `stages` that a pipeline reads is aggregated first, in
/// parallel, into memory: built where it is read it ran on one thread.
/// The shape lanes would run `stages` (a binding held in memory at their head, their
/// write last) in over its batches (`run/output.zig` `memLanes`), for EXPLAIN; empty
/// when they would run serially.
pub fn memLaneText(stages: []const ast.Stage, n: usize) []const u8 {
    if (n < 2 or stages.len < 3) return "";
    const shape = lanes.classifyLaneShape(stages) orelse return "";
    if (!lanes.memLaneShape(shape) or !lanes.shapeLaneSafe(shape)) return "";
    return switch (shape) {
        .sink => |x| if (x.joins.len > 0) "aggregate over joins" else "aggregate",
        .distinct => "distinct",
        .top_n => "top-n",
        .sort => |x| if (x.srt.node == .window) "window" else "sort",
        .map, .map_join => "",
    };
}

pub const mem_lane_rows = lanes.mem_lane_rows;

pub fn materializes(stages: []const ast.Stage, n: usize) bool {
    if (n < 2 or stages.len < 2 or stages[0].node != .read) return false;
    for (stages) |st| {
        if (st.node == .aggregate) return true;
    }
    return false;
}

fn branchesAreReads(u: ast.Union) bool {
    for (u.branches) |b| if (b.pipeline != null) return false;
    return true;
}

fn streamsOnly(stages: []const ast.Stage) bool {
    for (stages) |s| if (!isMap(s.node, true)) return false;
    return true;
}

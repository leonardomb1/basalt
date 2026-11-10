//! The one rule for which lane pipeline a stage list is: `classify` reads past the
//! leading filters and selects, and the first other stage decides the shape. A
//! join heads an aggregate over joins when an aggregate follows, else a single
//! map and join; an aggregate, a DISTINCT, an ORDER BY with or without its LIMIT
//! and a window are the breakers of their own shapes; filters, selects and
//! explodes alone are a map. The shapes' rules on what may follow their breaker
//! are here too, so no two classifiers can claim one pipeline differently.

const std = @import("std");
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const op = @import("../../exec/op.zig");
const lanes = @import("../lanes.zig");

pub const LaneShape = union(enum) {
    sink: SinkShape,
    distinct: DistinctShape,
    top_n: TopNShape,
    map: []const ast.Stage,
    map_join: MapJoinShape,
    sort: SortShape,
};

/// A pipeline whose breaker is a parallel sink, the aggregate: map stages, then
/// lane-safe keyed joins, the aggregate, and a tail its finalize writes through.
pub const SinkShape = struct { prefix: []const ast.Stage, joins: []const ast.Stage, agg: ast.Stage, tail: []const ast.Stage };

pub const MapJoinShape = struct { prefix: []const ast.Stage, join: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage, stage: ast.Stage };

pub const DistinctShape = struct { prefix: []const ast.Stage, dist: ast.Stage, tail: []const ast.Stage };

pub const TopNShape = struct { prefix: []const ast.Stage, srt: ast.Stage, lim: ast.Limit };

pub const SortShape = struct { prefix: []const ast.Stage, srt: ast.Stage, tail: []const ast.Stage };

/// The lane shape of `stages` (a read first, its write last), or null when none
/// fits and the pipeline runs serially.
pub fn classify(stages: []const ast.Stage) ?LaneShape {
    const m = stages[1 .. stages.len - 1];
    var i: usize = 0;
    while (i < m.len and isFilterOrSelect(m[i])) i += 1;
    if (i == m.len) return .{ .map = m };
    const st = m[i];
    return switch (st.node) {
        .explode => if (allMap(m, true)) .{ .map = m } else null,
        .join => if (sinkAt(m, i)) |s| .{ .sink = s } else if (mapJoin(stages, m, i)) |s| .{ .map_join = s } else null,
        .aggregate => if (sinkAt(m, i)) |s| .{ .sink = s } else null,
        .distinct => if (distinctAt(m, i)) |s| .{ .distinct = s } else null,
        .sort => if (topN(m, i)) |s| .{ .top_n = s } else if (sortAt(m, i)) |s| .{ .sort = s } else null,
        .window => if (sortAt(m, i)) |s| .{ .sort = s } else null,
        else => null,
    };
}

fn isFilterOrSelect(st: ast.Stage) bool {
    return analyze.isMap(st.node, false);
}

fn allMap(m: []const ast.Stage, explode: bool) bool {
    for (m) |st| if (!analyze.isMap(st.node, explode)) return false;
    return true;
}

/// `m[0..i]` filters and selects, then lane-safe keyed joins with filters and
/// selects between, the aggregate, and a tail of filters, selects, sorts and limits;
/// a distinct or window in the tail only without joins (TPC-H q01's reordering
/// projection and HAVING ride the tail; refusing them sent it serial, 379 vs 88 ms).
fn sinkAt(m: []const ast.Stage, i: usize) ?SinkShape {
    var a: ?usize = null;
    var joined = false;
    for (m[i..], i..) |st, k| switch (st.node) {
        .filter, .select => {},
        .join => |j| {
            if (a != null or !lanes.joinKindLaneSafe(j.kind) or j.keyless()) return null;
            joined = true;
        },
        .aggregate => {
            if (a != null) return null;
            a = k;
        },
        .sort, .limit => if (a == null) return null,
        .distinct, .window => if (a == null or joined) return null,
        else => return null,
    };
    const ai = a orelse return null;
    return .{ .prefix = m[0..i], .joins = m[i..ai], .agg = m[ai], .tail = m[ai + 1 ..] };
}

/// One keyed join with filters and selects around it, into a write.
fn mapJoin(stages: []const ast.Stage, m: []const ast.Stage, i: usize) ?MapJoinShape {
    if (stages.len < 3 or stages[stages.len - 1].node != .write) return null;
    for (m[i + 1 ..]) |st| if (!isFilterOrSelect(st)) return null;
    const j = m[i];
    if (j.node.join.keyless()) return null;
    return .{ .prefix = m[0..i], .join = j.node.join, .join_hints = j.hints, .suffix = m[i + 1 ..], .stage = j };
}

/// The DISTINCT, then filters, selects, sorts, limits, aggregates and windows.
fn distinctAt(m: []const ast.Stage, i: usize) ?DistinctShape {
    for (m[i + 1 ..]) |st| switch (st.node) {
        .filter, .select, .sort, .limit, .aggregate, .window => {},
        else => return null,
    };
    return .{ .prefix = m[0..i], .dist = m[i], .tail = m[i + 1 ..] };
}

/// An ORDER BY as the last stage but its LIMIT, which the top-N keeps.
fn topN(m: []const ast.Stage, i: usize) ?TopNShape {
    if (i + 2 != m.len or m[i + 1].node != .limit or !op.TopN.fits(m[i + 1].node.limit)) return null;
    return .{ .prefix = m[0..i], .srt = m[i], .lim = m[i + 1].node.limit };
}

/// An ORDER BY not followed by a LIMIT the top-N takes, or a window that keeps
/// every row (no top-k, no filter after it, `WHERE rn <= k` keeping `k` a
/// partition as they stream); anything after it is the tail.
fn sortAt(m: []const ast.Stage, i: usize) ?SortShape {
    const st = m[i];
    const tail = m[i + 1 ..];
    switch (st.node) {
        .sort => if (tail.len > 0 and tail[0].node == .limit and op.TopN.fits(tail[0].node.limit)) return null,
        .window => |wd| {
            if (wd.top_k != null) return null;
            for (tail) |t| if (t.node == .filter) return null;
        },
        else => unreachable,
    }
    return .{ .prefix = m[0..i], .srt = st, .tail = tail };
}

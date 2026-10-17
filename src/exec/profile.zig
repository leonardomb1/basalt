//! The actuals `EXPLAIN ANALYZE` prints: one node per plan operator, its time, rows
//! and batches summed over every chain that ran it, and the same figures per lane, so
//! a lane that did the work of seven others shows as the spread between them.
//!
//! A node is named by the stage it was built from (`Stats.at`, the stage's position,
//! stamped as the plan builds), the operator kind it became, and its path from the
//! chain's root, so the chains a parallel path builds for one pipeline, one per unit
//! of work on any thread, fold node by node. The path is needed besides the stage: a
//! chain of joins, or a union's branches, are operators of one kind one stage built.
//! The tree's shape is the first chain's.
//!
//! Nothing here runs per batch. A chain is folded once, when its unit of work is
//! done (`absorb`, under a mutex), from the counters `Op.next` keeps anyway, and a
//! run without `EXPLAIN ANALYZE` has no profile at all, so it pays nothing. A lane is
//! the thread that folded the chain, numbered as first seen.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const op = @import("op.zig");
const print = @import("../lang/print.zig");

pub const Kind = std.meta.Tag(op.Op);

pub const Id = struct { at: u64, kind: Kind, path: u64 = root_path, scope: u64 = 0 };

const root_path: u64 = 1;

const Said = struct { at: u64, kind: Kind, scope: u64 = 0 };

/// The name of the scan a parallel breaker's tail reads: the lanes' partial results,
/// merged. No stage stands for it, and the stage it follows shares its position with
/// the pipeline's read.
pub const merged_at: u64 = std.math.maxInt(u64);

fn childPath(parent: Id, i: usize) u64 {
    var h = std.hash.Wyhash.init(parent.path);
    h.update(std.mem.asBytes(&parent.kind));
    h.update(std.mem.asBytes(&i));
    return h.final();
}

pub const Role = enum { input, probe, build };

/// An input of a node; `nested` unless it ran outside the node's timer (a build
/// side made before the lanes, the lanes under a merge), so the node's own time
/// keeps it.
pub const Child = struct { id: Id, role: Role, nested: bool = true };

pub const Tally = struct {
    ns: u64 = 0,
    rows: u64 = 0,
    calls: u64 = 0,

    pub fn add(self: *Tally, o: Tally) void {
        self.ns += o.ns;
        self.rows += o.rows;
        self.calls += o.calls;
    }
};

pub const Unit = op.Unit;
pub const Fact = op.Fact;

pub const Node = struct {
    id: Id,
    children: []const Child = &.{},
    total: Tally = .{},
    lanes: std.ArrayListUnmanaged(Tally) = .empty,
    facts: std.ArrayListUnmanaged(Fact) = .empty,
    note: []const u8 = "",
    chains: u64 = 0,

    /// The node's own time, its inputs' taken out: a pull pipeline nests a child's
    /// `next` inside its parent's.
    pub fn exclusive(self: *const Node, p: *const Profile) u64 {
        var kids: u64 = 0;
        for (self.children) |c| if (c.nested) if (p.nodes.getPtr(c.id)) |k| {
            kids += k.total.ns;
        };
        return self.total.ns -| kids;
    }

    /// `exclusive` on one lane.
    pub fn laneExclusive(self: *const Node, p: *const Profile, lane: usize) u64 {
        const own = if (lane < self.lanes.items.len) self.lanes.items[lane].ns else 0;
        var kids: u64 = 0;
        for (self.children) |c| if (c.nested) if (p.nodes.getPtr(c.id)) |k| {
            if (lane < k.lanes.items.len) kids += k.lanes.items[lane].ns;
        };
        return own -| kids;
    }

    /// Rows into the node: what its inputs produced, a join's probe side only, or for
    /// a scan what it read.
    pub fn rowsIn(self: *const Node, p: *const Profile) u64 {
        if (self.children.len == 0) return self.total.rows;
        var n: u64 = 0;
        for (self.children) |c| if (c.role != .build) if (p.nodes.getPtr(c.id)) |k| {
            n += k.total.rows;
        };
        return n;
    }

    pub fn fact(self: *const Node, name: []const u8) ?u64 {
        for (self.facts.items) |f| if (std.mem.eql(u8, f.name, name)) return f.value;
        return null;
    }
};

/// A named slice of the run outside any operator: opening the sink, a parallel
/// merge, writing the result.
pub const Phase = struct { name: []const u8, ns: u64 };

pub const Profile = struct {
    gpa: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},
    nodes: std.AutoArrayHashMapUnmanaged(Id, Node) = .empty,
    roots: std.ArrayListUnmanaged(Id) = .empty,
    threads: std.AutoArrayHashMapUnmanaged(std.Thread.Id, void) = .empty,
    details: std.AutoHashMapUnmanaged(Said, []const u8) = .empty,
    phases: std.ArrayListUnmanaged(Phase) = .empty,
    /// What the run asked its sources for, as sent: a SQL read's query, an HTTP
    /// read's URL. Pushdown is only visible here.
    sent: std.ArrayListUnmanaged([]const u8) = .empty,
    /// The stage an operator with no position of its own was built for, by kind: a
    /// lane's scan opens over a morsel of the pipeline's read, and its aggregate,
    /// top-N, distinct or join are built outside the planner, from the one stage of
    /// that kind the lane shape allows.
    fallback: std.EnumArray(Kind, u64) = .initFill(0),
    /// How the pipeline ran, as the run chose: `serial`, `parquet morsel-parallel`, ...
    path: []const u8 = "serial",
    /// The run of a pipeline that executes in several separate runs over one stage
    /// list, a split union's branches: its nodes and their descriptions are kept
    /// apart from another run's of the same shape. 0 outside any.
    scope: u64 = 0,

    pub fn init(gpa: std.mem.Allocator) Profile {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Profile) void {
        var it = self.nodes.iterator();
        while (it.next()) |e| {
            e.value_ptr.lanes.deinit(self.gpa);
            e.value_ptr.facts.deinit(self.gpa);
            self.gpa.free(e.value_ptr.children);
            self.gpa.free(e.value_ptr.note);
        }
        self.nodes.deinit(self.gpa);
        self.roots.deinit(self.gpa);
        self.threads.deinit(self.gpa);
        var dt = self.details.valueIterator();
        while (dt.next()) |d| self.gpa.free(d.*);
        self.details.deinit(self.gpa);
        self.phases.deinit(self.gpa);
        for (self.sent.items) |s| self.gpa.free(s);
        self.sent.deinit(self.gpa);
    }

    /// Records a request made of a source, once however many lanes made it.
    pub fn noteSent(self: *Profile, comptime fmt: []const u8, args: anytype) !void {
        const text = try std.fmt.allocPrint(self.gpa, fmt, args);
        self.mtx.lock();
        defer self.mtx.unlock();
        for (self.sent.items) |s| if (std.mem.eql(u8, s, text)) {
            self.gpa.free(text);
            return;
        };
        try self.sent.append(self.gpa, text);
    }

    /// Fold a finished chain into the profile, on the lane of the calling thread, and
    /// return its root's id.
    pub fn absorb(self: *Profile, root: op.Op) !Id {
        return self.fold(root, true);
    }

    /// Register a chain's shape and no figures: for a path that runs the operators'
    /// work without pulling them (the SQL split lanes apply a plan's filters and
    /// projections batch by batch), whose lanes then `credit` each node.
    pub fn shape(self: *Profile, root: op.Op) !Id {
        return self.fold(root, false);
    }

    fn fold(self: *Profile, root: op.Op, count: bool) !Id {
        self.mtx.lock();
        defer self.mtx.unlock();
        const lane = try self.laneOfCaller();
        const id = try self.walk(root, lane, root_path, count);
        for (self.roots.items) |r| if (std.meta.eql(r, id)) return id;
        try self.roots.append(self.gpa, id);
        return id;
    }

    /// Add `t` to node `id`, on the lane of the calling thread.
    pub fn credit(self: *Profile, id: Id, t: Tally) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        const lane = try self.laneOfCaller();
        const n = self.nodes.getPtr(id) orelse return;
        try addTally(self.gpa, n, lane, t);
    }

    /// The ids of a linear chain, root first, down its first inputs to the scan.
    pub fn chainIds(self: *const Profile, root: Id, buf: []Id) []const Id {
        var n: usize = 0;
        var cur: ?Id = root;
        while (cur) |id| {
            if (n == buf.len) break;
            buf[n] = id;
            n += 1;
            const node = self.nodes.getPtr(id) orelse break;
            cur = if (node.children.len > 0) node.children[0].id else null;
        }
        return buf[0..n];
    }

    /// Put the chain rooted at `below` under the leaf of the chain rooted at `above`:
    /// a parallel aggregate's merged groups are the scan its tail reads.
    pub fn graftLeaf(self: *Profile, above: Id, below: Id) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        var cur = above;
        while (self.nodes.getPtr(cur)) |n| {
            if (n.children.len == 0) {
                const kids = try self.gpa.alloc(Child, 1);
                kids[0] = .{ .id = below, .role = .input, .nested = false };
                n.children = kids;
                self.dropRoot(below);
                return;
            }
            cur = n.children[0].id;
        }
    }

    /// Put the chain rooted at `build` under the joins built from the stage at
    /// `join_at` that have no build side of their own: a lane's join probes an index
    /// built once, before the lanes.
    pub fn graftBuild(self: *Profile, join_at: u64, build: Id) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        const at = if (join_at == 0) self.fallback.get(.join) else join_at;
        var grafted = false;
        for (self.nodes.values()) |*n| {
            if (n.id.kind != .join or n.id.at != at or n.id.scope != self.scope or n.children.len != 1) continue;
            const kids = try self.gpa.alloc(Child, 2);
            kids[0] = n.children[0];
            kids[1] = .{ .id = build, .role = .build, .nested = false };
            self.gpa.free(n.children);
            n.children = kids;
            grafted = true;
        }
        if (grafted) self.dropRoot(build);
    }

    /// The first root folded whose operator is `kind`.
    pub fn rootOf(self: *Profile, kind: Kind) ?Id {
        self.mtx.lock();
        defer self.mtx.unlock();
        for (self.roots.items) |r| if (r.kind == kind and r.scope == self.scope) return r;
        return null;
    }

    fn dropRoot(self: *Profile, id: Id) void {
        for (self.roots.items, 0..) |r, i| if (std.meta.eql(r, id)) {
            _ = self.roots.orderedRemove(i);
            return;
        };
    }

    fn laneOfCaller(self: *Profile) !usize {
        const t = try self.threads.getOrPut(self.gpa, std.Thread.getCurrentId());
        return t.index;
    }

    /// Folds `node`, found at `path` below its chain's root, and returns its id.
    fn walk(self: *Profile, node: op.Op, lane: usize, path: u64, count: bool) std.mem.Allocator.Error!Id {
        const st = node.stats();
        const at = if (st.at == 0) self.fallback.get(node) else st.at;
        const id = Id{ .at = at, .kind = node, .path = path, .scope = self.scope };

        const kids = try self.kidsOf(node, id, lane, count);
        if (self.nodes.contains(id)) {
            self.gpa.free(kids);
        } else {
            errdefer self.gpa.free(kids);
            const note = try self.gpa.dupe(u8, node.note());
            errdefer self.gpa.free(note);
            try self.nodes.put(self.gpa, id, .{ .id = id, .children = kids, .note = note });
        }
        if (!count) return id;
        const n = self.nodes.getPtr(id).?;
        try addTally(self.gpa, n, lane, .{ .ns = st.ns, .rows = st.rows, .calls = st.calls });
        var facts_buf: [16]Fact = undefined;
        for (node.facts(&facts_buf)) |f| try addFact(self.gpa, &n.facts, f);
        return id;
    }

    /// `node`'s inputs, folded, as many as it has.
    fn kidsOf(self: *Profile, node: op.Op, id: Id, lane: usize, count: bool) std.mem.Allocator.Error![]Child {
        const len: usize = switch (node) {
            .scan => 0,
            .join => |j| 1 + @as(usize, @intFromBool(j.build != null)),
            .nl_join => |j| 1 + @as(usize, @intFromBool(j.build != null)),
            .union_ => |u| u.children.len,
            else => 1,
        };
        const kids = try self.gpa.alloc(Child, len);
        errdefer self.gpa.free(kids);
        switch (node) {
            .scan => {},
            .join => |j| {
                kids[0] = .{ .id = try self.walk(j.probe, lane, childPath(id, 0), count), .role = .probe };
                if (j.build) |b| kids[1] = .{ .id = try self.walk(b, lane, childPath(id, 1), count), .role = .build };
            },
            .nl_join => |j| {
                kids[0] = .{ .id = try self.walk(j.probe, lane, childPath(id, 0), count), .role = .probe };
                if (j.build) |b| kids[1] = .{ .id = try self.walk(b, lane, childPath(id, 1), count), .role = .build };
            },
            .union_ => |u| for (u.children, kids, 0..) |c, *k, i| {
                k.* = .{ .id = try self.walk(c, lane, childPath(id, i), count), .role = .input };
            },
            inline else => |o| kids[0] = .{ .id = try self.walk(o.child, lane, childPath(id, 0), count), .role = .input },
        }
        return kids;
    }

    /// Text shown beside the `kind` operators built from the stage at `at`; the
    /// first wins. Keyed by kind as well: the stages one SELECT parses into share
    /// its position, so the read and the aggregate over it are told apart by kind.
    pub fn describe(self: *Profile, at: u64, kind: Kind, text: []const u8) !void {
        if (at == 0 or text.len == 0) return;
        self.mtx.lock();
        defer self.mtx.unlock();
        const e = try self.details.getOrPut(self.gpa, .{ .at = at, .kind = kind, .scope = self.scope });
        if (!e.found_existing) e.value_ptr.* = try self.gpa.dupe(u8, text);
    }

    pub fn detail(self: *const Profile, id: Id) []const u8 {
        return self.details.get(.{ .at = id.at, .kind = id.kind, .scope = id.scope }) orelse "";
    }

    pub fn phase(self: *Profile, name: []const u8, ns: u64) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        for (self.phases.items) |*p| if (std.mem.eql(u8, p.name, name)) {
            p.ns += ns;
            return;
        };
        try self.phases.append(self.gpa, .{ .name = name, .ns = ns });
    }

    pub fn laneCount(self: *const Profile) usize {
        return self.threads.count();
    }
};

fn addTally(gpa: std.mem.Allocator, n: *Node, lane: usize, t: Tally) !void {
    n.total.add(t);
    n.chains += 1;
    while (n.lanes.items.len <= lane) try n.lanes.append(gpa, .{});
    n.lanes.items[lane].add(t);
}

fn addFact(gpa: std.mem.Allocator, list: *std.ArrayListUnmanaged(Fact), f: Fact) !void {
    for (list.items) |*have| if (std.mem.eql(u8, have.name, f.name)) {
        have.value = switch (f.fold) {
            .sum => have.value + f.value,
            .max => @max(have.value, f.value),
        };
        return;
    };
    try list.append(gpa, f);
}

/// The operators a stage of this kind can become.
pub fn kindsOf(node: std.meta.Tag(ast.Stage.Node)) []const Kind {
    return switch (node) {
        .read => &.{.scan},
        .union_ => &.{.union_},
        .filter => &.{.filter},
        .select => &.{.project},
        .limit => &.{.limit},
        .distinct => &.{.distinct},
        .sort => &.{ .sort, .top_n },
        .window => &.{.window},
        .aggregate => &.{.aggregate},
        .join => &.{ .join, .nl_join },
        .explode => &.{.explode},
        .ref, .write => &.{},
    };
}

/// A stage's position as a node name: never 0 for a parsed stage, whose line starts at 1.
pub fn keyOf(pos: ast.Pos) u64 {
    return (@as(u64, pos.file) << 48) | (@as(u64, pos.line) << 24) | @as(u64, pos.col);
}

/// What `EXPLAIN ANALYZE` prints beside an operator built from `st`, in SQL: the
/// predicate, the keys, the columns. A read is labelled by the caller, which knows
/// the connection behind it.
pub fn stageText(arena: std.mem.Allocator, st: ast.Stage) ![]const u8 {
    var out = std.Io.Writer.Allocating.init(arena);
    const w = &out.writer;
    switch (st.node) {
        .read, .ref => {},
        .union_ => |u| try w.print("{d} branches", .{u.branches.len}),
        .filter => |p| try print.expr(w, p),
        .select => |items| try selectText(w, items),
        .limit => |l| {
            if (l.count != std.math.maxInt(u64)) try w.print("{d}", .{l.count}) else try w.writeAll("all");
            if (l.offset > 0) try w.print(" offset {d}", .{l.offset});
        },
        .distinct => |d| if (d.on) |on| {
            try w.writeAll("on ");
            for (on, 0..) |q, i| {
                if (i > 0) try w.writeAll(", ");
                try print.name(w, q);
            }
        },
        .sort => |s| try sortText(w, s),
        .window => |wd| try w.writeAll(try ast.windowDetail(arena, wd)),
        .aggregate => |ag| {
            if (ag.by.len > 0) {
                try w.writeAll("by ");
                for (ag.by, 0..) |q, i| {
                    if (i > 0) try w.writeAll(", ");
                    try print.name(w, q);
                }
                try w.writeAll("; ");
            }
            for (ag.aggs, 0..) |a, i| {
                if (i > 0) try w.writeAll(", ");
                try w.print("{s}(", .{@tagName(a.func)});
                if (a.distinct) try w.writeAll("DISTINCT ");
                if (a.arg) |e| try print.expr(w, e) else try w.writeByte('*');
                try w.writeByte(')');
            }
        },
        .join => |j| {
            try w.print("{s}", .{@tagName(j.kind)});
            for (j.left_keys, j.right_keys, 0..) |l, r, i| {
                try w.writeAll(if (i == 0) " on " else " and ");
                try print.name(w, l);
                try w.writeAll(" = ");
                try print.name(w, r);
            }
            if (j.right_filter) |f| {
                try w.writeAll(if (j.left_keys.len == 0) " on " else " and ");
                try print.expr(w, f);
            }
        },
        .explode => |ex| try w.writeAll(ex.field),
        .write => {},
    }
    return out.written();
}

/// `stageText` of a sort fused with its limit into a top-N.
pub fn topNText(arena: std.mem.Allocator, s: ast.Sort, lim: ast.Limit) ![]const u8 {
    var out = std.Io.Writer.Allocating.init(arena);
    const w = &out.writer;
    try w.print("{d}", .{lim.count});
    if (lim.offset > 0) try w.print(" offset {d}", .{lim.offset});
    try w.writeAll(" by ");
    try sortText(w, s);
    return out.written();
}

fn sortText(w: *std.Io.Writer, s: ast.Sort) !void {
    for (s.keys, 0..) |k, i| {
        if (i > 0) try w.writeAll(", ");
        try print.name(w, k.field);
        if (k.desc) try w.writeAll(" DESC");
        try w.writeAll(k.nullsLabel());
    }
}

/// The output columns, the first few and a count of the rest: a 300-column SELECT
/// would bury the tree.
fn selectText(w: *std.Io.Writer, items: []const ast.SelectItem) !void {
    const shown = 6;
    for (items[0..@min(items.len, shown)], 0..) |it, i| {
        if (i > 0) try w.writeAll(", ");
        switch (it) {
            .star, .star_of => try w.writeByte('*'),
            .star_except => try w.writeAll("* except ..."),
            .star_rename => try w.writeAll("* rename ..."),
            .field => |q| try print.name(w, q),
            .computed => |c| {
                try print.expr(w, c.expr);
                try w.print(" AS {s}", .{c.name});
            },
        }
    }
    if (items.len > shown) try w.print(", ... (+{d})", .{items.len - shown});
}

const testing = std.testing;

test "profile: two lanes' chains of one stage fold into one node, with each lane's share" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    p.fallback.set(.scan, 7);
    const Lane = struct {
        fn run(prof: *Profile, rows: u64) !void {
            var scan = op.Scan{ .src = undefined };
            scan.stats = .{ .ns = 100 * rows, .rows = rows, .calls = 2 };
            var f = op.Filter{ .child = .{ .scan = &scan }, .pred = undefined, .back = testing.allocator };
            f.stats = .{ .at = 9, .ns = 150 * rows, .rows = rows / 2, .calls = 2 };
            _ = try prof.absorb(.{ .filter = &f });
        }
    };
    const t = try std.Thread.spawn(.{}, Lane.run, .{ &p, 10 });
    t.join();
    try Lane.run(&p, 30);

    try testing.expectEqual(@as(usize, 2), p.laneCount());
    try testing.expectEqual(@as(usize, 1), p.roots.items.len);
    const f = p.nodes.getPtr(.{ .at = 9, .kind = .filter }).?;
    try testing.expectEqual(@as(u64, 20), f.total.rows);
    try testing.expectEqual(@as(u64, 2), f.chains);
    try testing.expectEqual(@as(u64, 40), f.rowsIn(&p));
    try testing.expectEqual(@as(u64, 2000), f.exclusive(&p));
    try testing.expectEqual(@as(u64, 500), f.laneExclusive(&p, 0));
    try testing.expectEqual(@as(u64, 1500), f.laneExclusive(&p, 1));
    try testing.expectEqual(@as(usize, 1), f.children.len);
    try testing.expectEqual(@as(u64, 7), f.children[0].id.at);
}

test "profile: a union's branches built from one stage stay apart" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    var a = op.Scan{ .src = undefined };
    a.stats = .{ .at = 3, .rows = 1 };
    var b = op.Scan{ .src = undefined };
    b.stats = .{ .at = 3, .rows = 2 };
    const kids = [_]op.Op{ .{ .scan = &a }, .{ .scan = &b } };
    var u = op.Union{ .children = &kids };
    u.stats = .{ .at = 3, .rows = 3 };
    _ = try p.absorb(.{ .union_ = &u });
    const un = p.nodes.getPtr(.{ .at = 3, .kind = .union_ }).?;
    try testing.expectEqual(@as(usize, 2), un.children.len);
    try testing.expectEqual(@as(u64, 1), p.nodes.getPtr(un.children[0].id).?.total.rows);
    try testing.expectEqual(@as(u64, 2), p.nodes.getPtr(un.children[1].id).?.total.rows);
    try testing.expectEqual(@as(usize, 3), p.nodes.count());
}

test "profile: a union past 64 branches keeps every one" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    var scans: [80]op.Scan = undefined;
    var kids: [80]op.Op = undefined;
    for (&scans, &kids, 0..) |*sc, *k, i| {
        sc.* = .{ .src = undefined };
        sc.stats = .{ .at = 3, .rows = i };
        k.* = .{ .scan = sc };
    }
    var u = op.Union{ .children = &kids };
    u.stats = .{ .at = 3 };
    _ = try p.absorb(.{ .union_ = &u });
    const un = p.nodes.getPtr(.{ .at = 3, .kind = .union_ }).?;
    try testing.expectEqual(@as(usize, 80), un.children.len);
    try testing.expectEqual(@as(u64, 79 * 80 / 2), un.rowsIn(&p));
}

test "profile: separate runs of one stage list in their own scopes stay apart, each with its description" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    for ([_]u64{ 11, 22 }, [_][]const u8{ "csv a.csv", "csv b.csv" }, 1..) |rows, text, branch| {
        p.scope = branch;
        try p.describe(4, .scan, text);
        var scan = op.Scan{ .src = undefined };
        scan.stats = .{ .at = 4, .rows = rows };
        _ = try p.absorb(.{ .scan = &scan });
    }
    try testing.expectEqual(@as(usize, 2), p.roots.items.len);
    try testing.expectEqual(@as(u64, 11), p.nodes.getPtr(p.roots.items[0]).?.total.rows);
    try testing.expectEqual(@as(u64, 22), p.nodes.getPtr(p.roots.items[1]).?.total.rows);
    try testing.expectEqualStrings("csv a.csv", p.detail(p.roots.items[0]));
    try testing.expectEqualStrings("csv b.csv", p.detail(p.roots.items[1]));
}

test "profile: work grafted under a node ran outside its timer and is not taken from its own time" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    const sch = @import("../lang/types.zig").Schema{ .fields = &.{} };
    var bscan = op.Scan{ .src = undefined };
    bscan.stats = .{ .at = 5, .ns = 100 };
    const build = try p.absorb(.{ .scan = &bscan });
    var pscan = op.Scan{ .src = undefined };
    pscan.stats = .{ .at = 1, .ns = 30 };
    var j = op.Join{ .probe = .{ .scan = &pscan }, .build = null, .left_keys = &.{}, .right_keys = &.{}, .left_schema = &sch, .right_schema = &sch, .out_schema = &sch, .kind = .inner, .state = testing.allocator };
    j.stats = .{ .at = 2, .ns = 50 };
    const lane = try p.absorb(.{ .join = &j });
    try p.graftBuild(2, build);
    try testing.expectEqual(@as(u64, 20), p.nodes.getPtr(lane).?.exclusive(&p));
    try testing.expectEqual(@as(u64, 20), p.nodes.getPtr(lane).?.laneExclusive(&p, 0));

    var merged = op.Scan{ .src = undefined };
    merged.stats = .{ .ns = 40 };
    var tail = op.Filter{ .child = .{ .scan = &merged }, .pred = undefined, .back = testing.allocator };
    tail.stats = .{ .at = 3, .ns = 60 };
    const top = try p.absorb(.{ .filter = &tail });
    try p.graftLeaf(top, lane);
    const leaf = p.nodes.getPtr(top).?.children[0].id;
    try testing.expectEqual(@as(u64, 40), p.nodes.getPtr(leaf).?.exclusive(&p));
}

test "profile: a lane path's shared build side and its tail hang under the operators they meet" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    const sch = @import("../lang/types.zig").Schema{ .fields = &.{} };

    var bscan = op.Scan{ .src = undefined };
    bscan.stats = .{ .at = 5, .rows = 4 };
    const build = try p.absorb(.{ .scan = &bscan });

    var pscan = op.Scan{ .src = undefined };
    pscan.stats = .{ .at = 1, .rows = 10 };
    var j = op.Join{ .probe = .{ .scan = &pscan }, .build = null, .left_keys = &.{}, .right_keys = &.{}, .left_schema = &sch, .right_schema = &sch, .out_schema = &sch, .kind = .inner, .state = testing.allocator };
    j.stats = .{ .at = 2, .rows = 6 };
    const lane = try p.absorb(.{ .join = &j });
    try p.graftBuild(2, build);

    var merged = op.Scan{ .src = undefined };
    var tail = op.Filter{ .child = .{ .scan = &merged }, .pred = undefined, .back = testing.allocator };
    tail.stats = .{ .at = 3, .rows = 2 };
    const top = try p.absorb(.{ .filter = &tail });
    try p.graftLeaf(top, lane);

    try testing.expectEqual(@as(usize, 1), p.roots.items.len);
    try testing.expect(std.meta.eql(top, p.roots.items[0]));
    const jn = p.nodes.getPtr(lane).?;
    try testing.expectEqual(@as(usize, 2), jn.children.len);
    try testing.expectEqual(Role.build, jn.children[1].role);
    try testing.expectEqual(@as(u64, 10), jn.rowsIn(&p));
}

test "profile: a path that does not pull its operators registers their shape and credits them" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    var scan = op.Scan{ .src = undefined };
    scan.stats = .{ .at = 1 };
    var f = op.Filter{ .child = .{ .scan = &scan }, .pred = undefined, .back = testing.allocator };
    f.stats = .{ .at = 2 };
    const root = try p.shape(.{ .filter = &f });
    var buf: [4]Id = undefined;
    const ids = p.chainIds(root, &buf);
    try testing.expectEqual(@as(usize, 2), ids.len);
    try p.credit(ids[1], .{ .ns = 30, .rows = 8, .calls = 1 });
    try p.credit(ids[0], .{ .ns = 50, .rows = 3, .calls = 1 });
    try testing.expectEqual(@as(u64, 20), p.nodes.getPtr(ids[0]).?.exclusive(&p));
    try testing.expectEqual(@as(u64, 8), p.nodes.getPtr(ids[0]).?.rowsIn(&p));
}

test "profile: chains' facts add up, but one every chain shares is counted once" {
    var list: std.ArrayListUnmanaged(Fact) = .empty;
    defer list.deinit(testing.allocator);
    for (0..3) |_| {
        try addFact(testing.allocator, &list, .{ .name = "sorted runs spilled", .value = 2 });
        try addFact(testing.allocator, &list, .{ .name = "build rows", .value = 81, .fold = .max });
    }
    try testing.expectEqual(@as(u64, 6), list.items[0].value);
    try testing.expectEqual(@as(u64, 81), list.items[1].value);
}

test "profile: a stage's text is its SQL" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var k = [_][]const u8{"n"};
    const keys = [_]ast.SortKey{.{ .field = .{ .parts = &k }, .desc = true, .nulls_first = true }};
    try testing.expectEqualStrings("5 by n DESC", try topNText(a, .{ .keys = &keys }, .{ .count = 5 }));
    var g = [_][]const u8{"pais"};
    const by = [_]ast.QualName{.{ .parts = &g }};
    const aggs = [_]ast.AggItem{.{ .name = "n", .func = .count, .arg = null }};
    const st = ast.Stage{ .node = .{ .aggregate = .{ .aggs = &aggs, .by = &by } }, .hints = &.{}, .pos = .{ .line = 1, .col = 1 } };
    try testing.expectEqualStrings("by pais; count(*)", try stageText(a, st));
}

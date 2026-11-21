//! A table function reading an http connection, called with each row's arguments
//! (`JOIN LATERAL f(args)`): the rows' arguments go out `batch` at a time,
//! `concurrency` requests in flight, through the runtime's `Caller`; each row gains a
//! list of the items its reply holds, typed as the function declares, which the
//! explode after it turns into the join's alias.
//!
//! Rows are cut into requests in input order, the last request alone short, and
//! answers are joined back in that order, so which rows share a request, and the
//! rows out, never depend on `-j` or timing. The requests out form a sliding window:
//! as soon as one comes back the next leaves, so a slow request holds back only the
//! rows behind it, never the other requests. Each request reserves its reply's place
//! when it leaves, which keeps the replies in request order whatever order they come
//! back in, and no more than `ahead` replies wait on rows not yet emitted, which bounds
//! what a stalled request makes the others hold. Rows are read ahead only to fill the
//! requests in flight; a batch of rows leaves once every one of its rows is answered.
//! No request goes out before the first row is asked for.
//!
//! In batch mode (`batch` > 0) a reply holds one item per row of its request, the
//! k-th answering the k-th; without it each row is its own request and every item of
//! its reply is the row's. `outer` keeps a row whose reply holds no item, its item
//! NULL.

const std = @import("std");
const types = @import("../../lang/types.zig");
const ast = @import("../../lang/ast.zig");
const Batch = @import("../batch.zig").Batch;
const column = @import("../column.zig");
const Column = column.Column;
const Stats = @import("../op.zig").Stats;
const Op = @import("../op.zig").Op;
const ErrCtx = @import("../op.zig").ErrCtx;
const Value = @import("../value.zig").Value;
const evalColumn = @import("../eval/vec.zig").evalColumn;
const nullColumn = @import("join.zig").nullColumn;

/// The runtime's side of a lookup: one request, given each parameter's values
/// (`args[p][r]` for row `r`), answered as its items, columns typed as the function
/// declares. Called from worker threads; `arena` is the call's own.
pub const Caller = struct {
    ptr: *anyopaque,
    call: *const fn (ptr: *anyopaque, arena: std.mem.Allocator, args: []const []const Value, rows: usize) anyerror!Batch,
};

/// Where a row's items are: rows `[lo, hi)` of reply `reply`.
const Span = struct { reply: usize, lo: usize, hi: usize };

/// Rows waiting for their answers: a batch from below, its arguments, and each
/// row's items once answered, all owned by `arena`.
const Pending = struct {
    batch: Batch,
    args: []const []const Value,
    spans: []?Span,
    answered: usize = 0,
    arena: std.heap.ArenaAllocator,
};

/// A reply owned by its request's arena, freed once its `left` rows are emitted.
const Reply = struct {
    batch: Batch,
    arena: std.heap.ArenaAllocator,
    left: usize,
};

pub const Lookup = struct {
    stats: Stats = .{},
    child: Op,
    args: []const *const ast.Expr,
    arg_types: []const types.Type,
    caller: Caller,
    batch: usize,
    concurrency: usize,
    outer: bool,
    item_schema: *const types.Schema,
    item_fields: []const types.Type.Field,
    list_ty: types.Type,
    out_schema: *const types.Schema,
    errctx: ?*ErrCtx = null,
    gpa: std.mem.Allocator,
    state: ?*State = null,

    /// Lives across pulls on `gpa`, since the driver resets the per-pull arena.
    const State = struct {
        pending: std.array_list.Managed(Pending),
        /// The next pending row not yet sent: its batch and row.
        at: [2]usize = .{ 0, 0 },
        done_below: bool = false,
        /// Pending batches already emitted, so a request's batch numbers, counted from
        /// the first batch ever pending, stay right as batches leave.
        emitted: usize = 0,
        /// Replies still holding unemitted rows, reserved as their requests leave; a
        /// span's `reply` is `base` plus its index.
        replies: std.array_list.Managed(Reply),
        base: usize = 0,
        /// Requests out and not yet filed, in the order they left.
        flights: std.array_list.Managed(*Flight),
        /// Guards each flight's `done`, and is signalled as one comes back.
        mu: std.Thread.Mutex = .{},
        cv: std.Thread.Condition = .{},
        /// A one-row NULL item, which a row with none takes under `outer`.
        none: []const Column = &.{},
        keep: std.heap.ArenaAllocator,
    };

    /// A request out: its rows (batch numbered as `State.emitted` counts, and row),
    /// arguments and answer, all owned by `arena`, which its reply takes over; `ri` is
    /// the reply it reserved as it left.
    const Flight = struct {
        rows: []const [2]usize = &.{},
        args: []const []const Value = &.{},
        arena: std.heap.ArenaAllocator,
        ri: usize,
        thread: ?std.Thread = null,
        reply: ?Batch = null,
        err: ?anyerror = null,
        done: bool = false,
    };

    pub fn next(self: *Lookup, arena: std.mem.Allocator) anyerror!?Batch {
        const st = self.state orelse blk: {
            const s = try self.gpa.create(State);
            s.* = .{ .pending = .init(self.gpa), .replies = .init(self.gpa), .flights = .init(self.gpa), .keep = .init(self.gpa) };
            self.state = s;
            const none = try s.keep.allocator().alloc(Column, self.item_schema.fields.len);
            for (none, self.item_schema.fields) |*c, f| c.* = try nullColumn(s.keep.allocator(), f.ty, 1);
            s.none = none;
            break :blk s;
        };
        while (true) {
            if (st.pending.items.len > 0 and st.pending.items[0].answered == st.pending.items[0].batch.len) {
                var p = st.pending.orderedRemove(0);
                defer p.arena.deinit();
                st.at[0] -|= 1;
                st.emitted += 1;
                return try self.emit(arena, st, &p);
            }
            if (st.done_below and self.unsent(st) == 0 and st.flights.items.len == 0) return null;
            try self.fill(arena, st);
            try self.dispatch(st);
            try self.collect(st);
        }
    }

    fn per(self: *const Lookup) usize {
        return if (self.batch > 0) self.batch else 1;
    }

    /// The replies that may wait on rows not yet emitted: past them no request leaves
    /// until the rows ahead are answered.
    fn ahead(self: *const Lookup) usize {
        return @max(self.concurrency, 1) * 4;
    }

    fn unsent(_: *const Lookup, st: *const State) usize {
        var n: usize = 0;
        for (st.pending.items, 0..) |p, i| {
            if (i < st.at[0]) continue;
            n += p.batch.len - (if (i == st.at[0]) st.at[1] else 0);
        }
        return n;
    }

    /// Batches from below until the window holds `concurrency` whole requests.
    fn fill(self: *Lookup, arena: std.mem.Allocator, st: *State) !void {
        const want = self.per() * self.concurrency;
        while (!st.done_below and self.unsent(st) < want) {
            const b = (try self.child.next(arena)) orelse {
                st.done_below = true;
                break;
            };
            if (b.len == 0) continue;
            var pa = std.heap.ArenaAllocator.init(self.gpa);
            errdefer pa.deinit();
            const own = pa.allocator();
            const owned = try b.deepCopy(own);
            const args = try own.alloc([]const Value, self.args.len);
            for (self.args, self.arg_types, args) |e, ty, *out| {
                const col = try evalColumn(own, e, owned, ty);
                const vals = try own.alloc(Value, owned.len);
                for (vals, 0..) |*v, r| v.* = col.getValue(r);
                out.* = vals;
            }
            const spans = try own.alloc(?Span, owned.len);
            @memset(spans, null);
            try st.pending.append(.{ .batch = owned, .args = args, .spans = spans, .arena = pa });
        }
    }

    pub fn release(self: *Lookup) void {
        const st = self.state orelse return;
        for (st.flights.items) |f| {
            if (f.thread) |t| t.join();
            f.arena.deinit();
            self.gpa.destroy(f);
        }
        st.flights.deinit();
        for (st.pending.items) |*p| p.arena.deinit();
        st.pending.deinit();
        for (st.replies.items) |*r| r.arena.deinit();
        st.replies.deinit();
        st.keep.deinit();
        self.gpa.destroy(st);
        self.state = null;
    }

    /// Sends whole requests (and a short last one once nothing is left below) while
    /// fewer than `concurrency` are out, each on a thread of its own, its reply's place
    /// reserved now so replies stay in order. Past `ahead` waiting replies only the
    /// first pending batch's rows go out, since that batch must be answered whole
    /// before anything is emitted.
    fn dispatch(self: *Lookup, st: *State) !void {
        const size = self.per();
        while (st.flights.items.len < self.concurrency and (st.at[0] == 0 or st.replies.items.len < self.ahead())) {
            const left = self.unsent(st);
            if (left == 0 or (left < size and !st.done_below)) return;
            try st.flights.ensureUnusedCapacity(1);
            try st.replies.ensureUnusedCapacity(1);
            const f = try self.gpa.create(Flight);
            f.* = .{ .arena = .init(std.heap.page_allocator), .ri = st.base + st.replies.items.len };
            errdefer {
                f.arena.deinit();
                self.gpa.destroy(f);
            }
            const fa = f.arena.allocator();
            var rows = std.array_list.Managed([2]usize).init(fa);
            while (rows.items.len < size and st.at[0] < st.pending.items.len) {
                try rows.append(.{ st.emitted + st.at[0], st.at[1] });
                st.at[1] += 1;
                if (st.at[1] == st.pending.items[st.at[0]].batch.len) st.at = .{ st.at[0] + 1, 0 };
            }
            const args = try fa.alloc([]const Value, self.args.len);
            for (args, 0..) |*a, p| {
                const vals = try fa.alloc(Value, rows.items.len);
                for (vals, rows.items) |*v, at| v.* = st.pending.items[at[0] - st.emitted].args[p][at[1]];
                a.* = vals;
            }
            f.rows = rows.items;
            f.args = args;
            st.replies.appendAssumeCapacity(.{ .batch = .{ .schema = self.item_schema, .columns = &.{}, .len = 0 }, .arena = .init(std.heap.page_allocator), .left = rows.items.len });
            st.flights.appendAssumeCapacity(f);
            f.thread = std.Thread.spawn(.{}, run, .{ self, st, f }) catch blk: {
                run(self, st, f);
                break :blk null;
            };
        }
    }

    /// Waits until a request out comes back, then files every one that has: its items
    /// under its rows, in the reply it reserved. A failed request fails the read; the
    /// ones still out are joined by `release`.
    fn collect(self: *Lookup, st: *State) !void {
        if (st.flights.items.len == 0) return;
        st.mu.lock();
        while (for (st.flights.items) |f| {
            if (f.done) break false;
        } else true) st.cv.wait(&st.mu);
        st.mu.unlock();
        var i: usize = 0;
        while (i < st.flights.items.len) {
            const f = st.flights.items[i];
            st.mu.lock();
            const done = f.done;
            st.mu.unlock();
            if (!done) {
                i += 1;
                continue;
            }
            _ = st.flights.orderedRemove(i);
            if (f.thread) |t| t.join();
            defer self.gpa.destroy(f);
            errdefer f.arena.deinit();
            if (f.err) |e| return e;
            const reply = f.reply.?;
            if (self.batch > 0 and reply.len != f.rows.len) {
                if (self.errctx) |c| c.set("the lookup's request of {d} rows got {d} items back; a batched function answers each input with one item", .{ f.rows.len, reply.len });
                return error.LookupItemCount;
            }
            const slot = &st.replies.items[f.ri - st.base];
            slot.arena.deinit();
            slot.* = .{ .batch = reply, .arena = f.arena, .left = f.rows.len };
            for (f.rows, 0..) |at, k| {
                const p = &st.pending.items[at[0] - st.emitted];
                p.spans[at[1]] = if (self.batch > 0) .{ .reply = f.ri, .lo = k, .hi = k + 1 } else .{ .reply = f.ri, .lo = 0, .hi = reply.len };
                p.answered += 1;
            }
        }
    }

    fn run(self: *Lookup, st: *State, f: *Flight) void {
        const reply: ?Batch = self.caller.call(self.caller.ptr, f.arena.allocator(), f.args, f.rows.len) catch |e| blk: {
            f.err = e;
            break :blk null;
        };
        st.mu.lock();
        defer st.mu.unlock();
        f.reply = reply;
        f.done = true;
        st.cv.signal();
    }

    /// `p`'s rows with their items as a list column, copied onto `arena`; replies
    /// whose rows are all emitted are freed.
    fn emit(self: *Lookup, arena: std.mem.Allocator, st: *State, p: *const Pending) !Batch {
        const n = p.batch.len;
        const Ref = struct { chunk: usize, row: usize };
        const offsets = try arena.alloc(i32, n + 1);
        offsets[0] = 0;
        var refs = std.array_list.Managed(Ref).init(arena);
        var nulls = std.array_list.Managed(usize).init(arena);
        var first: usize = std.math.maxInt(usize);
        var last: usize = 0;
        for (p.spans) |sp| {
            first = @min(first, sp.?.reply);
            last = @max(last, sp.?.reply);
        }
        const used = st.replies.items[first - st.base .. last - st.base + 1];
        const none_chunk = used.len;
        for (p.spans, 0..) |sp, r| {
            const s = sp.?;
            if (s.hi == s.lo and self.outer) {
                try nulls.append(refs.items.len);
                try refs.append(.{ .chunk = none_chunk, .row = 0 });
            }
            for (s.lo..s.hi) |i| try refs.append(.{ .chunk = s.reply - first, .row = i });
            offsets[r + 1] = @intCast(refs.items.len);
        }
        const kids = try arena.alloc(Column, self.item_schema.fields.len);
        const srcs = try arena.alloc(Column, none_chunk + 1);
        for (kids, 0..) |*k, f| {
            for (used, 0..) |rb, i| srcs[i] = rb.batch.columns[f];
            srcs[none_chunk] = st.none[f];
            k.* = try column.takeAcross(arena, srcs, refs.items);
        }
        const rows = try p.batch.deepCopy(arena);
        for (p.spans) |sp| st.replies.items[sp.?.reply - st.base].left -= 1;
        while (st.replies.items.len > 0 and st.replies.items[0].left == 0) {
            var done = st.replies.orderedRemove(0);
            done.arena.deinit();
            st.base += 1;
        }
        var vb = try column.Bitmap.initFull(arena, refs.items.len);
        for (nulls.items) |i| vb.setValid(i, false);
        const struct_ty = types.Type{ .kind = .@"struct", .fields = self.item_fields, .nullable = true };
        const st_node = try arena.create(column.Nested);
        st_node.* = .{ .ty = struct_ty, .body = .{ .fields = kids } };
        const child = Column.derived(struct_ty, refs.items.len, vb, .{ .nested = st_node });
        const list_node = try arena.create(column.Nested);
        list_node.* = .{ .ty = self.list_ty, .body = .{ .list = .{ .offsets = offsets, .child = child } } };
        const cols = try arena.alloc(Column, rows.columns.len + 1);
        @memcpy(cols[0..rows.columns.len], rows.columns);
        cols[cols.len - 1] = Column.derived(self.list_ty, n, try column.Bitmap.initFull(arena, n), .{ .nested = list_node });
        return .{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

const testing = std.testing;

/// A caller whose first request (row 0) comes back only once six others have, or
/// after three seconds; every other comes back at once. Each row's item is `id * 10`.
const SlowFirst = struct {
    finished: std.atomic.Value(u32) = .init(0),
    seen: std.atomic.Value(u32) = .init(0),
    six: std.Thread.ResetEvent = .{},
    items: *const types.Schema,

    fn call(ptr: *anyopaque, arena: std.mem.Allocator, args: []const []const Value, rows: usize) anyerror!Batch {
        const self: *SlowFirst = @ptrCast(@alignCast(ptr));
        const id = args[0][0].int;
        if (id == 0) {
            self.six.timedWait(3 * std.time.ns_per_s) catch {};
            self.seen.store(self.finished.load(.acquire), .release);
        }
        defer if (id != 0) {
            if (self.finished.fetchAdd(1, .acq_rel) + 1 == 6) self.six.set();
        };
        var b = column.Builder.init(arena, types.Type.init(.int).asNullable());
        for (0..rows) |r| try b.append(.{ .int = args[0][r].int * 10 });
        const cols = try arena.alloc(Column, 1);
        cols[0] = try b.finish();
        return .{ .schema = self.items, .columns = cols, .len = rows };
    }
};

test "lookup: requests go out as others come back, so a slow one holds back only its own rows, and rows leave in order with their own items" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const int_ty = types.Type.init(.int).asNullable();
    const in_schema = types.Schema{ .fields = &.{.{ .name = "id", .ty = int_ty }} };
    var batches: [2]Batch = undefined;
    for (&batches, 0..) |*bt, k| {
        var b = column.Builder.init(a, int_ty);
        for (0..6) |r| try b.append(.{ .int = @intCast(k * 6 + r) });
        const cols = try a.alloc(Column, 1);
        cols[0] = try b.finish();
        bt.* = .{ .schema = &in_schema, .columns = cols, .len = 6 };
    }
    var ts = @import("testing_util.zig").TestSource{ .schema_ = in_schema, .batches = &batches };
    var scan = @import("../op.zig").Scan{ .src = ts.src() };

    const tfields = [_]types.Type.Field{.{ .name = "v", .ty = int_ty }};
    const items = types.Schema{ .fields = &.{.{ .name = "v", .ty = int_ty }} };
    const st_ty = types.Type{ .kind = .@"struct", .fields = &tfields, .nullable = true };
    const list_ty = types.Type{ .kind = .array, .elem = &st_ty, .nullable = true };
    const out_schema = types.Schema{ .fields = &.{ .{ .name = "id", .ty = int_ty }, .{ .name = "e", .ty = list_ty } } };
    const id_expr = ast.Expr{ .field = .{ .parts = &.{"id"} } };
    var caller = SlowFirst{ .items = &items };
    var lk = Lookup{
        .child = .{ .scan = &scan },
        .args = &.{&id_expr},
        .arg_types = &.{int_ty},
        .caller = .{ .ptr = &caller, .call = SlowFirst.call },
        .batch = 0,
        .concurrency = 2,
        .outer = false,
        .item_schema = &items,
        .item_fields = &tfields,
        .list_ty = list_ty,
        .out_schema = &out_schema,
        .gpa = testing.allocator,
    };
    defer lk.release();

    var next_id: i64 = 0;
    while (try lk.next(a)) |b| {
        const vals = b.columns[1].data.nested.body.list.child.data.nested.body.fields[0];
        for (0..b.len) |r| {
            try testing.expectEqual(next_id, b.columns[0].getValue(r).int);
            try testing.expectEqual(next_id * 10, vals.getValue(r).int);
            next_id += 1;
        }
    }
    try testing.expectEqual(@as(i64, 12), next_id);
    try testing.expect(caller.seen.load(.acquire) >= 6);
}

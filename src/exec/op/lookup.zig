//! A table function reading an http connection, called with each row's arguments
//! (`JOIN LATERAL f(args)`): the rows' arguments go out `batch` at a time,
//! `concurrency` requests in flight, through the runtime's `Caller`; each row gains a
//! list of the items its reply holds, typed as the function declares, which the
//! explode after it turns into the join's alias.
//!
//! Rows are cut into requests in input order, the last request alone short, and
//! answers are joined back in that order, so which rows share a request, and the
//! rows out, never depend on `-j` or timing. Rows are read ahead only to fill the
//! requests in flight; a batch of rows leaves once every one of its rows is
//! answered. No request goes out before the first row is asked for.
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
/// row's items once answered.
const Pending = struct {
    batch: Batch,
    args: []const []const Value,
    spans: []?Span,
    answered: usize = 0,
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
    state: ?*State = null,

    const State = struct {
        pending: std.array_list.Managed(Pending),
        /// The next pending row not yet sent: its batch and row.
        at: [2]usize = .{ 0, 0 },
        done_below: bool = false,
        /// Every reply so far, its items' columns typed as declared.
        replies: std.array_list.Managed(Batch),
        /// A one-row NULL item, which a row with none takes under `outer`.
        none: []const Column,
    };

    pub fn next(self: *Lookup, arena: std.mem.Allocator) anyerror!?Batch {
        const st = self.state orelse blk: {
            const s = try arena.create(State);
            const none = try arena.alloc(Column, self.item_schema.fields.len);
            for (none, self.item_schema.fields) |*c, f| c.* = try nullColumn(arena, f.ty, 1);
            s.* = .{ .pending = .init(arena), .replies = .init(arena), .none = none };
            self.state = s;
            break :blk s;
        };
        while (true) {
            if (st.pending.items.len > 0 and st.pending.items[0].answered == st.pending.items[0].batch.len) {
                const p = st.pending.orderedRemove(0);
                st.at[0] -|= 1;
                return try self.emit(arena, st, p);
            }
            if (st.done_below and self.unsent(st) == 0) return null;
            try self.fill(arena, st);
            try self.send(arena, st);
        }
    }

    fn per(self: *const Lookup) usize {
        return if (self.batch > 0) self.batch else 1;
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
            const owned = try b.deepCopy(arena);
            const args = try arena.alloc([]const Value, self.args.len);
            for (self.args, self.arg_types, args) |e, ty, *out| {
                const col = try evalColumn(arena, e, owned, ty);
                const vals = try arena.alloc(Value, owned.len);
                for (vals, 0..) |*v, r| v.* = col.getValue(r);
                out.* = vals;
            }
            const spans = try arena.alloc(?Span, owned.len);
            @memset(spans, null);
            try st.pending.append(.{ .batch = owned, .args = args, .spans = spans });
        }
    }

    const Req = struct { rows: std.array_list.Managed([2]usize), args: []const []const Value, reply: ?Batch = null, err: ?anyerror = null, arena: std.heap.ArenaAllocator };

    /// The window's whole requests (and a short last one once nothing is left
    /// below) sent at once, and their items filed under their rows in order.
    fn send(self: *Lookup, arena: std.mem.Allocator, st: *State) !void {
        const size = self.per();
        var reqs = std.array_list.Managed(Req).init(arena);
        defer for (reqs.items) |*q| q.arena.deinit();
        while (reqs.items.len < self.concurrency) {
            const left = self.unsent(st);
            if (left == 0 or (left < size and !st.done_below)) break;
            var rows = std.array_list.Managed([2]usize).init(arena);
            while (rows.items.len < size and st.at[0] < st.pending.items.len) {
                try rows.append(st.at);
                st.at[1] += 1;
                if (st.at[1] == st.pending.items[st.at[0]].batch.len) st.at = .{ st.at[0] + 1, 0 };
            }
            const args = try arena.alloc([]const Value, self.args.len);
            for (args, 0..) |*a, p| {
                const vals = try arena.alloc(Value, rows.items.len);
                for (vals, rows.items) |*v, at| v.* = st.pending.items[at[0]].args[p][at[1]];
                a.* = vals;
            }
            try reqs.append(.{ .rows = rows, .args = args, .arena = .init(std.heap.page_allocator) });
        }
        if (reqs.items.len == 0) return;
        const threads = try arena.alloc(?std.Thread, reqs.items.len);
        for (reqs.items, threads) |*q, *t| t.* = std.Thread.spawn(.{}, run, .{ self, q }) catch blk: {
            run(self, q);
            break :blk null;
        };
        for (threads) |t| if (t) |th| th.join();
        for (reqs.items) |*q| {
            if (q.err) |e| return e;
            const reply = q.reply.?;
            if (self.batch > 0 and reply.len != q.rows.items.len) {
                if (self.errctx) |c| c.set("the lookup's request of {d} rows got {d} items back; a batched function answers each input with one item", .{ q.rows.items.len, reply.len });
                return error.LookupItemCount;
            }
            const ri = st.replies.items.len;
            try st.replies.append(try reply.deepCopy(arena));
            for (q.rows.items, 0..) |at, k| {
                const p = &st.pending.items[at[0]];
                p.spans[at[1]] = if (self.batch > 0) .{ .reply = ri, .lo = k, .hi = k + 1 } else .{ .reply = ri, .lo = 0, .hi = reply.len };
                p.answered += 1;
            }
        }
    }

    fn run(self: *Lookup, q: *Req) void {
        const reply = self.caller.call(self.caller.ptr, q.arena.allocator(), q.args, q.rows.items.len) catch |e| {
            q.err = e;
            return;
        };
        q.reply = reply;
    }

    /// `p`'s rows with their items as a list column.
    fn emit(self: *Lookup, arena: std.mem.Allocator, st: *State, p: Pending) !Batch {
        const n = p.batch.len;
        const Ref = struct { chunk: usize, row: usize };
        const offsets = try arena.alloc(i32, n + 1);
        offsets[0] = 0;
        var refs = std.array_list.Managed(Ref).init(arena);
        var nulls = std.array_list.Managed(usize).init(arena);
        const none_chunk = st.replies.items.len;
        for (p.spans, 0..) |sp, r| {
            const s = sp.?;
            if (s.hi == s.lo and self.outer) {
                try nulls.append(refs.items.len);
                try refs.append(.{ .chunk = none_chunk, .row = 0 });
            }
            for (s.lo..s.hi) |i| try refs.append(.{ .chunk = s.reply, .row = i });
            offsets[r + 1] = @intCast(refs.items.len);
        }
        const kids = try arena.alloc(Column, self.item_schema.fields.len);
        const srcs = try arena.alloc(Column, none_chunk + 1);
        for (kids, 0..) |*k, f| {
            for (st.replies.items, 0..) |rb, i| srcs[i] = rb.columns[f];
            srcs[none_chunk] = st.none[f];
            k.* = try column.takeAcross(arena, srcs, refs.items);
        }
        var vb = try column.Bitmap.initFull(arena, refs.items.len);
        for (nulls.items) |i| vb.setValid(i, false);
        const struct_ty = types.Type{ .kind = .@"struct", .fields = self.item_fields, .nullable = true };
        const st_node = try arena.create(column.Nested);
        st_node.* = .{ .ty = struct_ty, .body = .{ .fields = kids } };
        const child = Column.derived(struct_ty, refs.items.len, vb, .{ .nested = st_node });
        const list_node = try arena.create(column.Nested);
        list_node.* = .{ .ty = self.list_ty, .body = .{ .list = .{ .offsets = offsets, .child = child } } };
        const cols = try arena.alloc(Column, p.batch.columns.len + 1);
        @memcpy(cols[0..p.batch.columns.len], p.batch.columns);
        cols[cols.len - 1] = Column.derived(self.list_ty, n, try column.Bitmap.initFull(arena, n), .{ .nested = list_node });
        return .{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

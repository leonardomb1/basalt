//! UPSERT into a Delta table, copy-on-write: an incoming row whose key matches a
//! live row replaces it (under PARTIAL COLS only the listed columns change), one
//! whose key matches none is inserted, its missing columns NULL, and every file
//! holding a matched row is rewritten and removed in the same commit; a file
//! holding none is kept as it is. A NULL in a key column, or a key two incoming
//! rows share, fails the UPSERT, naming it. A live row whose key is NULL matches
//! nothing and stays.
//!
//! Under `USING changes` the rows are a change feed: of a key's rows the one of
//! the highest `sequence_by` wins and the others are dropped (`dropped`, by row
//! number, found by the same self-join that refuses repeated keys otherwise, and
//! left out of every later replay); two winners of one sequence fail the run. A
//! winner its `delete_when` marks removes the key's live row and inserts nothing.
//! The batch's largest position (`top`) is what the commit records; one behind
//! the position the table holds fails the run, as the query read applied changes.
//!
//! The incoming rows are spooled as they arrive, to the run's spill space (in
//! memory up to `--op-memory` when there is none), each numbered, while every key
//! column's smallest and largest value is kept. At `finish`:
//!   - the candidates are the live files whose partition values (for a key column
//!     that partitions the table) and statistics (`nullCount`, `minValues`,
//!     `maxValues`) may hold a key within those ranges; a file without them is
//!     one. Ranges are taken per column, so for a composite key this errs toward
//!     reading a file;
//!   - one join reads the candidates' key columns, and the spool's, against the
//!     spool's keys: a candidate a key matches is touched, and a spooled row
//!     matching another spooled row is a repeated key;
//!   - a FULL join streams the touched files' rows against the spool, each output
//!     row kept, merged or inserted, and written through the sink's partition
//!     routing, a partitioned table's rows sorted by partition first (spilling
//!     like any sort) so each partition gets one file; with no file touched the
//!     spool is inserted as it is.
//! On a table writing deletion vectors (`write.writesDeletionVectors`) the FULL
//! join's rows also carry their place in their file, and the rewrite merges on
//! read: a kept row is not written, a merged or deleted one is recorded at its
//! file's position, and each touched file's deletion vector (its old one with
//! those rows) is what the commit adds it again with (`vectors`); a file left
//! with no row is only removed.
//! Both are the hash join (`join.zig`) with the incoming rows as the build side,
//! spilling past `--op-memory` like any join, and the files are read a row group
//! at a time. So memory is bounded by `--op-memory` for each join and the sort, one row group per
//! file read, and the sink's open files; disk holds the spool and the join's spill.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Folder = @import("../parquet/folder.zig").Folder;
const Space = @import("../../exec/space.zig").Space;
const Value = @import("../../exec/value.zig").Value;
const cast = @import("../../exec/eval/cast.zig");
const ast = @import("../../lang/ast.zig");
const decimal = @import("../../exec/decimal.zig");
const column = @import("../../exec/column.zig");
const delta = @import("delta.zig");
const dlog = @import("delta_log.zig");
const dtypes = @import("delta_types.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const fit = @import("../parquet/fit.zig");
const dvs = @import("delta_dv.zig");
const roaring = @import("roaring.zig");
const vcol = @import("variant_col.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const join = @import("../../exec/op/join.zig");
const msgfmt = @import("../msgfmt.zig");
const op = @import("../../exec/op.zig");
const pqstats = @import("../parquet/stats.zig");
const spill = @import("../../exec/spill.zig");
const table = @import("table.zig");
const types = @import("../../lang/types.zig");
const write = @import("delta_write.zig");
const testing = std.testing;

pub const Error = error{ DeltaNullKey, DeltaDuplicateKey, DeltaUpsertTooLarge, DeltaChangeOrder };

pub const Opts = struct {
    space: ?Space = null,
    op_memory: usize = 2 << 30,
    threads: usize = 1,
    err: ?*op.ErrCtx = null,
    warn: ?Warn = null,
};

pub const Warn = struct { ctx: *anyopaque, f: *const fn (*anyopaque, []const u8) void };

/// The file tag a read of the table's files carries, a column no table can name.
const tag_name = "\x00file";
const row_name = "\x00row";
const pos_name = "\x00pos";

/// The span of an UPSERT's keys: per key column its name and type, whether it
/// partitions the table, and the smallest and largest incoming value. A key of a
/// 4-byte `float` column is held as its 4-byte value, and so is a partition value
/// or statistic read for it, which another writer may have written in fewer digits.
pub const Bounds = struct {
    names: []const []const u8,
    tys: []const types.Type,
    part: []const bool,
    lo: []Value,
    hi: []Value,
    float32: []const bool = &.{},

    fn narrowed(self: *const Bounds, j: usize, v: Value) Value {
        if (v != .float or j >= self.float32.len or !self.float32[j]) return v;
        return .{ .float = @as(f32, @floatCast(v.float)) };
    }

    fn widen(self: *Bounds, a: std.mem.Allocator, j: usize, v: Value) !void {
        if (v == .null) return;
        if (self.lo[j] == .null or before(v, self.lo[j])) self.lo[j] = try own(a, v);
        if (self.hi[j] == .null or before(self.hi[j], v)) self.hi[j] = try own(a, v);
    }

    /// Whether a file with these partition values and statistics may hold a key
    /// within the bounds: false only when some key column's values provably lie
    /// outside them (or are all NULL). A writer may leave NaN out of a float
    /// column's statistics, so a NaN key is never ruled out by them.
    pub fn mayHold(self: *const Bounds, arena: std.mem.Allocator, parts: ?std.json.Value, stats_text: ?[]const u8) !bool {
        const stats: ?std.json.ObjectMap = if (stats_text) |t| dtypes.parseStats(arena, t) else null;
        for (self.names, self.tys, self.part, self.lo, self.hi, 0..) |name, ty, is_part, lo, hi, j| {
            if (lo == .null) continue;
            if (is_part) {
                const pv = parts orelse continue;
                if (pv != .object) continue;
                const e = pv.object.get(name) orelse continue;
                if (e == .null) return false;
                if (e != .string) continue;
                const v = self.narrowed(j, (try table.partitionValue(arena, e.string, ty)) orelse continue);
                if (v == .null or before(v, lo) or before(hi, v)) return false;
                continue;
            }
            const s = stats orelse continue;
            if (dtypes.allNull(s, name)) return false;
            if (hi == .float and std.math.isNan(hi.float)) continue;
            if (try dtypes.statValue(arena, s, .max, name, ty)) |x| if (before(self.narrowed(j, x), lo)) return false;
            if (try dtypes.statValue(arena, s, .min, name, ty)) |x| if (before(hi, self.narrowed(j, x))) return false;
        }
        return true;
    }

    fn mayHoldAdd(ctx: *const anyopaque, arena: std.mem.Allocator, add: std.json.ObjectMap) anyerror!bool {
        const self: *const Bounds = @ptrCast(@alignCast(ctx));
        const st = if (add.get("stats")) |x| (if (x == .string) x.string else null) else null;
        return self.mayHold(arena, add.get("partitionValues"), st);
    }
};

fn before(a: Value, b: Value) bool {
    return (pqstats.cmp(a, b) orelse return false) == .lt;
}

fn own(a: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try a.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try a.dupe(u8, s) },
        else => v,
    };
}

/// The files an UPSERT rewrote and the ones it read, for its commit.
pub const Outcome = struct {
    removed: []const delta.Add,
    read_set: []const []const u8,
    readds: []const ReAdd = &.{},
};

pub const ReAdd = struct { add: delta.Add, rows: []const u64 };

pub const Merge = struct {
    gpa: std.mem.Allocator,
    mem: std.heap.ArenaAllocator,
    /// The plan of the incoming rows: `cols[k].src` their positions.
    plan: *const write.Plan,
    opts: Opts,
    /// The spool's columns: the incoming ones, typed as the table types them, then
    /// the row number.
    schema: types.Schema,
    bounds: Bounds,
    rows: u64 = 0,
    writer: ?spill.Writer = null,
    run: ?spill.Run = null,
    held: std.ArrayListUnmanaged(Batch) = .empty,
    held_bytes: usize = 0,
    /// Whether a join went to disk.
    spilled: bool = false,
    dropped: std.DynamicBitSetUnmanaged = .{},
    any_dropped: bool = false,
    top: ?i64 = null,
    feed: ?*write.Sink = null,
    dv: bool = false,
    hits: []std.ArrayListUnmanaged(u64) = &.{},

    pub fn init(gpa: std.mem.Allocator, p: *const write.Plan, opts: Opts) !*Merge {
        const self = try gpa.create(Merge);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .mem = std.heap.ArenaAllocator.init(gpa), .plan = p, .opts = opts, .schema = undefined, .bounds = undefined };
        errdefer self.mem.deinit();
        const a = self.mem.allocator();
        const fields = try a.alloc(types.Schema.Field, p.in_fields.len + 1);
        for (p.in_fields, fields[0..p.in_fields.len]) |f, *o| o.* = .{ .name = f.name, .ty = f.ty.asNullable() };
        for (p.cols) |c| if (c.src != write.absent) {
            fields[c.src].ty = c.carried().asNullable();
        };
        fields[p.in_fields.len] = .{ .name = row_name, .ty = types.Type.init(.int).asNullable() };
        self.schema = .{ .fields = fields };
        const nk = p.keys.len;
        const names = try a.alloc([]const u8, nk);
        const tys = try a.alloc(types.Type, nk);
        const part = try a.alloc(bool, nk);
        const lo = try a.alloc(Value, nk);
        const hi = try a.alloc(Value, nk);
        const f32s = try a.alloc(bool, nk);
        for (p.keys, names, tys, part, lo, hi, f32s) |k, *n, *t, *is_part, *l, *h, *f| {
            f.* = p.cols[k].phys.float32;
            n.* = p.cols[k].fileName();
            t.* = p.cols[k].ty;
            is_part.* = std.mem.indexOfScalar(usize, p.parts, k) != null;
            l.* = .null;
            h.* = .null;
        }
        self.bounds = .{ .names = names, .tys = tys, .part = part, .lo = lo, .hi = hi, .float32 = f32s };
        if (p.snap) |st| self.dv = write.writesDeletionVectors(st);
        return self;
    }

    pub fn deinit(self: *Merge) void {
        self.dropped.deinit(self.gpa);
        if (self.writer) |*w| w.abort();
        if (self.run) |r| spill.discard(r);
        self.mem.deinit();
        self.gpa.destroy(self);
    }

    /// The commit's test of a concurrent commit's added file.
    pub fn hold(self: *const Merge) dlog.Hold {
        return .{ .ctx = &self.bounds, .may = Bounds.mayHoldAdd };
    }

    fn fail(self: *Merge, e: anyerror, comptime fmt: []const u8, args: anytype) anyerror {
        @branchHint(.cold);
        return eval.explain(e, try msgfmt.allocPrint(self.mem.allocator(), fmt, args));
    }

    /// Spools one batch of incoming rows: a NULL key is refused, a value put as
    /// the table holds it (`normal`), and every row numbered.
    pub fn take(self: *Merge, b: Batch) !void {
        if (b.len == 0) return;
        const p = self.plan;
        for (p.keys) |k| {
            const c = b.columns[p.cols[k].src];
            for (0..b.len) |r| if (!c.validity.get(r))
                return self.fail(Error.DeltaNullKey, "column `{s}` is a key of this UPSERT into {s}, but a row holds NULL in it; a key matches rows only when it has a value", .{ p.cols[k].name, p.name });
        }
        var tmp = std.heap.ArenaAllocator.init(self.gpa);
        defer tmp.deinit();
        const t = tmp.allocator();
        const n_in = p.in_fields.len;
        const cols = try t.alloc(column.Column, n_in + 1);
        @memcpy(cols[0..n_in], b.columns[0..n_in]);
        for (p.cols) |c| if (c.src != write.absent) {
            cols[c.src] = try self.normal(t, b, c);
        };
        var bd = try column.Builder.initCapacity(t, types.Type.init(.int), b.len);
        for (0..b.len) |r| try bd.append(.{ .int = @intCast(self.rows + r) });
        cols[n_in] = try bd.finish();
        const a = self.mem.allocator();
        for (p.keys, 0..) |k, j| {
            const c = cols[p.cols[k].src];
            for (0..b.len) |r| try self.bounds.widen(a, j, c.getValue(r));
        }
        if (p.change) |ch| {
            const pc = b.columns[ch.pos];
            const sc = b.columns[ch.seq];
            for (0..b.len) |r| {
                if (!sc.validity.get(r)) return self.fail(Error.DeltaChangeOrder, "`sequence_by` of this UPSERT into {s} is NULL in a row; every change needs its place in the order", .{p.name});
                const v = pc.getValue(r);
                const x: i64 = switch (v) {
                    .int, .timestamp => |n| n,
                    .date => |d| d,
                    .null => return self.fail(Error.DeltaChangeOrder, "`position` of this UPSERT into {s} is NULL in a row; every change needs the position it was read at", .{p.name}),
                    else => unreachable,
                };
                if (self.top == null or x > self.top.?) self.top = x;
            }
        }
        const nb = Batch{ .schema = &self.schema, .columns = cols, .len = b.len };
        if (self.opts.space) |sp| {
            if (self.writer == null) self.writer = try spill.Writer.init(sp, a, &self.schema, "delta-upsert");
            try self.writer.?.write(nb);
        } else {
            self.held_bytes += nb.heapBytes();
            if (self.held_bytes > self.opts.op_memory)
                return self.fail(Error.DeltaUpsertTooLarge, "the rows of this UPSERT into {s} pass --op-memory ({d} bytes) and the run has no spill space to hold them", .{ p.name, self.opts.op_memory });
            try self.held.append(a, try nb.deepCopy(a));
        }
        self.rows += b.len;
    }

    /// Column `c` of the incoming batch as the table holds it: a decimal at the
    /// table's scale, a 4-byte `float` rounded to one, so keys match the rows read
    /// back and are told apart as the table tells them; a bare NULL as a NULL of
    /// the column's type.
    fn normal(self: *Merge, t: std.mem.Allocator, b: Batch, c: write.Col) !column.Column {
        const in = b.columns[c.src];
        if (self.plan.in_fields[c.src].ty.unknown) return join.nullColumn(t, c.carried(), b.len);
        if (c.variant_json) |jt| return vcol.convert(t, in, c.ty, jt, null, .raw) catch |e| {
            if (e == error.OutOfMemory) return e;
            const bad = vcol.failed_text;
            return self.fail(e, "column `{s}` holds a variant in {s}, but a row holds {s}, which is not JSON", .{ c.name, self.plan.name, bad[0..@min(bad.len, 60)] });
        };
        var out = in;
        out.ty = c.ty.asNullable();
        if (c.phys.float32 and in.data == .f64) {
            const xs = try t.alloc(f64, b.len);
            for (xs, in.data.f64[0..b.len]) |*o, x| o.* = @as(f32, @floatCast(x));
            out.data = .{ .f64 = xs };
            out.dict = null;
            return out;
        }
        if (c.ty.kind != .decimal) return out;
        const same = switch (column.decimals(in)) {
            .wide => |v| for (v[0..b.len], 0..) |d, r| {
                if (in.validity.get(r) and d.scale != c.ty.scale) break false;
            } else true,
            .narrow => |nw| nw.scale == c.ty.scale,
        };
        if (same) return out;
        var bd = try column.Builder.initCapacity(t, c.ty.asNullable(), b.len);
        for (0..b.len) |r| {
            const v = in.getValue(r);
            if (v != .decimal) {
                try bd.append(v);
                continue;
            }
            const d = decimal.rescaleTo(v.decimal, c.ty.scale) orelse
                return self.fail(write.Error.DeltaValueOutOfRange, "column `{s}` is `{s}` in {s}, which cannot hold a value of the rows' scale {d}", .{ c.name, c.delta_type, self.plan.name, v.decimal.scale });
            try bd.append(.{ .decimal = d });
        }
        return bd.finish();
    }

    /// Merges the spool into the table through `out`, returning the files to remove
    /// and the ones read.
    pub fn finish(self: *Merge, out: *write.Sink) !Outcome {
        if (self.writer) |*w| {
            self.run = try w.finish();
            self.writer = null;
        }
        const p = self.plan;
        const a = self.mem.allocator();
        if (out.feed) self.feed = out;
        if (p.change) |ch| if (ch.since) |was| if (self.top) |now| if (now < was)
            return self.fail(Error.DeltaChangeOrder, "the changes applied to {s} reach position {d}, behind the {d} it holds for `{s}`; the query read changes already applied (filter them with `WHERE <position> > $since`)", .{ p.name, now, was, ch.app });
        if (p.change != null) try self.dropped.resize(self.gpa, @intCast(self.rows), false);
        var cands = std.array_list.Managed(delta.Add).init(a);
        if (p.snap) |st| for (st.files.values()) |f| if (try self.bounds.mayHold(a, f.parts, f.stats)) try cands.append(f);
        const read_set = try a.alloc([]const u8, cands.items.len);
        for (read_set, cands.items) |*r, f| r.* = f.raw;
        const lay = if (cands.items.len > 0) try self.tagged(a, cands.items) else null;
        const hit = try self.touch(lay, cands.items.len);
        var touched = std.array_list.Managed(delta.Add).init(a);
        for (cands.items, hit) |f, h| if (h) try touched.append(f);
        self.hits = try a.alloc(std.ArrayListUnmanaged(u64), touched.items.len);
        for (self.hits) |*h| h.* = .empty;
        try self.rewrite(out, if (touched.items.len > 0) try self.tagged(a, touched.items) else null);
        if (!self.dv) return .{ .removed = touched.items, .read_set = read_set };
        return .{ .removed = touched.items, .read_set = read_set, .readds = try self.vectors(a, touched.items) };
    }

    /// Each touched file's rows deleted once this UPSERT commits: those its
    /// deletion vector deleted and those the rewrite matched or deleted, as a file
    /// to add again with that vector; a file left with no row is only removed.
    fn vectors(self: *Merge, a: std.mem.Allocator, touched: []const delta.Add) ![]const ReAdd {
        var out = std.array_list.Managed(ReAdd).init(a);
        for (touched, self.hits) |f, h| {
            var all = std.array_list.Managed(u64).init(a);
            try all.appendSlice(h.items);
            if (f.dv) |d| {
                const ref = dvs.Ref{ .name = self.plan.name, .root = self.plan.root, .file = f.path, .d = try dvs.descriptor(self.plan.name, f.path, d) };
                try all.appendSlice(try roaring.rowsIn(a, try dvs.load(a, &ref, std.math.maxInt(u64))));
            }
            std.mem.sort(u64, all.items, {}, std.sort.asc(u64));
            var n: usize = 0;
            for (all.items, 0..) |x, i| if (i == 0 or x != all.items[n - 1]) {
                all.items[n] = x;
                n += 1;
            };
            const rows = all.items[0..n];
            if (fileRows(a, f.stats)) |total| if (rows.len >= total) continue;
            try out.append(.{ .add = f, .rows = rows });
        }
        return out.items;
    }

    /// A layout over `adds` whose rows carry their file's position in `adds` as
    /// a last column, `tag_name`.
    fn tagged(self: *Merge, a: std.mem.Allocator, adds: []const delta.Add) !*const fit.Layout {
        const fl = try delta.layoutOf(a, self.plan.snap.?, self.plan.root, adds);
        const base = fl.layout;
        const nc = self.plan.cols.len;
        const nf = nc + self.rowCols();
        const fields = try a.alloc(types.Schema.Field, nf + 1 + @intFromBool(self.dv));
        for (fields[0..nc], self.plan.cols) |*f, c| f.* = .{ .name = c.name, .ty = c.carried().asNullable() };
        if (self.rowCols() == 2) {
            fields[nc] = .{ .name = delta.row_id_name, .ty = types.Type.init(.int).asNullable() };
            fields[nc + 1] = .{ .name = delta.row_version_name, .ty = types.Type.init(.int).asNullable() };
        }
        fields[nf] = .{ .name = tag_name, .ty = types.Type.init(.int).asNullable() };
        if (self.dv) fields[nf + 1] = .{ .name = pos_name, .ty = types.Type.init(.int).asNullable() };
        const part_fields = try a.alloc(usize, base.part_fields.len + 1);
        @memcpy(part_fields[0..base.part_fields.len], base.part_fields);
        part_fields[base.part_fields.len] = nf;
        const files = try a.alloc(fit.Layout.File, fl.files.len);
        for (files, fl.files, 0..) |*f, src, i| {
            const parts = try a.alloc(Value, src.parts.len + 1);
            @memcpy(parts[0..src.parts.len], src.parts);
            for (parts[0..src.parts.len], base.part_fields) |*v, fi| {
                const c = self.plan.cols[write.colIndex(self.plan.cols, base.schema.fields[fi].name) orelse continue];
                if (c.phys.float32 and v.* == .float) v.* = .{ .float = @as(f32, @floatCast(v.float)) };
            }
            parts[src.parts.len] = .{ .int = @intCast(i) };
            f.* = .{ .path = src.path, .size = src.size, .parts = parts, .deletes = src.deletes, .row_base = src.row_base, .row_version = src.row_version };
        }
        const out = try a.create(fit.Layout);
        out.* = .{ .name = base.name, .root = base.root, .schema = .{ .fields = fields }, .part_fields = part_fields, .files = files, .mapping = base.mapping, .variant_types = base.variant_types, .raw_variants = true, .pos_field = if (self.dv) nf + 1 else null };
        if (self.rowCols() == 2) {
            const st = self.plan.snap.?;
            out.rows = .{
                .id = nc,
                .version = nc + 1,
                .mat_id = st.property("delta.rowTracking.materializedRowIdColumnName") orelse "",
                .mat_version = st.property("delta.rowTracking.materializedRowCommitVersionColumnName") orelse "",
            };
        }
        return out;
    }

    /// The hidden row-tracking columns the rewrite carries: 2 where the table keeps
    /// row ids, else 0.
    fn rowCols(self: *const Merge) usize {
        const r = self.plan.row orelse return 0;
        return if (r.keep) 2 else 0;
    }

    /// The spool replayed, each batch fitted by `f` when given.
    fn replay(self: *Merge, a: std.mem.Allocator, f: ?*const fit.Fit) !*Replay {
        const r = try a.create(Replay);
        r.* = .{ .m = self, .fit = f, .schema = if (f) |x| x.schema.* else self.schema };
        if (self.run) |run| r.reader = try spill.Reader.open(run);
        return r;
    }

    fn fitOf(a: std.mem.Allocator, fields: []const types.Schema.Field, outs: []const fit.Fit.Out) !*const fit.Fit {
        const s = try a.create(types.Schema);
        s.* = .{ .fields = fields };
        const f = try a.create(fit.Fit);
        f.* = .{ .schema = s, .outs = outs };
        return f;
    }

    /// Which of the `n` candidates hold an incoming key, refusing a key two
    /// incoming rows share: the candidates' keys and the spool's, each row with its
    /// file (-1 for the spool) and number, joined against the spool's keys.
    fn touch(self: *Merge, lay: ?*const fit.Layout, n: usize) ![]bool {
        const p = self.plan;
        const hit = try self.mem.allocator().alloc(bool, n);
        @memset(hit, false);
        var pass = std.heap.ArenaAllocator.init(self.gpa);
        defer pass.deinit();
        const a = pass.allocator();
        const nk = p.keys.len;
        const rn = p.in_fields.len;
        const int = types.Type.init(.int).asNullable();
        const ns: usize = @intFromBool(p.change != null);

        const pf = try a.alloc(types.Schema.Field, nk + 2 + ns);
        const spool_outs = try a.alloc(fit.Fit.Out, nk + 2 + ns);
        const bf = try a.alloc(types.Schema.Field, nk + 1 + ns);
        const build_outs = try a.alloc(fit.Fit.Out, nk + 1 + ns);
        const names = try a.alloc([]const u8, nk + 1);
        for (p.keys, 0..) |k, j| {
            const c = p.cols[k];
            pf[j] = .{ .name = c.name, .ty = c.ty.asNullable() };
            bf[j] = pf[j];
            spool_outs[j] = .{ .column = c.src };
            build_outs[j] = .{ .column = c.src };
            names[j] = c.name;
        }
        pf[nk] = .{ .name = tag_name, .ty = int };
        pf[nk + 1] = .{ .name = row_name, .ty = int };
        bf[nk] = .{ .name = row_name, .ty = int };
        spool_outs[nk] = .{ .value = .{ .int = -1 } };
        spool_outs[nk + 1] = .{ .column = rn };
        build_outs[nk] = .{ .column = rn };
        names[nk] = tag_name;
        if (p.change) |ch| {
            const st = p.in_fields[ch.seq].ty.asNullable();
            pf[nk + 2] = .{ .name = ast.Changes.seq_col, .ty = st };
            bf[nk + 1] = pf[nk + 2];
            spool_outs[nk + 2] = .{ .column = ch.seq };
            build_outs[nk + 1] = .{ .column = ch.seq };
        }

        var parts = std.array_list.Managed(Feed.Part).init(a);
        const spooled = try self.replay(a, try fitOf(a, pf, spool_outs));
        try parts.append(.{ .src = spooled.source() });
        if (lay) |l| {
            const folder = try Folder.openLayout(a, l, names);
            const file_outs = try a.alloc(fit.Fit.Out, nk + 2 + ns);
            for (names, 0..) |nm, j| file_outs[j] = .{ .column = folder.schema.indexOf(nm).? };
            file_outs[nk + 1] = .{ .value = .null };
            if (ns == 1) file_outs[nk + 2] = .{ .value = .null };
            try parts.append(.{ .src = folder.source(), .fit = try fitOf(a, pf, file_outs) });
        }
        const feed = try a.create(Feed);
        feed.* = .{ .parts = parts.items, .schema = .{ .fields = pf } };
        defer feed.close();
        const build = try self.replay(a, try fitOf(a, bf, build_outs));
        defer build.close();

        const lk = try a.alloc(usize, nk);
        for (lk, 0..) |*x, j| x.* = j;
        const jn = try self.joinOf(a, feed, build, .inner, lk, lk);
        defer jn.release();
        var bm = std.heap.ArenaAllocator.init(self.gpa);
        defer bm.deinit();
        const top = op.Op{ .join = jn };
        var ties = std.array_list.Managed(struct { a: usize, b: usize, msg: []const u8 }).init(a);
        while (try top.next(bm.allocator())) |b| {
            const tags = b.columns[nk];
            for (0..b.len) |r| {
                const tag = tags.getValue(r).int;
                if (tag >= 0) {
                    hit[@intCast(tag)] = true;
                    continue;
                }
                const lw = nk + 2 + ns;
                const mine = b.columns[nk + 1].getValue(r).int;
                const other = b.columns[lw + nk].getValue(r).int;
                if (mine == other) continue;
                var vals = std.array_list.Managed(u8).init(a);
                for (0..nk) |j| {
                    if (j > 0) try vals.appendSlice(", ");
                    try vals.appendSlice(try fmtnum.valueToString(a, b.columns[j].getValue(r)));
                }
                if (ns == 1) {
                    const sm = b.columns[nk + 2].getValue(r);
                    const so = b.columns[lw + nk + 1].getValue(r);
                    const o = eval.compareValues(sm, so) orelse .eq;
                    if (o == .lt) {
                        self.dropped.set(@intCast(mine));
                        continue;
                    }
                    if (o == .gt) continue;
                    try ties.append(.{ .a = @intCast(mine), .b = @intCast(other), .msg = try msgfmt.allocPrint(a, "the changes hold key ({s}) = ({s}) twice at sequence {s}; `sequence_by` must order a key's changes, or which applies would depend on the run", .{ try std.mem.join(a, ", ", names[0..nk]), vals.items, try fmtnum.valueToString(a, sm) }) });
                    continue;
                }
                return self.fail(Error.DeltaDuplicateKey, "the rows hold key ({s}) = ({s}) more than once; an UPSERT into {s} takes one row per key, as a primary key does", .{ try std.mem.join(a, ", ", names[0..nk]), vals.items, p.name });
            }
            _ = bm.reset(.retain_capacity);
        }
        self.spilled = self.spilled or jn.grace != null;
        for (ties.items) |t| if (!self.dropped.isSet(t.a) and !self.dropped.isSet(t.b))
            return self.fail(Error.DeltaChangeOrder, "{s}", .{t.msg});
        if (p.change != null) self.any_dropped = self.dropped.count() > 0;
        return hit;
    }

    /// The touched files' rows FULL-joined against the spool (`Merged`), written
    /// through `out`; with none touched, the spool inserted. A partitioned table's
    /// rows are sorted by partition first, spilling past `--op-memory` as any sort
    /// does, so each partition is written to one file however the join (a spilled
    /// one by hash partition) ordered them.
    fn rewrite(self: *Merge, out: *write.Sink, lay: ?*const fit.Layout) !void {
        const p = self.plan;
        var pass = std.heap.ArenaAllocator.init(self.gpa);
        defer pass.deinit();
        const a = pass.allocator();
        var bm = std.heap.ArenaAllocator.init(self.gpa);
        defer bm.deinit();
        const shape = try a.create(types.Schema);
        const fed: usize = @intFromBool(self.feed != null);
        const tf = try a.alloc(types.Schema.Field, p.cols.len + self.rowCols() + fed);
        for (tf[0..p.cols.len], p.cols) |*f, c| f.* = .{ .name = c.name, .ty = c.carried().asNullable() };
        if (self.rowCols() == 2) {
            tf[p.cols.len] = .{ .name = delta.row_id_name, .ty = types.Type.init(.int).asNullable() };
            tf[p.cols.len + 1] = .{ .name = delta.row_version_name, .ty = types.Type.init(.int).asNullable() };
        }
        if (fed == 1) tf[tf.len - 1] = .{ .name = fit.change_type_name, .ty = types.Type.init(.string).asNullable() };
        shape.* = .{ .fields = tf };

        const build = try self.replay(a, null);
        defer build.close();
        var feed: ?*Feed = null;
        defer if (feed) |f| f.close();
        var jn: ?*op.Join = null;
        defer if (jn) |j| j.release();
        if (lay) |l| {
            const folder = try Folder.openLayout(a, l, null);
            const fd = try a.create(Feed);
            const parts = try a.alloc(Feed.Part, 1);
            parts[0] = .{ .src = folder.source() };
            fd.* = .{ .parts = parts, .schema = folder.schema };
            feed = fd;
            const rk = try a.alloc(usize, p.keys.len);
            for (rk, p.keys) |*x, k| x.* = p.cols[k].src;
            jn = try self.joinOf(a, fd, build, .full, p.keys, rk);
        }
        const merged = try a.create(Merged);
        merged.* = .{ .m = self, .schema = shape, .join = jn, .build = build };
        const scan = try a.create(op.Scan);
        scan.* = .{ .src = merged.source() };
        var top = op.Op{ .scan = scan };
        var sort: ?*op.Sort = null;
        defer if (sort) |s| s.release();
        if (p.parts.len > 0) if (self.opts.space) |sp| {
            const keys = try a.alloc(op.Sort.Key, p.parts.len);
            for (keys, p.parts) |*k, i| k.* = .{ .idx = i, .desc = false, .nulls_first = false };
            const s = try a.create(op.Sort);
            s.* = .{ .child = top, .in_schema = shape, .keys = keys, .threads = self.opts.threads, .space = sp, .spill_at = self.opts.op_memory, .gpa = self.gpa };
            sort = s;
            top = .{ .sort = s };
        };
        while (try top.next(bm.allocator())) |b| {
            if (self.feed != null) try split(bm.allocator(), out, b) else try out.route(bm.allocator(), b);
            _ = bm.reset(.retain_capacity);
        }
        if (jn) |j| self.spilled = self.spilled or j.grace != null;
        if (sort) |s| self.spilled = self.spilled or s.runs > 0;
    }

    /// One batch of the FULL join as the table's rows: the kept rows, then the
    /// merged ones (under PARTIAL COLS only the listed columns from the incoming
    /// row), then the inserted ones.
    fn combine(self: *Merge, t: std.mem.Allocator, shape: *const types.Schema, b: Batch) !Batch {
        const p = self.plan;
        const nc = p.cols.len;
        const nr = self.rowCols();
        const tag_at = nc + nr;
        const nt = tag_at + 1 + @intFromBool(self.dv);
        const rn = nt + p.in_fields.len;
        const masks = [3][]bool{ try t.alloc(bool, b.len), try t.alloc(bool, b.len), try t.alloc(bool, b.len) };
        var counts = [3]usize{ 0, 0, 0 };
        const gone = try t.alloc(bool, b.len);
        var n_gone: usize = 0;
        const del: ?column.Column = if (p.change) |ch| b.columns[nt + ch.del] else null;
        for (0..b.len) |r| {
            const exists = b.columns[tag_at].validity.get(r);
            var class: usize = if (!exists) 2 else if (b.columns[rn].validity.get(r)) 1 else 0;
            if (class != 0) if (del) |d| if (deletes(d, r)) {
                class = 3;
            };
            if (self.dv and exists) {
                if (class == 0) {
                    class = 4;
                } else try self.hits[@intCast(b.columns[tag_at].getValue(r).int)].append(self.mem.allocator(), @intCast(b.columns[tag_at + 1].getValue(r).int));
            }
            for (masks, 0..) |m, i| m[r] = i == class;
            if (class < 3) counts[class] += 1;
            gone[r] = class == 3 and exists;
            n_gone += @intFromBool(gone[r]);
        }
        const kept = counts[0] + counts[1] + counts[2];
        const cols = try t.alloc(column.Column, nc + nr);
        var chunks: [3]column.Column = undefined;
        if (nr == 2) {
            const int_ty = types.Type.init(.int).asNullable();
            const none = try join.nullColumn(t, int_ty, b.len);
            const sources = [2][3]column.Column{
                .{ b.columns[nc], b.columns[nc], none },
                .{ b.columns[nc + 1], none, none },
            };
            for (sources, cols[nc..]) |from, *c| {
                var n: usize = 0;
                for (from, masks, counts) |src, m, cnt| if (cnt > 0) {
                    chunks[n] = try column.gather(t, src, m, cnt);
                    n += 1;
                };
                c.* = if (n == 0) try join.nullColumn(t, int_ty, 0) else if (n == 1) chunks[0] else try column.concat(t, chunks[0..n], kept);
            }
        }
        for (cols[0..nc], p.cols, 0..) |*c, pc, k| {
            const fresh = if (p.partial) |pl| std.mem.indexOfScalar(usize, pl, k) != null else true;
            const incoming = if (pc.src == write.absent) try join.nullColumn(t, pc.carried(), b.len) else b.columns[nt + pc.src];
            const from = [3]column.Column{ b.columns[k], if (fresh) incoming else b.columns[k], incoming };
            var n: usize = 0;
            for (from, masks, counts) |src, m, cnt| if (cnt > 0) {
                chunks[n] = try column.gather(t, src, m, cnt);
                n += 1;
            };
            c.* = if (n == 0) try join.nullColumn(t, pc.carried(), 0) else if (n == 1) chunks[0] else try column.concat(t, chunks[0..n], kept);
        }
        if (self.feed == null) return .{ .schema = shape, .columns = cols, .len = kept };
        const ch = try self.changes(t, b, masks, counts, gone, n_gone);
        const all = try t.alloc(column.Column, nc + nr + 1);
        for (all[0 .. nc + nr], cols, ch.columns[0 .. nc + nr]) |*o, d, c| o.* = try column.concat(t, &.{ d, c }, kept + ch.len);
        const none = try join.nullColumn(t, types.Type.init(.string).asNullable(), kept);
        all[nc + nr] = try column.concat(t, &.{ none, ch.columns[nc + nr] }, kept + ch.len);
        return .{ .schema = shape, .columns = all, .len = kept + ch.len };
    }

    /// One batch of the rewrite, with the change rows its last column marks, as
    /// the table's rows and its change data rows, each to its own files.
    fn split(t: std.mem.Allocator, out: *write.Sink, b: Batch) !void {
        const kind = b.columns[b.columns.len - 1];
        const is_change = try t.alloc(bool, b.len);
        const is_row = try t.alloc(bool, b.len);
        var n: usize = 0;
        for (is_change, is_row, 0..) |*c, *r, i| {
            c.* = kind.validity.get(i);
            r.* = !c.*;
            n += @intFromBool(c.*);
        }
        for ([_]struct { keep: []bool, n: usize, changes: bool }{ .{ .keep = is_row, .n = b.len - n, .changes = false }, .{ .keep = is_change, .n = n, .changes = true } }) |part| {
            if (part.n == 0) continue;
            const cols = try t.alloc(column.Column, b.columns.len);
            try column.gatherColumns(t, b.columns, cols, part.keep, part.n);
            const sub = Batch{ .schema = b.schema, .columns = cols, .len = part.n };
            if (part.changes) try out.routeChanges(t, sub) else try out.route(t, sub);
        }
    }

    /// The change rows of one batch of the FULL join, in the rewrite's shape with
    /// their `_change_type` last: a merged row's old values (`update_preimage`)
    /// and new ones (`update_postimage`), an inserted row, and a live row a change
    /// deletes.
    fn changes(self: *Merge, t: std.mem.Allocator, b: Batch, masks: [3][]bool, counts: [3]usize, gone: []bool, n_gone: usize) !Batch {
        const p = self.plan;
        const nc = p.cols.len;
        const nr = self.rowCols();
        const nt = nc + nr + 1 + @intFromBool(self.dv);
        const Part = struct { mask: []bool, n: usize, kind: []const u8, side: enum { live, merged, incoming } };
        const parts = [_]Part{
            .{ .mask = masks[1], .n = counts[1], .kind = "update_preimage", .side = .live },
            .{ .mask = masks[1], .n = counts[1], .kind = "update_postimage", .side = .merged },
            .{ .mask = masks[2], .n = counts[2], .kind = "insert", .side = .incoming },
            .{ .mask = gone, .n = n_gone, .kind = "delete", .side = .live },
        };
        var total: usize = 0;
        for (parts) |part| total += part.n;
        const cols = try t.alloc(column.Column, nc + nr + 1);
        var chunks: [parts.len]column.Column = undefined;
        for (cols[0..nc], p.cols, 0..) |*c, pc, k| {
            const fresh = if (p.partial) |pl| std.mem.indexOfScalar(usize, pl, k) != null else true;
            const incoming = if (pc.src == write.absent) try join.nullColumn(t, pc.carried(), b.len) else b.columns[nt + pc.src];
            for (parts, &chunks) |part, *o| o.* = try column.gather(t, switch (part.side) {
                .live => b.columns[k],
                .merged => if (fresh) incoming else b.columns[k],
                .incoming => incoming,
            }, part.mask, part.n);
            c.* = try column.concat(t, &chunks, total);
        }
        for (cols[nc .. nc + nr]) |*c| c.* = try join.nullColumn(t, types.Type.init(.int).asNullable(), total);
        var bd = try column.Builder.initCapacity(t, types.Type.init(.string).asNullable(), total);
        for (parts) |part| try bd.appendRepeated(.{ .string = part.kind }, part.n);
        cols[nc + nr] = try bd.finish();
        return .{ .schema = b.schema, .columns = cols, .len = total };
    }

    /// Spooled rows `b` as the table's rows to insert, a column they lack as NULL;
    /// a change that deletes inserts nothing.
    fn inserts(self: *Merge, t: std.mem.Allocator, shape: *const types.Schema, b_in: Batch) !Batch {
        var b = b_in;
        if (self.plan.change) |ch| {
            const d = b.columns[ch.del];
            const keep = try t.alloc(bool, b.len);
            var n: usize = 0;
            for (keep, 0..) |*k, r| {
                k.* = !deletes(d, r);
                n += @intFromBool(k.*);
            }
            if (n < b.len) {
                const cols = try t.alloc(column.Column, b.columns.len);
                try column.gatherColumns(t, b.columns, cols, keep, n);
                b = .{ .schema = b.schema, .columns = cols, .len = n };
            }
        }
        const nc = self.plan.cols.len;
        const nr = self.rowCols();
        const cols = try t.alloc(column.Column, nc + nr + @intFromBool(self.feed != null));
        for (cols[0..nc], self.plan.cols) |*c, pc| c.* = if (pc.src == write.absent) try join.nullColumn(t, pc.carried(), b.len) else b.columns[pc.src];
        for (cols[nc .. nc + nr]) |*c| c.* = try join.nullColumn(t, types.Type.init(.int).asNullable(), b.len);
        if (self.feed != null) cols[nc + nr] = try join.nullColumn(t, types.Type.init(.string).asNullable(), b.len);
        return .{ .schema = shape, .columns = cols, .len = b.len };
    }

    /// Spooled rows `b` less the changes a later one of their key replaces.
    fn winners(self: *Merge, t: std.mem.Allocator, b: Batch) !Batch {
        const rc = b.columns[self.plan.in_fields.len];
        const keep = try t.alloc(bool, b.len);
        var n: usize = 0;
        for (keep, 0..) |*k, r| {
            k.* = !self.dropped.isSet(@intCast(rc.getValue(r).int));
            n += @intFromBool(k.*);
        }
        if (n == b.len) return b;
        const cols = try t.alloc(column.Column, b.columns.len);
        try column.gatherColumns(t, b.columns, cols, keep, n);
        return .{ .schema = b.schema, .columns = cols, .len = n };
    }

    /// A hash join of `probe` against `build` (the spool), spilling as the run's
    /// joins do.
    fn joinOf(self: *Merge, a: std.mem.Allocator, probe: *Feed, build: *Replay, kind: @import("../../lang/ast.zig").JoinKind, lk: []const usize, rk: []const usize) !*op.Join {
        const lscan = try a.create(op.Scan);
        lscan.* = .{ .src = probe.source() };
        const rscan = try a.create(op.Scan);
        rscan.* = .{ .src = build.source() };
        const ls = try a.create(types.Schema);
        ls.* = probe.schema;
        const rs = try a.create(types.Schema);
        rs.* = build.schema;
        const of = try a.alloc(types.Schema.Field, ls.fields.len + rs.fields.len);
        for (of[0..ls.fields.len], ls.fields) |*o, f| o.* = .{ .name = f.name, .ty = f.ty.asNullable() };
        for (of[ls.fields.len..], rs.fields) |*o, f| o.* = .{ .name = f.name, .ty = f.ty.asNullable() };
        const os = try a.create(types.Schema);
        os.* = .{ .fields = of };
        const jn = try a.create(op.Join);
        jn.* = .{
            .probe = .{ .scan = lscan },
            .build = .{ .scan = rscan },
            .left_keys = lk,
            .right_keys = rk,
            .left_schema = ls,
            .right_schema = rs,
            .out_schema = os,
            .kind = kind,
            .state = a,
            .err = self.opts.err,
            .threads = self.opts.threads,
            .space = self.opts.space,
            .spill_at = self.opts.op_memory,
        };
        return jn;
    }
};

/// A file's row count by its statistics (`numRecords`), null when they lack it.
fn fileRows(a: std.mem.Allocator, stats: ?[]const u8) ?u64 {
    const s = dtypes.parseStats(a, stats orelse return null) orelse return null;
    return switch (s.get("numRecords") orelse return null) {
        .integer => |x| std.math.cast(u64, x),
        .number_string => |t| std.fmt.parseInt(u64, t, 10) catch null,
        else => null,
    };
}

/// Whether row `r` of a change's `delete_when` column deletes its key.
fn deletes(d: column.Column, r: usize) bool {
    const v = d.getValue(r);
    return v == .bool and v.bool;
}

/// The rewrite's rows in the table's shape, a batch per batch of the join
/// (`Merge.combine`); with no join, every spooled row inserted.
const Merged = struct {
    m: *Merge,
    schema: *const types.Schema,
    join: ?*op.Join,
    build: *Replay,

    pub fn next(self: *Merged, arena: std.mem.Allocator) !?Batch {
        const jn = self.join orelse {
            while (try self.build.next(arena)) |b| if (b.len > 0) return try self.m.inserts(arena, self.schema, b);
            return null;
        };
        const top = op.Op{ .join = jn };
        while (try top.next(arena)) |b| if (b.len > 0) return try self.m.combine(arena, self.schema, b);
        return null;
    }

    pub fn close(_: *Merged) void {}

    fn source(self: *Merged) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sourceVTable(Merged);
};

/// The spool read back, from its spill file or memory.
const Replay = struct {
    m: *Merge,
    fit: ?*const fit.Fit,
    schema: types.Schema,
    reader: ?spill.Reader = null,
    i: usize = 0,

    pub fn next(self: *Replay, arena: std.mem.Allocator) !?Batch {
        var b = if (self.reader) |*r| (try r.next(arena)) orelse return null else blk: {
            if (self.i >= self.m.held.items.len) return null;
            self.i += 1;
            break :blk self.m.held.items[self.i - 1];
        };
        if (self.m.any_dropped) b = try self.m.winners(arena, b);
        if (self.fit) |f| return try f.apply(arena, b);
        var out = b;
        out.schema = &self.schema;
        return out;
    }

    pub fn close(self: *Replay) void {
        if (self.reader) |*r| r.close();
        self.reader = null;
    }

    fn source(self: *Replay) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sourceVTable(Replay);
};

/// Sources read one after another, each batch fitted to one schema.
const Feed = struct {
    parts: []const Part,
    schema: types.Schema,
    i: usize = 0,

    const Part = struct { src: driver.Source, fit: ?*const fit.Fit = null };

    pub fn next(self: *Feed, arena: std.mem.Allocator) !?Batch {
        while (self.i < self.parts.len) {
            const p = self.parts[self.i];
            const b = (try p.src.next(arena)) orelse {
                self.i += 1;
                continue;
            };
            var out = if (p.fit) |f| try f.apply(arena, b) else b;
            out.schema = &self.schema;
            return out;
        }
        return null;
    }

    pub fn close(self: *Feed) void {
        for (self.parts) |p| p.src.close();
    }

    fn source(self: *Feed) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sourceVTable(Feed);
};

test "bounds: partition values and statistics rule a file out only when provably outside" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var lo = [_]Value{ .{ .int = 10 }, .{ .string = "eu" } };
    var hi = [_]Value{ .{ .int = 20 }, .{ .string = "us" } };
    const b = Bounds{
        .names = &.{ "id", "region" },
        .tys = &.{ types.Type.init(.int), types.Type.init(.string) },
        .part = &.{ false, true },
        .lo = &lo,
        .hi = &hi,
    };
    const P = struct {
        fn parts(al: std.mem.Allocator, text: []const u8) !std.json.Value {
            return std.json.parseFromSliceLeaky(std.json.Value, al, text, .{});
        }
    };
    const eu = try P.parts(a, "{\"region\":\"eu\"}");
    try testing.expect(try b.mayHold(a, eu, null));
    try testing.expect(try b.mayHold(a, eu, "{\"numRecords\":3,\"minValues\":{\"id\":15},\"maxValues\":{\"id\":30},\"nullCount\":{\"id\":0}}"));
    try testing.expect(!try b.mayHold(a, eu, "{\"numRecords\":3,\"minValues\":{\"id\":21},\"maxValues\":{\"id\":30},\"nullCount\":{\"id\":0}}"));
    try testing.expect(!try b.mayHold(a, eu, "{\"numRecords\":3,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":0}}"));
    try testing.expect(!try b.mayHold(a, eu, "{\"numRecords\":3,\"nullCount\":{\"id\":3}}"));
    try testing.expect(try b.mayHold(a, eu, "{\"numRecords\":3,\"minValues\":{\"id\":1}}"));
    try testing.expect(try b.mayHold(a, eu, "not json"));
    try testing.expect(!try b.mayHold(a, try P.parts(a, "{\"region\":\"zz\"}"), null));
    try testing.expect(!try b.mayHold(a, try P.parts(a, "{\"region\":null}"), null));
    try testing.expect(try b.mayHold(a, try P.parts(a, "{}"), null));

    var tlo = [_]Value{.{ .timestamp = 1_000_500 }};
    var thi = [_]Value{.{ .timestamp = 1_000_900 }};
    var slo = [_]Value{.{ .string = "m" }};
    var shi = [_]Value{.{ .string = "n" }};
    const ts = Bounds{ .names = &.{"t"}, .tys = &.{types.Type.init(.timestamp)}, .part = &.{false}, .lo = &tlo, .hi = &thi };
    try testing.expect(try ts.mayHold(a, null, "{\"maxValues\":{\"t\":\"1970-01-01T00:00:01.000Z\"}}"));
    try testing.expect(!try ts.mayHold(a, null, "{\"maxValues\":{\"t\":\"1970-01-01T00:00:00.999Z\"}}"));
    var flo = [_]Value{.{ .float = @as(f32, 0.1) }};
    var fhi = [_]Value{.{ .float = @as(f32, 0.1) }};
    const fl = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{false}, .lo = &flo, .hi = &fhi, .float32 = &.{true} };
    try testing.expect(try fl.mayHold(a, null, "{\"minValues\":{\"f\":0.1},\"maxValues\":{\"f\":0.1}}"));
    const fp = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{true}, .lo = &flo, .hi = &fhi, .float32 = &.{true} };
    try testing.expect(try fp.mayHold(a, try P.parts(a, "{\"f\":\"0.1\"}"), null));
    const fd = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{false}, .lo = &flo, .hi = &fhi };
    try testing.expect(!try fd.mayHold(a, null, "{\"maxValues\":{\"f\":0.1}}"));
    const str = Bounds{ .names = &.{"s"}, .tys = &.{types.Type.init(.string)}, .part = &.{false}, .lo = &slo, .hi = &shi };
    try testing.expect(!try str.mayHold(a, null, "{\"maxValues\":{\"s\":\"abc\"}}"));
    try testing.expect(try str.mayHold(a, null, "{\"maxValues\":{\"s\":\"" ++ "a" ** 32 ++ "\"}}"));
}

test "bounds: a NaN key is never ruled out by statistics a writer may have left NaN out of" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nan = std.math.nan(f64);
    const stats = "{\"numRecords\":2,\"minValues\":{\"f\":1.0},\"maxValues\":{\"f\":1.0},\"nullCount\":{\"f\":0}}";
    var lo = [_]Value{.{ .float = nan }};
    var hi = [_]Value{.{ .float = nan }};
    const all_nan = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{false}, .lo = &lo, .hi = &hi };
    try testing.expect(try all_nan.mayHold(a, null, stats));
    var lo5 = [_]Value{.{ .float = 5 }};
    const some_nan = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{false}, .lo = &lo5, .hi = &hi };
    try testing.expect(try some_nan.mayHold(a, null, stats));
    try testing.expect(!try some_nan.mayHold(a, null, "{\"numRecords\":2,\"nullCount\":{\"f\":2}}"));
    var hi5 = [_]Value{.{ .float = 5 }};
    const no_nan = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{false}, .lo = &lo5, .hi = &hi5 };
    try testing.expect(!try no_nan.mayHold(a, null, stats));
    const part = Bounds{ .names = &.{"f"}, .tys = &.{types.Type.init(.float)}, .part = &.{true}, .lo = &lo5, .hi = &hi };
    try testing.expect(!try part.mayHold(a, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"f\":\"1.0\"}", .{}), null));
    try testing.expect(try part.mayHold(a, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"f\":\"NaN\"}", .{}), null));
}

const TestRows = struct {
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "v", .ty = types.Type.init(.string) },
    } };

    fn batch(a: std.mem.Allocator, ids: []const i64, vs: []const []const u8) !Batch {
        var bi = try column.Builder.initCapacity(a, types.Type.init(.int), ids.len);
        var bs = try column.Builder.initCapacity(a, types.Type.init(.string), vs.len);
        for (ids, vs) |i, v| {
            try bi.append(.{ .int = i });
            try bs.append(.{ .string = v });
        }
        const cols = try a.alloc(column.Column, 2);
        cols[0] = try bi.finish();
        cols[1] = try bs.finish();
        return .{ .schema = &schema, .columns = cols, .len = ids.len };
    }

    fn open(a: std.mem.Allocator, root: []const u8, mode: write.Mode) !*write.Sink {
        return openWith(a, root, mode, .{});
    }

    fn openWith(a: std.mem.Allocator, root: []const u8, mode: write.Mode, opts: Opts) !*write.Sink {
        const p = try write.plan(a, root, mode, true, null, .snappy, schema);
        return write.Sink.open(testing.allocator, a, p, opts);
    }

    /// The table's rows: their count and the sum of their ids.
    fn tally(a: std.mem.Allocator, root: []const u8) ![2]i64 {
        const r = try delta.resolve(a, root, .{}, null);
        const f = try Folder.openLayout(a, r.layout, &.{"id"});
        defer f.close();
        var out = [2]i64{ 0, 0 };
        while (try f.next(a)) |b| for (0..b.len) |i| {
            out[0] += 1;
            out[1] += b.columns[0].getValue(i).int;
        };
        return out;
    }

    fn load(a: std.mem.Allocator, root: []const u8, mode: write.Mode, ids: []const i64, vs: []const []const u8) !void {
        const s = try open(a, root, mode);
        s.writeBatch(a, try batch(a, ids, vs)) catch |e| {
            s.abort();
            return e;
        };
        try s.close();
    }
};

test "UPSERT racing a concurrent writer: an append of a key it writes, or a REPLACE, fails it; a disjoint append does not" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const root = try msgfmt.allocPrint(a, "{s}/", .{try tmp.dir.realpathAlloc(a, ".")});
    const up: write.Mode = .{ .upsert = .{ .keys = &.{"id"} } };
    try TestRows.load(a, root, .append, &.{ 1, 2 }, &.{ "a", "b" });

    const s1 = try TestRows.open(a, root, up);
    try s1.writeBatch(a, try TestRows.batch(a, &.{1}, &.{"z"}));
    try TestRows.load(a, root, .append, &.{1}, &.{"dup"});
    try testing.expectError(dlog.Error.DeltaCommitConflict, s1.close());
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(dlog.Error.DeltaCommitConflict).?, "version 1, committed concurrently, added `part-") != null);

    const s2 = try TestRows.open(a, root, up);
    try s2.writeBatch(a, try TestRows.batch(a, &.{2}, &.{"z"}));
    try TestRows.load(a, root, .append, &.{50}, &.{"far"});
    try s2.close();
    try testing.expectEqual(@as(?u64, 3), s2.committed);
    const st = try delta.snapshot(a, root, .{}, null);
    try testing.expectEqual(@as(u64, 3), st.version);
    try testing.expectEqual(@as(usize, 3), st.files.count());

    const s3 = try TestRows.open(a, root, up);
    try s3.writeBatch(a, try TestRows.batch(a, &.{2}, &.{"y"}));
    try TestRows.load(a, root, .replace, &.{9}, &.{"r"});
    try testing.expectError(dlog.Error.DeltaCommitConflict, s3.close());
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(dlog.Error.DeltaCommitConflict).?, "which this write read") != null);
}

test "UPSERT past its memory: the spool and both joins go to disk, the rows are a roomy run's, and the spill is cleared" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const root = try msgfmt.allocPrint(a, "{s}/", .{try tmp.dir.realpathAlloc(a, ".")});
    var ids: [600]i64 = undefined;
    var vs: [600][]const u8 = undefined;
    for (&ids, &vs, 0..) |*i, *v, k| {
        i.* = @intCast(k);
        v.* = "old";
    }
    try TestRows.load(a, root, .append, ids[0..300], vs[0..300]);
    for (vs[200..500]) |*v| v.* = "new";
    const td = try spill.TestDir.init(1 << 30);
    defer td.deinit();
    const s = try TestRows.openWith(a, root, .{ .upsert = .{ .keys = &.{"id"} } }, .{ .space = td.ds.space(), .op_memory = 1 });
    for (0..3) |c| try s.writeBatch(a, try TestRows.batch(a, ids[200 + c * 100 .. 300 + c * 100], vs[200 + c * 100 .. 300 + c * 100]));
    try testing.expect(s.merging.?.writer != null);
    try s.close();
    try testing.expect(s.spilled);
    try testing.expectEqual(@as(usize, 0), try td.count());
    const t = try TestRows.tally(a, root);
    try testing.expectEqual(@as(i64, 500), t[0]);
    try testing.expectEqual(@as(i64, 499 * 500 / 2), t[1]);
}

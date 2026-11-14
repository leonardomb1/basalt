//! A PostgreSQL table read `WITH (changes_from = <position>, slot = '<slot>')` as
//! its change feed, by logical decoding: the same columns SQL Server's feed reads
//! (`changes.zig`), the table's own and `_change_type`, `_commit_version`,
//! `_commit_timestamp`, `_change_seq`, for `UPSERT ... USING changes` to apply.
//!
//! Setup is the user's, as SQL Server's `sp_cdc_enable_table` is: a publication
//! carrying the table (`CREATE PUBLICATION p FOR TABLE t`), found from the table or
//! picked by `publication`, and a `pgoutput` slot named by `slot`
//! (`SELECT pg_create_logical_replication_slot('s', 'pgoutput')`); a missing one
//! fails naming the statement. The one thing basalt does to the source is advance
//! the slot to the position its target holds, so WAL is not kept forever.
//!
//! The stream is read over SQL (`pg_logical_slot_peek_binary_changes`), never
//! consumed, up to the WAL end fixed as the statement starts, so a rerun reads the
//! same commits; those at or before the position are skipped. The `pgoutput`
//! messages are decoded here (`Decoder`) and their text values typed as the
//! PostgreSQL reader types them. A run decodes its range once, into memory, and the
//! read becomes those rows (`materialize`), so no WHERE or projection is pushed into
//! SQL that never runs. A position is a commit's LSN as 16 uppercase hex digits.
//!
//! With no position (a feed's first run, or a gap) the table is read whole, as SQL,
//! as inserts at the WAL end. A gap is a slot invalidated or moved past the
//! position, or a TRUNCATE of the table in the range, whose deleted keys only a
//! snapshot can tell.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const column = @import("../../exec/column.zig");
const cell = @import("../../exec/cell.zig");
const sql = @import("../../db/sql.zig");
const split = @import("split.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const pgType = @import("../../db/postgres.zig").pgType;
const hex = @import("../../base/hex.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const SqlConnInfo = @import("../connect.zig").SqlConnInfo;
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const catalogQuery = @import("../plan/estimate.zig").catalogQuery;
const planErr = @import("../env.zig").planErr;
const hasFlagHint = @import("../env.zig").hasFlagHint;

/// The microseconds from 1970 to PostgreSQL's epoch, 2000-01-01.
const pg_epoch_us: i64 = 946_684_800_000_000;

pub const change_cols = [_][]const u8{ "_change_type", "_commit_version", "_commit_timestamp", "_change_seq" };

/// What a statement learnt of a table's feed, asked of the server once.
pub const Meta = struct {
    cols: []const types.Schema.Field,
    schema_name: []const u8,
    publication: []const u8,
    slot: []const u8,
    confirmed: ?u64,
    lost: bool,
    end: u64,
};

/// A range decoded: the rows as one batch; whether the table was truncated in it,
/// and whether an update left a TOASTed value unsent (`unsent`), either a gap only a
/// snapshot fills.
pub const Feed = struct { schema: types.Schema, batch: Batch, truncated: bool, unsent: bool };

pub const Entry = struct { key: []const u8, meta: Meta, from: ?u64 = null, feed: ?Feed = null };

/// `lsn` as 16 uppercase hex digits, which order as LSNs do.
pub fn hex16(a: std.mem.Allocator, lsn: u64) ![]const u8 {
    return msgfmt.allocPrint(a, "{X:0>16}", .{lsn});
}

/// `lsn` as PostgreSQL writes it, `hi/lo` in hex.
fn pgLsn(a: std.mem.Allocator, lsn: u64) ![]const u8 {
    return msgfmt.allocPrint(a, "{X}/{X}", .{ lsn >> 32, lsn & 0xffff_ffff });
}

/// A position as text: 16 hex digits, as `_commit_version` reads, or `hi/lo` as
/// PostgreSQL writes an LSN; null when empty.
pub fn parseLsn(text: []const u8) error{InvalidLsn}!?u64 {
    const t = std.mem.trim(u8, text, " ");
    if (t.len == 0) return null;
    if (std.mem.indexOfScalar(u8, t, '/')) |slash| {
        const hi = hex.int(u32, t[0..slash]) orelse return error.InvalidLsn;
        const lo = hex.int(u32, t[slash + 1 ..]) orelse return error.InvalidLsn;
        return (@as(u64, hi) << 32) | lo;
    }
    if (t.len != 16) return error.InvalidLsn;
    return hex.int(u64, t) orelse error.InvalidLsn;
}

fn hint(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| if (std.mem.eql(u8, h.key, key)) return switch (h.value) {
        .str, .ident => |s| s,
        else => null,
    };
    return null;
}

/// The position option `key` names, resolved through the run's parameters.
fn positionOf(env: *Env, hints: []const ast.Hint, key: []const u8) !?u64 {
    for (hints) |h| if (std.mem.eql(u8, h.key, key)) {
        const text: []const u8 = switch (h.value) {
            .str => |s| s,
            .ident => |s| if (std.mem.startsWith(u8, s, "$")) blk: {
                const v = env.params.get(s[1..]) orelse return null;
                if (v.isNull()) return null;
                break :blk try fmtnum.valueToString(env.arena, v);
            } else s,
            .flag => return null,
            .int => return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s}` on a PostgreSQL table is an LSN as text, 16 hex digits as `_commit_version` reads it (`0000000001A4E3B8`) or `0/1A4E3B8`", .{key})),
        };
        return parseLsn(text) catch planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s} = '{s}'` is not an LSN: it takes 16 hex digits, as `_commit_version` reads it, or `0/1A4E3B8`", .{ key, text }));
    };
    return null;
}

fn label(env: *Env, rd: ast.Read) ![]const u8 {
    return std.mem.join(env.arena, ".", rd.form.table.parts);
}

/// The statement's entry for `rd`'s feed, its meta asked of the server the first time.
fn entryOf(env: *Env, rd: ast.Read, hints: []const ast.Hint, conn: ast.Connection, info: SqlConnInfo) !*Entry {
    const a = env.arena;
    const key = try std.mem.concat(a, u8, &.{ rd.connector, "\x00", try label(env, rd) });
    for (env.pg_feeds.items) |*e| if (std.mem.eql(u8, e.key, key)) return e;
    const parts = rd.form.table.parts;
    const obj = try split.catalogObjectLit(a, .postgres, parts);
    const name = try label(env, rd);

    const crows = try askRows(env, conn, info, try msgfmt.allocPrint(a, "SELECT a.attname::text, a.atttypid::int, a.atttypmod::int FROM pg_attribute a WHERE a.attrelid = to_regclass({s}) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", .{obj}));
    if (crows.len == 0) return planErr(env.diag, try msgfmt.allocPrint(a, "{s} is not a PostgreSQL table basalt can see", .{name}));
    const cols = try a.alloc(types.Schema.Field, crows.len);
    for (crows, cols) |r, *c| c.* = .{ .name = r[0], .ty = pgType(try std.fmt.parseInt(i32, r[1], 10), try std.fmt.parseInt(i32, r[2], 10)) };
    const nrows = try askRows(env, conn, info, try msgfmt.allocPrint(a, "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass({s})", .{obj}));

    const table_lit = try split.catalogNameLit(a, .postgres, parts[parts.len - 1]);
    const schema_lit = if (parts.len > 1) try split.catalogNameLit(a, .postgres, parts[parts.len - 2]) else "current_schema()";
    const prows = try askRows(env, conn, info, try msgfmt.allocPrint(a, "SELECT p.pubname::text FROM pg_publication_tables p WHERE p.schemaname = {s} AND p.tablename = {s} ORDER BY 1", .{ schema_lit, table_lit }));
    const asked_pub = hint(hints, "publication");
    var publication: ?[]const u8 = null;
    for (prows) |r| {
        if (asked_pub) |p| {
            if (std.mem.eql(u8, p, r[0])) publication = r[0];
        } else if (publication == null) publication = r[0];
    }
    if (publication == null) {
        if (asked_pub) |p| return planErr(env.diag, try msgfmt.allocPrint(a, "publication `{s}` does not carry {s}; add it with `ALTER PUBLICATION {s} ADD TABLE {s};`", .{ p, name, p, name }));
        return planErr(env.diag, try msgfmt.allocPrint(a, "{s} is in no publication, so logical decoding sends none of its changes; create one with `CREATE PUBLICATION basalt_pub FOR TABLE {s};`", .{ name, name }));
    }
    if (asked_pub == null and prows.len > 1) return planErr(env.diag, try msgfmt.allocPrint(a, "{s} is in {d} publications; name the one to read with `publication = '...'`", .{ name, prows.len }));

    const slot = hint(hints, "slot") orelse
        return planErr(env.diag, try msgfmt.allocPrint(a, "a PostgreSQL change feed reads a replication slot: name it with `slot = '...'`, created once with `SELECT pg_create_logical_replication_slot('basalt_{s}', 'pgoutput');`", .{parts[parts.len - 1]}));
    const slot_lit = try split.catalogNameLit(a, .postgres, slot);
    const end_rows = try askRows(env, conn, info, "SELECT pg_current_wal_lsn()::text");
    const end = (parseLsn(end_rows[0][0]) catch null) orelse 0;
    const srows = try askRows(env, conn, info, try msgfmt.allocPrint(a, "SELECT plugin::text, coalesce(wal_status::text, ''), coalesce(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_name = {s}", .{slot_lit}));
    if (srows.len == 0) return planErr(env.diag, try msgfmt.allocPrint(a, "replication slot `{s}` does not exist; create it with `SELECT pg_create_logical_replication_slot('{s}', 'pgoutput');` (a role with REPLICATION)", .{ slot, slot }));
    if (!std.mem.eql(u8, srows[0][0], "pgoutput")) return planErr(env.diag, try msgfmt.allocPrint(a, "replication slot `{s}` decodes with `{s}`; basalt reads `pgoutput` slots", .{ slot, srows[0][0] }));

    try env.pg_feeds.append(a, .{ .key = key, .meta = .{
        .cols = cols,
        .schema_name = nrows[0][0],
        .publication = publication.?,
        .slot = slot,
        .confirmed = parseLsn(srows[0][2]) catch null,
        .lost = std.mem.eql(u8, srows[0][1], "lost"),
        .end = end,
    } });
    return &env.pg_feeds.items[env.pg_feeds.items.len - 1];
}

/// `rd` as the SQL that reads the table whole as inserts at the WAL end: the feed's
/// first run, and the shape a schema probe sees (the decoded rows match it).
pub fn read(env: *Env, rd: ast.Read, hints: []const ast.Hint, conn: ast.Connection, info: SqlConnInfo) !ast.Read {
    const e = try entryOf(env, rd, hints, conn, info);
    if (try positionOf(env, hints, "changes_from") != null and !std.mem.eql(u8, rd.where, "1 = 0"))
        return planErr(env.diag, "a PostgreSQL change feed is read as a statement's FROM (or the CTE it starts with), not as a joined table");
    var out = rd;
    out.cols = &.{};
    out.form = .{ .query = try msgfmt.allocPrint(env.arena, "SELECT t.*, CAST('insert' AS VARCHAR(16)) AS _change_type, CAST('{s}' AS CHAR(16)) AS _commit_version, CAST(now() AS TIMESTAMP) AS _commit_timestamp, ROW_NUMBER() OVER () AS _change_seq FROM {s} t", .{ try hex16(env.arena, e.meta.end), try sql.quoteParts(env.arena, .postgres, rd.form.table.parts) }) };
    return out;
}

/// Whether the feed `rd` reads no longer reaches back to `since`: its slot was
/// invalidated or moved past it, or the table was truncated after it. A slot behind
/// `since` is advanced to it first, the target holding `since` for good.
pub fn cleanedPast(env: *Env, rd: ast.Read, hints: []const ast.Hint, conn: ast.Connection, info: SqlConnInfo, since: []const u8) !bool {
    const from = (parseLsn(since) catch return false) orelse return false;
    const e = try entryOf(env, rd, hints, conn, info);
    if (e.meta.lost) return true;
    if (e.meta.confirmed) |c| {
        if (c > from) return true;
        if (c < from) {
            _ = try askRows(env, conn, info, try msgfmt.allocPrint(env.arena, "SELECT pg_replication_slot_advance({s}, '{s}')::text", .{ try split.catalogNameLit(env.arena, .postgres, e.meta.slot), try pgLsn(env.arena, from) }));
            e.meta.confirmed = from;
        }
    }
    const f = try decode(env, e, conn, info, rd, from);
    if (f.unsent) env.log.log(.warn, "an update of {s} left a large (TOASTed) value unchanged, which logical decoding does not send unless the table logs whole rows; `ALTER TABLE {s} REPLICA IDENTITY FULL;` saves the snapshot this costs", .{ try label(env, rd), try label(env, rd) });
    if (f.truncated) env.log.log(.warn, "{s} was truncated after {s}, which deletes keys no change names", .{ try label(env, rd), since });
    return f.truncated or f.unsent;
}

/// The range after `from` decoded, once per statement.
fn decode(env: *Env, e: *Entry, conn: ast.Connection, info: SqlConnInfo, rd: ast.Read, from: u64) !Feed {
    if (e.feed) |f| if (e.from == from) return f;
    const a = env.arena;
    const parts = rd.form.table.parts;
    var d = try Decoder.init(a, e.meta.cols, e.meta.schema_name, parts[parts.len - 1], from);
    const q = try msgfmt.allocPrint(a, "SELECT encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes({s}, '{s}', NULL, 'proto_version', '1', 'publication_names', {s})", .{ try split.catalogNameLit(a, .postgres, e.meta.slot), try pgLsn(a, e.meta.end), try split.catalogNameLit(a, .postgres, e.meta.publication) });
    const Ctx = struct { d: *Decoder, env: *Env };
    catalogQuery(env, conn, info, q, Ctx{ .d = &d, .env = env }, struct {
        fn rows(cx: Ctx, b: anytype) !void {
            for (0..b.len) |r| {
                const text = try fmtnum.valueToString(cx.d.arena, b.columns[0].getValue(r));
                const bytes = try cx.d.arena.alloc(u8, text.len / 2);
                try cx.d.message(hex.decode(bytes, text) catch return error.CorruptChangeStream);
            }
        }
    }.rows) catch |err| return switch (err) {
        error.CorruptChangeStream, error.UnparseableNumber => planErr(env.diag, try msgfmt.allocPrint(a, "the change stream of {s} from slot `{s}` could not be decoded ({s})", .{ try label(env, rd), e.meta.slot, @errorName(err) })),
        else => err,
    };
    const f = try d.finish();
    e.feed = f;
    e.from = from;
    return f;
}

/// `stages` with a PostgreSQL feed read at its head (or at the head of the CTE it
/// starts with) replaced by its decoded rows in memory, when it has a position.
pub fn materialize(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    if (stages.len == 0) return stages;
    const st = stages[0];
    if (st.node == .ref) {
        const b = env.bindings.getPtr(st.node.ref) orelse return stages;
        const inner = try materialize(env, b.stages);
        if (inner.ptr != b.stages.ptr) b.stages = inner;
        return stages;
    }
    if (st.node != .read) return stages;
    const rd = st.node.read;
    if (rd.form != .table or !hasFlagHint(st.hints, "changes_from")) return stages;
    const conn = env.connections.get(rd.connector) orelse return stages;
    const info = sqlConnInfo(conn) orelse return stages;
    if (info.dialect != .postgres) return stages;
    const from = (try positionOf(env, st.hints, "changes_from")) orelse return stages;
    if (try positionOf(env, st.hints, "changes_to")) |_| return planErr(env.diag, "`changes_to` is not read on a PostgreSQL feed: it reads up to the WAL end the statement starts at");
    const e = try entryOf(env, rd, st.hints, conn, info);
    const f = try decode(env, e, conn, info, rd, from);
    if (f.truncated) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} was truncated in the range this read covers, which no change row can say; read it under `UPSERT ... USING changes`, which reads the table whole then", .{try label(env, rd)}));
    if (f.unsent) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "an update of {s} in the range left a large (TOASTed) value unchanged, which logical decoding sends only when the table logs whole rows: `ALTER TABLE {s} REPLICA IDENTITY FULL;` (under `UPSERT ... USING changes` such a range is read whole instead)", .{ try label(env, rd), try label(env, rd) }));
    const name = try msgfmt.allocPrint(env.arena, "\x00pg_feed{d}", .{env.pg_feed_seq});
    env.pg_feed_seq += 1;
    const batches = try env.arena.dupe(Batch, &.{f.batch});
    try env.materialized.put(env.arena, name, .{ .schema = f.schema, .batches = batches });
    const out = try env.arena.dupe(ast.Stage, stages);
    out[0] = .{ .node = .{ .ref = name }, .hints = &.{}, .pos = st.pos };
    return out;
}

/// The rows of catalog query `q`, each cell as text, NULL as empty.
fn askRows(env: *Env, conn: ast.Connection, info: SqlConnInfo, q: []const u8) ![]const []const []const u8 {
    return @import("changes.zig").askRows(env, conn, info, q);
}

/// `pgoutput` (protocol 1) messages decoded into the read table's change rows. A
/// transaction's changes take its commit LSN (Begin's final LSN) and time; those
/// committed at or before `from` are skipped; only the table's relation is kept.
pub const Decoder = struct {
    arena: std.mem.Allocator,
    cols: []const types.Schema.Field,
    schema_name: []const u8,
    table: []const u8,
    from: u64,
    builders: []column.Builder,
    convs: []cell.Converter,
    relid: ?u32 = null,
    /// For the table's relation, each of its columns' place among `cols`.
    map: []?usize = &.{},
    lsn: u64 = 0,
    ts: i64 = 0,
    seq: i64 = 0,
    rows: usize = 0,
    truncated: bool = false,
    unsent: bool = false,

    pub fn init(a: std.mem.Allocator, cols: []const types.Schema.Field, schema_name: []const u8, table: []const u8, from: u64) !Decoder {
        const n = cols.len + change_cols.len;
        const builders = try a.alloc(column.Builder, n);
        const convs = try a.alloc(cell.Converter, cols.len);
        for (cols, 0..) |c, i| {
            builders[i] = column.Builder.init(a, c.ty);
            convs[i] = cell.converter(c.ty.kind, cell.pg_wire);
        }
        builders[cols.len] = column.Builder.init(a, types.Type.init(.string).asNullable());
        builders[cols.len + 1] = column.Builder.init(a, types.Type.init(.string).asNullable());
        builders[cols.len + 2] = column.Builder.init(a, types.Type.init(.timestamp).asNullable());
        builders[cols.len + 3] = column.Builder.init(a, types.Type.init(.int).asNullable());
        return .{ .arena = a, .cols = cols, .schema_name = schema_name, .table = table, .from = from, .builders = builders, .convs = convs };
    }

    pub fn message(self: *Decoder, msg: []const u8) !void {
        var r = Reader{ .p = msg };
        switch (try r.int(u8)) {
            'B' => {
                self.lsn = try r.int(u64);
                self.ts = @as(i64, @bitCast(try r.int(u64))) + pg_epoch_us;
            },
            'R' => try self.relation(&r),
            'I' => {
                if (!try self.ours(&r)) return;
                if (try r.int(u8) != 'N') return error.CorruptChangeStream;
                try self.row(&r, null, "insert");
            },
            'U' => {
                if (!try self.ours(&r)) return;
                var old: ?[]const ?[]const u8 = null;
                var tag = try r.int(u8);
                if (tag == 'K' or tag == 'O') {
                    old = try self.tuple(&r, null);
                    tag = try r.int(u8);
                }
                if (tag != 'N') return error.CorruptChangeStream;
                try self.row(&r, old, "update_postimage");
            },
            'D' => {
                if (!try self.ours(&r)) return;
                const tag = try r.int(u8);
                if (tag != 'K' and tag != 'O') return error.CorruptChangeStream;
                try self.row(&r, null, "delete");
            },
            'T' => {
                const n = try r.int(u32);
                _ = try r.int(u8);
                for (0..n) |_| {
                    const id = try r.int(u32);
                    if (self.relid != null and id == self.relid.? and self.lsn > self.from) self.truncated = true;
                }
            },
            else => {},
        }
    }

    /// Whether the change that `r` is at is the table's and after `from`.
    fn ours(self: *Decoder, r: *Reader) !bool {
        const id = try r.int(u32);
        return self.relid != null and id == self.relid.? and self.lsn > self.from;
    }

    fn relation(self: *Decoder, r: *Reader) !void {
        const id = try r.int(u32);
        const ns = try r.cstr();
        const name = try r.cstr();
        _ = try r.int(u8);
        const n = try r.int(u16);
        const mine = std.mem.eql(u8, ns, self.schema_name) and std.mem.eql(u8, name, self.table);
        const map = try self.arena.alloc(?usize, n);
        for (map) |*m| {
            _ = try r.int(u8);
            const col = try r.cstr();
            _ = try r.int(u32);
            _ = try r.int(u32);
            m.* = null;
            for (self.cols, 0..) |c, i| if (std.mem.eql(u8, c.name, col)) {
                m.* = i;
            };
        }
        if (mine) {
            self.relid = id;
            self.map = map;
        }
    }

    /// A tuple's values in the relation's order: text, null for NULL, and `u` (an
    /// unchanged TOASTed value) as the matching value of `old`, which a table that
    /// logs whole rows sends.
    fn tuple(self: *Decoder, r: *Reader, old: ?[]const ?[]const u8) ![]const ?[]const u8 {
        const n = try r.int(u16);
        if (n != self.map.len) return error.CorruptChangeStream;
        const out = try self.arena.alloc(?[]const u8, n);
        for (out, 0..) |*v, i| switch (try r.int(u8)) {
            'n' => v.* = null,
            'u' => if (old) |o| {
                v.* = o[i];
            } else {
                self.unsent = true;
                v.* = null;
            },
            't' => v.* = try r.bytes(try r.int(u32)),
            else => return error.CorruptChangeStream,
        };
        return out;
    }

    fn row(self: *Decoder, r: *Reader, old: ?[]const ?[]const u8, kind: []const u8) !void {
        const vals = try self.tuple(r, old);
        const filled = try self.arena.alloc(bool, self.cols.len);
        @memset(filled, false);
        for (vals, self.map) |v, m| {
            const i = m orelse continue;
            if (v) |text| try self.convs[i](&self.builders[i], text) else try self.builders[i].appendNull();
            filled[i] = true;
        }
        for (filled, 0..) |f, i| if (!f) try self.builders[i].appendNull();
        const k = self.cols.len;
        try self.builders[k].appendStr(kind);
        try self.builders[k + 1].appendStr(try hex16(self.arena, self.lsn));
        try self.builders[k + 2].appendInt(self.ts);
        self.seq += 1;
        try self.builders[k + 3].appendInt(self.seq);
        self.rows += 1;
    }

    pub fn finish(self: *Decoder) !Feed {
        const fields = try self.arena.alloc(types.Schema.Field, self.builders.len);
        const columns = try self.arena.alloc(column.Column, self.builders.len);
        for (self.builders, columns, 0..) |*b, *c, i| {
            c.* = try b.finish();
            fields[i] = .{ .name = if (i < self.cols.len) self.cols[i].name else change_cols[i - self.cols.len], .ty = c.ty };
        }
        const schema = try self.arena.create(types.Schema);
        schema.* = .{ .fields = fields };
        return .{ .schema = schema.*, .batch = .{ .schema = schema, .columns = columns, .len = self.rows }, .truncated = self.truncated, .unsent = self.unsent };
    }
};

const Reader = @import("../../base/cursor.zig").Reader(.big, error.CorruptChangeStream);

const Msg = struct {
    buf: std.array_list.Managed(u8),

    fn init(a: std.mem.Allocator, tag: u8) !Msg {
        var m = Msg{ .buf = std.array_list.Managed(u8).init(a) };
        try m.buf.append(tag);
        return m;
    }

    fn int(self: *Msg, comptime T: type, v: T) !void {
        var b: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &b, v, .big);
        try self.buf.appendSlice(&b);
    }

    fn str(self: *Msg, s: []const u8) !void {
        try self.buf.appendSlice(s);
        try self.buf.append(0);
    }

    fn tuple(self: *Msg, vals: []const ?[]const u8) !void {
        try self.int(u16, @intCast(vals.len));
        for (vals) |v| if (v) |t| {
            if (std.mem.eql(u8, t, "\x00u")) {
                try self.buf.append('u');
            } else {
                try self.buf.append('t');
                try self.int(u32, @intCast(t.len));
                try self.buf.appendSlice(t);
            }
        } else try self.buf.append('n');
    }
};

fn begin(a: std.mem.Allocator, lsn: u64) ![]const u8 {
    var m = try Msg.init(a, 'B');
    try m.int(u64, lsn);
    try m.int(u64, 0);
    try m.int(u32, 7);
    return m.buf.items;
}

fn relationMsg(a: std.mem.Allocator, id: u32, ns: []const u8, name: []const u8, cols: []const []const u8) ![]const u8 {
    var m = try Msg.init(a, 'R');
    try m.int(u32, id);
    try m.str(ns);
    try m.str(name);
    try m.buf.append('d');
    try m.int(u16, @intCast(cols.len));
    for (cols) |c| {
        try m.buf.append(0);
        try m.str(c);
        try m.int(u32, 25);
        try m.int(u32, 0xffff_ffff);
    }
    return m.buf.items;
}

fn change(a: std.mem.Allocator, tag: u8, id: u32, old: ?struct { u8, []const ?[]const u8 }, new: ?[]const ?[]const u8) ![]const u8 {
    var m = try Msg.init(a, tag);
    try m.int(u32, id);
    if (old) |o| {
        try m.buf.append(o[0]);
        try m.tuple(o[1]);
    }
    if (new) |n| {
        try m.buf.append('N');
        try m.tuple(n);
    }
    return m.buf.items;
}

fn testCols() []const types.Schema.Field {
    const S = struct {
        const cols = [_]types.Schema.Field{
            .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
            .{ .name = "status", .ty = types.Type.init(.string).asNullable() },
            .{ .name = "note", .ty = types.Type.init(.string).asNullable() },
        };
    };
    return &S.cols;
}

test "pgoutput: the read table's inserts, updates and deletes after the position, its commit's LSN and order; other tables and earlier commits skipped" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var d = try Decoder.init(a, testCols(), "public", "orders", 0x100);
    const cols = &[_][]const u8{ "id", "status", "note" };
    try d.message(try begin(a, 0x100));
    try d.message(try relationMsg(a, 16, "public", "orders", cols));
    try d.message(try change(a, 'I', 16, null, &.{ "1", "old", "x" }));
    try d.message(try begin(a, 0x2A0));
    try d.message(try relationMsg(a, 17, "public", "other", cols));
    try d.message(try change(a, 'I', 17, null, &.{ "9", "no", "z" }));
    try d.message(try change(a, 'I', 16, null, &.{ "2", "new", null }));
    try d.message(try change(a, 'U', 16, null, &.{ "2", "paid", "y" }));
    try d.message(try begin(a, 0x300));
    try d.message(try change(a, 'D', 16, .{ 'K', &.{ "2", null, null } }, null));
    const f = try d.finish();
    try std.testing.expectEqual(@as(usize, 3), f.batch.len);
    const want = [_][6][]const u8{
        .{ "2", "new", "", "insert", "00000000000002A0", "1" },
        .{ "2", "paid", "y", "update_postimage", "00000000000002A0", "2" },
        .{ "2", "", "", "delete", "0000000000000300", "3" },
    };
    for (want, 0..) |w, r| for ([_]usize{ 0, 1, 2, 3, 4, 6 }, w) |c, text| {
        const v = f.batch.columns[c].getValue(r);
        const got = if (v.isNull()) "" else try fmtnum.valueToString(a, v);
        try std.testing.expectEqualStrings(text, got);
    };
    try std.testing.expect(!f.truncated and !f.unsent);
    try std.testing.expectEqualStrings("_commit_timestamp", f.schema.fields[5].name);
}

test "pgoutput: an unchanged TOASTed value comes from the old row when the table logs it, else marks the range unsent; a TRUNCATE of the table marks it truncated" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cols = &[_][]const u8{ "id", "status", "note" };
    var full = try Decoder.init(a, testCols(), "public", "orders", 0);
    try full.message(try begin(a, 0x10));
    try full.message(try relationMsg(a, 16, "public", "orders", cols));
    try full.message(try change(a, 'U', 16, .{ 'O', &.{ "5", "new", "big text" } }, &.{ "5", "paid", "\x00u" }));
    const ff = try full.finish();
    try std.testing.expectEqualStrings("big text", ff.batch.columns[2].getValue(0).string);
    try std.testing.expect(!ff.unsent);

    var bare = try Decoder.init(a, testCols(), "public", "orders", 0);
    try bare.message(try begin(a, 0x10));
    try bare.message(try relationMsg(a, 16, "public", "orders", cols));
    try bare.message(try change(a, 'U', 16, null, &.{ "5", "paid", "\x00u" }));
    try std.testing.expect((try bare.finish()).unsent);

    var tr = try Decoder.init(a, testCols(), "public", "orders", 0);
    try tr.message(try begin(a, 0x10));
    try tr.message(try relationMsg(a, 16, "public", "orders", cols));
    var t = try Msg.init(a, 'T');
    try t.int(u32, 2);
    try t.buf.append(0);
    try t.int(u32, 99);
    try t.int(u32, 16);
    try tr.message(t.buf.items);
    try std.testing.expect((try tr.finish()).truncated);

    var other = try Decoder.init(a, testCols(), "public", "orders", 0);
    try other.message(try begin(a, 0x10));
    try other.message(try relationMsg(a, 16, "public", "orders", cols));
    var t2 = try Msg.init(a, 'T');
    try t2.int(u32, 1);
    try t2.buf.append(0);
    try t2.int(u32, 99);
    try other.message(t2.buf.items);
    try std.testing.expect(!(try other.finish()).truncated);
    try std.testing.expectError(error.CorruptChangeStream, other.message(t.buf.items[0..12]));
}

test "an LSN reads as 16 hex digits or hi/lo and writes back as 16 hex digits that order as LSNs do" {
    try std.testing.expectEqual(@as(?u64, 0x1A4E3B8), try parseLsn("0/1A4E3B8"));
    try std.testing.expectEqual(@as(?u64, (0x2 << 32) | 0x10), try parseLsn("2/10"));
    try std.testing.expectEqual(@as(?u64, 0x1A4E3B8), try parseLsn("0000000001A4E3B8"));
    try std.testing.expectEqual(@as(?u64, null), try parseLsn(" "));
    try std.testing.expectError(error.InvalidLsn, parseLsn("1A4E3B8"));
    try std.testing.expectError(error.InvalidLsn, parseLsn("x/1"));
    const h = try hex16(std.testing.allocator, 0x1A4E3B8);
    defer std.testing.allocator.free(h);
    try std.testing.expectEqualStrings("0000000001A4E3B8", h);
}

//! The options each connection kind takes, one table per connector in the order
//! `unknownOptionMsg` lists them, and the one set of readers `check` and the run
//! both use to turn an option's expression into a typed value: integers in a range,
//! bools, literal or env()/secret() text. `bind` is the one loop that walks a
//! connection's options into a config struct; each caller supplies only how a value
//! of each field type is read.

const ast = @import("../../lang/ast.zig");
const options = @import("../analyze/options.zig");
const textconv = @import("../../exec/textconv.zig");
const registry = @import("../../connect/registry.zig");
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");

pub const Kind = enum {
    text,
    port,
    count,
    flag,

    pub fn Int(comptime k: Kind) type {
        return switch (k) {
            .port => u16,
            .count => u32,
            else => @compileError("not an integer option"),
        };
    }
};

pub const Opt = struct { key: []const u8, kind: Kind = .text };

/// Seconds the login may go without an answer, and each read or write after it
/// (`driver.Timeouts`); 0 is no limit.
pub const timeouts = [_]Opt{ .{ .key = "connect_timeout", .kind = .count }, .{ .key = "query_timeout", .kind = .count } };

pub const sftp = [_]Opt{
    .{ .key = "host" },     .{ .key = "port", .kind = .port }, .{ .key = "user" },        .{ .key = "password" },
    .{ .key = "key_file" }, .{ .key = "key_passphrase" },      .{ .key = "known_hosts" }, .{ .key = "host_key" },
} ++ timeouts;

pub const smb = [_]Opt{
    .{ .key = "host" },  .{ .key = "port", .kind = .port }, .{ .key = "user" },  .{ .key = "password" }, .{ .key = "domain" },
    .{ .key = "share" }, .{ .key = "auth" },                .{ .key = "realm" }, .{ .key = "kdc" },      .{ .key = "spn" },
} ++ timeouts;

pub const ftp = [_]Opt{ .{ .key = "host" }, .{ .key = "port", .kind = .port }, .{ .key = "user" }, .{ .key = "password" } };

pub const sql = [_]Opt{
    .{ .key = "host" }, .{ .key = "port", .kind = .port }, .{ .key = "database" }, .{ .key = "user" }, .{ .key = "password" }, .{ .key = "tls" },
} ++ timeouts;

pub const sqlserver = sql ++ [_]Opt{
    .{ .key = "auth" }, .{ .key = "domain" },    .{ .key = "realm" },    .{ .key = "kdc" },
    .{ .key = "spn" },  .{ .key = "client_id" }, .{ .key = "resource" }, .{ .key = "token" },
};

pub const stream = sql ++ [_]Opt{
    .{ .key = "fe_host" },                    .{ .key = "fe_port", .kind = .port },  .{ .key = "be_url" },
    .{ .key = "load_url" },                   .{ .key = "buckets", .kind = .count }, .{ .key = "replication_num", .kind = .count },
    .{ .key = "auto_create", .kind = .flag }, .{ .key = "label_prefix" },            .{ .key = "load_size" },
    .{ .key = "compression" },
};

/// What `parseDbConfig` reads for any SQL connection, StarRocks' `fe_` spellings included.
pub const db_read = sqlserver ++ [_]Opt{ .{ .key = "fe_host" }, .{ .key = "fe_port", .kind = .port } };

pub const Alias = struct { key: []const u8, field: []const u8 };

/// `fe_host`/`fe_port` into `DbConfig.host`/`port`.
pub const db_aliases = [_]Alias{ .{ .key = "fe_host", .field = "host" }, .{ .key = "fe_port", .field = "port" } };

/// `host`/`port`/`be_url` into `streamload.Config.fe_host`/`fe_port`/`load_url`.
pub const stream_aliases = [_]Alias{ .{ .key = "host", .field = "fe_host" }, .{ .key = "port", .field = "fe_port" }, .{ .key = "be_url", .field = "load_url" } };

/// The keys of a table, in its order, as the plain list error messages join.
pub fn keyNames(comptime opts: []const Opt) [opts.len][]const u8 {
    var out: [opts.len][]const u8 = undefined;
    for (opts, 0..) |o, i| out[i] = o.key;
    return out;
}

const Table = struct { opts: []const Opt, names: []const []const u8 };

fn table(comptime opts: []const Opt) Table {
    const names = comptime keyNames(opts);
    return .{ .opts = opts, .names = &names };
}

/// The table of a file connection (sftp, smb, ftp), or null.
pub fn fileOpts(connector: []const u8) ?[]const Opt {
    return (fileTable(connector) orelse return null).opts;
}

fn fileTable(connector: []const u8) ?Table {
    if (std.mem.eql(u8, connector, "sftp")) return table(&sftp);
    if (std.mem.eql(u8, connector, "smb")) return table(&smb);
    if (std.mem.eql(u8, connector, "ftp")) return table(&ftp);
    return null;
}

fn tableFor(connector: []const u8) ?Table {
    if (fileTable(connector)) |t| return t;
    const conn = registry.Connector.parse(connector) orelse return null;
    if (conn.sqlRead() == null) return null;
    if (conn.streamLoad()) return table(&stream);
    if (conn == .sqlserver) return table(&sqlserver);
    return table(&sql);
}

/// The table a connector reads its options from: a file connection's, a StarRocks or
/// Doris one's, SQL Server's or any other SQL read's. Null for one with no table (http
/// goes through `connFromKvs`).
pub fn forConnector(connector: []const u8) ?[]const Opt {
    return (tableFor(connector) orelse return null).opts;
}

/// The same table as plain keys, for `unknownOptionMsg`.
pub fn namesFor(connector: []const u8) ?[]const []const u8 {
    return (tableFor(connector) orelse return null).names;
}

pub fn find(opts: []const Opt, key: []const u8) ?Opt {
    for (opts) |o| if (std.mem.eql(u8, o.key, key)) return o;
    return null;
}

pub const Problem = enum {
    int_text,
    int_kind,
    not_bool,
    not_literal,
    out_of_range,

    /// The message of a problem that names neither connector nor key.
    pub fn fixedMsg(p: Problem) ?[]const u8 {
        return switch (p) {
            .int_text => options.int_cfg_msg,
            .int_kind => options.int_cfg_kind_msg,
            .not_bool => options.bool_cfg_msg,
            .not_literal => options.literal_cfg_msg,
            .out_of_range => null,
        };
    }
};

pub fn message(arena: std.mem.Allocator, p: Problem, connector: []const u8, key: []const u8) ![]const u8 {
    return p.fixedMsg() orelse options.rangeMsg(arena, connector, key);
}

pub fn unsetEnvMsg(arena: std.mem.Allocator, name: []const u8) ![]const u8 {
    return msgfmt.allocPrint(arena, "env var `{s}` is not set", .{name});
}

pub fn Read(comptime T: type) type {
    return union(enum) { ok: T, problem: Problem };
}

/// An integer option: an integer literal or text `std.fmt.parseInt` reads in base 10
/// (so `'+5'` and `'1_000'` pass). Anything else, `-1` and env() included, is the
/// wrong kind.
pub fn cfgInt(e: *const ast.Expr) Read(i64) {
    return switch (e.*) {
        .int_lit => |i| .{ .ok = i },
        .str_lit => |s| if (std.fmt.parseInt(i64, s, 10)) |i| .{ .ok = i } else |_| .{ .problem = .int_text },
        else => .{ .problem = .int_kind },
    };
}

pub fn cfgIntIn(comptime T: type, e: *const ast.Expr) Read(T) {
    return switch (cfgInt(e)) {
        .ok => |i| if (std.math.cast(T, i)) |v| .{ .ok = v } else .{ .problem = .out_of_range },
        .problem => |p| .{ .problem = p },
    };
}

/// A bool option: a bool literal, or the text `true` or `false` in any case.
pub fn cfgBool(e: *const ast.Expr) Read(bool) {
    return switch (e.*) {
        .bool_lit => |b| .{ .ok = b },
        .str_lit => |s| if (textconv.boolText(.hint, s)) |b| .{ .ok = b } else .{ .problem = .not_bool },
        else => .{ .problem = .not_bool },
    };
}

/// The text a literal option value reads as, or null for one only the run evaluates.
pub fn literalText(arena: std.mem.Allocator, e: *const ast.Expr) !?[]const u8 {
    return switch (e.*) {
        .str_lit => |s| s,
        .int_lit => |i| try msgfmt.allocPrint(arena, "{d}", .{i}),
        .bool_lit => |b| if (b) "true" else "false",
        else => null,
    };
}

/// The variable an `env('X')` or `secret('X')` value names, or null.
pub fn envName(e: *const ast.Expr) ?[]const u8 {
    if (e.* != .call) return null;
    const c = e.call;
    if (!std.mem.eql(u8, c.name, "env") and !std.mem.eql(u8, c.name, "secret")) return null;
    if (c.args.len != 1 or c.args[0].* != .str_lit) return null;
    return c.args[0].str_lit;
}

pub const Text = union(enum) { ok: []const u8, unset: []const u8, problem: Problem };

/// A text option at run time: a literal, or the value of the variable env()/secret()
/// names; `unset` carries that name so the caller picks null or `unsetEnvMsg`.
pub fn cfgText(arena: std.mem.Allocator, e: *const ast.Expr) !Text {
    if (try literalText(arena, e)) |t| return .{ .ok = t };
    const name = envName(e) orelse return .{ .problem = .not_literal };
    const v = std.process.getEnvVarOwned(arena, name) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return .{ .unset = name };
    };
    return .{ .ok = v };
}

/// Why `check` refuses an option's value before the run, or null; an env()/secret()
/// text value is left to the run.
pub fn checkValue(arena: std.mem.Allocator, opt: Opt, e: *const ast.Expr) !?Problem {
    switch (opt.kind) {
        inline .port, .count => |k| return switch (cfgIntIn(k.Int(), e)) {
            .ok => null,
            .problem => |p| p,
        },
        .flag => return switch (cfgBool(e)) {
            .ok => null,
            .problem => |p| p,
        },
        .text => {
            if ((try literalText(arena, e)) != null or envName(e) != null) return null;
            return .not_literal;
        },
    }
}

pub const Unknown = enum { refuse, ignore };

pub const Finding = union(enum) {
    unknown: []const u8,
    value: struct { key: []const u8, problem: Problem },
};

/// The first option `check` refuses: per option in order, its value first (a key not
/// in the table must still be a literal or env()/secret()), then, under `.refuse`, a
/// key not in the table.
pub fn checkLiterals(arena: std.mem.Allocator, opts: []const Opt, config: []const ast.Attr, comptime unknown: Unknown) !?Finding {
    for (config) |attr| {
        const opt = find(opts, attr.key) orelse {
            if (unknown == .ignore) continue;
            if ((try checkValue(arena, .{ .key = attr.key }, attr.value)) != null)
                return .{ .value = .{ .key = attr.key, .problem = .not_literal } };
            return .{ .unknown = attr.key };
        };
        if (try checkValue(arena, opt, attr.value)) |p| return .{ .value = .{ .key = attr.key, .problem = p } };
    }
    return null;
}

pub fn findingMsg(arena: std.mem.Allocator, f: Finding, c: ast.Connection) ![]const u8 {
    return switch (f) {
        .unknown => |k| options.unknownOptionMsg(arena, c, k),
        .value => |v| message(arena, v.problem, c.connector, v.key),
    };
}

fn fieldOf(comptime aliases: []const Alias, key: []const u8) []const u8 {
    inline for (aliases) |a| if (std.mem.eql(u8, a.key, key)) return a.field;
    return key;
}

fn bound(comptime opts: []const Opt, comptime aliases: []const Alias, comptime name: []const u8) bool {
    for (opts) |o| if (std.mem.eql(u8, o.key, name)) return true;
    for (aliases) |a| if (std.mem.eql(u8, a.field, name)) return true;
    return false;
}

/// Walks `config` in order into `out`, the last of a repeated key winning. A key is
/// looked up in `opts` (under `.refuse` an absent one returns `r.unknown(key, expr)`), mapped
/// through `aliases` to a field of `T`, and skipped when `T` has no such field. The
/// reader's `value(F, opt, expr) !?F` reads it, null leaving the field as it was; a
/// set field `x` also sets a bool field `x_explicit` when `T` has one.
pub fn bind(comptime T: type, comptime opts: []const Opt, comptime unknown: Unknown, comptime aliases: []const Alias, out: *T, config: []const ast.Attr, r: anytype) !void {
    for (config) |attr| {
        const opt = find(opts, attr.key) orelse switch (unknown) {
            .refuse => return r.unknown(attr.key, attr.value),
            .ignore => continue,
        };
        const name = fieldOf(aliases, opt.key);
        inline for (std.meta.fields(T)) |f| {
            if (comptime bound(opts, aliases, f.name)) {
                if (std.mem.eql(u8, f.name, name)) {
                    if (try r.value(f.type, opt, attr.value)) |v| {
                        @field(out, f.name) = v;
                        if (@hasField(T, f.name ++ "_explicit")) @field(out, f.name ++ "_explicit") = true;
                    }
                }
            }
        }
    }
}

const testing = std.testing;

const Fx = struct {
    const analyze = @import("../analyze.zig");
    const dbconfig = @import("dbconfig.zig");
    const env_mod = @import("../env.zig");
    const obs = @import("../obs.zig");
    const op = @import("../../exec/op.zig");
    const parser = @import("../../lang/sql_parser.zig");
    const old = @import("testing_util.zig").old;
    const register = @import("register.zig");
    const sinkmod = @import("sink.zig");
    const sqlmod = @import("../../db/sql.zig");
    const streamload = @import("../../db/streamload.zig");
    const Value = @import("../../exec/value.zig").Value;
    const LitCfg = @import("testing_util.zig").LitCfg;

    const unset_var = "BASALT_OPTS_SCHEMA_SURELY_UNSET";

    fn conn(a: std.mem.Allocator, src: []const u8) !ast.Connection {
        var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, src, &pd);
        for (prog.stmts) |st| if (st == .connection) return st.connection;
        return error.NoConnection;
    }

    fn opts(a: std.mem.Allocator, connector: []const u8, body: []const u8) !ast.Connection {
        return conn(a, try msgfmt.allocPrint(a, "CREATE CONNECTION optsx TYPE {s} OPTIONS ({s});", .{ connector, body }));
    }

    fn expr(a: std.mem.Allocator, src: []const u8) !*const ast.Expr {
        return (try opts(a, "sftp", try msgfmt.allocPrint(a, "v = {s}", .{src}))).config[0].value;
    }

    fn env(a: std.mem.Allocator) !*env_mod.Env {
        return @import("testing_util.zig").testEnv(a);
    }

    const int_corpus = [_][]const u8{ "0", "22", "65535", "65536", "70000", "4294967295", "4294967296", "-1", "'-1'", "'42'", "'+5'", "'1_000'", "' 5'", "'0x10'", "'x'", "''", "true", "1 + 2", "env('" ++ unset_var ++ "')" };
    const bool_corpus = [_][]const u8{ "true", "false", "'true'", "'false'", "'TRUE'", "'False'", "'yes'", "'1'", "''", "1", "0", "env('" ++ unset_var ++ "')" };
    const text_corpus = [_][]const u8{ "'h'", "''", "7", "-7", "true", "false", "1 + 2", "env('" ++ unset_var ++ "')", "secret('" ++ unset_var ++ "')", "env('PATH')", "upper('x')" };
};

test "every table lists its keys in the order unknownOptionMsg names them today" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const want = [_]struct { []const u8, []const u8 }{
        .{ "sftp", "host, port, user, password, key_file, key_passphrase, known_hosts, host_key, connect_timeout, query_timeout" },
        .{ "smb", "host, port, user, password, domain, share, auth, realm, kdc, spn, connect_timeout, query_timeout" },
        .{ "ftp", "host, port, user, password" },
        .{ "postgres", "host, port, database, user, password, tls, connect_timeout, query_timeout" },
        .{ "mysql", "host, port, database, user, password, tls, connect_timeout, query_timeout" },
        .{ "sqlserver", "host, port, database, user, password, tls, connect_timeout, query_timeout, auth, domain, realm, kdc, spn, client_id, resource, token" },
        .{ "starrocks", "host, port, database, user, password, tls, connect_timeout, query_timeout, fe_host, fe_port, be_url, load_url, buckets, replication_num, auto_create, label_prefix, load_size, compression" },
        .{ "doris", "host, port, database, user, password, tls, connect_timeout, query_timeout, fe_host, fe_port, be_url, load_url, buckets, replication_num, auto_create, label_prefix, load_size, compression" },
    };
    for (want) |w| {
        const c = try Fx.opts(a, w[0], "host = 'h'");
        const today = try options.unknownOptionMsg(a, c, "zz");
        const ours = try msgfmt.allocPrint(a, "{s} connection `optsx`: unknown option `zz` ({s})", .{ w[0], try std.mem.join(a, ", ", namesFor(w[0]).?) });
        try testing.expectEqualStrings(today, ours);
        try testing.expect(std.mem.endsWith(u8, today, try msgfmt.allocPrint(a, "({s})", .{w[1]})));
        try testing.expectEqual(namesFor(w[0]).?.len, forConnector(w[0]).?.len);
    }
    for ([_][]const u8{ "http", "csv", "stdout", "stdin", "unit", "nope" }) |none| {
        try testing.expect(forConnector(none) == null);
        try testing.expect(namesFor(none) == null);
        const c = try Fx.opts(a, none, "host = 'h'");
        try testing.expect(std.mem.endsWith(u8, try options.unknownOptionMsg(a, c, "zz"), "()"));
    }
    try testing.expect(fileOpts("postgres") == null);
    try testing.expectEqual(@as(usize, 4), fileOpts("ftp").?.len);
}

test "integer kinds: port is u16, buckets, replication_num and the timeouts u32, the rest of the stream table text or flag" {
    for (stream) |o| {
        const want: Kind = if (std.mem.endsWith(u8, o.key, "port"))
            .port
        else if (std.mem.eql(u8, o.key, "buckets") or std.mem.eql(u8, o.key, "replication_num") or std.mem.endsWith(u8, o.key, "_timeout"))
            .count
        else if (std.mem.eql(u8, o.key, "auto_create"))
            .flag
        else
            .text;
        try testing.expectEqual(want, o.kind);
    }
    try testing.expectEqual(u16, Kind.port.Int());
    try testing.expectEqual(u32, Kind.count.Int());
    for ([_][]const Opt{ &sftp, &smb, &ftp, &sql, &sqlserver }) |tbl| for (tbl) |o|
        try testing.expectEqual(if (std.mem.eql(u8, o.key, "port")) Kind.port else if (std.mem.endsWith(u8, o.key, "_timeout")) Kind.count else Kind.text, o.kind);
}

test "cfgInt reads every value as the run's evalCfgInt did" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const env = try Fx.env(a);
    for (Fx.int_corpus) |src| {
        const e = try Fx.expr(a, src);
        env.diag.* = .{};
        const today = Fx.old.evalCfgInt(env, e);
        switch (cfgInt(e)) {
            .ok => |v| try testing.expectEqual(v, try today),
            .problem => |p| {
                try testing.expectError(error.PlanFailed, today);
                try testing.expectEqualStrings(p.fixedMsg().?, env.diag.msg);
            },
        }
    }
}

test "cfgInt: parseInt's base-10 rules, a negative literal and env() are the wrong kind" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqual(@as(i64, 5), cfgInt(try Fx.expr(a, "'+5'")).ok);
    try testing.expectEqual(@as(i64, 1000), cfgInt(try Fx.expr(a, "'1_000'")).ok);
    try testing.expectEqual(@as(i64, -1), cfgInt(try Fx.expr(a, "'-1'")).ok);
    try testing.expectEqual(Problem.int_text, cfgInt(try Fx.expr(a, "' 5'")).problem);
    try testing.expectEqual(Problem.int_text, cfgInt(try Fx.expr(a, "'0x10'")).problem);
    try testing.expectEqual(Problem.int_text, cfgInt(try Fx.expr(a, "'9223372036854775808'")).problem);
    try testing.expectEqual(Problem.int_kind, cfgInt(try Fx.expr(a, "-1")).problem);
    try testing.expectEqual(Problem.int_kind, cfgInt(try Fx.expr(a, "env('P')")).problem);
    try testing.expectEqual(Problem.int_kind, cfgInt(try Fx.expr(a, "true")).problem);
}

test "cfgIntIn: both ends of u16 and u32, one past each, and negatives" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqual(@as(u16, 0), cfgIntIn(u16, try Fx.expr(a, "0")).ok);
    try testing.expectEqual(@as(u16, 65535), cfgIntIn(u16, try Fx.expr(a, "65535")).ok);
    try testing.expectEqual(@as(u16, 65535), cfgIntIn(u16, try Fx.expr(a, "'65535'")).ok);
    try testing.expectEqual(Problem.out_of_range, cfgIntIn(u16, try Fx.expr(a, "65536")).problem);
    try testing.expectEqual(Problem.out_of_range, cfgIntIn(u16, try Fx.expr(a, "70000")).problem);
    try testing.expectEqual(Problem.out_of_range, cfgIntIn(u16, try Fx.expr(a, "'-1'")).problem);
    try testing.expectEqual(@as(u32, 4294967295), cfgIntIn(u32, try Fx.expr(a, "4294967295")).ok);
    try testing.expectEqual(Problem.out_of_range, cfgIntIn(u32, try Fx.expr(a, "4294967296")).problem);
    try testing.expectEqual(Problem.int_text, cfgIntIn(u16, try Fx.expr(a, "'x'")).problem);
    try testing.expectEqual(Problem.int_kind, cfgIntIn(u32, try Fx.expr(a, "1 + 2")).problem);
}

test "cfgIntIn on a port matches what sftp, smb and ftp registration refuse today, message included" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_][]const u8{ "sftp", "smb", "ftp" }) |connector| for (Fx.int_corpus) |src| {
        const c = try Fx.opts(a, connector, try msgfmt.allocPrint(a, "port = {s}", .{src}));
        const e = c.config[0].value;
        const env = try Fx.env(a);
        const today = std.math.cast(u16, Fx.old.evalCfgInt(env, e) catch {
            try testing.expectEqualStrings(env.diag.msg, try message(a, cfgIntIn(u16, e).problem, connector, "port"));
            continue;
        });
        switch (cfgIntIn(u16, e)) {
            .ok => |v| try testing.expectEqual(today.?, v),
            .problem => |p| {
                try testing.expect(today == null);
                try testing.expectEqualStrings(try options.portRangeMsg(a, connector), try message(a, p, connector, "port"));
            },
        }
    };
}

test "cfgBool is strict and case-blind; evalCfgBool agrees on every row but the pre-F2 text rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const env = try Fx.env(a);
    const Want = struct { []const u8, ?bool, ?bool, bool };
    const rows = [_]Want{
        .{ "true", true, true, true },      .{ "false", false, false, true },  .{ "'true'", true, true, true },
        .{ "'false'", false, false, true }, .{ "'TRUE'", false, true, false }, .{ "'False'", false, false, true },
        .{ "'yes'", false, null, false },   .{ "'1'", false, null, false },    .{ "''", false, null, false },
        .{ "1", null, null, true },         .{ "0", null, null, true },        .{ "env('" ++ Fx.unset_var ++ "')", null, null, true },
    };
    try testing.expectEqual(Fx.bool_corpus.len, rows.len);
    for (rows, Fx.bool_corpus) |w, src| {
        try testing.expectEqualStrings(w[0], src);
        const e = try Fx.expr(a, src);
        if (!w[3]) {} else if (w[1]) |b| {
            try testing.expectEqual(b, try Fx.old.evalCfgBool(env, e));
        } else {
            try testing.expectError(error.PlanFailed, Fx.old.evalCfgBool(env, e));
            try testing.expectEqualStrings(options.bool_cfg_msg, env.diag.msg);
        }
        switch (cfgBool(e)) {
            .ok => |b| try testing.expectEqual(w[2].?, b),
            .problem => |p| {
                try testing.expect(w[2] == null);
                try testing.expectEqual(Problem.not_bool, p);
            },
        }
    }
}

test "cfgText reads literals and env() as the run's evalCfgStr did, and an unset var as optCfgStr's null or evalCfgStr's refusal" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const env = try Fx.env(a);
    for (Fx.text_corpus) |src| {
        const e = try Fx.expr(a, src);
        env.diag.* = .{};
        const today = Fx.old.evalCfgStr(env, e);
        switch (try cfgText(a, e)) {
            .ok => |t| try testing.expectEqualStrings(try today, t),
            .unset => |name| {
                try testing.expectEqualStrings(Fx.unset_var, name);
                try testing.expectError(error.PlanFailed, today);
                try testing.expectEqualStrings(try unsetEnvMsg(a, name), env.diag.msg);
            },
            .problem => |p| {
                try testing.expectEqual(Problem.not_literal, p);
                try testing.expectError(error.PlanFailed, today);
                try testing.expectEqualStrings(options.literal_cfg_msg, env.diag.msg);
            },
        }
    }
    try testing.expectEqualStrings("-7", (try literalText(a, try Fx.expr(a, "'-7'"))).?);
    try testing.expect((try literalText(a, try Fx.expr(a, "-7"))) == null);
    try testing.expectEqualStrings("7", (try literalText(a, try Fx.expr(a, "7"))).?);
    try testing.expectEqualStrings("X", envName(try Fx.expr(a, "secret('X')")).?);
    try testing.expect(envName(try Fx.expr(a, "env(1)")) == null);
    try testing.expect(envName(try Fx.expr(a, "upper('X')")) == null);
}

test "checkLiterals over a file connection gives connectionProblem's message today" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bodies = [_][]const u8{
        "host = 'h'",                   "host = 'h', port = 22",         "host = 'h', port = 70000",
        "host = 'h', port = '70000'",   "host = 'h', port = 'x'",        "host = 'h', port = -1",
        "host = 'h', port = '-1'",      "host = 'h', port = 1 + 2",      "host = 'h', port = env('P')",
        "host = 'h', zz = 1",           "host = 'h', zz = 1 + 2",        "host = 'h', user = 1 + 2",
        "host = 'h', user = env('U')",  "host = 'h', user = upper('u')", "host = 1 + 2",
        "host = 'h', zz = 1, port = 0", "host = 'h', port = 0, zz = 1",  "host = 'h', password = secret('S')",
        "host = 'h', domain = 'd'",     "host = 'h', share = true",      "host = 'h', key_file = 3",
    };
    for ([_][]const u8{ "sftp", "smb", "ftp" }) |connector| for (bodies) |body| {
        const c = try Fx.opts(a, connector, body);
        const today = try options.connectionProblem(a, c);
        const ours = if (try checkLiterals(a, forConnector(connector).?, c.config, .refuse)) |f| try findingMsg(a, f, c) else null;
        if (today == null or ours == null) {
            try testing.expectEqual(today == null, ours == null);
        } else try testing.expectEqualStrings(today.?, ours.?);
    };
}

test "checkLiterals on a SQL connection refuses a bad port and a computed text; check lets the computed text through" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const refused_now = [_]struct { []const u8, []const u8 }{
        .{ "host = 'h', port = 70000", "postgres connection `port` is out of range" },
        .{ "host = 'h', port = 'x'", options.int_cfg_msg },
        .{ "host = 'h', port = -1", options.int_cfg_kind_msg },
        .{ "host = 'h', database = 1 + 2", options.literal_cfg_msg },
    };
    for (refused_now) |w| {
        const c = try Fx.opts(a, "postgres", w[0]);
        const f = (try checkLiterals(a, &sql, c.config, .refuse)).?;
        try testing.expectEqualStrings(w[1], try findingMsg(a, f, c));
    }
    const computed = try Fx.opts(a, "postgres", "host = 'h', database = 1 + 2");
    try testing.expect((try options.connectionProblem(a, computed)) == null);
    const c = try Fx.opts(a, "postgres", "host = 'h', domain = 'd'");
    try testing.expectEqualStrings((try options.connectionProblem(a, c)).?, try findingMsg(a, (try checkLiterals(a, &sql, c.config, .refuse)).?, c));
    try testing.expect((try checkLiterals(a, &sqlserver, c.config, .refuse)) == null);
    const order = try Fx.opts(a, "postgres", "host = 'h', zz = 1 + 2");
    try testing.expect(std.mem.indexOf(u8, (try options.connectionProblem(a, order)).?, "unknown option `zz`") != null);
    try testing.expectEqualStrings(options.literal_cfg_msg, try findingMsg(a, (try checkLiterals(a, &sql, order.config, .refuse)).?, order));
}

test "connect_timeout and query_timeout take whole seconds on every SQL, SMB and SFTP connection; check refuses anything else" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_][]const u8{ "postgres", "mysql", "sqlserver", "starrocks", "doris", "smb", "sftp" }) |connector| {
        for ([_][]const u8{ "connect_timeout", "query_timeout" }) |key| {
            for ([_][]const u8{ "0", "30", "'86400'" }) |v| {
                const c = try Fx.opts(a, connector, try msgfmt.allocPrint(a, "host = 'h', {s} = {s}", .{ key, v }));
                try testing.expect((try options.connectionProblem(a, c)) == null);
                try testing.expect((try checkLiterals(a, forConnector(connector).?, c.config, .refuse)) == null);
            }
            const refused = [_]struct { []const u8, []const u8 }{
                .{ "-1", options.int_cfg_kind_msg },
                .{ "'soon'", options.int_cfg_msg },
                .{ "env('T')", options.int_cfg_kind_msg },
                .{ "4294967296", try options.rangeMsg(a, connector, key) },
            };
            for (refused) |w| {
                const c = try Fx.opts(a, connector, try msgfmt.allocPrint(a, "host = 'h', {s} = {s}", .{ key, w[0] }));
                try testing.expectEqualStrings(w[1], (try options.connectionProblem(a, c)).?);
                try testing.expectEqualStrings(w[1], try findingMsg(a, (try checkLiterals(a, forConnector(connector).?, c.config, .refuse)).?, c));
            }
        }
    }
    const ftp_c = try Fx.opts(a, "ftp", "host = 'h', connect_timeout = 5");
    try testing.expect(std.mem.indexOf(u8, (try options.connectionProblem(a, ftp_c)).?, "unknown option `connect_timeout`") != null);
}

test "checkLiterals on a StarRocks load agrees with checkSinkConnection, except a computed tls; a non-bool auto_create is refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const agree = [_][]const u8{
        "database = 'd'",                     "database = 'd', port = 70000",          "database = 'd', fe_port = 65536",
        "database = 'd', fe_port = '9030'",   "database = 'd', buckets = 4294967296",  "database = 'd', replication_num = -1",
        "database = 'd', buckets = 'x'",      "database = 'd', user = 1 + 2",          "database = 'd', label_prefix = upper('x')",
        "database = 'd', auto_create = true", "database = 'd', auto_create = 'false'", "database = 'd', auto_create = 1",
        "database = 'd', zz = 1 + 2",         "database = 'd', host = env('H')",       "database = 'd', load_url = 'http://x'",
    };
    for (agree) |body| {
        const c = try Fx.opts(a, "starrocks", body);
        var diag = Fx.analyze.Diag{};
        const today: ?[]const u8 = if (options.checkSinkConnection(a, c, &diag)) null else |_| diag.msg;
        const ours = if (try checkLiterals(a, &stream, c.config, .ignore)) |f| try findingMsg(a, f, c) else null;
        if (today == null or ours == null) {
            try testing.expectEqual(today == null, ours == null);
        } else try testing.expectEqualStrings(today.?, ours.?);
    }
    const differ = [_]struct { []const u8, Problem }{
        .{ "database = 'd', tls = upper('x')", .not_literal },
        .{ "database = 'd', auto_create = 'yes'", .not_bool },
        .{ "database = 'd', auto_create = 'on'", .not_bool },
    };
    for (differ) |w| {
        const c = try Fx.opts(a, "starrocks", w[0]);
        var diag = Fx.analyze.Diag{};
        if (w[1] == .not_literal) try options.checkSinkConnection(a, c, &diag);
        try testing.expectEqual(w[1], (try checkLiterals(a, &stream, c.config, .ignore)).?.value.problem);
    }
}

test "checkValue leaves an env() text to the run and checks a literal's kind and range" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expect((try checkValue(a, .{ .key = "user" }, try Fx.expr(a, "env('U')"))) == null);
    try testing.expect((try checkValue(a, .{ .key = "user" }, try Fx.expr(a, "false"))) == null);
    try testing.expectEqual(Problem.not_literal, (try checkValue(a, .{ .key = "user" }, try Fx.expr(a, "upper('u')"))).?);
    try testing.expectEqual(Problem.out_of_range, (try checkValue(a, .{ .key = "p", .kind = .port }, try Fx.expr(a, "65536"))).?);
    try testing.expect((try checkValue(a, .{ .key = "p", .kind = .count }, try Fx.expr(a, "65536"))) == null);
    try testing.expectEqual(Problem.not_bool, (try checkValue(a, .{ .key = "f", .kind = .flag }, try Fx.expr(a, "'on'"))).?);
}

const LitReader = struct {
    pub fn unknown(_: LitReader, _: []const u8, _: *const ast.Expr) !void {
        return error.UnknownOption;
    }
    pub fn value(_: LitReader, comptime F: type, _: Opt, e: *const ast.Expr) !?F {
        const lit = Fx.LitCfg{};
        if (F == u16) return lit.port(e);
        if (F == u32) return switch (cfgIntIn(u32, e)) {
            .ok => |v| v,
            .problem => error.BadOption,
        };
        if (F == []const u8) return lit.str(e);
        const v = (try lit.str(e)) orelse return null;
        if (F == Fx.sqlmod.TlsMode) return try lit.tls(v);
        if (F == Fx.dbconfig.DbAuth) return try lit.auth(v);
        @compileError("no reader for " ++ @typeName(F));
    }
};

test "bind over sqlserver's table plus fe_host/fe_port, aliased, reproduces parseDbConfig for every SQL connector" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bodies = [_][]const u8{
        "fe_host = 'fe.internal', user = 'etl'",
        "host = 'h', fe_port = 9031",
        "host = 'h', port = 1433, port = 1434, host = 'g'",
        "host = 'h', port = '7', database = 'db', password = 'pw', tls = 'require', auth = 'ntlm'",
        "host = 'h', domain = 'D', realm = 'R', kdc = 'K', spn = 'S', client_id = 'C', resource = 'Rs', token = 'T'",
        "host = 'h', buckets = 3, load_url = 'u', zz = 1, user = env('U')",
        "host = 'h', port = env('P')",
    };
    for ([_][]const u8{ "postgres", "mysql", "sqlserver", "starrocks" }) |connector| for (bodies) |body| {
        const c = try Fx.opts(a, connector, body);
        const today = try Fx.old.parseDbConfig(c, 9030, Fx.LitCfg{});
        var ours = Fx.env_mod.DbConfig{ .port = 9030 };
        try bind(Fx.env_mod.DbConfig, &db_read, .ignore, &db_aliases, &ours, c.config, LitReader{});
        try testing.expectEqualDeep(today, ours);
    };
}

const FileReader = struct {
    arena: std.mem.Allocator,
    pub fn unknown(_: FileReader, _: []const u8, _: *const ast.Expr) !void {
        return error.UnknownOption;
    }
    pub fn value(self: FileReader, comptime F: type, _: Opt, e: *const ast.Expr) !?F {
        if (F == u16 or F == u32) return switch (cfgIntIn(F, e)) {
            .ok => |v| v,
            .problem => error.BadOption,
        };
        const t: ?[]const u8 = switch (try cfgText(self.arena, e)) {
            .ok => |s| s,
            .unset => null,
            .problem => return error.BadOption,
        };
        if (F == ?[]const u8) return t;
        if (F == []const u8) return t orelse "";
        @compileError("no reader for " ++ @typeName(F));
    }
};

test "bind refuses an unknown file option, reads an unset env() as null and keeps the last of a repeated key" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sftpmod = @import("../../store/sftp.zig");
    const ftpmod = @import("../../store/ftp.zig");
    var c = sftpmod.Conn{ .host = "" };
    const conn = try Fx.opts(a, "sftp", "host = 'a', port = '2222', key_file = '/k', host = 'b', known_hosts = env('" ++ Fx.unset_var ++ "')");
    try bind(sftpmod.Conn, &sftp, .refuse, &.{}, &c, conn.config, FileReader{ .arena = a });
    try testing.expectEqualStrings("b", c.host);
    try testing.expectEqual(@as(u16, 2222), c.port);
    try testing.expectEqualStrings("/k", c.key_file.?);
    try testing.expect(c.known_hosts == null);
    try testing.expect(c.user == null);
    try testing.expect(c.password == null);

    var f = ftpmod.Conn{ .host = "" };
    const bad = try Fx.opts(a, "ftp", "host = 'a', key_file = '/k'");
    try testing.expectError(error.UnknownOption, bind(ftpmod.Conn, &ftp, .refuse, &.{}, &f, bad.config, FileReader{ .arena = a }));
    const range = try Fx.opts(a, "ftp", "host = 'a', port = 70000");
    try testing.expectError(error.BadOption, bind(ftpmod.Conn, &ftp, .refuse, &.{}, &f, range.config, FileReader{ .arena = a }));
}

const StreamReader = struct {
    arena: std.mem.Allocator,
    pub fn unknown(_: StreamReader, _: []const u8, _: *const ast.Expr) !void {}
    pub fn value(self: StreamReader, comptime F: type, _: Opt, e: *const ast.Expr) !?F {
        if (F == bool) return switch (cfgBool(e)) {
            .ok => |b| b,
            .problem => error.BadOption,
        };
        if (F == u16 or F == u32) return switch (cfgIntIn(F, e)) {
            .ok => |v| v,
            .problem => error.BadOption,
        };
        const t = switch (try cfgText(self.arena, e)) {
            .ok => |s| s,
            else => return error.BadOption,
        };
        if (F == []const u8) return t;
        if (F == u64) return Fx.env_mod.parseByteSize(t) orelse error.BadOption;
        if (@typeInfo(F) == .@"enum") return std.meta.stringToEnum(F, t) orelse error.BadOption;
        @compileError("no reader for " ++ @typeName(F));
    }
};

test "bind over the stream table with host/port/be_url aliases reproduces resolveStreamLoadConfig" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bodies = [_][]const u8{
        "database = 'd', user = 'u', password = 'p'",
        "database = 'd', user = 'u', password = 'p', host = 'h', fe_port = 9031, be_url = 'http://be:8040'",
        "database = 'd', user = 'u', password = 'p', fe_host = 'f', port = '9032', load_url = 'http://l', be_url = 'http://b'",
        "database = 'd', user = 'u', password = 'p', tls = 'require', buckets = 8, replication_num = '3'",
        "database = 'd', user = 'u', password = 'p', auto_create = false, label_prefix = 'lp', load_size = '64MB', compression = 'gzip'",
        "database = 'd', user = 'u', password = 'p', auto_create = 'true', zz = 1, domain = 'x'",
        "database = 'd', user = 'u', password = 'p', auto_create = 'false', buckets = 4294967295",
    };
    for (bodies) |body| {
        const c = try Fx.opts(a, "starrocks", body);
        const env = try Fx.env(a);
        const today = try Fx.old.resolveStreamLoadConfig(env, c, .starrocks);
        var ours = Fx.streamload.Config{ .flavor = .starrocks, .database = "", .errctx = env.errctx, .space = env.space };
        try bind(Fx.streamload.Config, &stream, .ignore, &stream_aliases, &ours, c.config, StreamReader{ .arena = a });
        try testing.expectEqualDeep(today, ours);
    }
}

test "bind sets a field's _explicit companion only when the option is read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var cfg = Fx.env_mod.DbConfig{ .port = 5432 };
    const none = try Fx.opts(a, "postgres", "host = 'h', port = env('P')");
    try bind(Fx.env_mod.DbConfig, &sql, .ignore, &.{}, &cfg, none.config, LitReader{});
    try testing.expect(!cfg.port_explicit);
    const some = try Fx.opts(a, "postgres", "host = 'h', port = 5433");
    try bind(Fx.env_mod.DbConfig, &sql, .ignore, &.{}, &cfg, some.config, LitReader{});
    try testing.expect(cfg.port_explicit);
    try testing.expectEqual(@as(u16, 5433), cfg.port);
}

const SmbReader = struct {
    arena: std.mem.Allocator,
    const Auth = @import("../../store/smb.zig").Auth;
    pub fn unknown(_: SmbReader, _: []const u8, _: *const ast.Expr) !void {
        return error.UnknownOption;
    }
    pub fn value(self: SmbReader, comptime F: type, opt: Opt, e: *const ast.Expr) !?F {
        const file = FileReader{ .arena = self.arena };
        if (F == ?Auth) {
            const t = ((try file.value(?[]const u8, opt, e)) orelse null) orelse "";
            if (!options.smbAuthValid(t)) return error.BadOption;
            return if (std.ascii.eqlIgnoreCase(t, "kerberos")) .kerberos else .ntlm;
        }
        const v = try file.value(F, opt, e) orelse return null;
        if (F != ?[]const u8) return v;
        const t = v orelse return @as(F, null);
        if (std.mem.eql(u8, opt.key, "share")) return std.mem.trim(u8, t, "/\\");
        if (std.mem.eql(u8, opt.key, "realm")) return try std.ascii.allocUpperString(self.arena, t);
        return t;
    }
};

test "bind into an smb connection: the reader trims share, uppercases realm and reads auth in any case" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const smbmod = @import("../../store/smb.zig");
    var c = smbmod.Conn{ .host = "" };
    const conn = try Fx.opts(a, "smb", "host = 'fs', port = 1445, share = '/data\\\\', realm = 'corp.local', auth = 'Kerberos', domain = 'CORP', kdc = 'k', spn = 's'");
    try bind(smbmod.Conn, &smb, .refuse, &.{}, &c, conn.config, SmbReader{ .arena = a });
    try testing.expectEqualStrings("fs", c.host);
    try testing.expectEqual(@as(u16, 1445), c.port);
    try testing.expectEqualStrings("data", c.share.?);
    try testing.expectEqualStrings("CORP.LOCAL", c.realm.?);
    try testing.expectEqual(smbmod.Auth.kerberos, c.auth.?);
    try testing.expectEqualStrings("CORP", c.domain.?);
    try testing.expect(c.user == null);

    var d = smbmod.Conn{ .host = "" };
    const ntlm = try Fx.opts(a, "smb", "host = 'fs', auth = 'NTLM'");
    try bind(smbmod.Conn, &smb, .refuse, &.{}, &d, ntlm.config, SmbReader{ .arena = a });
    try testing.expectEqual(smbmod.Auth.ntlm, d.auth.?);
    const bad = try Fx.opts(a, "smb", "host = 'fs', auth = 'basic'");
    try testing.expectError(error.BadOption, bind(smbmod.Conn, &smb, .refuse, &.{}, &d, bad.config, SmbReader{ .arena = a }));
}

fn expectSameRun(a: std.mem.Allocator, today: anytype, ours: anytype, today_msg: []const u8, ours_msg: []const u8) !void {
    const t = today catch |e| {
        try testing.expectError(e, ours);
        try testing.expectEqualStrings(today_msg, ours_msg);
        return;
    };
    const o = ours catch |e| {
        std.debug.print("old read it, new refused: {s}\n", .{ours_msg});
        return e;
    };
    try testing.expectEqualDeep(t, o);
    _ = a;
}

fn runPair(a: std.mem.Allocator, comptime f_old: anytype, comptime f_new: anytype, c: ast.Connection, extra: anytype) !void {
    const env_old = try Fx.env(a);
    const env_new = try Fx.env(a);
    const today = @call(.auto, f_old, .{ env_old, c } ++ extra);
    const ours = @call(.auto, f_new, .{ env_new, c } ++ extra);
    try expectSameRun(a, today, ours, env_old.diag.msg, env_new.diag.msg);
}

const set_var = "PATH";

test "file connections: bind gives the old loop's connection or its message, unknown keys after their value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bodies = [_][]const u8{
        "host = 'h'",                                                                                                      "host = 'h', port = 2222",                                        "host = 'h', port = '2222'",
        "host = 'h', port = 70000",                                                                                        "host = 'h', port = 'x'",                                         "host = 'h', port = -1",
        "host = 'h', port = env('P')",                                                                                     "host = 'h', zz = 1",                                             "host = 'h', zz = 1 + 2",
        "host = 'h', zz = env('" ++ Fx.unset_var ++ "')",                                                                  "host = 'h', user = 1 + 2",                                       "host = 'h', user = env('" ++ Fx.unset_var ++ "')",
        "host = 'h', user = 'a', user = env('" ++ Fx.unset_var ++ "')",                                                    "host = env('" ++ Fx.unset_var ++ "')",                           "host = ''",
        "host = 'h', host = ''",                                                                                           "user = 'u'",                                                     "host = 'h', password = secret('" ++ set_var ++ "')",
        "host = 'h', share = '/data\\\\', realm = 'corp.local'",                                                           "host = 'h', share = 's', share = env('" ++ Fx.unset_var ++ "')", "host = 'h', auth = 'Kerberos', realm = 'R'",
        "host = 'h', auth = 'kerberos'",                                                                                   "host = 'h', auth = 'kerberos', user = 'me@R'",                   "host = 'h', auth = 'NTLM'",
        "host = 'h', auth = 'basic'",                                                                                      "host = 'h', auth = env('" ++ Fx.unset_var ++ "')",               "host = 'h', auth = 3",
        "host = 'h', key_file = '/k', known_hosts = env('" ++ Fx.unset_var ++ "'), host_key = 'hk', key_passphrase = 'p'", "host = 'h', domain = 'D', kdc = 'K', spn = 'S'",                 "host = 'h', port = 0, zz = 1 + 2",
        "host = true, user = 7",
    };
    for (bodies) |body| {
        try runPair(a, Fx.old.sftpConn, Fx.register.sftpConn, try Fx.opts(a, "sftp", body), .{});
        try runPair(a, Fx.old.smbConn, Fx.register.smbConn, try Fx.opts(a, "smb", body), .{});
        try runPair(a, Fx.old.ftpConn, Fx.register.ftpConn, try Fx.opts(a, "ftp", body), .{});
    }
}

test "SQL connections: resolveDbConfig on bind gives the old config or its message" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bodies = [_][]const u8{
        "host = 'h'",                                          "fe_host = 'fe', user = 'etl'",                           "host = 'h', fe_port = 9031",
        "host = 'h', port = 1433, port = 1434, host = 'g'",    "host = 'h', port = '7', tls = 'require', auth = 'ntlm'", "host = 'h', domain = 'D', realm = 'R', kdc = 'K', spn = 'S', client_id = 'C', resource = 'Rs', token = 'T'",
        "host = 'h', port = 70000",                            "host = 'h', fe_port = 70000",                            "host = 'h', port = 'x'",
        "host = 'h', port = -1",                               "host = 'h', port = env('P')",                            "host = 'h', tls = 'maybe'",
        "host = 'h', tls = 1 + 2",                             "host = 'h', auth = 'basic'",                             "host = 'h', auth = 'ntlm'",
        "host = 'h', auth = 'kerberos'",                       "host = 'h', auth = 'kerberos', user = 'me@R'",           "host = 'h', auth = 'kerberos', realm = 'R'",
        "host = env('" ++ Fx.unset_var ++ "')",                "host = secret('" ++ set_var ++ "'), database = 'd'",     "user = 'u'",
        "host = 'h', buckets = 'x', zz = 1 + 2, load_url = 3", "host = 'h', database = 1 + 2",                           "host = ''",
        "host = 'h', user = 7, password = true",
    };
    for ([_][]const u8{ "postgres", "mysql", "sqlserver", "starrocks", "doris" }) |connector| for (bodies) |body| {
        try runPair(a, Fx.old.resolveDbConfig, Fx.dbconfig.resolveDbConfig, try Fx.opts(a, connector, body), .{@as(u16, 9030)});
    };
}

test "stream load: resolveStreamLoadConfig on bind gives the old config or its message" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bodies = [_][]const u8{
        "database = 'd'",                                                        "",                                                                           "database = ''",
        "database = 'd', host = 'h', fe_port = 9031, be_url = 'http://be:8040'", "database = 'd', fe_host = 'f', port = '9032', load_url = 'l', be_url = 'b'", "database = 'd', port = 70000",
        "database = 'd', fe_port = 70000",                                       "database = 'd', buckets = 4294967296",                                       "database = 'd', replication_num = -1",
        "database = 'd', buckets = 'x'",                                         "database = 'd', buckets = 4294967295, replication_num = '3'",                "database = 'd', auto_create = 'yes'",
        "database = 'd', auto_create = 'TRUE'",                                  "database = 'd', auto_create = false",                                        "database = 'd', auto_create = 1",
        "database = 'd', tls = 'require'",                                       "database = 'd', tls = 'maybe'",                                              "database = 'd', load_size = '64MB'",
        "database = 'd', load_size = '0'",                                       "database = 'd', load_size = 'lots'",                                         "database = 'd', compression = 'gzip'",
        "database = 'd', compression = 'zip'",                                   "database = 'd', label_prefix = 'lp'",                                        "database = 'd', user = env('" ++ Fx.unset_var ++ "')",
        "database = secret('" ++ set_var ++ "')",                                "database = 'd', zz = 1 + 2, domain = 'x'",                                   "database = 'd', user = 1 + 2",
        "database = 'd', password = 7",
    };
    for (bodies) |body| {
        const c = try Fx.opts(a, "starrocks", if (body.len == 0) "zz = 1" else body);
        try runPair(a, Fx.old.resolveStreamLoadConfig, Fx.sinkmod.resolveStreamLoadConfig, c, .{Fx.streamload.Flavor.starrocks});
        try runPair(a, Fx.old.resolveStreamLoadConfig, Fx.sinkmod.resolveStreamLoadConfig, c, .{Fx.streamload.Flavor.doris});
    }
}

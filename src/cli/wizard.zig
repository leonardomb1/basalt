//! `\\connect`: the connector forms, and the `CREATE CONNECTION` they build.

const Session = @import("repl.zig").Session;
const connTables = @import("catalog.zig").connTables;
const form = @import("form.zig");
const hilite = @import("hilite.zig");
const runBlock = @import("repl.zig").runBlock;
const saveDecls = @import("repl.zig").saveDecls;
const std = @import("std");

const Connector = struct {
    name: []const u8,
    blurb: []const u8,
    fields: []const Field,
    const Field = struct {
        key: []const u8,
        hint: []const u8 = "",
        default: []const u8 = "",
        secret: bool = false,
        int: bool = false,
        choices: []const []const u8 = &.{},
        omit: []const u8 = "",
        when: ?struct { key: []const u8, value: []const u8 } = null,
    };
};

const tls_choices: []const []const u8 = &.{ "off", "require", "insecure" };

const connectors = [_]Connector{
    .{ .name = "postgres", .blurb = "PostgreSQL (source and sink)", .fields = &.{
        .{ .key = "host", .default = "localhost" },
        .{ .key = "port", .default = "5432", .int = true },
        .{ .key = "database" },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "tls", .choices = tls_choices, .default = "off" },
    } },
    .{ .name = "mysql", .blurb = "MySQL / MariaDB (source and sink)", .fields = &.{
        .{ .key = "host", .default = "localhost" },
        .{ .key = "port", .default = "3306", .int = true },
        .{ .key = "database" },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "tls", .choices = tls_choices, .default = "off" },
    } },
    .{ .name = "sqlserver", .blurb = "SQL Server (source and sink; host\\INSTANCE resolves the port)", .fields = &.{
        .{ .key = "host", .hint = "host\\INSTANCE for a named instance" },
        .{ .key = "port", .hint = "blank: 1433, or the instance's", .int = true },
        .{ .key = "database" },
        .{ .key = "auth", .choices = &.{ "sql", "ntlm", "kerberos", "aad" }, .default = "sql", .omit = "sql", .hint = "a SQL login, a domain account, Entra ID" },
        .{ .key = "realm", .hint = "the domain's DNS name, as CORP.LOCAL", .when = .{ .key = "auth", .value = "kerberos" } },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "tls", .choices = tls_choices, .default = "require" },
    } },
    .{ .name = "starrocks", .blurb = "StarRocks (read through the FE, write by stream load)", .fields = &.{
        .{ .key = "host", .hint = "the FE" },
        .{ .key = "port", .default = "9030", .int = true, .hint = "the FE's query port" },
        .{ .key = "load_url", .default = "http://<be-host>:8040", .hint = "a BE or CN, for stream load" },
        .{ .key = "database" },
        .{ .key = "user", .default = "root" },
        .{ .key = "password", .secret = true },
    } },
    .{ .name = "doris", .blurb = "Apache Doris (read through the FE, write by stream load)", .fields = &.{
        .{ .key = "host", .hint = "the FE" },
        .{ .key = "port", .default = "9030", .int = true, .hint = "the FE's query port" },
        .{ .key = "load_url", .default = "http://<be-host>:8040", .hint = "a BE, for stream load" },
        .{ .key = "database" },
        .{ .key = "user", .default = "root" },
        .{ .key = "password", .secret = true },
    } },
    .{ .name = "sftp", .blurb = "an SFTP server (files read and written as sftp://<name>/path)", .fields = &.{
        .{ .key = "host" },
        .{ .key = "port", .default = "22", .int = true },
        .{ .key = "user" },
        .{ .key = "key_file", .hint = "blank for a password" },
        .{ .key = "password", .secret = true },
        .{ .key = "host_key", .hint = "pinned; blank for ~/.ssh/known_hosts" },
    } },
    .{ .name = "smb", .blurb = "a Windows file share or Samba server (files read and written as smb://<name>/path)", .fields = &.{
        .{ .key = "host" },
        .{ .key = "port", .default = "445", .int = true },
        .{ .key = "domain", .hint = "blank for a local account" },
        .{ .key = "realm", .hint = "Kerberos, as CORP.LOCAL; blank for NTLM" },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "share", .hint = "blank to name it in each path" },
    } },
    .{ .name = "http", .blurb = "a REST API (paginated sources, an endpoint sink)", .fields = &.{
        .{ .key = "base_url" },
        .{ .key = "auth", .choices = &.{ "none", "bearer", "basic" }, .default = "none", .omit = "none" },
    } },
};

const Wizard = struct {
    conn: Connector,
    user_ph: [96]u8 = undefined,
    pass_ph: [96]u8 = undefined,

    fn convention(name: []const u8, suffix: []const u8, buf: []u8) []const u8 {
        var n: usize = 0;
        for (name) |ch| {
            if (n + suffix.len + 1 >= buf.len) break;
            buf[n] = std.ascii.toUpper(ch);
            n += 1;
        }
        if (n == 0) {
            @memcpy(buf[0..4], "NAME");
            n = 4;
        }
        buf[n] = '_';
        @memcpy(buf[n + 1 ..][0..suffix.len], suffix);
        return buf[0 .. n + 1 + suffix.len];
    }

    fn refresh(ctx: *anyopaque, fields: []form.Field) void {
        const self: *Wizard = @ptrCast(@alignCast(ctx));
        const name = fields[0].value();
        for (self.conn.fields, fields[1..]) |cf, *f| {
            if (cf.when) |w| {
                f.hidden = true;
                for (self.conn.fields, fields[1..]) |other, of| if (std.mem.eql(u8, other.key, w.key)) {
                    f.hidden = !std.mem.eql(u8, of.value(), w.value);
                };
            }
            if (cf.default.len > 0) continue;
            if (cf.secret) {
                @memcpy(self.pass_ph[0..4], "env:");
                f.placeholder = self.pass_ph[0 .. 4 + convention(name, "PASS", self.pass_ph[4..]).len];
            } else if (std.mem.eql(u8, cf.key, "user")) {
                @memcpy(self.user_ph[0..4], "env:");
                f.placeholder = self.user_ph[0 .. 4 + convention(name, "USER", self.user_ph[4..]).len];
            }
        }
    }

    fn check(_: *anyopaque, fields: []form.Field) ?form.Form.Problem {
        const name = fields[0].value();
        if (name.len == 0) return .{ .field = 0, .why = "a name is needed — the script calls the connection by it" };
        if (!std.ascii.isAlphabetic(name[0])) return .{ .field = 0, .why = "a name starts with a letter" };
        for (name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_')
            return .{ .field = 0, .why = "a name is letters, digits and _" };
        return null;
    }
};

/// Blank credentials are left out, meaning the runtime's `env(NAME_USER)` and
/// `env(NAME_PASS)` convention; a typed password is never echoed.
pub fn connectWizard(alloc: std.mem.Allocator, type_arg: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    var term = form.Term.init(alloc, msg);
    var which: ?Connector = null;
    if (type_arg.len > 0) {
        for (connectors) |c| if (std.ascii.eqlIgnoreCase(c.name, type_arg)) {
            which = c;
        };
        if (which == null) {
            try msg.print("error: no connector `{s}` — one of", .{type_arg});
            for (connectors, 0..) |c, i| try msg.print("{s} {s}", .{ if (i == 0) "" else ",", c.name });
            return msg.writeAll("\n");
        }
    } else {
        var items: [connectors.len]form.Item = undefined;
        for (connectors, &items) |c, *it| it.* = .{ .name = c.name, .detail = c.blurb };
        var picker = form.Picker{ .title = "new connection", .items = &items };
        if (!try term.run(&picker)) return msg.writeAll("cancelled; nothing made\n");
        which = connectors[picker.focus];
    }
    const conn = which.?;

    var fields = std.array_list.Managed(form.Field).init(alloc);
    defer {
        for (fields.items) |*f| f.buf.deinit();
        fields.deinit();
    }
    try fields.append(form.Field.init(alloc, .{ .label = "name", .hint = "how scripts refer to it, e.g. erp" }));
    for (conn.fields) |cf| {
        var choice: usize = 0;
        for (cf.choices, 0..) |ch, i| if (std.mem.eql(u8, ch, cf.default)) {
            choice = i;
        };
        try fields.append(form.Field.init(alloc, .{
            .label = cf.key,
            .hint = cf.hint,
            .placeholder = cf.default,
            .secret = cf.secret,
            .digits = cf.int,
            .choices = cf.choices,
            .choice = choice,
        }));
    }
    var wiz = Wizard{ .conn = conn };
    var title_buf: [64]u8 = undefined;
    var f = form.Form{
        .title = std.fmt.bufPrint(&title_buf, "new {s} connection", .{conn.name}) catch "new connection",
        .fields = fields.items,
        .hooks = .{ .ctx = &wiz, .refresh = Wizard.refresh, .check = Wizard.check },
    };
    if (!try term.run(&f)) return msg.writeAll("cancelled; nothing made\n");

    const name = fields.items[0].value();
    var upper_buf: [96]u8 = undefined;
    const user_conv = Wizard.convention(name, "USER", &upper_buf);
    var pass_buf: [96]u8 = undefined;
    const pass_conv = Wizard.convention(name, "PASS", &pass_buf);

    var stmt = std.array_list.Managed(u8).init(alloc);
    defer stmt.deinit();
    var shown = std.array_list.Managed(u8).init(alloc);
    defer shown.deinit();
    const out = [_]*std.array_list.Managed(u8){ &stmt, &shown };
    for (out) |o| try o.writer().print("CREATE CONNECTION {s} TYPE {s} OPTIONS (", .{ name, conn.name });
    var first = true;
    for (conn.fields, fields.items[1..]) |cf, *fld| {
        if (fld.hidden) continue;
        var v = fld.value();
        if (cf.choices.len > 0 and std.mem.eql(u8, v, cf.omit)) continue;
        if (v.len == 0) {
            if (cf.secret or std.mem.eql(u8, cf.key, "user")) continue;
            if (std.mem.indexOfScalar(u8, cf.default, '<') != null) continue;
            v = cf.default;
        }
        if (v.len == 0) continue;
        const sep: []const u8 = if (first) "" else ", ";
        first = false;
        if (std.mem.startsWith(u8, v, "env:")) {
            const var_name = v[4..];
            if (std.mem.eql(u8, var_name, if (cf.secret) pass_conv else user_conv)) {
                first = sep.len == 0;
                continue;
            }
            for (out) |o| try o.writer().print("{s}{s} = env('{s}')", .{ sep, cf.key, var_name });
        } else if (cf.int) {
            for (out) |o| try o.writer().print("{s}{s} = {s}", .{ sep, cf.key, v });
        } else {
            for (out) |o| try o.writer().print("{s}{s} = '", .{ sep, cf.key });
            for (v) |ch| try stmt.appendSlice(if (ch == '\'') "''" else &.{ch});
            if (cf.secret) try shown.appendSlice("********") else for (v) |ch| try shown.appendSlice(if (ch == '\'') "''" else &.{ch});
            for (out) |o| try o.append('\'');
        }
    }
    for (out) |o| try o.appendSlice(");");
    try msg.writeAll("\n");
    try writeHighlighted(alloc, msg, shown.items, term.colors.off.len > 0);
    try msg.writeAll("\n");
    try runBlock(alloc, stmt.items, sess, msg);
    var declared = false;
    for (sess.decls.items.items) |e| if (e.kind == .connection and std.ascii.eqlIgnoreCase(e.name, name)) {
        declared = true;
    };
    if (!declared) return;

    if (!std.mem.eql(u8, conn.name, "http")) {
        var reach = form.Confirm{ .question = "reach it now?", .yes = true };
        if (try term.run(&reach) and reach.yes) {
            const reached = connTables(&sess.completer(), name).len > 0;
            try msg.print("  {s}\n", .{if (reached) "reached" else "could not reach it (or it has no tables) — \\c test retries; the connection stays declared"});
        }
    }
    var save = form.Confirm{ .question = "save to the startup file, so every session has it?", .yes = false };
    if (try term.run(&save) and save.yes) try saveDecls(alloc, "", sess, msg);
}

fn writeHighlighted(gpa: std.mem.Allocator, w: *std.Io.Writer, text: []const u8, color: bool) !void {
    if (!color) return w.writeAll(text);
    const styles = try gpa.alloc(hilite.Style, text.len);
    defer gpa.free(styles);
    hilite.scan(text, styles);
    var style: hilite.Style = .plain;
    for (text, styles) |ch, st| {
        if (st != style) {
            style = st;
            try w.writeAll(hilite.sgr(st));
        }
        try w.writeByte(ch);
    }
    try w.writeAll(hilite.sgr_reset);
}

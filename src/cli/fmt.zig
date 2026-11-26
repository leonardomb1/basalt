//! `basalt fmt` and the kernel's `format` op: a script laid out in the house style,
//! changing nothing but blanks, or refused and left as it was.
//!
//! The layout reads the text, not the AST: the parser expands functions, statistics
//! and other dialects' spellings and drops comments as it reads, so a script
//! printed back from its AST would be another script. Strings, `$$` bodies, quoted
//! names and comments are copied as written, `${...}` holes and http bodies within
//! them; between them only blanks change. A run inside a line becomes one space,
//! none after `(` or `[` nor before `)`, `]`, `,` or `;`, and a `,` is followed by
//! one; each line's indent is set by its nesting, blank lines collapse to one,
//! trailing blanks go, line ends become `\n` and the text ends in one. No line is
//! broken or joined, and no other characters that touch are parted: `x'0D0A'` is a
//! BYTES literal only so. The leading `@include` block keeps its lines, trailing blanks
//! cut. Keyword case is left as written: a word is a keyword only where the parser
//! reads one (`name`, `type` and `count` are columns too), which the text alone
//! cannot tell. There are no options, as `zig fmt` has none.
//!
//! Indentation is two spaces a level. A level opens at `(`, `BEGIN`, `FOR EACH` and
//! `CASE`, and closes at `)` or `END` (with the `FOR`, `CASE` or `IF` after it); a
//! line that opens with the closer stands where its opener's line stood. The top
//! level, a block, and parentheses that open on a query hold statements: a line
//! that carries a statement on from the line before stands two further in, unless
//! it opens with a clause keyword (`FROM`, `WHERE`, `JOIN`, `ORDER`, `AS`, …), and
//! a `;` ends the statement. Other parentheses (a list, a call's arguments) and a
//! `CASE` hold items, each at the level's indent; a `CASE` opens where a carried
//! line of its statement stands. A comment line is indented as any other.
//!
//! The safety check: the original and the formatted text are both parsed, with the
//! same includes and, in the kernel, after the session's declarations, and must
//! give the same program, positions aside (`ast.sameShape`). A script that does not
//! parse, or whose layout would read differently, is refused.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const include = @import("../lang/include.zig");
const remote = @import("../lang/remote.zig");
const sql_lexer = @import("../lang/sql_lexer.zig");
const builtins = @import("../exec/builtins.zig");
const loadSource = @import("args.zig").loadSource;
const loadExit = @import("args.zig").loadExit;
const unknownOption = @import("args.zig").unknownOption;

const Level = struct {
    /// The indent of the line the level opened on; a closer's line stands there.
    open: usize,
    statements: bool,
    paren: bool,
    /// A statement has begun at this level and not ended.
    started: bool = false,
    /// The level's header runs to the end of its line (`FOR EACH … AS (…)`,
    /// `AS BEGIN …`): what follows it starts afresh.
    header: bool = false,
};

const clause_words = [_][]const u8{
    "select",   "from",  "where",   "group",     "having", "qualify", "window", "order",   "limit", "offset",
    "fetch",    "union", "except",  "intersect", "join",   "inner",   "left",   "right",   "full",  "cross",
    "semi",     "anti",  "natural", "with",      "load",   "values",  "pivot",  "unpivot", "split", "jobs",
    "paginate", "retry", "upsert",  "append",    "as",
};

fn isIdentByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

fn wordAt(s: []const u8, i: usize) []const u8 {
    var e = i;
    while (e < s.len and isIdentByte(s[e])) e += 1;
    return s[i..e];
}

fn inWords(list: []const []const u8, w: []const u8) bool {
    for (list) |x| if (std.ascii.eqlIgnoreCase(x, w)) return true;
    return false;
}

/// The next word at or after `i` past blanks and comments, empty when none follows.
fn nextWord(s: []const u8, from: usize) []const u8 {
    var i = from;
    while (i < s.len) {
        if (std.ascii.isWhitespace(s[i])) {
            i += 1;
            continue;
        }
        if (sql_lexer.spanAt(s, i)) |sp| if (sp.kind == .line_comment or sp.kind == .block_comment) {
            i = sp.end;
            continue;
        };
        break;
    }
    return if (i < s.len and isIdentByte(s[i])) wordAt(s, i) else "";
}

const Out = struct {
    buf: std.array_list.Managed(u8),
    levels: std.array_list.Managed(Level),
    at_line_start: bool = true,
    space: bool = false,
    newlines: usize = 0,
    indent: usize = 0,
    after_end: bool = false,

    fn top(self: *Out) *Level {
        return &self.levels.items[self.levels.items.len - 1];
    }

    fn push(self: *Out, statements: bool, paren: bool, header: bool) !void {
        try self.levels.append(.{ .open = self.indent, .statements = statements, .paren = paren, .header = header });
    }

    /// A `CASE` opens where a carried line of its statement stands, as it continues one.
    fn pushCase(self: *Out) !void {
        const t = self.top();
        const carried = if (t.statements) (if (self.levels.items.len > 1) t.open + 2 else 0) + 2 else 0;
        try self.levels.append(.{ .open = @max(self.indent, carried), .statements = false, .paren = false });
    }

    fn pop(self: *Out, paren: bool) void {
        if (self.levels.items.len > 1 and self.top().paren == paren) _ = self.levels.pop();
    }

    /// Ends the line, or counts a blank one.
    fn newline(self: *Out) !void {
        if (!self.at_line_start) {
            try self.buf.append('\n');
            self.at_line_start = true;
            self.newlines = 1;
            const t = self.top();
            if (t.header) {
                t.header = false;
                t.started = false;
            }
        } else self.newlines += 1;
        self.space = false;
    }

    /// Starts the line `s[i..]` opens, at the indent its first word or closer asks.
    fn startLine(self: *Out, s: []const u8, i: usize) !void {
        if (self.buf.items.len > 0 and self.newlines >= 2) try self.buf.append('\n');
        const t = self.top();
        const w = wordAt(s, i);
        self.indent = if (s[i] == ')' and t.paren and self.levels.items.len > 1)
            t.open
        else if (std.ascii.eqlIgnoreCase(w, "end") and !t.paren and self.levels.items.len > 1)
            t.open
        else blk: {
            const base = if (self.levels.items.len > 1) t.open + 2 else 0;
            const carried = t.statements and t.started and !inWords(&clause_words, w);
            break :blk base + @as(usize, if (carried) 2 else 0);
        };
        try self.buf.appendNTimes(' ', self.indent);
        self.at_line_start = false;
        self.space = false;
    }

    /// Starts the line, or puts the space `s[i]` takes after what is already out:
    /// none after `(` or `[` or before `)`, `]`, `,` or `;`, one after `,`, else one
    /// where blanks stood.
    fn content(self: *Out, s: []const u8, i: usize, comment: bool) !void {
        if (self.at_line_start) {
            try self.startLine(s, i);
        } else {
            const prev = self.buf.items[self.buf.items.len - 1];
            const tight = std.mem.indexOfScalar(u8, ")],;", s[i]) != null or prev == '(' or prev == '[';
            if (!tight and (self.space or prev == ',')) try self.buf.append(' ');
            self.space = false;
        }
        if (!comment) self.top().started = true;
    }
};

/// The body's layout, as the file comment says.
fn layoutBody(out: *Out, s: []const u8) !void {
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (c == '\n') {
            try out.newline();
            i += 1;
            continue;
        }
        if (c == ' ' or c == '\t' or c == '\r') {
            if (!out.at_line_start) out.space = true;
            i += 1;
            continue;
        }
        if (sql_lexer.spanAt(s, i)) |sp| {
            const comment = sp.kind == .line_comment or sp.kind == .block_comment;
            try out.content(s, i, comment);
            const end = if (sp.kind == .line_comment) std.mem.trimRight(u8, s[i..sp.end], " \t\r").len + i else sp.end;
            try out.buf.appendSlice(s[i..end]);
            i = sp.end;
            continue;
        }
        try out.content(s, i, false);
        if (isIdentByte(c)) {
            const w = wordAt(s, i);
            try out.buf.appendSlice(w);
            i += w.len;
            const after_end = out.after_end;
            out.after_end = false;
            if (std.ascii.eqlIgnoreCase(w, "begin")) {
                try out.push(true, false, true);
            } else if (std.ascii.eqlIgnoreCase(w, "for") and !after_end and std.ascii.eqlIgnoreCase(nextWord(s, i), "each")) {
                try out.push(true, false, true);
            } else if (std.ascii.eqlIgnoreCase(w, "case") and !after_end) {
                try out.pushCase();
            } else if (std.ascii.eqlIgnoreCase(w, "end")) {
                out.pop(false);
                out.after_end = true;
            }
            continue;
        }
        try out.buf.append(c);
        i += 1;
        switch (c) {
            '(' => {
                const w = nextWord(s, i);
                try out.push(std.ascii.eqlIgnoreCase(w, "select") or std.ascii.eqlIgnoreCase(w, "with") or std.ascii.eqlIgnoreCase(w, "from"), true, false);
            },
            ')' => out.pop(true),
            ';' => out.top().started = false,
            else => {},
        }
    }
}

/// `text` laid out in the house style. It is not checked here: `format` checks it.
pub fn layout(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    var out = Out{ .buf = std.array_list.Managed(u8).init(arena), .levels = std.array_list.Managed(Level).init(arena) };
    try out.levels.append(.{ .open = 0, .statements = true, .paren = false });
    const cut = include.directivesEnd(text);
    var lines = std.mem.splitScalar(u8, text[0..cut], '\n');
    while (lines.next()) |ln| {
        if (lines.index == null and ln.len == 0) break;
        try out.buf.appendSlice(std.mem.trimRight(u8, ln, " \t\r"));
        try out.buf.append('\n');
    }
    try layoutBody(&out, text[cut..]);
    const trimmed = std.mem.trimRight(u8, out.buf.items, " \t\r\n");
    if (trimmed.len == 0) return "";
    out.buf.shrinkRetainingCapacity(trimmed.len);
    try out.buf.append('\n');
    return out.buf.items;
}

pub const Refusal = struct {
    msg: []const u8,
    /// Where the original fails to parse, when that is why: `label` or an included
    /// file's path, in its own lines, or `session` for the prelude, with no line.
    file: []const u8 = "",
    line: u32 = 0,
    col: u32 = 0,
};

pub const Formatted = union(enum) { ok: []const u8, refused: Refusal };

/// `text` laid out and checked: each parsed as `label`, its includes resolved
/// against `dir` (a remote one from the cache, never fetched), after `prelude`,
/// which ends in a newline or is empty. The prelude goes after the include block,
/// as the kernel places the session's declarations.
pub fn format(arena: std.mem.Allocator, text: []const u8, label: []const u8, dir: []const u8, prelude: []const u8) !Formatted {
    const laid = try layout(arena, text);
    var d1: include.Diag = .{};
    const before = parse(arena, text, label, dir, prelude, &d1) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ParseFailed => {
            const msg = try std.fmt.allocPrint(arena, "the script does not parse, so it is not formatted: {s}", .{d1.parse.msg});
            if (!std.mem.eql(u8, d1.label, label)) return .{ .refused = .{ .msg = msg, .file = d1.label, .line = d1.parse.line, .col = d1.parse.col } };
            const pre_lines: u32 = @intCast(std.mem.count(u8, prelude, "\n"));
            const head: u32 = @intCast(std.mem.count(u8, text[0..include.directivesEnd(text)], "\n"));
            if (d1.parse.line > head and d1.parse.line <= head + pre_lines) return .{ .refused = .{ .msg = msg, .file = "session" } };
            return .{ .refused = .{
                .msg = msg,
                .file = label,
                .line = if (d1.parse.line > head) d1.parse.line - pre_lines else d1.parse.line,
                .col = d1.parse.col,
            } };
        },
    };
    var d2: include.Diag = .{};
    const after = parse(arena, laid, label, dir, prelude, &d2) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ParseFailed => return .{ .refused = .{ .msg = "laid out, the script would no longer parse; it is left as it was" } },
    };
    if (!ast.sameShape(ast.Program, before, after))
        return .{ .refused = .{ .msg = "laid out, the script would read differently; it is left as it was" } };
    return .{ .ok = laid };
}

fn parse(arena: std.mem.Allocator, text: []const u8, label: []const u8, dir: []const u8, prelude: []const u8, diag: *include.Diag) !ast.Program {
    var src = text;
    if (prelude.len > 0) {
        const cut = include.directivesEnd(text);
        const sep = if (cut > 0 and text[cut - 1] != '\n') "\n" else "";
        src = try std.mem.concat(arena, u8, &.{ text[0..cut], sep, prelude, text[cut..] });
    }
    var resolver = remote.Resolver.init(arena, dir, null, false) catch null;
    return include.loadProgramOpts(arena, src, label, dir, diag, .{
        .parse = .{ .is_builtin = builtins.isBuiltin },
        .remote = if (resolver) |*r| r else null,
    });
}

/// `basalt fmt [--check] <script>|-|-c "<inline>"`: a file is rewritten in place when
/// its layout changes, stdin and `-c` print theirs; `--check` writes nothing and
/// exits 1 when the layout would change, naming the file.
pub fn cmdFmt(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writerStreaming(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [2048]u8 = undefined;
    var err_file = std.fs.File.stderr().writerStreaming(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var check = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            i += 1;
        } else if (std.mem.eql(u8, arg, "--check")) {
            check = true;
        } else if (try unknownOption(arg, "fmt", stderr)) return 2;
    }
    const src = loadSource(a, "fmt", args, stderr) catch |e| switch (e) {
        error.Usage, error.Unreadable => |le| return loadExit(le),
        else => return e,
    };
    const is_file = !std.mem.eql(u8, src.label, "<command>") and !std.mem.eql(u8, src.label, "<stdin>");
    const got = switch (try format(a, src.text, src.label, src.dir, "")) {
        .ok => |t| t,
        .refused => |r| {
            if (r.line > 0) {
                try stderr.print("error: {s}:{d}:{d}: {s}\n", .{ r.file, r.line, r.col, r.msg });
            } else try stderr.print("error: {s}: {s}\n", .{ src.label, r.msg });
            return 1;
        },
    };
    const changed = if (is_file)
        !std.mem.eql(u8, got, src.text)
    else
        !std.mem.eql(u8, std.mem.trimRight(u8, got, "\n"), std.mem.trimRight(u8, src.text, "\n"));
    if (check) {
        if (changed) try stdout.print("{s}: not formatted\n", .{src.label});
        return if (changed) 1 else 0;
    }
    if (!is_file) {
        try stdout.writeAll(got);
        return 0;
    }
    if (changed) std.fs.cwd().writeFile(.{ .sub_path = src.label, .data = got }) catch |e| {
        try stderr.print("error: cannot write `{s}`: {s}\n", .{ src.label, @errorName(e) });
        return 1;
    };
    return 0;
}

const testing = std.testing;

fn expectLayout(want: []const u8, text: []const u8) !void {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const got = try layout(ar.allocator(), text);
    try testing.expectEqualStrings(want, got);
    try testing.expectEqualStrings(want, try layout(ar.allocator(), got));
}

test "layout: clauses at the statement's indent, a carried line two in, blanks collapsed, comments and strings as written" {
    try expectLayout(
        \\-- totals
        \\SELECT a,
        \\  b -- the second
        \\FROM 't.csv'
        \\WHERE a = 'x    y'
        \\  AND b > 1;
        \\
        \\SELECT 2 AS c;
        \\
    ,
        "-- totals\n  SELECT   a,\n       b  -- the second   \n FROM 't.csv'\nWHERE a = 'x    y'\n   AND b >   1;\n\n\n\nSELECT 2 AS c;   \n\n",
    );
}

test "layout: a subquery, a list in parentheses, a CASE and a block each nest by two" {
    try expectLayout(
        \\WITH w AS (
        \\  SELECT x
        \\  FROM t
        \\)
        \\SELECT CASE
        \\    WHEN x > 1 THEN 'big'
        \\    ELSE 'small'
        \\  END AS size,
        \\  f(x,
        \\    2) AS y
        \\FROM w;
        \\CREATE FUNCTION p(a) AS BEGIN
        \\  PRINT $a;
        \\END;
        \\FOR EACH ROW OF ('n.csv') AS (n)
        \\  PRINT $n;
        \\END FOR;
        \\
    ,
        "WITH w AS (\nSELECT x\nFROM t\n)\nSELECT CASE\nWHEN x > 1 THEN 'big'\nELSE 'small'\nEND AS size,\nf(x,\n2) AS y\nFROM w;\nCREATE FUNCTION p(a) AS BEGIN\nPRINT $a;\nEND;\nFOR EACH ROW OF ('n.csv') AS (n)\nPRINT $n;\nEND FOR;\n",
    );
}

test "layout: touching characters keep touching, a $$ body and the include block stay as written" {
    try expectLayout(
        \\@include 'lib.sql';
        \\SELECT x'0D0A' AS b, $n, a
        \\FROM pg.QUERY($$SELECT  1
        \\     FROM   dual$$);
        \\
    ,
        "@include 'lib.sql';   \nSELECT  x'0D0A' AS b,   $n ,a\nFROM pg.QUERY( $$SELECT  1\n     FROM   dual$$ );",
    );
}

test "format: the laid-out script reads as the original; one that does not parse is refused where it fails" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ok = try format(a, "CREATE FUNCTION tax(x) AS x * 2;\nSELECT   tax(1) AS t,\n x'0A' AS b\n  FROM range(3);", "<test>", ".", "");
    try testing.expectEqualStrings("CREATE FUNCTION tax(x) AS x * 2;\nSELECT tax(1) AS t,\n  x'0A' AS b\nFROM range(3);\n", ok.ok);
    const bad = try format(a, "SELECT 1 AS a;\nSELECT FROM WHERE;", "<test>", ".", "");
    try testing.expectEqual(@as(u32, 2), bad.refused.line);
    const sess = try format(a, "SELECT   twice(2) AS v;", "<repl>", ".", "CREATE FUNCTION twice(x) AS x * 2;\n");
    try testing.expectEqualStrings("SELECT twice(2) AS v;\n", sess.ok);
}

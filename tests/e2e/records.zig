//! End-to-end reads of a file's raw records: `lines`, `fixed`, `split` and `chunks`
//! over local, gzipped and zip-member files, their arguments as constants, and what
//! check and run refuse.

const std = @import("std");
const basalt = @import("basalt");
const exec = @import("harness.zig").exec;
const Diag = basalt.env.Diag;
const harness = @import("harness.zig");
const checkAndRun = harness.checkAndRun;
const expectRefusedAlike = harness.expectRefusedAlike;
const expectFile = harness.expectFile;

test "lines, fixed, split and chunks: n, offset and the raw record" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.txt", .data = "a\r\nbb\n\nccc" });
    try tmp.dir.writeFile(.{ .sub_path = "f.txt", .data = "abc\r\ndef\nghi\n" });
    try tmp.dir.writeFile(.{ .sub_path = "e.edi", .data = "ISA*1'GS*2'ST'\n" });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT n, offset, rec, length(rec) AS len FROM lines('$B/l.txt');", 1, &.{});
    try expectFile(&tmp, "o.csv", "n,offset,rec,len\n1,0,a,1\n2,3,bb,2\n3,6,\"\",0\n4,7,ccc,3\n");
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT n, offset, rec FROM fixed('$B/f.txt', 3, TRUE);", 1, &.{});
    try expectFile(&tmp, "o.csv", "n,offset,rec\n1,0,abc\n2,5,def\n3,9,ghi\n");
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT n, offset, rec FROM split('$B/e.edi', '''') WHERE offset > 0;", 1, &.{});
    try expectFile(&tmp, "o.csv", "n,offset,rec\n2,6,GS*2\n3,11,ST\n");
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT n, offset, length(rec) AS len FROM chunks('$B/l.txt', 4);", 1, &.{});
    try expectFile(&tmp, "o.csv", "n,offset,len\n1,0,4\n2,4,4\n3,8,2\n");
}

test "records read through the stream: a gzipped file and a zip member" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = basalt.fixtures.two_members_zip });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/g.csv.gz' AS SELECT range AS v FROM RANGE(3000);", 1, &.{});
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT count(*) AS c, max(n) AS m, sum(length(rec)) AS b FROM lines('$B/g.csv.gz');", 1, &.{});
    try expectFile(&tmp, "o.csv", "c,m,b\n3001,3001,10891\n");
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT n, rec FROM lines('$B/t.zip :: a.csv');", 1, &.{});
    try expectFile(&tmp, "o.csv", "n,rec\n1,\"id,v\"\n2,\"1,x\"\n3,\"2,y\"\n");
}

test "a record source's path and sizes are constants: params, LETs, loop variables, expressions" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "x.txt", .data = "abcdef" });
    try checkAndRun(alloc, &tmp,
        \\PARAM w INT DEFAULT 2;
        \\LET stem = 'x';
        \\LOAD INTO '$B/o.csv' AS SELECT n, rec FROM fixed('$B/' || $stem || '.txt', $w + 1);
    , 1, &.{});
    try expectFile(&tmp, "o.csv", "n,rec\n1,abc\n2,def\n");
    try checkAndRun(alloc, &tmp,
        \\FOR EACH ROW OF (SELECT 'x' AS f) AS (f)
        \\  LOAD INTO '$B/o.csv' AS SELECT n, rec FROM chunks('$B/' || $f || '.txt', 4);
        \\END FOR;
    , 1, &.{});
    try expectFile(&tmp, "o.csv", "n,rec\n1,abcd\n2,ef\n");
}

test "the record sources' names stay free as CTE and column names" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/o.csv' AS WITH split AS (SELECT 1 AS lines, 2 AS fixed, 3 AS chunks, 4 AS split)
        \\SELECT lines, fixed, chunks, split, split_part('a-b', '-', 2) AS sp FROM split;
    , 1, &.{});
    try expectFile(&tmp, "o.csv", "lines,fixed,chunks,split,sp\n1,2,3,4,b\n");
}

test "check and run refuse a record source's bad arguments alike" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][2][]const u8{
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM fixed('$B/x.txt', 0);", "positive integer width" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM chunks('$B/x.txt', -4);", "positive integer size" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM split('$B/x.txt', '');", "non-empty text terminator" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM lines('$B/');", "not a `dir/` folder" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM lines(1);", "LINES takes a file path" },
        .{ "LOAD INTO '$B/o.csv' AS SELECT * FROM fixed('$B/x.txt', 3, 'yes');", "TRUE/FALSE skip_newlines" },
    }) |c| try expectRefusedAlike(alloc, &tmp, c[0], c[1]);
}

test "a short last fixed record, a missing file and a folder fail the run naming the file, on one lane or several" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "x.txt", .data = "abcdefgh" });
    try tmp.dir.makeDir("sub");
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    for ([_]usize{ 1, 4 }) |threads| for ([_][2][]const u8{
        .{ "fixed('{s}/x.txt', 3)", "x.txt`: the last record, at byte 6, is 2 bytes, short of the 3-byte width" },
        .{ "lines('{s}/nope.txt')", "nope.txt` (local file: FileNotFound)" },
        .{ "lines('{s}/sub')", "sub` is a folder; end it with `/` to read the files in it" },
    }) |c| {
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        const a = parena.allocator();
        const src = try std.mem.replaceOwned(u8, a, c[0], "{s}", base);
        const script = try std.fmt.allocPrint(a, "LOAD INTO '{s}/o.csv' AS SELECT * FROM {s};", .{ base, src });
        var rdiag: Diag = .{};
        const msg = if (exec(alloc, script, .{ .log = .{ .quiet = true }, .threads = threads }, &rdiag)) |_|
            return error.TestUnexpectedResult
        else |e| if (rdiag.msg.len > 0) rdiag.msg else basalt.runtime.failLabel(e);
        if (std.mem.indexOf(u8, msg, c[1]) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ c[1], msg });
            return error.TestUnexpectedResult;
        }
    };
}

test "lanes read a mapped file's records as one lane does: n, offset and bytes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var lines = std.array_list.Managed(u8).init(alloc);
    defer lines.deinit();
    var fixed = std.array_list.Managed(u8).init(alloc);
    defer fixed.deinit();
    for (0..1500) |i| {
        try lines.writer().print("{d}{s}", .{ i * 7919 % 100003, if (i % 3 == 0) "\r\n" else "\n" });
        try fixed.writer().print("{d:0>6}\r\n", .{i * 31 % 999983});
    }
    try tmp.dir.writeFile(.{ .sub_path = "l.txt", .data = lines.items });
    try tmp.dir.writeFile(.{ .sub_path = "f.txt", .data = fixed.items });
    for ([_][]const u8{ "lines('$B/l.txt')", "fixed('$B/f.txt', 6, TRUE)", "split('$B/l.txt', '1')", "chunks('$B/f.txt', 7)" }) |src| {
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        const a = parena.allocator();
        const script = try std.fmt.allocPrint(a, "LOAD INTO '$B/o.csv' AS SELECT n, offset, hex(rec) AS h FROM {s} WHERE n % 2 = 0;", .{src});
        try checkAndRun(alloc, &tmp, script, 1, &.{});
        const one = try tmp.dir.readFileAlloc(a, "o.csv", 1 << 24);
        try checkAndRun(alloc, &tmp, script, 4, &.{});
        try expectFile(&tmp, "o.csv", one);
    }
}

test "read_bytes and file_size read a file by range; a LET of bytes folds; a range past the end fails the run" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "h.bin", .data = "000042HEADER\x01\x02rest" });
    try checkAndRun(alloc, &tmp,
        \\LET h = read_bytes('$B/h.bin', 0, 6);
        \\LOAD INTO '$B/out.csv' AS
        \\SELECT int_at($h, 0, 6) AS id, file_size('$B/h.bin') AS size,
        \\       hex(read_bytes('$B/h.bin', 12, 2)) AS bin,
        \\       decode(read_bytes('$B/h.bin', range * 3 + 6, 3), 'utf8') AS part
        \\FROM RANGE(2);
    , 1, &.{});
    try expectFile(&tmp, "out.csv", "id,size,bin,part\n42,18,0102,HEA\n42,18,0102,DER\n");

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    const base = try tmp.dir.realpathAlloc(parena.allocator(), ".");
    const script = try std.fmt.allocPrint(parena.allocator(), "SELECT read_bytes('{s}/h.bin', 10, 20) AS x;", .{base});
    var rdiag: Diag = .{};
    try std.testing.expectError(error.CastFailed, exec(alloc, script, .{ .log = .{ .quiet = true } }, &rdiag));
}

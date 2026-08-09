//! One member of a local zip archive, as a byte stream.
//!
//! The member is inflated on demand and never materialized. That is the whole
//! point: the comparable implementations hold it in memory — duckdb-zipfs
//! documents that the selected file "will be read entirely into memory, not
//! streamed", and a gzip under Polars is decompressed up front — which puts a
//! ceiling at RAM. The archives this exists for are above it: Receita's CNPJ
//! registry ships about 7.6 GB a month and CVM's monthly `inf_diario` holds a
//! 59 MB CSV in a 12 MB zip.
//!
//! Local files only. A zip's central directory lives at the *end*, so a remote
//! archive needs a tail range request before anything else can be read;
//! `pqdecode` already does exactly that for a parquet footer, so the machinery
//! exists when this grows a URL path.

const std = @import("std");
const zip = std.zip;

pub const Error = error{
    ZipMemberNotFound,
    /// The archive holds more than one member and the script did not say which.
    /// Reading the first would be a plausible answer to a question nobody asked;
    /// pandas refuses the same case for the same reason.
    ZipMemberAmbiguous,
    /// Stored and deflated are the two methods a zip in the wild uses. The rest
    /// (bzip2, lzma, ppmd, xz) are legal in the container and essentially unseen.
    ZipMemberCompression,
};

/// Everything the stream borrows, kept together so it can live in the caller's
/// arena and never move: the reader interfaces hold pointers to each other.
pub const Member = struct {
    name: []const u8,
    /// The member's uncompressed bytes.
    reader: *std.Io.Reader,
    file: std.fs.File,
    fr: std.fs.File.Reader,
    limited: std.Io.Reader.Limited,
    inflate: std.compress.flate.Decompress,

    pub fn close(self: *Member) void {
        self.file.close();
    }
};

/// Read one central-directory entry's filename into `arena`.
fn entryName(arena: std.mem.Allocator, fr: *std.fs.File.Reader, e: zip.Iterator.Entry) ![]const u8 {
    const name = try arena.alloc(u8, e.filename_len);
    try fr.seekTo(e.header_zip_offset + @sizeOf(zip.CentralDirectoryFileHeader));
    try fr.interface.readSliceAll(name);
    return name;
}

/// Every member name in the archive, in central-directory order. Used to name the
/// choices when a script has to pick one.
pub fn names(arena: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const buf = try arena.alloc(u8, 64 * 1024);
    var fr = file.reader(buf);
    var it = try zip.Iterator.init(&fr);
    var out = std.array_list.Managed([]const u8).init(arena);
    while (try it.next()) |e| {
        const name = try entryName(arena, &fr, e);
        // Directory entries are structure, not data.
        if (name.len > 0 and name[name.len - 1] == '/') continue;
        try out.append(name);
    }
    return out.toOwnedSlice();
}

/// Open `want` inside `path`, or the archive's only member when `want` is null.
///
/// The returned `Member` is allocated in `arena` because its readers point at each
/// other; moving it by value would dangle those pointers.
pub fn openMember(arena: std.mem.Allocator, path: []const u8, want: ?[]const u8) !*Member {
    const m = try arena.create(Member);
    m.file = try std.fs.cwd().openFile(path, .{});
    errdefer m.file.close();

    const buf = try arena.alloc(u8, 64 * 1024);
    m.fr = m.file.reader(buf);
    var it = try zip.Iterator.init(&m.fr);

    var found: ?zip.Iterator.Entry = null;
    var found_name: []const u8 = "";
    var data_members: usize = 0;
    while (try it.next()) |e| {
        const name = try entryName(arena, &m.fr, e);
        if (name.len > 0 and name[name.len - 1] == '/') continue;
        data_members += 1;
        if (want) |w| {
            if (std.mem.eql(u8, name, w)) {
                found = e;
                found_name = name;
            }
        } else if (found == null) {
            found = e;
            found_name = name;
        }
    }
    if (want == null and data_members > 1) return Error.ZipMemberAmbiguous;
    const e = found orelse return Error.ZipMemberNotFound;
    switch (e.compression_method) {
        .store, .deflate => {},
        else => return Error.ZipMemberCompression,
    }

    // The central directory's copy of the sizes is authoritative, but the data
    // itself begins after the *local* header, whose name and extra fields have
    // their own lengths — a zip may pad the local extra field differently.
    try m.fr.seekTo(e.file_offset);
    const lh = try m.fr.interface.takeStruct(zip.LocalFileHeader, .little);
    if (!std.mem.eql(u8, &lh.signature, &zip.local_file_header_sig)) return error.ZipBadFileOffset;
    const data_off = e.file_offset + @sizeOf(zip.LocalFileHeader) + lh.filename_len + lh.extra_len;
    try m.fr.seekTo(data_off);

    // Stop at the member's end: the compressed stream is followed by the next
    // member, then the central directory. Inflate would stop on its own, but a
    // stored member has no terminator of its own.
    const lim_buf = try arena.alloc(u8, 64 * 1024);
    m.limited = std.Io.Reader.Limited.init(&m.fr.interface, .limited64(e.compressed_size), lim_buf);

    m.name = found_name;
    switch (e.compression_method) {
        .store => m.reader = &m.limited.interface,
        .deflate => {
            const window = try arena.alloc(u8, std.compress.flate.max_window_len);
            // `.raw`: a zip member is a bare deflate stream, with the checksum and
            // sizes held in the zip headers rather than a gzip footer.
            m.inflate = std.compress.flate.Decompress.init(&m.limited.interface, .raw, window);
            m.reader = &m.inflate.reader;
        },
        else => unreachable,
    }
    return m;
}

/// Committed fixtures, alongside the parquet ones. `two_members.zip` holds `a.csv`
/// stored and `b.csv` deflated, so both branches are exercised without depending
/// on a `zip` binary. Embedded and written out per test because the reader needs a
/// real path to seek in — the same shape the parquet fixture tests use.
const fx_two = @embedFile("testdata/two_members.zip");
const fx_one = @embedFile("testdata/one_member.zip");

fn writeFixture(a: std.mem.Allocator, tmp: *std.testing.TmpDir, name: []const u8, bytes: []const u8) ![]const u8 {
    try tmp.dir.writeFile(.{ .sub_path = name, .data = bytes });
    return tmp.dir.realpathAlloc(a, name);
}

test "openMember: a stored member streams and stops at its own end" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const m = try openMember(a, try writeFixture(a, &tmp, "t.zip", fx_two), "a.csv");
    defer m.close();
    // A stored member has no terminator of its own, so this is the case that
    // proves the read is bounded by the member's size rather than running on into
    // the next member and the central directory.
    const got = try m.reader.allocRemaining(a, .limited(1 << 20));
    try std.testing.expectEqualStrings("id,v\n1,x\n2,y\n", got);
}

test "openMember: a deflated member inflates" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const m = try openMember(a, try writeFixture(a, &tmp, "t.zip", fx_two), "b.csv");
    defer m.close();
    const got = try m.reader.allocRemaining(a, .limited(1 << 20));
    try std.testing.expect(std.mem.startsWith(u8, got, "id,v\n0,row0\n"));
    try std.testing.expect(std.mem.endsWith(u8, got, "499,row499\n"));
    try std.testing.expectEqual(@as(usize, 501), std.mem.count(u8, got, "\n"));
}

test "openMember: one member needs no name, several refuse to guess" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const one = try writeFixture(a, &tmp, "one.zip", fx_one);
    const solo = try openMember(a, one, null);
    defer solo.close();
    try std.testing.expectEqualStrings("only.csv", solo.name);
    try std.testing.expectEqualStrings("id,v\n7,solo\n", try solo.reader.allocRemaining(a, .limited(1 << 20)));

    const two = try writeFixture(a, &tmp, "two.zip", fx_two);
    try std.testing.expectError(Error.ZipMemberAmbiguous, openMember(a, two, null));
    try std.testing.expectError(Error.ZipMemberNotFound, openMember(a, two, "nope.csv"));

    const ns = try names(a, two);
    try std.testing.expectEqual(@as(usize, 2), ns.len);
    try std.testing.expectEqualStrings("a.csv", ns[0]);
    try std.testing.expectEqualStrings("b.csv", ns[1]);
}

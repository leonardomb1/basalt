//! One member of a zip archive, as a byte stream.
//!
//! The member is inflated on demand and never materialized. That is the point:
//! comparable implementations hold it in memory (duckdb-zipfs reads the selected
//! file "entirely into memory", Polars decompresses a gzip up front), which puts a
//! ceiling at RAM, and the public registry archives this exists for run to several
//! GB a month.
//!
//! A remote archive is read the same way, by range: a HEAD for the size, one GET
//! for the tail holding the central directory, one for the member's local header,
//! then a single ranged GET streamed through inflate. Other members are never
//! transferred. The data begins after the local header, whose name and extra
//! lengths may differ from the central directory's copy, so it is read too. The
//! read is bounded by the member's compressed size, since a stored member has no
//! terminator and would run on into the next member.
//!
//! Only stored and deflated members are read; the other legal methods (bzip2, lzma,
//! ppmd, xz) are essentially unseen. An archive with several members and no member
//! named is refused rather than reading the first, as pandas does. A `Member` lives
//! in the caller's arena and must never move: its reader interfaces point at each
//! other.

const std = @import("std");
const zip = std.zip;
const pqdecode = @import("pqdecode.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");

pub const Error = error{
    ZipMemberNotFound,
    ZipMemberAmbiguous,
    ZipMemberCompression,
    ZipNoEndRecord,
    ZipMultiDiskUnsupported,
    ZipBadCentralDirectory,
    ZipBadFileOffset,
};

pub const Member = struct {
    name: []const u8,
    reader: *std.Io.Reader,
    src: pqdecode.Bytes,
    body: union(enum) {
        file: std.fs.File.Reader,
        fixed: std.Io.Reader,
        http: struct {
            req: std.http.Client.Request,
            response: std.http.Client.Response,
            redirect_buf: [8 * 1024]u8,
        },
        sftp,
        smb,
    },
    limited: std.Io.Reader.Limited,
    inflate: std.compress.flate.Decompress,

    pub fn close(self: *Member) void {
        switch (self.body) {
            .http => |*h| h.req.deinit(),
            .file, .fixed, .sftp, .smb => {},
        }
        self.src.close();
    }
};

const Entry = struct {
    name: []const u8,
    method: zip.CompressionMethod,
    compressed_size: u64,
    local_offset: u64,
};

fn isMax(v: anytype) bool {
    return v == std.math.maxInt(@TypeOf(v));
}

fn openBytes(arena: std.mem.Allocator, path: []const u8) !pqdecode.Bytes {
    return pqdecode.Bytes.open(arena, path);
}

/// Every data member in central-directory order; directory entries are dropped.
/// Zip64 widens only the fields that overflowed, in a fixed order.
fn directory(arena: std.mem.Allocator, src: pqdecode.Bytes) ![]const Entry {
    const eocd_len = @sizeOf(zip.EndRecord);
    const total = src.size();
    if (total < eocd_len) return Error.ZipNoEndRecord;
    const tail_len: usize = @intCast(@min(total, eocd_len + std.math.maxInt(u16)));
    const tail_off = total - tail_len;
    const tail = try src.range(arena, tail_off, tail_len);

    const pos = blk: {
        var i = tail.len - eocd_len;
        while (true) : (i -= 1) {
            if (std.mem.eql(u8, tail[i..][0..4], &zip.end_record_sig) and
                i + eocd_len + std.mem.readInt(u16, tail[i + 20 ..][0..2], .little) == tail.len)
                break :blk i;
            if (i == 0) return Error.ZipNoEndRecord;
        }
    };
    var er = std.Io.Reader.fixed(tail[pos..]);
    const end = try er.takeStruct(zip.EndRecord, .little);
    if (end.disk_number != 0 or end.central_directory_disk_number != 0) return Error.ZipMultiDiskUnsupported;

    var count: u64 = end.record_count_total;
    var cd_size: u64 = end.central_directory_size;
    var cd_off: u64 = end.central_directory_offset;
    if (end.need_zip64()) {
        const loc_len = @sizeOf(zip.EndLocator64);
        if (pos < loc_len) return Error.ZipBadCentralDirectory;
        var lr = std.Io.Reader.fixed(tail[pos - loc_len .. pos]);
        const loc = try lr.takeStruct(zip.EndLocator64, .little);
        if (!std.mem.eql(u8, &loc.signature, &zip.end_locator64_sig)) return Error.ZipBadCentralDirectory;
        if (loc.total_disk_count != 1) return Error.ZipMultiDiskUnsupported;
        if (loc.record_file_offset + @sizeOf(zip.EndRecord64) > total) return Error.ZipBadCentralDirectory;
        var rr = std.Io.Reader.fixed(try src.range(arena, loc.record_file_offset, @sizeOf(zip.EndRecord64)));
        const end64 = try rr.takeStruct(zip.EndRecord64, .little);
        if (!std.mem.eql(u8, &end64.signature, &zip.end_record64_sig)) return Error.ZipBadCentralDirectory;
        count = end64.record_count_total;
        cd_size = end64.central_directory_size;
        cd_off = end64.central_directory_offset;
    }
    if (cd_off + cd_size > total) return Error.ZipBadCentralDirectory;

    const cd = if (cd_off >= tail_off)
        tail[@intCast(cd_off - tail_off)..][0..@intCast(cd_size)]
    else
        try src.range(arena, cd_off, @intCast(cd_size));

    var r = std.Io.Reader.fixed(cd);
    var out = std.array_list.Managed(Entry).init(arena);
    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const h = r.takeStruct(zip.CentralDirectoryFileHeader, .little) catch return Error.ZipBadCentralDirectory;
        if (!std.mem.eql(u8, &h.signature, &zip.central_file_header_sig)) return Error.ZipBadCentralDirectory;
        const name = r.take(h.filename_len) catch return Error.ZipBadCentralDirectory;
        const extra = r.take(h.extra_len) catch return Error.ZipBadCentralDirectory;
        r.discardAll(h.comment_len) catch return Error.ZipBadCentralDirectory;

        var e = Entry{
            .name = name,
            .method = h.compression_method,
            .compressed_size = h.compressed_size,
            .local_offset = h.local_file_header_offset,
        };
        if (isMax(h.uncompressed_size) or isMax(h.compressed_size) or isMax(h.local_file_header_offset)) {
            var xr = std.Io.Reader.fixed(extra);
            while (xr.takeInt(u16, .little)) |id| {
                const len = xr.takeInt(u16, .little) catch return Error.ZipBadCentralDirectory;
                const body = xr.take(len) catch return Error.ZipBadCentralDirectory;
                if (id != @intFromEnum(zip.ExtraHeader.zip64_info)) continue;
                var zr = std.Io.Reader.fixed(body);
                if (isMax(h.uncompressed_size)) _ = zr.takeInt(u64, .little) catch return Error.ZipBadCentralDirectory;
                if (isMax(h.compressed_size)) e.compressed_size = zr.takeInt(u64, .little) catch return Error.ZipBadCentralDirectory;
                if (isMax(h.local_file_header_offset)) e.local_offset = zr.takeInt(u64, .little) catch return Error.ZipBadCentralDirectory;
                break;
            } else |_| {}
        }
        if (name.len > 0 and name[name.len - 1] == '/') continue;
        try out.append(e);
    }
    return out.toOwnedSlice();
}

pub fn names(arena: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    const src = try openBytes(arena, path);
    defer src.close();
    const entries = try directory(arena, src);
    const out = try arena.alloc([]const u8, entries.len);
    for (entries, out) |e, *n| n.* = e.name;
    return out;
}

/// Open `want` inside `path`, or the archive's only member when `want` is null.
pub fn openMember(arena: std.mem.Allocator, path: []const u8, want: ?[]const u8) !*Member {
    const m = try arena.create(Member);
    m.src = try openBytes(arena, path);
    errdefer m.src.close();

    const entries = try directory(arena, m.src);
    const e = if (want) |w| blk: {
        for (entries) |e| if (std.mem.eql(u8, e.name, w)) break :blk e;
        return Error.ZipMemberNotFound;
    } else switch (entries.len) {
        0 => return Error.ZipMemberNotFound,
        1 => entries[0],
        else => return Error.ZipMemberAmbiguous,
    };
    switch (e.method) {
        .store, .deflate => {},
        else => return Error.ZipMemberCompression,
    }

    const lh_len = @sizeOf(zip.LocalFileHeader);
    if (e.local_offset + lh_len > m.src.size()) return Error.ZipBadFileOffset;
    var hr = std.Io.Reader.fixed(try m.src.range(arena, e.local_offset, lh_len));
    const lh = try hr.takeStruct(zip.LocalFileHeader, .little);
    if (!std.mem.eql(u8, &lh.signature, &zip.local_file_header_sig)) return Error.ZipBadFileOffset;
    const data_off = e.local_offset + lh_len + lh.filename_len + lh.extra_len;
    if (data_off + e.compressed_size > m.src.size()) return Error.ZipBadFileOffset;

    const inner: *std.Io.Reader = switch (m.src) {
        .memory => unreachable,
        .file => |x| blk: {
            m.body = .{ .file = x.f.reader(try arena.alloc(u8, 64 * 1024)) };
            try m.body.file.seekTo(data_off);
            break :blk &m.body.file.interface;
        },
        .remote => |rm| blk: {
            if (rm.whole != null or e.compressed_size == 0) {
                const w = rm.whole orelse "";
                if (e.compressed_size > 0 and data_off + e.compressed_size > w.len) return Error.ZipBadFileOffset;
                m.body = .{ .fixed = .fixed(if (e.compressed_size == 0) "" else w[@intCast(data_off)..][0..@intCast(e.compressed_size)]) };
                break :blk &m.body.fixed;
            }
            break :blk try streamRange(arena, m, rm, data_off, e.compressed_size);
        },
        .sftp => |f| blk: {
            m.body = .sftp;
            break :blk &(try sftp.Stream.window(arena, f, data_off, e.compressed_size)).interface;
        },
        .smb => |f| blk: {
            m.body = .smb;
            break :blk &(try smb.Stream.window(arena, f, data_off, e.compressed_size)).interface;
        },
    };

    const lim_buf = try arena.alloc(u8, 64 * 1024);
    m.limited = std.Io.Reader.Limited.init(inner, .limited64(e.compressed_size), lim_buf);

    m.name = e.name;
    switch (e.method) {
        .store => m.reader = &m.limited.interface,
        .deflate => {
            const window = try arena.alloc(u8, std.compress.flate.max_window_len);
            m.inflate = std.compress.flate.Decompress.init(&m.limited.interface, .raw, window);
            m.reader = &m.inflate.reader;
        },
        else => unreachable,
    }
    return m;
}

/// One identity-encoded GET for the member's compressed extent, streamed, so a
/// multi-gigabyte member costs a window of memory. A 200 (Range ignored) is skipped to `off`.
fn streamRange(arena: std.mem.Allocator, m: *Member, rm: *pqdecode.Remote, off: u64, len: u64) !*std.Io.Reader {
    const range = try std.fmt.allocPrint(arena, "bytes={d}-{d}", .{ off, off + len - 1 });
    const extra = try rm.headers(arena, .GET, range);
    const uri = std.Uri.parse(rm.url) catch return error.InvalidUrl;
    m.body = .{ .http = .{ .req = undefined, .response = undefined, .redirect_buf = undefined } };
    const h = &m.body.http;
    h.req = try rm.client.request(.GET, uri, .{ .extra_headers = extra, .headers = .{ .accept_encoding = .omit } });
    errdefer h.req.deinit();
    try h.req.sendBodiless();
    h.response = try h.req.receiveHead(&h.redirect_buf);
    const rdr = h.response.reader(try arena.alloc(u8, 64 * 1024));
    switch (@intFromEnum(h.response.head.status)) {
        206 => {},
        200 => try rdr.discardAll64(off),
        else => |code| return rm.statusError(code, ""),
    }
    return rdr;
}

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

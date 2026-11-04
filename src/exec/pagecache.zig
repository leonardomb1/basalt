//! The home for large blocks: every request of `min_block` bytes or more that the
//! process makes, rounded up to a power of two and kept for reuse once freed. A
//! run allocates and frees the same large buffers over and over (decompressed
//! pages, arena blocks, group stores, join builds and sorted runs, one lane after
//! another), and a block mapped fresh each time costs a page fault and a zeroed
//! page per 4 KiB touched: on TPC-H SF1 runs mapped 3-14x their peak memory, and
//! the kernel's faulting and zeroing took 15-35% of their time. A freed block goes
//! on its size class's list, pages still resident, and the next request of that
//! class takes it back, or halves a block of a class or few above it, since a
//! run's arenas grow to other sizes than the ones that freed their blocks; past
//! `cache_cap` bytes held, a freed block is unmapped, and unmapping is costly in
//! itself (each `munmap` shoots down every thread's TLB).
//! A reused block holds whatever its last owner wrote, as the `Allocator`
//! contract allows; safety-checked builds fill it with 0xaa so that code reading
//! memory it never wrote fails in tests rather than by luck.
//!
//! `huge` also maps a block of 2 MiB or more on a 2 MiB boundary and advises it to
//! the kernel as huge pages, for a buffer a lane fills whole before it reads it (a
//! row group's decoded columns): one fault per 2 MiB rather than per 4 KiB. Only
//! such buffers want it, since a huge page is zeroed whole on its first touch (as
//! the process allocator, the advice slowed CSV runs 3-8%). `large` gives the
//! same blocks without the advice. `process` is the release build's allocator:
//! requests under `min_block` go to `std.heap.smp_allocator`, the rest here.
//!
//! A block's size alone says which side owns it: a resize never moves a block
//! across `min_block`, nor out of its class except through `remap`, which moves
//! the mapping with `mremap` and keeps its pages. Off Linux everything passes to
//! the page allocator.

const std = @import("std");
const builtin = @import("builtin");

pub const min_block: usize = 64 << 10;
pub const cache_cap: usize = 512 << 20;
const split_classes = 3;

const huge_len: usize = 2 << 20;
const linux = builtin.os.tag == .linux;
const fill = std.debug.runtime_safety;

pub const huge: std.mem.Allocator = .{ .ptr = @ptrFromInt(1), .vtable = &block_vtable };
pub const large: std.mem.Allocator = .{ .ptr = @ptrFromInt(2), .vtable = &block_vtable };
pub const process: std.mem.Allocator = .{ .ptr = undefined, .vtable = &process_vtable };

const block_vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
const process_vtable = std.mem.Allocator.VTable{ .alloc = pAlloc, .resize = pResize, .remap = pRemap, .free = pFree };
const small = std.heap.smp_allocator;

const Free = struct { next: ?*Free };

const Cache = struct {
    mtx: std.Thread.Mutex = .{},
    lists: [64]?*Free = @splat(null),
    held: usize = 0,
};

var cache: Cache = .{};

fn classLen(len: usize) usize {
    return std.math.ceilPowerOfTwoAssert(usize, @max(len, min_block));
}

fn classOf(len: usize) usize {
    return std.math.log2_int(usize, classLen(len));
}

fn advised(ctx: *anyopaque) bool {
    return @intFromPtr(ctx) == 1;
}

/// A cached block of `class` on an `alignment` boundary, or one of up to
/// `split_classes` classes above it halved down to `class`, the halves not taken
/// kept on their own lists: a run's arenas grow to other sizes than the ones
/// that freed their blocks, and a part of a mapping is a mapping of its own.
fn take(class: usize, alignment: usize) ?[*]u8 {
    cache.mtx.lock();
    defer cache.mtx.unlock();
    for (class..@min(class + split_classes + 1, cache.lists.len)) |c| {
        const f = pop(c, alignment) orelse continue;
        var k = c;
        while (k > class) {
            k -= 1;
            const half: *Free = @ptrFromInt(@intFromPtr(f) + (@as(usize, 1) << @intCast(k)));
            half.next = cache.lists[k];
            cache.lists[k] = half;
        }
        cache.held -= @as(usize, 1) << @intCast(class);
        return @ptrCast(f);
    }
    return null;
}

fn pop(class: usize, alignment: usize) ?*Free {
    var prev: *?*Free = &cache.lists[class];
    while (prev.*) |f| : (prev = &f.next) {
        if (@intFromPtr(f) % alignment != 0) continue;
        prev.* = f.next;
        return f;
    }
    return null;
}

fn keep(ptr: [*]u8, class: usize) bool {
    const n = @as(usize, 1) << @intCast(class);
    cache.mtx.lock();
    defer cache.mtx.unlock();
    if (cache.held + n > cache_cap) return false;
    const f: *Free = @ptrCast(@alignCast(ptr));
    f.next = cache.lists[class];
    cache.lists[class] = f;
    cache.held += n;
    return true;
}

/// A fresh mapping of `len` bytes on an `alignment` boundary, advised to the
/// kernel as huge pages when `advise` and it is a multiple of 2 MiB.
fn map(len: usize, alignment: usize, advise: bool) ?[*]u8 {
    const raw = std.posix.mmap(null, len + alignment - std.heap.pageSize(), std.posix.PROT.READ | std.posix.PROT.WRITE, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0) catch return null;
    const base = @intFromPtr(raw.ptr);
    const start = std.mem.alignForward(usize, base, alignment);
    if (start > base) std.posix.munmap(@alignCast(raw[0 .. start - base]));
    const end = base + raw.len;
    if (end > start + len) std.posix.munmap(@alignCast(@as([*]u8, @ptrFromInt(start + len))[0 .. end - start - len]));
    const ptr: [*]align(std.heap.page_size_min) u8 = @ptrFromInt(start);
    if (advise and len % huge_len == 0) std.posix.madvise(ptr, len, std.os.linux.MADV.HUGEPAGE) catch {};
    return ptr;
}

fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    if (!linux) return std.heap.page_allocator.rawAlloc(len, alignment, ra);
    const n = classLen(len);
    const advise = advised(ctx) and n >= huge_len;
    const want = @max(alignment.toByteUnits(), if (advise) huge_len else std.heap.pageSize());
    if (want > n) return null;
    if (take(classOf(len), want)) |p| {
        if (fill) @memset(p[0..len], 0xaa);
        return p;
    }
    return map(n, want, advise);
}

fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
    if (!linux) return std.heap.page_allocator.rawResize(memory, alignment, new_len, ra);
    return classLen(new_len) == classLen(memory.len);
}

fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
    if (!linux) return std.heap.page_allocator.rawRemap(memory, alignment, new_len, ra);
    if (resize(ctx, memory, alignment, new_len, ra)) return memory.ptr;
    if (alignment.toByteUnits() > std.heap.pageSize()) return null;
    const old: []align(std.heap.page_size_min) u8 = @alignCast(memory.ptr[0..classLen(memory.len)]);
    const moved = std.posix.mremap(old.ptr, old.len, classLen(new_len), .{ .MAYMOVE = true }, null) catch return null;
    return moved.ptr;
}

fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
    if (!linux) return std.heap.page_allocator.rawFree(memory, alignment, ra);
    if (keep(memory.ptr, classOf(memory.len))) return;
    std.posix.munmap(@alignCast(memory.ptr[0..classLen(memory.len)]));
}

fn pAlloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    if (len < min_block) return small.rawAlloc(len, alignment, ra);
    return large.rawAlloc(len, alignment, ra);
}

fn pResize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
    if ((memory.len < min_block) != (new_len < min_block)) return false;
    if (memory.len < min_block) return small.rawResize(memory, alignment, new_len, ra);
    return large.rawResize(memory, alignment, new_len, ra);
}

fn pRemap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
    if ((memory.len < min_block) != (new_len < min_block)) return null;
    if (memory.len < min_block) return small.rawRemap(memory, alignment, new_len, ra);
    return large.rawRemap(memory, alignment, new_len, ra);
}

fn pFree(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
    if (memory.len < min_block) return small.rawFree(memory, alignment, ra);
    large.rawFree(memory, alignment, ra);
}

test "pages: a freed block is reused with its pages, filled in safe builds; huge blocks are 2 MiB aligned; growth keeps contents" {
    const a = large;
    const one = try a.alloc(u8, 100 << 10);
    @memset(one, 7);
    const at = one.ptr;
    a.free(one);
    const two = try a.alloc(u8, 120 << 10);
    defer a.free(two);
    if (linux) try std.testing.expectEqual(at, two.ptr);
    if (linux and fill) try std.testing.expectEqual(@as(u8, 0xaa), two[0]);
    try std.testing.expect(a.resize(two, 128 << 10) or !linux);
    try std.testing.expect(!a.resize(two, 129 << 10));

    const big = try huge.alloc(u8, huge_len + 100);
    defer huge.free(big);
    try std.testing.expect(@intFromPtr(big.ptr) % huge_len == 0 or !linux);

    var grow = try a.alloc(u8, 70 << 10);
    grow[100] = 3;
    grow = try a.realloc(grow, 5 << 20);
    try std.testing.expectEqual(@as(u8, 3), grow[100]);
    grow[(5 << 20) - 1] = 4;
    grow = try a.realloc(grow, 80 << 10);
    try std.testing.expectEqual(@as(u8, 3), grow[100]);
    a.free(grow);

    const fresh = cache.lists[18] == null and cache.lists[19] == null;
    const whole = try a.alloc(u8, 1 << 20);
    const whole_at = @intFromPtr(whole.ptr);
    a.free(whole);
    const quarter = try a.alloc(u8, 256 << 10);
    const half = try a.alloc(u8, 512 << 10);
    if (linux and fresh) {
        try std.testing.expectEqual(whole_at, @intFromPtr(quarter.ptr));
        try std.testing.expectEqual(whole_at + (512 << 10), @intFromPtr(half.ptr));
    }
    @memset(half, 1);
    @memset(quarter, 2);
    try std.testing.expectEqual(@as(u8, 1), half[0]);
    a.free(quarter);
    a.free(half);

    const p = process;
    const s = try p.alloc(u8, 100);
    defer p.free(s);
    const l = try p.alloc(u8, 1 << 20);
    defer p.free(l);
    try std.testing.expect(!p.resize(s, 1 << 20));
    try std.testing.expect(!p.resize(l, 100));
}

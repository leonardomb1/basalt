//! An allocator for large, short-lived buffers that a lane fills and refills: a
//! request of 2 MiB or more is mapped on a 2 MiB boundary, rounded up to whole 2 MiB
//! pages, and advised to the kernel as huge pages, so that touching it costs one
//! fault per 2 MiB rather than one per 4 KiB (a lane's row group faulted in 4 KiB at
//! a time spent as long in the kernel as decoding it). Smaller requests go to the
//! page allocator. A resize never moves a buffer across 2 MiB, so its size alone says
//! which side owns it. Only buffers filled whole belong here: a huge page is zeroed
//! whole on its first touch, so one barely used costs more than its 4 KiB pages
//! would (as the process allocator, it slowed CSV runs 3-8%). Where the kernel gives
//! no huge pages the advice is ignored and the buffer behaves as any other mapping.

const std = @import("std");
const builtin = @import("builtin");

const huge = 2 << 20;

pub const pages: std.mem.Allocator = .{ .ptr = undefined, .vtable = &vtable };

const small = std.heap.page_allocator;

const vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

fn eligible(len: usize) bool {
    return builtin.os.tag == .linux and len >= huge;
}

fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    if (!eligible(len)) return small.rawAlloc(len, alignment, ra);
    if (alignment.toByteUnits() > huge) return null;
    const size = std.mem.alignForward(usize, len, huge);
    const raw = std.posix.mmap(null, size + huge, std.posix.PROT.READ | std.posix.PROT.WRITE, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0) catch return null;
    const base = @intFromPtr(raw.ptr);
    const start = std.mem.alignForward(usize, base, huge);
    if (start > base) std.posix.munmap(@alignCast(raw[0 .. start - base]));
    const tail = base + size + huge - (start + size);
    if (tail > 0) std.posix.munmap(@alignCast(@as([*]u8, @ptrFromInt(start + size))[0..tail]));
    const ptr: [*]align(std.heap.page_size_min) u8 = @ptrFromInt(start);
    std.posix.madvise(ptr, size, std.os.linux.MADV.HUGEPAGE) catch {};
    return ptr;
}

fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
    if (!eligible(memory.len)) {
        if (eligible(new_len)) return false;
        return small.rawResize(memory, alignment, new_len, ra);
    }
    return eligible(new_len) and std.mem.alignForward(usize, new_len, huge) == std.mem.alignForward(usize, memory.len, huge);
}

/// A buffer of huge pages that grows or shrinks past its rounding is moved by the
/// kernel, its pages kept, as the page allocator's are; the new place may lose the
/// 2 MiB alignment, which costs only the huge pages.
fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
    if (!eligible(memory.len) and !eligible(new_len)) return small.rawRemap(memory, alignment, new_len, ra);
    if (!eligible(memory.len) or !eligible(new_len)) return null;
    if (resize(ctx, memory, alignment, new_len, ra)) return memory.ptr;
    const old: []align(std.heap.page_size_min) u8 = @alignCast(memory.ptr[0..std.mem.alignForward(usize, memory.len, huge)]);
    const moved = std.posix.mremap(old.ptr, old.len, std.mem.alignForward(usize, new_len, huge), .{ .MAYMOVE = true }, null) catch return null;
    return moved.ptr;
}

fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
    if (!eligible(memory.len)) return small.rawFree(memory, alignment, ra);
    std.posix.munmap(@alignCast(memory.ptr[0..std.mem.alignForward(usize, memory.len, huge)]));
}

test "hugepages: large buffers are 2 MiB aligned, writable, and resize within their pages; small ones pass through" {
    const a = pages;
    const big = try a.alloc(u8, huge + 100);
    defer a.free(big);
    try std.testing.expect(@intFromPtr(big.ptr) % huge == 0 or builtin.os.tag != .linux);
    @memset(big, 7);
    try std.testing.expectEqual(@as(u8, 7), big[big.len - 1]);
    try std.testing.expect(a.resize(big, 2 * huge) or builtin.os.tag != .linux);
    try std.testing.expect(!a.resize(big, 3 * huge));
    var grow = try a.alloc(u8, 4096);
    grow[4095] = 1;
    grow = try a.realloc(grow, huge * 2);
    try std.testing.expectEqual(@as(u8, 1), grow[4095]);
    grow = try a.realloc(grow, huge * 5);
    try std.testing.expectEqual(@as(u8, 1), grow[4095]);
    grow[huge * 5 - 1] = 2;
    grow = try a.realloc(grow, 100);
    a.free(grow);
}

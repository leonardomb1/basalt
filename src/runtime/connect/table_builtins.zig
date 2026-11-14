//! The table-builtin registry, the table-valued counterpart of the scalar one
//! (`exec/builtins.zig`): one `TableBuiltin` per signature in
//! `lang/table_builtins.zig`, read as `FROM name(args)`. The parser reads a call by
//! its signature; `check`, EXPLAIN and the run each `lookup` the read's entry here.
//! An entry adds to its signature the columns `check` sees, its EXPLAIN label, how
//! it opens a `driver.Source` and, for a record reader, the record `Mode` its
//! arguments give, which lets a local file split into lanes.
//! An argument is any plan-time constant (a literal, a `$param`, a loop variable or
//! an expression of them), folded and held to its kind by `argValue`.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const signature = @import("../../lang/table_builtins.zig");
const Value = @import("../../exec/value.zig").Value;
const driver = @import("../../exec/driver.zig");
const gen = @import("../../exec/gen.zig");
const folder = @import("../../store/folder.zig");
const eval = @import("../../exec/eval.zig");
const records = @import("../../format/records/read.zig");

pub const TableBuiltin = struct {
    sig: *const signature.Signature,
    schema: types.Schema,
    label: []const u8,
    mode: ?*const fn (args: []const Value) records.Mode,
    open: *const fn (arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source,
};

pub const table = [_]TableBuiltin{
    .{
        .sig = signature.named("range"),
        .schema = .{ .fields = &gen.RangeSource.fields },
        .label = "range",
        .mode = null,
        .open = openRange,
    },
    .{
        .sig = signature.named("lines"),
        .schema = .{ .fields = &records.fields },
        .label = "lines",
        .mode = modeLines,
        .open = openLines,
    },
    .{
        .sig = signature.named("fixed"),
        .schema = .{ .fields = &records.fields },
        .label = "fixed",
        .mode = modeFixed,
        .open = openFixed,
    },
    .{
        .sig = signature.named("split"),
        .schema = .{ .fields = &records.fields },
        .label = "split",
        .mode = modeSplit,
        .open = openSplit,
    },
    .{
        .sig = signature.named("chunks"),
        .schema = .{ .fields = &records.fields },
        .label = "chunks",
        .mode = modeChunks,
        .open = openChunks,
    },
};

/// The entry named `name`, in any case.
pub fn lookup(name: []const u8) ?*const TableBuiltin {
    for (&table) |*b| if (std.ascii.eqlIgnoreCase(b.sig.name, name)) return b;
    return null;
}

/// The entry a parsed `FROM name(args)` read names.
pub fn of(call: ast.BuiltinRead) *const TableBuiltin {
    return lookup(call.name).?;
}

/// An argument, its params already substituted, folded to its value; null when it
/// does not fold or is not one of `kind`: a size is a positive integer up to the
/// record cap, a path non-empty text naming a file, a text non-empty.
pub fn argValue(arena: std.mem.Allocator, kind: signature.ArgKind, e: *const ast.Expr) error{OutOfMemory}!?Value {
    const v = eval.constEval(arena, e, &.{}, &.{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    const ok = switch (kind) {
        .int => v == .int,
        .size => v == .int and v.int > 0 and v.int <= records.max_record,
        .path => v == .string and v.string.len > 0 and !folder.isFolder(v.string),
        .text => v == .string and v.string.len > 0,
        .bool => v == .bool,
    };
    return if (ok) v else null;
}

/// The path a builtin reads, for a message about opening it; null when it reads none.
pub fn pathArg(b: *const TableBuiltin, args: []const Value) ?[]const u8 {
    for (b.sig.args[0..args.len], args) |kind, v| if (kind == .path) return v.string;
    return null;
}

/// `range(hi)` is `range(0, hi)`.
fn openRange(arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source {
    _ = arena;
    const lo = if (args.len == 2) args[0].int else 0;
    const s = try gen.RangeSource.open(gpa, lo, args[args.len - 1].int);
    return s.source();
}

fn openLines(arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source {
    return openRecords(arena, gpa, args, modeLines(args));
}

fn openFixed(arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source {
    return openRecords(arena, gpa, args, modeFixed(args));
}

fn openSplit(arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source {
    return openRecords(arena, gpa, args, modeSplit(args));
}

fn openChunks(arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source {
    return openRecords(arena, gpa, args, modeChunks(args));
}

fn openRecords(arena: std.mem.Allocator, gpa: std.mem.Allocator, args: []const Value, mode: records.Mode) anyerror!driver.Source {
    const s = try records.RecordSource.open(arena, gpa, args[0].string, mode);
    return s.source();
}

fn modeLines(_: []const Value) records.Mode {
    return .lines;
}

/// `fixed(p, width)` does not skip line ends between records.
fn modeFixed(args: []const Value) records.Mode {
    return .{ .fixed = .{ .width = @intCast(args[1].int), .skip_newlines = args.len > 2 and args[2].bool } };
}

fn modeSplit(args: []const Value) records.Mode {
    return .{ .split = args[1].string };
}

fn modeChunks(args: []const Value) records.Mode {
    return .{ .chunks = @intCast(args[1].int) };
}

test "table builtins: one run entry per signature, found through lookup in any case" {
    try std.testing.expectEqual(signature.signatures.len, table.len);
    for (&signature.signatures) |*sig| try std.testing.expectEqual(sig, lookup(sig.name).?.sig);
    try std.testing.expectEqual(lookup("range").?, lookup("RANGE").?);
    try std.testing.expect(lookup("nope") == null);
}

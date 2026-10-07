//! Window items: `f(...) OVER (PARTITION BY ... ORDER BY ... ROWS ...)`.

const Parser = @import("../sql_parser.zig").Parser;
const Error = @import("../sql_parser.zig").Error;
const ast = @import("../ast.zig");
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const std = @import("std");

/// `<fn>(...) OVER (PARTITION BY .. ORDER BY .. [ROWS ...]) [AS name]` as a whole item,
/// or false with nothing consumed. Functions in a query share one PARTITION/ORDER BY,
/// each with its own frame; ranking and offsets require an ORDER BY.
pub fn parseWindowItem(
    self: *Parser,
    funcs: *std.array_list.Managed(ast.WindowFunc),
    part: *std.array_list.Managed(ast.QualName),
    ord: *std.array_list.Managed(ast.SortKey),
) Error!bool {
    if (!self.at(.ident)) return false;
    const kind: ast.WinKind = blk: {
        var buf: [64]u8 = undefined;
        const t = self.cur().text;
        const low = if (t.len <= buf.len) std.ascii.lowerString(buf[0..t.len], t) else t;
        if (std.mem.eql(u8, low, "row_number")) break :blk .row_number;
        if (std.mem.eql(u8, low, "rank")) break :blk .rank;
        if (std.mem.eql(u8, low, "dense_rank")) break :blk .dense_rank;
        if (std.mem.eql(u8, low, "lag")) break :blk .lag;
        if (std.mem.eql(u8, low, "lead")) break :blk .lead;
        if (std.mem.eql(u8, low, "sum")) break :blk .sum;
        if (std.mem.eql(u8, low, "count")) break :blk .count;
        if (std.mem.eql(u8, low, "min")) break :blk .min;
        if (std.mem.eql(u8, low, "max")) break :blk .max;
        if (std.mem.eql(u8, low, "avg")) break :blk .avg;
        if (self.peekTag() == .lparen and self.overFollows(self.i))
            return self.fail(self.curPos(), "`{s}` is not a window function — over a window, basalt computes sum, count, min, max, avg, row_number, rank, dense_rank, lag and lead", .{low});
        return false;
    };
    if (self.peekTag() != .lparen) return false;
    const save = self.i;
    _ = self.advance();
    _ = self.advance();
    var arg: ?ast.QualName = null;
    var offset: i64 = 1;
    if (kind == .sum or kind == .count or kind == .min or kind == .max or kind == .avg) {
        if (self.eat(.star)) {
            if (kind != .count) {
                return self.notWindow(save);
            }
        } else if (self.at(.ident)) {
            arg = try self.singleName(self.advance().text);
        } else {
            return self.notWindow(save);
        }
    } else if (kind == .lag or kind == .lead) {
        if (!self.at(.ident)) {
            return self.notWindow(save);
        }
        arg = try self.singleName(self.advance().text);
        if (self.eat(.comma)) {
            const n = try self.expect(.int);
            offset = std.fmt.parseInt(i64, n.text, 10) catch
                return self.fail(self.curPos(), "LAG/LEAD offset must be an integer", .{});
            if (offset < 0)
                return self.fail(self.curPos(), "LAG/LEAD offset must not be negative — use the other function", .{});
        }
    }
    if (!self.eat(.rparen) or !self.isKw("over")) return self.notWindow(save);
    const wpos = self.curPos();
    _ = self.advance();
    _ = try self.expect(.lparen);

    var this_part = std.array_list.Managed(ast.QualName).init(self.arena);
    var this_ord = std.array_list.Managed(ast.SortKey).init(self.arena);
    if (self.eatKw("partition")) {
        try self.expectKw("by");
        while (true) {
            try this_part.append(try self.singleName(try self.expectIdent()));
            if (!self.eat(.comma)) break;
        }
    }
    if (self.eatKw("order")) {
        try self.expectKw("by");
        while (true) {
            const f = try self.singleName(try self.expectIdent());
            var desc = false;
            if (self.eatKw("desc")) desc = true else _ = self.eatKw("asc");
            try this_ord.append(.{ .field = f, .desc = desc });
            if (!self.eat(.comma)) break;
        }
    }
    var this_frame: ast.WinFrame = .{};
    if (self.eatKw("rows")) {
        const fpos = self.curPos();
        switch (kind) {
            .sum, .count, .min, .max, .avg => {},
            else => return self.fail(fpos, "a frame applies to an aggregate over a window; ROW_NUMBER, RANK, DENSE_RANK, LAG and LEAD do not take one", .{}),
        }
        try self.expectKw("between");
        if (self.eatKw("unbounded")) {
            try self.expectKw("preceding");
            this_frame = .{ .rows = true, .unbounded = true };
        } else {
            const n = try self.expect(.int);
            try self.expectKw("preceding");
            const p2 = std.fmt.parseInt(i64, n.text, 10) catch
                return self.fail(fpos, "a frame's PRECEDING count must be an integer", .{});
            if (p2 < 0) return self.fail(fpos, "a frame's PRECEDING count must not be negative", .{});
            this_frame = .{ .rows = true, .preceding = p2 };
        }
        try self.expectKw("and");
        try self.expectKw("current");
        try self.expectKw("row");
    }
    _ = try self.expect(.rparen);

    if (funcs.items.len == 0) {
        try part.appendSlice(this_part.items);
        try ord.appendSlice(this_ord.items);
    } else {
        var same = this_part.items.len == part.items.len and this_ord.items.len == ord.items.len;
        if (same) {
            for (this_part.items, part.items) |a2, b2| {
                if (!std.mem.eql(u8, a2.last(), b2.last())) same = false;
            }
            for (this_ord.items, ord.items) |a2, b2| {
                if (!std.mem.eql(u8, a2.field.last(), b2.field.last()) or a2.desc != b2.desc) same = false;
            }
        }
        if (!same)
            return self.fail(wpos, "two window functions in one SELECT must share the same OVER (...) window", .{});
    }
    if (this_ord.items.len == 0 and switch (kind) {
        .sum, .count, .min, .max, .avg => false,
        else => true,
    })
        return self.fail(wpos, "a window function needs ORDER BY inside OVER (...) to number by", .{});

    const name = (try self.itemAlias()) orelse @tagName(kind);
    try funcs.append(.{ .kind = kind, .out = name, .arg = arg, .offset = offset, .frame = this_frame });
    return true;
}

/// Rewind a `name(...)` `parseWindowItem` does not take, unless `OVER` follows: then it
/// is a window over a non-column argument, which once parsed as an aggregate.
pub fn notWindow(self: *Parser, save: usize) Error!bool {
    self.i = save;
    if (self.overFollows(save))
        return self.fail(self.curPos(), "a window function takes a plain column (or `*` for COUNT) — compute `{s}(...)`'s argument in a CTE or derived table first", .{self.toks[save].text});
    return false;
}

pub fn overFollows(self: *Parser, name_at: usize) bool {
    var j = name_at + 1;
    var depth: usize = 0;
    while (j < self.toks.len) : (j += 1) {
        switch (self.toks[j].tag) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) break;
            },
            .eof => return false,
            else => {},
        }
    }
    return j + 1 < self.toks.len and self.toks[j + 1].tag == .ident and eqlNoCase(self.toks[j + 1].text, "over");
}

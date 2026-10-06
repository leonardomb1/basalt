//! End-to-end runtime tests: whole scripts through `run`, assertions on what they
//! write. `harness.zig` holds the shared helpers; each file covers one area.

test {
    _ = @import("query.zig");
    _ = @import("aggregate.zig");
    _ = @import("window.zig");
    _ = @import("parallel.zig");
    _ = @import("script.zig");
    _ = @import("format.zig");
    _ = @import("outcome.zig");
}

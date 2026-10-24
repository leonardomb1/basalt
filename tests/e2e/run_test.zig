//! End-to-end runtime tests: whole scripts through `run`, assertions on what they
//! write. `harness.zig` holds the shared helpers; each file covers one area.

test {
    _ = @import("query.zig");
    _ = @import("aggregate.zig");
    _ = @import("window.zig");
    _ = @import("parallel.zig");
    _ = @import("script.zig");
    _ = @import("format.zig");
    _ = @import("json.zig");
    _ = @import("xml.zig");
    _ = @import("records.zig");
    _ = @import("outcome.zig");
    _ = @import("records_write.zig");
    _ = @import("lake.zig");
    _ = @import("lake_write.zig");
    _ = @import("avro.zig");
}

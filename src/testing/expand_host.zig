//! The scalar-builtin check `lang/expand.zig`'s own tests expand with, the engine's
//! registry, which sits above `lang` and so reaches it only through test support.

pub const isBuiltin = @import("../exec/builtins.zig").isBuiltin;

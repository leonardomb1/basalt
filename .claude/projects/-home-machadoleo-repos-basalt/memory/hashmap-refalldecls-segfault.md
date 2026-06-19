---
name: hashmap-refalldecls-segfault
description: Top-level std.HashMap type decls segfault the test binary via refAllDeclsRecursive (Zig 0.15.2)
metadata:
  type: project
---

Never declare a `std.HashMap`/`AutoHashMap`/etc. instantiation as a **container-level (top-level/struct) `const` type decl** in this codebase. `src/main.zig`'s test root is `std.testing.refAllDeclsRecursive(root)`, which recurses into every container-level *type* declaration; diving through a `std.HashMap` instantiation produces a binary that **segfaults at startup in `main.test_0`** (exit 139, no stack trace) under Zig 0.15.2.

**Why:** the crash is in the test harness, not your logic — and zero-match `--test-filter` still crashes, so it looks like a process-level startup bug, not a specific test. Symptom seen: every `zig test` filter "crashed."

**How to apply:** keep HashMap instantiations **function-local** (`const Seen = std.HashMap(...)` inside a fn — see `op.zig` `Seen`/`Index`), or if a named type must cross modules, expose it as a **type-returning fn** `pub fn GroupMap() type { return std.HashMap(...); }` and call it as `GroupMap()` (refAllDeclsRecursive references the fn but cannot recurse into its return type). This is how `op.Aggregate.GroupMap()` is defined.

**WSL safety when a Zig binary segfaults:** never run the test binary under `ulimit -s unlimited` — an uncapped stack turns a contained stack-overflow segfault into an unbounded page-fault loop that OOM-kills the whole WSL VM (`Wsl/Service/E_UNEXPECTED`). Run with default 8MB stack + `ulimit -v 6291456` + `timeout -s KILL` to reproduce crashes safely.

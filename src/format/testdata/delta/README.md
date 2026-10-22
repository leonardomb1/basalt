# Delta test tables

Small Delta tables that Spark wrote, used by `tests/e2e/lake.zig` to check
deletion-vector and column-mapping reads. Each is copied unchanged, except that
Hadoop `.crc` side files and `_last_checkpoint` were left out. All are licensed
under the Apache License 2.0.

| Folder | Origin |
|-|-|
| `table-with-dv-small/` | delta-kernel-rs `kernel/tests/data/table-with-dv-small` (commit d9c48dc9) |
| `log-replay-dv-key-cases/` | delta-io/delta `connectors/golden-tables/src/main/resources/golden/log-replay-dv-key-cases` (commit 93d416a8) |
| `dv-with-columnmapping/` | delta-io/delta golden table `dv-with-columnmapping` (commit 93d416a8) |
| `table-with-columnmapping-mode-id/` | delta-io/delta golden table `table-with-columnmapping-mode-id` (commit 93d416a8) |
| `dat-deletion_vectors/` | Delta Acceptance Testing (delta-incubator/dat) v0.0.3, `reader_tests/generated/deletion_vectors/delta` |
| `dat-column_mapping/` | Delta Acceptance Testing (delta-incubator/dat) v0.0.3, `reader_tests/generated/column_mapping/delta` |

The expected rows in the tests are DAT's `expected/latest/table_content` where
DAT has them. For the other tables they are what DuckDB's `delta` extension
returns.

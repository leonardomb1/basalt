//! The fixture files tests outside src/ read, embedded here because `@embedFile`
//! reaches only inside its own module. A file is embedded only where a test refers
//! to it, so none of this reaches the CLI binary.

pub const rg2_parquet = @embedFile("rg2.parquet");
pub const two_members_zip = @embedFile("two_members.zip");
pub const openpyxl_xlsx = @embedFile("openpyxl.xlsx");
pub const logical_types_parquet = @embedFile("logical_types.parquet");
pub const lists_v1_parquet = @embedFile("lists_v1.parquet");
pub const lists_v2_parquet = @embedFile("lists_v2.parquet");
pub const delta_rs_checkpoint_parquet = @embedFile("delta_rs_checkpoint.parquet");

/// A table pyiceberg wrote (two appends, then a delete of one partition), each file
/// at its path under the table's root; its metadata names its original location.
pub const iceberg_py = [_][2][]const u8{
    .{ "data/day=2026-01-01/00000-0-efa6a477-7677-4bd7-885d-25752be098dc.parquet", @embedFile("iceberg_py/data/day=2026-01-01/00000-0-efa6a477-7677-4bd7-885d-25752be098dc.parquet") },
    .{ "data/day=2026-01-02/00000-0-dd688933-9127-4d7e-aa04-c3793a476cad.parquet", @embedFile("iceberg_py/data/day=2026-01-02/00000-0-dd688933-9127-4d7e-aa04-c3793a476cad.parquet") },
    .{ "data/day=2026-01-02/00000-1-efa6a477-7677-4bd7-885d-25752be098dc.parquet", @embedFile("iceberg_py/data/day=2026-01-02/00000-1-efa6a477-7677-4bd7-885d-25752be098dc.parquet") },
    .{ "data/day=null/00000-1-dd688933-9127-4d7e-aa04-c3793a476cad.parquet", @embedFile("iceberg_py/data/day=null/00000-1-dd688933-9127-4d7e-aa04-c3793a476cad.parquet") },
    .{ "metadata/00002-83198d25-577f-4aee-9c4a-9734541e16d5.metadata.json", @embedFile("iceberg_py/metadata/00002-83198d25-577f-4aee-9c4a-9734541e16d5.metadata.json") },
    .{ "metadata/00003-91ece667-5252-4853-b887-31591380dc23.metadata.json", @embedFile("iceberg_py/metadata/00003-91ece667-5252-4853-b887-31591380dc23.metadata.json") },
    .{ "metadata/dd688933-9127-4d7e-aa04-c3793a476cad-m0.avro", @embedFile("iceberg_py/metadata/dd688933-9127-4d7e-aa04-c3793a476cad-m0.avro") },
    .{ "metadata/e538b09e-2dd6-4ee1-9de4-86a6527d294c-m0.avro", @embedFile("iceberg_py/metadata/e538b09e-2dd6-4ee1-9de4-86a6527d294c-m0.avro") },
    .{ "metadata/e538b09e-2dd6-4ee1-9de4-86a6527d294c-m1.avro", @embedFile("iceberg_py/metadata/e538b09e-2dd6-4ee1-9de4-86a6527d294c-m1.avro") },
    .{ "metadata/efa6a477-7677-4bd7-885d-25752be098dc-m0.avro", @embedFile("iceberg_py/metadata/efa6a477-7677-4bd7-885d-25752be098dc-m0.avro") },
    .{ "metadata/snap-2072416895968069595-0-efa6a477-7677-4bd7-885d-25752be098dc.avro", @embedFile("iceberg_py/metadata/snap-2072416895968069595-0-efa6a477-7677-4bd7-885d-25752be098dc.avro") },
    .{ "metadata/snap-315444347732509867-0-e538b09e-2dd6-4ee1-9de4-86a6527d294c.avro", @embedFile("iceberg_py/metadata/snap-315444347732509867-0-e538b09e-2dd6-4ee1-9de4-86a6527d294c.avro") },
    .{ "metadata/snap-4854212885010183066-0-dd688933-9127-4d7e-aa04-c3793a476cad.avro", @embedFile("iceberg_py/metadata/snap-4854212885010183066-0-dd688933-9127-4d7e-aa04-c3793a476cad.avro") },
};

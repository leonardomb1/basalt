//! The fixture files tests outside src/ read, embedded here because `@embedFile`
//! reaches only inside its own module. A file is embedded only where a test refers
//! to it, so none of this reaches the CLI binary.

pub const rg2_parquet = @embedFile("rg2.parquet");
pub const two_members_zip = @embedFile("two_members.zip");
pub const openpyxl_xlsx = @embedFile("openpyxl.xlsx");
pub const logical_types_parquet = @embedFile("logical_types.parquet");
pub const lists_v1_parquet = @embedFile("lists_v1.parquet");
pub const lists_v2_parquet = @embedFile("lists_v2.parquet");

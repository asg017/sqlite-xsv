pub mod field_at;
pub mod meta;
pub mod util;
pub mod xsv;
pub mod xsv_reader;
pub mod xsv_records;

pub use crate::{
    field_at::{csv_field_at, tsv_field_at, xsv_field_at},
    meta::{xsv_debug, xsv_version},
    xsv::XsvTable,
    xsv_reader::XsvReaderTable,
    xsv_records::XsvRecordsTable,
};

use sqlite3_loadable::{
    errors::Result,
    scalar::define_scalar_function,
    sqlite3, sqlite3_entrypoint, sqlite3_imports,
    table::{define_table_function, define_virtual_table},
};

sqlite3_imports!();

#[sqlite3_entrypoint]
pub fn sqlite3_xsv_init(db: *mut sqlite3) -> Result<()> {
    let comma = b',';
    let tab = b'\t';

    define_scalar_function(db, "xsv_version", 0, xsv_version)?;
    define_scalar_function(db, "xsv_debug", 0, xsv_debug)?;

    define_virtual_table::<XsvTable>(db, "xsv", None)?;
    define_virtual_table::<XsvTable>(db, "csv", Some(comma))?;
    define_virtual_table::<XsvTable>(db, "tsv", Some(tab))?;

    define_virtual_table::<XsvReaderTable>(db, "xsv_reader", None)?;
    define_virtual_table::<XsvReaderTable>(db, "csv_reader", Some(comma))?;
    define_virtual_table::<XsvReaderTable>(db, "tsv_reader", Some(tab))?;

    define_table_function::<XsvRecordsTable>(db, "csv_records", Some(comma))?;
    define_table_function::<XsvRecordsTable>(db, "tsv_records", Some(tab))?;
    define_table_function::<XsvRecordsTable>(db, "xsv_records", None)?;

    define_scalar_function(db, "csv_field_at", 2, csv_field_at)?;
    define_scalar_function(db, "tsv_field_at", 2, tsv_field_at)?;
    define_scalar_function(db, "xsv_field_at", 3, xsv_field_at)?;

    Ok(())
}

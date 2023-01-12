mod field_at;
mod meta;
mod util;
mod xsv;
mod xsv_reader;

use crate::{
    //field_at::xsv_field_at,
    meta::{xsv_debug, xsv_version},
    xsv::XsvTable,
    xsv_reader::XsvReaderTable,
};
use sqlite_loadable::prelude::*;
use sqlite_loadable::{define_scalar_function, define_virtual_table, FunctionFlags, Result};

#[sqlite_entrypoint]
pub fn sqlite3_xsv_init(db: *mut sqlite3) -> Result<()> {
    let comma = b',';
    let tab = b'\t';

    define_scalar_function(
        db,
        "xsv_version",
        0,
        xsv_version,
        FunctionFlags::DETERMINISTIC,
    )?;
    define_scalar_function(db, "xsv_debug", 0, xsv_debug, FunctionFlags::DETERMINISTIC)?;

    define_virtual_table::<XsvTable>(db, "xsv", None)?;
    define_virtual_table::<XsvTable>(db, "csv", Some(comma))?;
    define_virtual_table::<XsvTable>(db, "tsv", Some(tab))?;

    define_virtual_table::<XsvReaderTable>(db, "xsv_reader", None)?;
    define_virtual_table::<XsvReaderTable>(db, "csv_reader", Some(comma))?;
    define_virtual_table::<XsvReaderTable>(db, "tsv_reader", Some(tab))?;

    // TODO re-add field_at and _records
    //let flags = FunctionFlags::UTF8 | FunctionFlags::DETERMINISTIC;
    //define_scalar_function_with_aux(db, "csv_field_at", 2, xsv_field_at, flags, Some(comma))?;
    //define_scalar_function_with_aux(db, "tsv_field_at", 2, xsv_field_at, flags, Some(tab))?;
    //define_scalar_function_with_aux(db, "xsv_field_at", 3, xsv_field_at, flags, None)?;

    Ok(())
}

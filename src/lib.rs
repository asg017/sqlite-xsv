mod meta;
mod util;
mod xsv;
mod xsv_fields;
mod xsv_reader;
mod xsv_rows;

use crate::{
    meta::{xsv_debug, xsv_version},
    xsv::XsvTable,
    xsv_fields::XsvFieldsTable,
    xsv_reader::XsvReaderTable,
    xsv_rows::XsvRowsTable,
};
use csv::StringRecord;
use sqlite_loadable::{
    api::{self, ValueType},
    define_scalar_function, define_table_function, define_virtual_table,
    define_virtual_table_with_find,
    prelude::*,
    table::define_table_function_with_find,
    FunctionFlags, Result,
};

struct RowPointer {
    row: StringRecord,
    headers: Option<StringRecord>,
}
const ROW_POINTER_NAME: &[u8] = b"sqlite-xsv-row0\0";
const HEADERS_POINTER_NAME: &[u8] = b"sqlite-xsv-headers0\0";

pub fn xsv_at(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    // values[0] may legally be:
    //  1. RowPointer (the "row" column from xsv_rows/csv_rows/tsv_rows)
    //  2. StringRecord (the "headers" column from xsv_rows/csv_rows/tsv_rows)
    //  3. TODO what else?
    //
    // values[1]
    if let Some(record) = unsafe { api::value_pointer::<RowPointer>(&values[0], ROW_POINTER_NAME) }
    {
        let at = match api::value_type(&values[1]) {
            ValueType::Integer => api::value_int64(&values[1]),
            ValueType::Text => {
                let header = api::value_text(&values[1]).unwrap();
                let headers = unsafe { (*record).headers.as_ref().unwrap() };
                match headers.iter().position(|h| h == header) {
                    Some(idx) => idx as i64,
                    None => {
                        api::result_null(context);
                        return Ok(());
                    }
                }
            }
            _ => todo!(),
        };
        match unsafe { (*record).row.get(at as usize) } {
            Some(field) => api::result_text(context, field)?,
            None => api::result_null(context),
        }
    } else if let Some(headers) =
        unsafe { api::value_pointer::<StringRecord>(&values[0], HEADERS_POINTER_NAME) }
    {
        let at = api::value_int64(&values[1]);
        match unsafe { (*headers).get(at as usize) } {
            Some(field) => api::result_text(context, field)?,
            None => api::result_null(context),
        }
    }
    Ok(())
}

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

    define_scalar_function(db, "xsv_at", 2, xsv_at, FunctionFlags::DETERMINISTIC)?;
    define_scalar_function(db, "csv_at", 2, xsv_at, FunctionFlags::DETERMINISTIC)?;
    define_scalar_function(db, "tsv_at", 2, xsv_at, FunctionFlags::DETERMINISTIC)?;

    define_virtual_table::<XsvTable>(db, "xsv", None)?;
    define_virtual_table_with_find::<XsvTable>(db, "csv", Some(comma))?;
    define_virtual_table::<XsvTable>(db, "tsv", Some(tab))?;

    api::overload_function(db, "xsv_path", 1)?;
    api::overload_function(db, "csv_path", 1)?;
    api::overload_function(db, "tsv_path", 1)?;

    api::overload_function(db, "xsv_line_number", 1)?;
    api::overload_function(db, "csv_line_number", 1)?;
    api::overload_function(db, "tsv_line_number", 1)?;

    define_virtual_table::<XsvReaderTable>(db, "xsv_reader", None)?;
    define_virtual_table::<XsvReaderTable>(db, "csv_reader", Some(comma))?;
    define_virtual_table::<XsvReaderTable>(db, "tsv_reader", Some(tab))?;

    define_table_function_with_find::<XsvRowsTable>(db, "xsv_rows", None)?;
    define_table_function_with_find::<XsvRowsTable>(db, "csv_rows", Some(comma))?;
    define_table_function_with_find::<XsvRowsTable>(db, "tsv_rows", Some(tab))?;

    define_table_function::<XsvFieldsTable>(db, "xsv_fields", None)?;
    define_table_function::<XsvFieldsTable>(db, "csv_fields", None)?;
    define_table_function::<XsvFieldsTable>(db, "tsv_fields", None)?;

    Ok(())
}

/**
 * A SQLite virtual table that yields each "field" in a given CSV record.
 *
 * ```
 * select * from csv_fields
 * ```
 *
 */
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Result,
};

use std::{mem, os::raw::c_int};

static CREATE_SQL: &str = "CREATE TABLE x(contents, record hidden)";
enum Columns {
    Contents,
    Record,
}
fn column(index: i32) -> Option<Columns> {
    match index {
        0 => Some(Columns::Contents),
        1 => Some(Columns::Record),
        _ => None,
    }
}

#[repr(C)]
pub struct XsvFieldsTable {
    /// must be first
    base: sqlite3_vtab,
}

impl<'vtab> VTab<'vtab> for XsvFieldsTable {
    type Aux = u8;
    type Cursor = XsvFieldsCursor;

    fn connect(
        _db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, XsvFieldsTable)> {
        let base: sqlite3_vtab = unsafe { mem::zeroed() };

        let vtab = XsvFieldsTable { base };

        Ok((CREATE_SQL.to_string(), vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_record = false;
        for mut constraint in info.constraints() {
            if let Some(Columns::Record) = column(constraint.column_idx()) {
                if !has_record && !constraint.usable()
                    || constraint.op() != Some(sqlite_loadable::table::ConstraintOperator::EQ)
                {
                    return Err(BestIndexError::Constraint);
                }
                has_record = true;
                constraint.set_omit(true);
                constraint.set_argv_index(1);
            }
        }
        if !has_record {
            return Err(BestIndexError::Error);
        }
        info.set_estimated_cost(100000.0);
        info.set_estimated_rows(100000);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<XsvFieldsCursor> {
        XsvFieldsCursor::new()
    }
}

#[repr(C)]
pub struct XsvFieldsCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    record: csv::StringRecord,
    rowid: i64,
}
impl XsvFieldsCursor {
    fn new() -> Result<XsvFieldsCursor> {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        let record = csv::StringRecord::new();

        let cursor = XsvFieldsCursor {
            base,
            rowid: 0,
            record,
        };
        Ok(cursor)
    }
}

impl VTabCursor for XsvFieldsCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        if let Some(record) =
            unsafe { api::value_pointer::<crate::RowPointer>(&values[0], crate::ROW_POINTER_NAME) }
        {
            unsafe {
                self.record = (*record).row.clone();
            }
        } else if let Some(record) = unsafe {
            api::value_pointer::<crate::StringRecord>(&values[0], crate::HEADERS_POINTER_NAME)
        } {
            unsafe {
                self.record = (*record).clone();
            }
        }
        self.rowid = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.record.get(self.rowid as usize).is_none()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        match column(i) {
            Some(Columns::Contents) => {
                if let Some(field) = self.record.get(self.rowid as usize) {
                    api::result_text(context, field)?
                } else {
                    api::result_null(context)
                }
            }
            Some(Columns::Record) => {
                // TODO re-supply the input record here?
                api::result_null(context);
            }
            None => (),
        }

        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

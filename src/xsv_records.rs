use csv::{Reader, ReaderBuilder};
use sqlite3_loadable::{
    errors::{BestIndexError, Result},
    table::{ConstraintOperator, SqliteXIndexInfo, VTab, VTabCursor, VTableArguments},
    SqliteContext, SqliteValue,
};
use sqlite3ext_sys::{sqlite3, sqlite3_vtab, sqlite3_vtab_cursor};

use std::{mem, os::raw::c_int};

static CREATE_SQL: &str = "CREATE TABLE x(record text, records hidden)";
static CREATE_SQL_DELIMITER: &str = "CREATE TABLE x(record text, delimiter hidden, records hidden)";
enum Columns {
    Record,
    Records,
}
fn column(index: i32) -> Option<Columns> {
    match index {
        0 => Some(Columns::Record),
        1 => Some(Columns::Records),
        _ => None,
    }
}

enum ColumnsDelimiter {
    Record,
    Delimiter,
    Records,
}
fn column_delimiter(index: i32) -> Option<ColumnsDelimiter> {
    match index {
        0 => Some(ColumnsDelimiter::Record),
        1 => Some(ColumnsDelimiter::Delimiter),
        2 => Some(ColumnsDelimiter::Records),
        _ => None,
    }
}

/// A cursor for the Series virtual table
#[repr(C)]
pub struct XsvRecordsCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    delimiter: Option<u8>,
    reader: Option<Box<Reader<&'vtab [u8]>>>,
    records: Option<Vec<String>>,
    current: usize,
    eof: bool,
}
impl XsvRecordsCursor<'_> {
    fn new<'vtab>(delimiter: Option<u8>) -> XsvRecordsCursor<'vtab> {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        XsvRecordsCursor {
            base,
            delimiter,
            reader: None,
            records: None,
            current: 0,
            eof: false,
        }
    }
}

unsafe impl VTabCursor for XsvRecordsCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: Vec<SqliteValue>,
    ) -> Result<()> {
        let (records, delimiter) = match self.delimiter {
            Some(d) => (values.get(0).unwrap().text()?, d),
            None => (
                values.get(1).unwrap().text()?,
                values.get(0).unwrap().text()?.as_bytes()[0],
            ),
        };
        let records: Vec<String> = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .from_reader(records.as_bytes())
            .records()
            .map(|x| {
                let mut wtr = csv::WriterBuilder::new()
                    .delimiter(delimiter)
                    .from_writer(vec![]);

                let record = x.unwrap();
                //record.
                wtr.write_record(&record).unwrap();
                //record.as_slice().to_owned()
                String::from_utf8(wtr.into_inner().unwrap()).unwrap()
            })
            .collect();
        self.records = Some(records);
        //self.reader = Some(Box::new());
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.current += 1;
        self.eof = self.current >= self.records.as_ref().unwrap().len();
        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, ctx: SqliteContext, i: c_int) -> Result<()> {
        match column(i) {
            Some(Columns::Record) => {
                ctx.result_text(self.records.as_ref().unwrap().get(self.current).unwrap())?;
            }
            _ => (),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.current.try_into().unwrap())
    }
}

#[repr(C)]
pub struct XsvRecordsTable {
    /// must be first
    base: sqlite3_vtab,
    delimiter: Option<u8>,
}

unsafe impl<'vtab> VTab<'vtab> for XsvRecordsTable {
    type Aux = u8;
    type Cursor = XsvRecordsCursor<'vtab>;

    fn connect(
        _db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        _args: VTableArguments,
    ) -> Result<(String, XsvRecordsTable)> {
        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        let vtab = XsvRecordsTable {
            base,
            delimiter: aux.map(|a| a.to_owned()),
        };
        // TODO db.config(VTabConfig::Innocuous)?;
        let sql = match aux {
            Some(_) => CREATE_SQL.to_owned(),
            None => CREATE_SQL_DELIMITER.to_owned(),
        };
        Ok((sql, vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: SqliteXIndexInfo) -> core::result::Result<(), BestIndexError> {
        match self.delimiter {
            Some(_) => {
                let mut has_records = false;
                for mut constraint in info.constraints() {
                    match column(constraint.icolumn()) {
                        Some(Columns::Records) => {
                            if constraint.usable()
                                && constraint.op() == Some(ConstraintOperator::EQ)
                            {
                                constraint.set_omit(true);
                                constraint.set_argv_index(1);
                                has_records = true;
                            } else {
                                return Err(BestIndexError::Constraint);
                            }
                        }
                        _ => todo!(),
                    }
                }
                if !has_records {
                    return Err(BestIndexError::Error);
                }
            }
            None => {
                let mut has_delimiter = false;
                let mut has_records = false;
                for mut constraint in info.constraints() {
                    match column_delimiter(constraint.icolumn()) {
                        Some(ColumnsDelimiter::Delimiter) => {
                            if constraint.usable()
                                && constraint.op() == Some(ConstraintOperator::EQ)
                            {
                                constraint.set_omit(true);
                                constraint.set_argv_index(1);
                                has_delimiter = true;
                            } else {
                                return Err(BestIndexError::Constraint);
                            }
                        }
                        Some(ColumnsDelimiter::Records) => {
                            if constraint.usable()
                                && constraint.op() == Some(ConstraintOperator::EQ)
                            {
                                constraint.set_omit(true);
                                constraint.set_argv_index(2);
                                has_records = true;
                            } else {
                                return Err(BestIndexError::Constraint);
                            }
                        }
                        _ => todo!(),
                    }
                }
                if !has_records || !has_delimiter {
                    return Err(BestIndexError::Error);
                }
            }
        }
        info.set_estimated_cost(100000.0);
        info.set_estimated_rows(100000);
        info.set_idxnum(1);

        Ok(())
    }

    fn open(&mut self) -> Result<XsvRecordsCursor<'_>> {
        Ok(XsvRecordsCursor::new(self.delimiter))
    }
}

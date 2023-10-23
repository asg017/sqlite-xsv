/**
 * A SQLite virtual table for iterating over every row in a CSV.
 *
 * The "source" can be one of:
 *  1. A raw CSV in a BLOB
 *  2. A SQLite "reader" object
 *
 *  Overloads the "->>" operator as an alias for `xsv_at()`.
 */
use sqlite_loadable::api::ValueType;
use sqlite_loadable::prelude::*;
use sqlite_loadable::table::VTabFind;
use sqlite_loadable::{
    api,
    scalar::scalar_function_raw,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};
use sqlite_reader::SqliteReader;
use std::os::raw::c_void;
use std::{io::Read, mem, os::raw::c_int};

static CREATE_SQL: &str =
    "CREATE TABLE x(row, headers, line, byte, length, source hidden, delimiter hidden)";
enum Columns {
    Row,
    Headers,
    Line,
    Byte,
    Length,
    Source,
    Delimiter,
}
fn column(index: i32) -> Option<Columns> {
    match index {
        0 => Some(Columns::Row),
        1 => Some(Columns::Headers),
        2 => Some(Columns::Line),
        3 => Some(Columns::Byte),
        4 => Some(Columns::Length),
        5 => Some(Columns::Source),
        6 => Some(Columns::Delimiter),
        _ => None,
    }
}

#[repr(C)]
pub struct XsvRowsTable {
    /// must be first
    base: sqlite3_vtab,
    delimiter: Option<u8>,
}

impl<'vtab> VTab<'vtab> for XsvRowsTable {
    type Aux = u8;
    type Cursor = XsvRowsCursor;

    fn connect(
        _db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTabArguments,
    ) -> Result<(String, XsvRowsTable)> {
        let base: sqlite3_vtab = unsafe { mem::zeroed() };

        let vtab = XsvRowsTable {
            base,
            delimiter: aux.copied(),
        };

        Ok((CREATE_SQL.to_string(), vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_source = false;
        let mut has_delimiter = false;
        let requires_delimiter = self.delimiter.is_none();
        for mut constraint in info.constraints() {
            match column(constraint.column_idx()) {
                Some(Columns::Source) => {
                    if !has_source && !constraint.usable()
                        || constraint.op() != Some(sqlite_loadable::table::ConstraintOperator::EQ)
                    {
                        return Err(BestIndexError::Constraint);
                    }
                    has_source = true;
                    constraint.set_omit(true);
                    constraint.set_argv_index(1);
                }
                Some(Columns::Delimiter) => {
                    if requires_delimiter {
                        if !constraint.usable() {
                            return Err(BestIndexError::Constraint);
                        }
                        has_delimiter = true;
                        constraint.set_omit(true);
                        constraint.set_argv_index(2);
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                _ => (),
            }
        }
        if !has_source {
            return Err(BestIndexError::Error);
        }
        info.set_estimated_cost(100000.0);
        info.set_estimated_rows(100000);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<XsvRowsCursor> {
        XsvRowsCursor::new(self.delimiter)
    }
}

impl<'vtab> VTabFind<'vtab> for XsvRowsTable {
    fn find_function(
        &mut self,
        argc: i32,
        name: &str,
    ) -> Option<(
        unsafe extern "C" fn(*mut sqlite3_context, i32, *mut *mut sqlite3_value),
        Option<i32>,
        Option<*mut c_void>,
    )> {
        if name == "->>" && argc == 2 {
            return Some((scalar_function_raw(crate::xsv_at), None, None));
        }
        None
    }
}

#[repr(C)]
pub struct XsvRowsCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    delimiter: Option<u8>,
    current_reader: Option<csv::Reader<Box<dyn Read>>>,
    headers: csv::StringRecord,
    record: csv::StringRecord,
    rowid: i64,
    eof: bool,
}
impl XsvRowsCursor {
    fn new(delimiter: Option<u8>) -> Result<XsvRowsCursor> {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        let record = csv::StringRecord::new();

        let cursor = XsvRowsCursor {
            base,
            delimiter,
            current_reader: None,
            rowid: 0,
            headers: csv::StringRecord::new(),
            record,
            eof: false,
        };
        Ok(cursor)
    }
}

impl VTabCursor for XsvRowsCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let input_arg = values
            .get(0)
            .ok_or_else(|| Error::new_message("Internal error: expected argv[0] in xFilter"))?;
        // either self.delimiter or argv[1] must define the delimiter
        let delimiter = values
            .get(1)
            .map_or(self.delimiter, |v| {
                api::value_text(v).unwrap().as_bytes().first().copied()
            })
            .unwrap();
        let reader = match api::value_type(input_arg) {
            ValueType::Blob => Box::new(std::io::Cursor::new(api::value_blob(input_arg))),
            ValueType::Null => match unsafe {
                api::value_pointer::<Box<dyn SqliteReader>>(input_arg, b"reader0\0")
            } {
                Some(reader) => unsafe {
                    let r = (*(*reader)).generate().unwrap();
                    r
                },
                None => todo!(),
            },
            _ => todo!("unknown value type?"),
        };
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(delimiter)
            .flexible(true)
            .from_reader(reader);
        self.headers = reader.headers().unwrap().clone();
        self.current_reader = Some(reader);
        self.next()
    }

    fn next(&mut self) -> Result<()> {
        match self
            .current_reader
            .as_mut()
            .ok_or_else(|| Error::new_message("Internal error: expected reader in xNext"))?
            .read_record(&mut self.record)
        {
            Ok(has_more) => {
                self.eof = !has_more;
                self.rowid += 1;
                Ok(())
            }
            Err(err) => match err.kind() {
                csv::ErrorKind::Utf8 { pos: _, err: _ } => Err(Error::new_message(
                    "Error: UTF8 error while reading next row",
                )),
                _ => Err(Error::new_message(
                    format!("Error reading: {}", err).as_str(),
                )),
            },
        }
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        match column(i) {
            Some(Columns::Row) => {
                api::result_pointer(
                    context,
                    crate::ROW_POINTER_NAME,
                    crate::RowPointer {
                        row: self.record.clone(),
                        headers: Some(self.headers.clone()),
                    },
                );
            }
            Some(Columns::Headers) => {
                let x = self.current_reader.as_ref().unwrap();
                api::result_pointer(context, crate::HEADERS_POINTER_NAME, self.headers.clone());
            }
            Some(Columns::Line) => match self.record.position() {
                Some(position) => api::result_int64(context, position.line().try_into().unwrap()),
                None => api::result_null(context),
            },
            Some(Columns::Byte) => match self.record.position() {
                Some(position) => api::result_int64(context, position.byte().try_into().unwrap()),
                None => api::result_null(context),
            },
            Some(Columns::Length) => {
                api::result_int64(context, self.record.len().try_into().unwrap())
            }
            Some(Columns::Source) => {
                api::result_null(context);
            }
            Some(Columns::Delimiter) => {
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

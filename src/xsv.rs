use sqlite3_loadable::{
    sqlite3, sqlite3_vtab, sqlite3_vtab_cursor,
    table::{SqliteXIndexInfo, VTab, VTabCursor, VTableArguments},
    BestIndexError, Error, Result, SqliteContext, SqliteValue,
};

use csv;
use flate2::read::GzDecoder;
use std::{
    ffi::OsStr,
    fs::File,
    io::{BufReader, Read},
    marker::PhantomData,
    mem,
    os::raw::c_int,
    path::Path,
};

use crate::util::{self, arg_is_parameter, quoted_value};

#[repr(C)]
pub struct XsvTable {
    /// must be first
    base: sqlite3_vtab,
    path: String,
    delimiter: u8,
}

unsafe impl<'vtab> VTab<'vtab> for XsvTable {
    type Aux = u8;
    type Cursor = XsvCursor<'vtab>;

    fn create(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTableArguments,
    ) -> Result<(String, Self)> {
        Self::connect(db, aux, args)
    }
    fn connect(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTableArguments,
    ) -> Result<(String, XsvTable)> {
        let arguments = parse_xsv_arguments(db, args.arguments, aux.map(|a| a.to_owned()))?;
        let r = get_reader(&arguments.filename)?;
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(arguments.delimiter)
            .from_reader(r);

        let base: sqlite3_vtab = unsafe { mem::zeroed() };

        let vtab = XsvTable {
            base,
            path: arguments.filename,
            delimiter: arguments.delimiter,
        };

        let mut sql = String::from("create table x(");
        let headers = reader
            .headers()
            .map_err(|_| Error::new_message("Error: invalid UTF8 in headers of CSV"))?;
        let mut it = headers.iter().peekable();
        loop {
            match it.next() {
                Some(header) => {
                    sql.push('"');
                    sql.push_str(header);
                    sql.push('"');
                    if it.peek().is_some() {
                        sql.push(',');
                    }
                }
                None => break,
            }
        }
        sql.push(')');
        Ok((sql, vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: SqliteXIndexInfo) -> core::result::Result<(), BestIndexError> {
        info.set_estimated_cost(10000.0);
        info.set_estimated_rows(10000);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<XsvCursor<'_>> {
        XsvCursor::new(&self.path, self.delimiter)
    }
}

fn get_reader(path: &str) -> Result<Box<dyn Read>> {
    match Path::new(path).extension().and_then(OsStr::to_str) {
        Some(ext) => match ext {
            "gz" => {
                let r = std::io::BufReader::new(File::open(path).map_err(|_| {
                    Error::new_message(
                        format!("Error: filename '{}' does not exist. ", path).as_str(),
                    )
                })?);
                let x = BufReader::new(GzDecoder::new(r));
                Ok(Box::new(x))
            }
            _ => Ok(Box::new(File::open(path).map_err(|_| {
                Error::new_message(format!("Error: filename '{}' does not exist.", path).as_str())
            })?)),
        },
        _ => Ok(Box::new(File::open(path).map_err(|_| {
            Error::new_message(format!("Error: filename '{}' does not exist.", path).as_str())
        })?)),
    }
}
#[repr(C)]
pub struct XsvCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    reader: csv::Reader<Box<dyn Read>>,
    record: csv::StringRecord,
    rowid: i64,
    eof: bool,
    phantom: PhantomData<&'vtab XsvTable>,
}
impl XsvCursor<'_> {
    fn new<'vtab>(path: &str, delimiter: u8) -> Result<XsvCursor<'vtab>> {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        let r = get_reader(path)?;
        let reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .from_reader(r);
        let record = csv::StringRecord::new();

        let mut cursor = XsvCursor {
            base,
            reader,
            rowid: 0,
            record,
            eof: false,
            phantom: PhantomData,
        };
        cursor.next().map(|_| cursor)
    }
}

unsafe impl VTabCursor for XsvCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _values: Vec<SqliteValue>,
    ) -> Result<()> {
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        match self.reader.read_record(&mut self.record) {
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
                    "Error: Unknown error while reading next row",
                )),
            },
        }
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, ctx: SqliteContext, i: c_int) -> Result<()> {
        let i = usize::try_from(i)
            .map_err(|_| Error::new_message(format!("what the fuck {}", i).as_str()))?;
        self.record.get(i);
        let s = &self
            .record
            .get(i)
            .ok_or_else(|| Error::new_message(format!("wut {}", i).as_str()))?;
        ctx.result_text(s)?;
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

struct XsvArguments {
    filename: String,
    delimiter: u8,
}

fn value_is_sqlite_parameter(value: &str) -> bool {
    value.starts_with(':')
}
fn parse_xsv_argument_filename(db: *mut sqlite3, value: &str) -> Result<Option<String>> {
    if let Some(value) = quoted_value(value.trim()) {
        Ok(Some(value.to_owned()))
    } else if value_is_sqlite_parameter(value) {
        match util::sqlite_parameter_value(db, value) {
            Ok(result) => match result {
                Some(path) => Ok(Some(path)),
                None => Err(Error::new_message(
                    format!("{value} is not defined in temp.sqlite_parameters table").as_str(),
                )),
            },
            Err(_) => Err(Error::new_message(
                "temp.sqlite_parameters is not defined, can't use sqlite parameters as value",
            )),
        }
    } else {
        Err(Error::new_message(
            "filename value not valid, wrap in single or double quotes",
        ))
    }
}
fn parse_xsv_argument_delimiter(value: &str, initial_delimiter: Option<u8>) -> Result<u8> {
    if let Some(value) = quoted_value(value.trim()) {
        let delimiter = u8::try_from(value.chars().nth(0).unwrap()).unwrap();
        if initial_delimiter.is_some() {
            Err(Error::new_message(
                "cannot override delimiter in this virtual table",
            ))
        } else {
            Ok(delimiter)
        }
    } else {
        Err(Error::new_message(
            "delimiter value not valid, wrap in single or double quotes",
        ))
    }
}
fn parse_xsv_arguments(
    db: *mut sqlite3,
    arguments: Vec<String>,
    initial_delimiter: Option<u8>,
) -> Result<XsvArguments> {
    let mut filename = None;
    let mut delimiter = initial_delimiter;
    for arg in arguments {
        match arg_is_parameter(&arg) {
            Some((key, value)) => match key.to_lowercase().as_str() {
                "filename" => {
                    filename = parse_xsv_argument_filename(db, value)?;
                }
                "delimiter" => {
                    delimiter = Some(parse_xsv_argument_delimiter(value, initial_delimiter)?);
                }
                _ => {
                    return Err(Error::new_message(
                        format!("Invalid argument, '{}' not a valid parameter key", key).as_str(),
                    ))
                }
            },
            None => return Err(Error::new_message("Invalid argument, not a parameter")),
        }
    }
    let filename = filename.ok_or_else(|| Error::new_message("no filename given. Specify a path to a CSV file to read from with 'filename=\"path.csv\"'"))?;
    let delimiter = delimiter.ok_or_else(|| {
        Error::new_message("no delimiter given. Specify a delimiter to use with 'delimiter=\"\t\"'")
    })?;
    Ok(XsvArguments {
        filename,
        delimiter,
    })
}

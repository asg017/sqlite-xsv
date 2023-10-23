use sqlite_loadable::scalar::scalar_function_raw_with_aux;
use sqlite_loadable::table::VTabFind;
use sqlite_loadable::vtab_argparse::{parse_argument, Argument, ColumnDeclaration};
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};
use sqlite_loadable::{prelude::*, table};

use glob::{glob, Paths};
use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::{io::Read, mem, os::raw::c_int};

use crate::util::{
    get_csv_source_reader, parse_delimiter_config_value, parse_filename_config_value,
    parse_header_config_value, parse_quote_config_value,
};

#[repr(C)]
pub struct XsvTable {
    /// must be first
    base: sqlite3_vtab,
    db: *mut sqlite3,
    input: String,
    header: bool,
    delimiter: u8,
    quote: u8,
    declared_columns: Option<Vec<ColumnDeclaration>>,

    // dynamically updated from a cursor's xNext. NOT threadsafe.
    current_path: String,

    // dynamically updated from a cursor's xNext. NOT threadsafe.
    current_line_number: u64,
}
impl XsvTable {
    fn reader<P: AsRef<Path>>(&self, path: P) -> Result<csv::Reader<Box<dyn Read>>> {
        let source_reader = get_csv_source_reader(path)?;

        Ok(csv::ReaderBuilder::new()
            .has_headers(self.header)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(source_reader))
    }
    fn schema_from_reader(&self) -> Result<String> {
        return match &self.declared_columns {
            // if supplied, make the CREATE statement from those names
            Some(columns) => {
                let mut sql = String::from("create table x(");
                let mut it = columns.iter().peekable();
                while let Some(column) = it.next() {
                    sql.push_str(column.vtab_declaration().as_str());
                    if it.peek().is_some() {
                        sql.push(',');
                    }
                }
                sql.push(')');
                Ok(sql)
            }

            // if no columns were provided, then sniff the headers from the CSV
            None => {
                let first_match = glob(self.input.as_str())
                    .map_err(|e| {
                        Error::new_message(format!(
                            "Invalid glob pattern for {}: {}",
                            self.input, e
                        ))
                    })?
                    .next()
                    .ok_or_else(|| {
                        Error::new_message(format!("No matching files found for {}", self.input))
                    })?
                    .map_err(|e| {
                        Error::new_message(format!(
                            "Error globbing first path for {}: {}",
                            self.input, e
                        ))
                    })?;
                let mut reader = self.reader(first_match)?;
                let mut sql = String::from("create table x(");

                let headers = reader
                    .headers()
                    .map_err(|_| Error::new_message("Error: invalid UTF8 in headers of CSV"))?;
                let mut it = headers.iter().peekable();

                let mut ci = 1;
                while let Some(header) = it.next() {
                    if self.header {
                        sql.push('"');
                        sql.push_str(header);
                        sql.push('"');
                    } else {
                        sql.push_str(format!("c{}", ci).as_str());
                    }
                    if it.peek().is_some() {
                        sql.push(',');
                    }
                    ci += 1;
                }

                sql.push(')');
                Ok(sql)
            }
        };
    }
}
impl<'vtab> VTab<'vtab> for XsvTable {
    type Aux = u8;
    type Cursor = XsvCursor;

    fn create(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTabArguments,
    ) -> Result<(String, Self)> {
        Self::connect(db, aux, args)
    }
    fn connect(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTabArguments,
    ) -> Result<(String, XsvTable)> {
        let arguments = parse_xsv_arguments(
            db,
            args.arguments,
            aux.map(|a| a.to_owned()),
            args.table_name.as_str(),
        )?;
        let vtab = XsvTable {
            base: unsafe { mem::zeroed() },
            db,
            input: arguments.filename.clone(),
            header: arguments.header,
            delimiter: arguments.delimiter,
            quote: arguments.quote,
            declared_columns: arguments.columns,
            current_path: "".to_owned(),
            current_line_number: 0,
        };

        Ok((vtab.schema_from_reader()?, vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        // No matter how the CSV is queried, always jsut read from top->bottom,
        // no shortcuts can be made.
        info.set_estimated_cost(10000.0);
        info.set_estimated_rows(10000);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<XsvCursor> {
        XsvCursor::new(self)
    }
}

impl<'vtab> VTabFind<'vtab> for XsvTable {
    fn find_function(
        &mut self,
        argc: i32,
        name: &str,
    ) -> Option<(
        unsafe extern "C" fn(*mut sqlite3_context, i32, *mut *mut sqlite3_value),
        Option<i32>,
        Option<*mut c_void>,
    )> {
        if argc == 1 && (name == "xsv_path" || name == "csv_path" || name == "tsv_path") {
            let x = scalar_function_raw_with_aux(csv_path, self as *mut XsvTable);
            return Some((x.0, None, Some(x.1)));
        }
        if argc == 1
            && (name == "xsv_line_number" || name == "csv_line_number" || name == "tsv_line_number")
        {
            let x = scalar_function_raw_with_aux(csv_line_number, self as *mut XsvTable);
            return Some((x.0, None, Some(x.1)));
        }
        None
    }
}

pub fn csv_path(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
    aux: &*mut XsvTable,
) -> Result<()> {
    unsafe {
        api::result_text(context, (**aux).current_path.as_str())?;
    }
    Ok(())
}
pub fn csv_line_number(
    context: *mut sqlite3_context,
    _values: &[*mut sqlite3_value],
    aux: &*mut XsvTable,
) -> Result<()> {
    let line_number = unsafe { (**aux).current_line_number };
    api::result_int64(
        context,
        line_number.try_into().map_err(|_| {
            Error::new_message(format!(
                "Integer overflow in line number: {line_number} is not an i64"
            ))
        })?,
    );
    Ok(())
}

#[repr(C)]
pub struct XsvCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    rowid: i64,
    paths: Paths,
    current_reader: Option<csv::Reader<Box<dyn Read>>>,
    current_path: Option<PathBuf>,
    current_line_number: i64,
    record: csv::StringRecord,
    eof: bool,
    declared_columns: Option<Vec<ColumnDeclaration>>,
    table: *mut XsvTable,
}
impl XsvCursor {
    fn new(table: &mut XsvTable) -> Result<XsvCursor> {
        let record = csv::StringRecord::new();
        let paths = glob(table.input.as_str()).map_err(|e| {
            Error::new_message(format!(
                "Invalid input glob pattern for {}: {}",
                table.input, e
            ))
        })?;
        let mut cursor = XsvCursor {
            base: unsafe { mem::zeroed() },
            rowid: 0,
            paths,
            current_path: None,
            current_reader: None,
            current_line_number: 0,
            record,
            eof: false,
            declared_columns: table.declared_columns.clone(),
            table: table as *mut XsvTable,
        };
        cursor.next().map(|_| cursor)
    }

    fn next_record(&mut self) -> Result<bool> {
        match self
            .current_reader
            .as_mut()
            .ok_or_else(|| {
                Error::new_message("Internal sqlite-xsv error: expected current_reader")
            })?
            .read_record(&mut self.record)
        {
            Ok(has_more) => {
                unsafe {
                    // position should always be Some(p) here, but rather be safe than sorry
                    (*self.table).current_line_number = match self.record.position() {
                        Some(p) => p.line(),
                        None => 0,
                    };
                }
                Ok(has_more)
            }
            Err(err) => match err.kind() {
                csv::ErrorKind::Utf8 { pos: _, err: _ } => Err(Error::new_message(
                    "Error: UTF8 error while reading next row",
                )),
                _ => Err(Error::new_message(
                    format!("Error while reading next row: {}", err).as_str(),
                )),
            },
        }
    }
    fn next_path_reader(&mut self) -> Result<Option<csv::Reader<Box<dyn Read>>>> {
        match self.paths.next() {
            Some(Ok(path)) => unsafe {
                let s = path.to_string_lossy();
                (*self.table).current_path = s.to_string();
                Ok(Some((*self.table).reader(path)?))
            },
            Some(Err(error)) => Err(Error::new_message(format!(
                "Error on next glob match: {}",
                error
            ))),
            None => Ok(None),
        }
    }
}

impl VTabCursor for XsvCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _values: &[*mut sqlite3_value],
    ) -> Result<()> {
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        loop {
            let has_more = match self.current_reader.as_mut() {
                Some(_) => self.next_record()?,
                None => match self.next_path_reader()? {
                    Some(r) => {
                        self.current_reader = Some(r);
                        self.next_record()?
                    }
                    None => {
                        self.current_reader = None;
                        break;
                    }
                },
            };
            if has_more {
                break;
            }
            self.current_reader = None;
        }
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.current_reader.is_none()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let i = usize::try_from(i)
            .map_err(|_| Error::new_message(format!("what the fuck {}", i).as_str()))?;

        // This will typically only be None when a glob pattern is used, and the 1st sniffed CSV
        // has more column than another CSV in the same glob pattern.
        // For now we just return NULL for missing columns, not sure how flexible we should be
        // across CSV files. If it's a single file, i'm pretty sure it's not flexible
        let value = &self.record.get(i);

        if let Some(value) = value {
            match self.declared_columns.as_ref().and_then(|c| c.get(i)) {
                Some(column) => column.affinity().result_text(context, value)?,
                None => api::result_text(context, value)?,
            }
        }

        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

#[derive(Debug, PartialEq)]
struct XsvArguments {
    filename: String,
    header: bool,
    delimiter: u8,
    quote: u8,
    columns: Option<Vec<ColumnDeclaration>>,
}

fn parse_xsv_arguments(
    db: *mut sqlite3,
    arguments: Vec<String>,
    initial_delimiter: Option<u8>,
    table_name: &str,
) -> Result<XsvArguments> {
    let mut filename: Option<String> = None;
    let mut header: bool = true;
    let mut delimiter = initial_delimiter;
    let mut quote = b'"';
    let mut columns = vec![];
    for arg in arguments {
        match parse_argument(arg.as_str()) {
            Ok(arg) => match arg {
                Argument::Column(column) => columns.push(column),
                Argument::Config(config) => match config.key.as_str() {
                    "filename" => {
                        filename = Some(parse_filename_config_value(db, config.value)?);
                    }
                    "header" => {
                        header = parse_header_config_value(config.value)?;
                    }
                    "delimiter" => {
                        delimiter = Some(parse_delimiter_config_value(config.value)?);
                    }
                    "quote" => {
                        quote = parse_quote_config_value(config.value)?;
                    }
                    _ => (),
                },
            },
            Err(err) => return Err(Error::new_message(err.as_str())),
        };
    }
    let filename = match filename {
        Some(filename) => Ok(filename),
        None => {
            if glob(table_name).map_or(false, |mut paths| paths.next().is_some()) {
                Ok(table_name.to_owned())
            } else {
                // TODO should this error message say "no filename given" and/or "table_name not a valid path"
                Err(Error::new_message("no filename given. Specify a path to a CSV file to read from with 'filename=\"path.csv\"'"))
            }
        }
    }?;
    let delimiter = delimiter.ok_or_else(|| {
        Error::new_message("no delimiter given. Specify a delimiter to use with 'delimiter=\"\t\"'")
    })?;
    let columns = if !columns.is_empty() {
        Some(columns)
    } else {
        None
    };
    Ok(XsvArguments {
        filename,
        header,
        delimiter,
        quote,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use crate::xsv::*;
    use sqlite_loadable::{Error, ErrorKind};
    #[test]
    fn it_works() {
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string()],
                Some(b','),
                "table_name"
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                header: true,
                delimiter: b',',
                quote: b'"',
                columns: None,
            })
        );
    }
    #[test]
    fn test_delimiter() {
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string()],
                Some(b','),
                "table_name"
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                header: true,
                delimiter: b',',
                quote: b'"',
                columns: None,
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec![
                    "filename='a.csv'".to_string(),
                    "a int".to_string(),
                    "b text".to_string()
                ],
                Some(b','),
                "table_name"
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                header: true,
                delimiter: b',',
                quote: b'"',
                columns: Some(vec![
                    ColumnDeclaration {
                        name: "a".to_string(),
                        declared_type: Some("int".to_string()),
                        constraints: None
                    },
                    ColumnDeclaration {
                        name: "b".to_string(),
                        declared_type: Some("text".to_string()),
                        constraints: None
                    }
                ]),
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec![
                    "filename='a.csv'".to_string(),
                    "delimiter='|'".to_string(),
                    "quote='x'".to_string()
                ],
                None,
                "table_name"
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                header: true,
                delimiter: b'|',
                quote: b'x',
                columns: None,
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec![
                    "filename='a.csv'".to_string(),
                    "delimiter='|'".to_string(),
                    "quote='\0'".to_string(),
                ],
                None,
                "table_name"
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                header: true,
                delimiter: b'|',
                quote: b'\0',
                columns: None,
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter=''".to_string()],
                None,
                "table_name"
            ),
            Err(Error::new(ErrorKind::Message(
                "delimiter must have at least 1 character".to_string()
            )))
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter='\t'".to_string()],
                None,
                "table_name"
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                header: true,
                delimiter: b'\t',
                quote: b'"',
                columns: None,
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter='💖'".to_string()],
                None,
                "table_name"
            ),
            Err(Error::new(ErrorKind::Message(
                "delimiter can only be 1 character long".to_string()
            )))
        );
    }
}

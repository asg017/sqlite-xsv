use sqlite_loadable::prelude::*;
use sqlite_loadable::vtab_argparse::{parse_argument, Argument, ColumnDeclaration};
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};

use std::{io::Read, mem, os::raw::c_int};

use crate::util::{
    get_csv_source_reader, parse_delimiter_config_value, parse_filename_config_value,
    parse_quote_config_value,
};

#[repr(C)]
pub struct XsvTable {
    /// must be first
    base: sqlite3_vtab,
    db: *mut sqlite3,
    path: String,
    delimiter: u8,
    quote: u8,
    declared_columns: Option<Vec<ColumnDeclaration>>,
}
impl XsvTable {
    fn reader(&self) -> Result<csv::Reader<Box<dyn Read>>> {
        let source_reader = get_csv_source_reader(&self.path)?;

        Ok(csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(source_reader))
    }
    fn schema_from_reader(
        &self,
        supplied_columns: Option<Vec<ColumnDeclaration>>,
    ) -> Result<String> {
        return match supplied_columns {
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
            // and compute from there
            None => {
                let mut reader = self.reader()?;
                let mut sql = String::from("create table x(");
                let headers = reader
                    .headers()
                    .map_err(|_| Error::new_message("Error: invalid UTF8 in headers of CSV"))?;
                let mut it = headers.iter().peekable();

                while let Some(header) = it.next() {
                    sql.push('"');
                    sql.push_str(header);
                    sql.push('"');
                    if it.peek().is_some() {
                        sql.push(',');
                    }
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
        let arguments = parse_xsv_arguments(db, args.arguments.clone(), aux.map(|a| a.to_owned()))?;
        let vtab = XsvTable {
            base: unsafe { mem::zeroed() },
            db,
            path: arguments.filename,
            delimiter: arguments.delimiter,
            quote: arguments.quote,
            declared_columns: arguments.columns.clone(),
        };

        Ok((vtab.schema_from_reader(arguments.columns)?, vtab))
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
        //XsvCursor::new(&self.path, self.delimiter, self.quote)
        XsvCursor::new(self)
    }
}

#[repr(C)]
pub struct XsvCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    reader: csv::Reader<Box<dyn Read>>,
    record: csv::StringRecord,
    rowid: i64,
    eof: bool,
    declared_columns: Option<Vec<ColumnDeclaration>>,
}
impl XsvCursor {
    fn new(table: &XsvTable) -> Result<XsvCursor> {
        let mut cursor = XsvCursor {
            base: unsafe { mem::zeroed() },
            reader: table.reader()?,
            rowid: 0,
            record: csv::StringRecord::new(),
            eof: false,
            declared_columns: table.declared_columns.clone(),
        };
        cursor.next().map(|_| cursor)
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
                    format!("Error while reading next row: {}", err).as_str(),
                )),
            },
        }
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let i = usize::try_from(i)
            .map_err(|_| Error::new_message(format!("what the fuck {}", i).as_str()))?;
        let s = &self
            .record
            .get(i)
            .ok_or_else(|| Error::new_message(format!("wut {}", i).as_str()))?;
        match self.declared_columns.as_ref().and_then(|c| c.get(i)) {
            Some(column) => column.affinity().result_text(context, s)?,
            None => api::result_text(context, s)?,
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
    delimiter: u8,
    quote: u8,
    columns: Option<Vec<ColumnDeclaration>>,
}

fn parse_xsv_arguments(
    db: *mut sqlite3,
    arguments: Vec<String>,
    initial_delimiter: Option<u8>,
) -> Result<XsvArguments> {
    let mut filename: Option<String> = None;
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
    let filename = filename.ok_or_else(|| Error::new_message("no filename given. Specify a path to a CSV file to read from with 'filename=\"path.csv\"'"))?;
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
                Some(b',')
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
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
                Some(b',')
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
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
                Some(b',')
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
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
                None
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
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
                None
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                delimiter: b'|',
                quote: b'\0',
                columns: None,
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter=''".to_string()],
                None
            ),
            Err(Error::new(ErrorKind::Message(
                "delimiter must have at least 1 character".to_string()
            )))
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter='\t'".to_string()],
                None
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                delimiter: b'\t',
                quote: b'"',
                columns: None,
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter='💖'".to_string()],
                None
            ),
            Err(Error::new(ErrorKind::Message(
                "delimiter can only be 1 character long".to_string()
            )))
        );
    }
}

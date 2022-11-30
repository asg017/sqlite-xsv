use sqlite_loadable::prelude::*;
use sqlite_loadable::vtab_argparse::{parse_argument, Argument};
use sqlite_loadable::{Result, Error, BestIndexError, api, table::{VTab, VTabCursor, VTabArguments, IndexInfo}};

use std::{io::Read, marker::PhantomData, mem, os::raw::c_int};

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
}
impl XsvTable {
    fn reader(&self) -> Result<csv::Reader<Box<dyn Read>>> {
        let source_reader = get_csv_source_reader(&self.path)?;

        Ok(csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(source_reader))
    }
    fn schema_from_reader(&self) -> Result<String> {
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
}
impl<'vtab> VTab<'vtab> for XsvTable {
    type Aux = u8;
    type Cursor = XsvCursor<'vtab>;

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
        let arguments = parse_xsv_arguments(db, args.arguments, aux.map(|a| a.to_owned()))?;
        let vtab = XsvTable {
            base: unsafe { mem::zeroed() },
            db,
            path: arguments.filename,
            delimiter: arguments.delimiter,
            quote: arguments.quote,
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

    fn open(&mut self) -> Result<XsvCursor<'_>> {
        //XsvCursor::new(&self.path, self.delimiter, self.quote)
        XsvCursor::new(self)
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
    fn new<'vtab>(table: &XsvTable) -> Result<XsvCursor<'vtab>> {
        let mut cursor = XsvCursor {
            base: unsafe { mem::zeroed() },
            reader: table.reader()?,
            rowid: 0,
            record: csv::StringRecord::new(),
            eof: false,
            phantom: PhantomData,
        };
        cursor.next().map(|_| cursor)
    }
}

impl VTabCursor for XsvCursor<'_> {
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
                    "Error: Unknown error while reading next row",
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
        self.record.get(i);
        let s = &self
            .record
            .get(i)
            .ok_or_else(|| Error::new_message(format!("wut {}", i).as_str()))?;
        api::result_text(context, s)?;
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
}

fn parse_xsv_arguments(
    db: *mut sqlite3,
    arguments: Vec<String>,
    initial_delimiter: Option<u8>,
) -> Result<XsvArguments> {
    let mut filename: Option<String> = None;
    let mut delimiter = initial_delimiter;
    let mut quote = b'"';
    for arg in arguments {
        match parse_argument(arg.as_str()) {
            Ok(arg) => match arg {
                Argument::Column(_) => {}
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
    Ok(XsvArguments {
        filename,
        delimiter,
        quote,
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
            })
        );
    }
    #[test]
    fn test_delimiter() {
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter='|'".to_string()],
                None
            ),
            Ok(XsvArguments {
                filename: "a.csv".to_string(),
                delimiter: b'|',
                quote: b'"',
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter=''".to_string()],
                None
            ),
            Err(Error::new(ErrorKind::Message(
                "empty string, 1 character required".to_string()
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
            })
        );
        assert_eq!(
            parse_xsv_arguments(
                std::ptr::null_mut(),
                vec!["filename='a.csv'".to_string(), "delimiter='💖'".to_string()],
                None
            ),
            Err(Error::new(ErrorKind::Message(
                "Invalid delimiter, must be 1 character long (8 bits)".to_string()
            )))
        );
    }
}

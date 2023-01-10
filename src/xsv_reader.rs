use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    vtab_argparse::*,
    BestIndexError, Error, Result,
};
use std::{io::Read, marker::PhantomData, mem, os::raw::c_int};

use crate::util::{
    get_csv_source_reader, parse_delimiter_config_value, parse_header_config_value,
    parse_quote_config_value,
};

#[repr(C)]
pub struct XsvReaderTable {
    /// must be first
    base: sqlite3_vtab,
    header: bool,
    delimiter: u8,
    quote: u8,
    columns: Vec<ColumnDeclaration>,
}

impl<'vtab> VTab<'vtab> for XsvReaderTable {
    type Aux = u8;
    type Cursor = XsvReaderCursor<'vtab>;

    fn create(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTabArguments,
    ) -> Result<(String, Self)> {
        Self::connect(db, aux, args)
    }
    fn connect(
        _db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTabArguments,
    ) -> Result<(String, XsvReaderTable)> {
        let arguments = parse_reader_arguments(args.arguments, aux.map(|a| a.to_owned()))?;
        let base: sqlite3_vtab = unsafe { mem::zeroed() };

        let vtab = XsvReaderTable {
            base,
            header: arguments.header,
            delimiter: arguments.delimiter,
            quote: arguments.quote,
            columns: arguments.columns,
        };

        let mut sql = String::from("create table x( _source hidden");
        for column in &vtab.columns {
            sql.push(',');
            sql.push_str(column.vtab_declaration().as_str());
        }
        sql.push(')');
        Ok((sql, vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_source = false;
        for mut constraint in info.constraints() {
            if constraint.column_idx() == 0 {
                if !has_source && !constraint.usable()
                    || constraint.op() != Some(sqlite_loadable::table::ConstraintOperator::EQ)
                {
                    return Err(BestIndexError::Constraint);
                }
                has_source = true;
                constraint.set_omit(true);
                constraint.set_argv_index(1);
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

    fn open(&mut self) -> Result<XsvReaderCursor<'_>> {
        XsvReaderCursor::new(self.delimiter, self.quote, &self.columns, self.header)
    }
}

#[repr(C)]
pub struct XsvReaderCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    header: bool,
    delimiter: u8,
    quote: u8,
    columns: &'vtab Vec<ColumnDeclaration>,
    path: Option<String>,
    current_reader: Option<csv::Reader<Box<dyn Read>>>,
    record: csv::StringRecord,
    rowid: i64,
    eof: bool,
    phantom: PhantomData<&'vtab XsvReaderTable>,
}
impl XsvReaderCursor<'_> {
    fn new(
        delimiter: u8,
        quote: u8,
        columns: &Vec<ColumnDeclaration>,
        header: bool,
    ) -> Result<XsvReaderCursor> {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        let record = csv::StringRecord::new();

        let cursor = XsvReaderCursor {
            base,
            header,
            delimiter,
            quote,
            columns,
            path: None,
            current_reader: None,
            rowid: 0,
            record,
            eof: false,
            phantom: PhantomData,
        };
        Ok(cursor)
    }
}

impl VTabCursor for XsvReaderCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let path =
            api::value_text(values.get(0).ok_or_else(|| {
                Error::new_message("Internal error: expected argv[0] in xFilter")
            })?)?;
        let r = get_csv_source_reader(path)?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(self.header)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(r);
        self.path = Some(path.to_owned());
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
                    format!(
                        "Error reading {}: {}",
                        self.path.as_ref().map_or("", |p| p),
                        err
                    )
                    .as_str(),
                )),
            },
        }
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        if i < 1 {
            return Ok(());
        }
        let i = usize::try_from(i - 1)
            .map_err(|_| Error::new_message(format!("what the fuck {}", i).as_str()))?;
        let column = self
            .columns
            .get(i)
            .ok_or_else(|| Error::new_message("what the fuck"))?;
        let s = &self
            .record
            .get(i)
            .ok_or_else(|| Error::new_message(format!("wut {}", i).as_str()))?;
        column.affinity().result_text(context, s)?;
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

struct ReaderArguments {
    columns: Vec<ColumnDeclaration>,
    header: bool,
    delimiter: u8,
    quote: u8,
}

fn parse_reader_arguments(
    arguments: Vec<String>,
    initial_delimiter: Option<u8>,
) -> Result<ReaderArguments> {
    let mut columns = vec![];
    let mut delimiter = initial_delimiter;
    let mut quote = b'"';
    let mut header = true;
    for arg in arguments {
        match parse_argument(arg.as_str()) {
            Ok(arg) => match arg {
                Argument::Column(column_definition) => {
                    columns.push(column_definition);
                }
                Argument::Config(config) => match config.key.as_str() {
                    "delimiter" => {
                        delimiter = Some(parse_delimiter_config_value(config.value)?);
                    }
                    "quote" => {
                        quote = parse_quote_config_value(config.value)?;
                    }
                    "header" => {
                        header = parse_header_config_value(config.value)?;
                    }
                    _ => (),
                },
            },
            Err(err) => return Err(Error::new_message(err.as_str())),
        };
    }
    let delimiter = delimiter.ok_or_else(|| {
        Error::new_message("no delimiter given. Specify a delimiter to use with 'delimiter=\"\t\"'")
    })?;

    Ok(ReaderArguments {
        columns,
        header,
        delimiter,
        quote,
    })
}

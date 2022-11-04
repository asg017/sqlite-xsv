/// create table xxx(something json, another any);
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

use crate::util::{
    arg_is_parameter, column_affinity, parse_column_definition, quoted_value, SqliteColumnAffinity,
};

#[repr(C)]
pub struct XsvReaderTable {
    /// must be first
    base: sqlite3_vtab,
    delimiter: u8,
    columns: Vec<ReaderColumn>,
}

unsafe impl<'vtab> VTab<'vtab> for XsvReaderTable {
    type Aux = u8;
    type Cursor = XsvReaderCursor<'vtab>;

    fn create(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTableArguments,
    ) -> Result<(String, Self)> {
        Self::connect(db, aux, args)
    }
    fn connect(
        _db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTableArguments,
    ) -> Result<(String, XsvReaderTable)> {
        let arguments = parse_reader_arguments(args.arguments, aux.map(|a| a.to_owned()))?;
        let base: sqlite3_vtab = unsafe { mem::zeroed() };

        let vtab = XsvReaderTable {
            base,
            delimiter: arguments.delimiter,
            columns: arguments.columns,
        };

        let mut sql = String::from("create table x( _source hidden");
        for column in &vtab.columns {
            sql.push_str(&format!(", {}", column.definition));
        }
        sql.push(')');
        Ok((sql, vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: SqliteXIndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_source = false;
        for mut constraint in info.constraints() {
            match constraint.icolumn() {
                0 => {
                    if !has_source && !constraint.usable()
                        || constraint.op() != Some(sqlite3_loadable::table::ConstraintOperator::EQ)
                    {
                        return Err(BestIndexError::Constraint);
                    }
                    has_source = true;
                    constraint.set_omit(true);
                    constraint.set_argv_index(1);
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

    fn open(&mut self) -> Result<XsvReaderCursor<'_>> {
        XsvReaderCursor::new(self.delimiter, &self.columns)
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
        _ => Err(Error::new_message(
            format!("Error: no file extension detected for '{}'", path).as_str(),
        )),
    }
}
#[repr(C)]
pub struct XsvReaderCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    delimiter: u8,
    columns: &'vtab Vec<ReaderColumn>,
    path: Option<String>,
    current_reader: Option<csv::Reader<Box<dyn Read>>>,
    record: csv::StringRecord,
    rowid: i64,
    eof: bool,
    phantom: PhantomData<&'vtab XsvReaderTable>,
}
impl XsvReaderCursor<'_> {
    fn new<'vtab>(
        delimiter: u8,
        columns: &'vtab Vec<ReaderColumn>,
    ) -> Result<XsvReaderCursor<'vtab>> {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        let record = csv::StringRecord::new();

        let cursor = XsvReaderCursor {
            base,
            delimiter,
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

unsafe impl VTabCursor for XsvReaderCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: Vec<SqliteValue>,
    ) -> Result<()> {
        let path = values
            .get(0)
            .ok_or_else(|| Error::new_message("Internal error: expected argv[0] in xFilter"))?
            .text()?;
        let r = get_reader(&path)?;
        let reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .quote(b'\0') // temporary for wiki project
            .from_reader(r);
        self.path = Some(path);
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
                kind => Err(Error::new_message(
                    format!("Error reading {}: {}", self.path.as_ref().unwrap(), err.to_string()).as_str(),
                )),
            },
        }
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, ctx: SqliteContext, i: c_int) -> Result<()> {
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

        match &column.affinity {
            Some(affinity) => match affinity {
                SqliteColumnAffinity::Numeric => {
                    todo!("fuck")
                }
                // if the value belongs to a declared int column, then
                // try to parse as i32. If it fails, no worries, just result as text
                // TODO result_int64?
                SqliteColumnAffinity::Integer => match s.parse::<i32>() {
                    Ok(i) => ctx.result_int(i),
                    Err(_) => ctx.result_text(s)?,
                },
                // if the value belongs to a declared real column, then
                // try to parse as f64. If it fails, no worries, just result as text
                SqliteColumnAffinity::Real => match s.parse::<f64>() {
                    Ok(i) => ctx.result_double(i),
                    Err(_) => ctx.result_text(s)?,
                },
                // TODO what if blob? maybe just result the text as blob
                _ => ctx.result_text(s)?,
            },
            None => ctx.result_text(s)?,
        };
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

struct ReaderColumn {
    //name: String,
    /// full definition of the column, name and declared type (ex. "age int", "name text")
    definition: String,
    //declared_type: Option<String>,
    /// determined affinity for the column, based on the definition
    /// TODO shouldn't this always be some?
    affinity: Option<SqliteColumnAffinity>,
}
struct ReaderArguments {
    columns: Vec<ReaderColumn>,
    delimiter: u8,
}

fn parse_reader_arguments(
    arguments: Vec<String>,
    initial_delimiter: Option<u8>,
) -> Result<ReaderArguments> {
    let mut columns = vec![];
    let mut delimiter = initial_delimiter;
    for arg in arguments {
        match arg_is_parameter(&arg) {
            Some((key, value)) => match key {
                "delimiter" => {
                    delimiter = Some(parse_reader_argument_delimiter(value, initial_delimiter)?);
                }
                _ => {
                    return Err(Error::new_message(
                        format!("unknown parameter key '{}'", key).as_str(),
                    ))
                }
            },
            None => {
                let def = parse_column_definition(arg.as_str())
                    .map_err(|_| Error::new_message("empty column definition"))?;
                columns.push(ReaderColumn {
                    //name: def.0.to_owned(),
                    definition: arg.to_owned(),
                    //declared_type: def.1.map(|s| s.to_owned()),
                    affinity: def.1.map(|s| column_affinity(s)),
                })
            }
        }
    }
    let delimiter = delimiter.ok_or_else(|| {
        Error::new_message("no delimiter given. Specify a delimiter to use with 'delimiter=\"\t\"'")
    })?;

    Ok(ReaderArguments { columns, delimiter })
}

fn parse_reader_argument_delimiter(value: &str, initial_delimiter: Option<u8>) -> Result<u8> {
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

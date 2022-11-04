use ::csv::{ReaderBuilder, StringRecord};
use sqlite3_loadable::{
    api::{context_result_null, context_result_text, value_int, value_text},
    errors::{Error, Result},
};
use sqlite3ext_sys::{sqlite3_context, sqlite3_value};

pub fn csv_field_at(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let arg_record = values
        .get(0)
        .ok_or_else(|| Error::new_message("expected 1st argument as record"))?
        .to_owned();
    let arg_index = values
        .get(1)
        .ok_or_else(|| Error::new_message("expected 2nd argument as index"))?
        .to_owned();

    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(value_text(arg_record)?.as_bytes());

    let index = usize::try_from(value_int(arg_index))
        .map_err(|_| Error::new_message("not a valid index"))?;

    let mut record = StringRecord::new();

    match reader.read_record(&mut record) {
        Ok(x) => {
            if x {
                match record.get(index) {
                    Some(field) => {
                        context_result_text(context, field)?;
                    }
                    None => {
                        context_result_null(context);
                    }
                }
            } else {
                return Err(Error::new_message("No CSV record found."));
            }
        }
        Err(_) => {
            return Err(Error::new_message("Error reading CSV record"));
        }
    };

    Ok(())
}

pub fn tsv_field_at(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let arg_record = values
        .get(0)
        .ok_or_else(|| Error::new_message("expected 1st argument as record"))?
        .to_owned();
    let arg_index = values
        .get(1)
        .ok_or_else(|| Error::new_message("expected 2nd argument as index"))?
        .to_owned();

    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_reader(value_text(arg_record)?.as_bytes());

    let index = usize::try_from(value_int(arg_index))
        .map_err(|_| Error::new_message("not a valid index"))?;

    let mut record = StringRecord::new();

    match reader.read_record(&mut record) {
        Ok(x) => {
            if x {
                match record.get(index) {
                    Some(field) => {
                        context_result_text(context, field)?;
                    }
                    None => {
                        context_result_null(context);
                    }
                }
            } else {
                return Err(Error::new_message("No TSV record found."));
            }
        }
        Err(_) => {
            return Err(Error::new_message("Error reading TSV record"));
        }
    };

    Ok(())
}

trait X {
    fn delimiter() -> u8;
}
pub fn xsv_field_at(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let arg_delimiter = values
        .get(0)
        .ok_or_else(|| Error::new_message("expected 1st argument as delimiter"))?
        .to_owned();
    let arg_record = values
        .get(1)
        .ok_or_else(|| Error::new_message("expected 2nd argument as record"))?
        .to_owned();
    let arg_index = values
        .get(2)
        .ok_or_else(|| Error::new_message("expected 3rd argument as index"))?
        .to_owned();

    let delimiter = value_text(arg_delimiter)?.as_bytes().get(0).unwrap();
    let mut reader = ReaderBuilder::new()
        .delimiter(*delimiter)
        .has_headers(false)
        .from_reader(value_text(arg_record)?.as_bytes());

    let index = usize::try_from(value_int(arg_index))
        .map_err(|_| Error::new_message("not a valid index"))?;

    let mut record = StringRecord::new();

    match reader.read_record(&mut record) {
        Ok(x) => {
            if x {
                match record.get(index) {
                    Some(field) => {
                        context_result_text(context, field)?;
                    }
                    None => {
                        context_result_null(context);
                    }
                }
            } else {
                return Err(Error::new_message("No CSV record found."));
            }
        }
        Err(_) => {
            return Err(Error::new_message("Error reading CSV record"));
        }
    };

    Ok(())
}

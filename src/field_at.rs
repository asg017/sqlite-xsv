use ::csv::{ReaderBuilder, StringRecord};
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    Error, Result, api
};

pub fn xsv_field_at(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
    delimiter: &Option<u8>,
) -> Result<()> {
    let (delimiter, record, index) = match delimiter {
        // A compile-time delimiter was given - 0=record, 1=index
        Some(delimiter) => {
            let record = api::value_text(
                values
                    .get(0)
                    .ok_or_else(|| Error::new_message("expected 2nd argument as record"))?
,
            )?;
            let index = api::value_int(
                values
                    .get(1)
                    .ok_or_else(|| Error::new_message("expected 3rd argument as index"))?
,
            );
            (delimiter.to_owned(), record, index)
        }
        None => {
            let delimiter = api::value_text(
                values
                    .get(0)
                    .ok_or_else(|| Error::new_message("expected 1st argument as delimiter"))?
,
            )?
            .as_bytes()
            .first()
            .ok_or_else(|| Error::new_message("delimiter must be 1 character"))?;
            let record = api::value_text(
                values
                    .get(1)
                    .ok_or_else(|| Error::new_message("expected 2nd argument as record"))?

            )?;
            let index = api::value_int(
                values
                    .get(2)
                    .ok_or_else(|| Error::new_message("expected 3rd argument as index"))?

            );
            (delimiter.to_owned(), record, index)
        }
    };

    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .from_reader(record.as_bytes());

    let index = usize::try_from(index).map_err(|_| Error::new_message("not a valid index"))?;

    let mut record = StringRecord::new();

    match reader.read_record(&mut record) {
        Ok(has_content) => {
            if has_content {
                match record.get(index) {
                    Some(field) => {
                      api::result_text(context, field)?;
                    }
                    None => {
                      api::result_null(context);
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

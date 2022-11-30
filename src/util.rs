#![allow(clippy::not_unsafe_ptr_arg_deref)]

use flate2::bufread::GzDecoder;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    Error, Result,
    vtab_argparse::ConfigOptionValue
};
use std::{
    ffi::{CStr, CString, OsStr},
    fs::File,
    io::{BufReader, Read},
    os::raw::c_char,
    path::Path,
};


#[cfg(feature = "http_support")]
use sqlite_loadable::{api::value_pointer, ext::sqlite3ext_column_value};
#[cfg(feature = "http_support")]
use url::Url;

/// given a "path" (which can be a filepath or URL, if http_support or
/// s3_support is enabled), return an std::io::Reader that can be passed
/// into rust-csv ReadBuilder. Determines whether or not to use gzip
/// decompressing  (based on file extension only for now), or whether
/// to use sqlite-http/sqlite-s3 if a URL is supplied.
pub fn get_csv_source_reader(path: &str) -> Result<Box<dyn Read>> {
    #[cfg(feature = "http_support")]
    if let Ok(url) = Url::parse(path) {
        match url.scheme() {
            "http" | "https" => {
                let call = sqlite_http_call(db, path).expect("http_call to succes");
                return Ok(Box::new(call.read()));
            }
            _ => {
                let call = sqlite_s3_http(db, path).expect("http_call to succes");
                return Ok(Box::new(call.read()));
            }
        }
    }
    match Path::new(path).extension().and_then(OsStr::to_str) {
        Some(ext) => match ext {
            #[cfg(feature = "gzip_support")]
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

/* 
pub fn sqlite_parameter_value(
    db: *mut sqlite3,
    key: &str,
) -> std::result::Result<Option<String>, QueryRowError> {
    let lookup = "select value from temp.sqlite_parameters where key = ?";
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
    let clookup = CString::new(lookup).unwrap();
    let code =
        unsafe { sqlite3ext_prepare_v2(db, clookup.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    if code != 0 {
        return Err(QueryRowError::Prepare(code));
    }
    if stmt.is_null() {
        return Err(QueryRowError::NullQuery);
    }
    let ckey = CString::new(key.as_bytes()).unwrap();
    unsafe {
        sqlite3ext_bind_text(stmt, 1, ckey.as_ptr(), -1);
    }
    let step = unsafe { sqlite3ext_step(stmt) };

    if step == 100 {
        let cvalue = unsafe { sqlite3ext_column_text(stmt, 0) };
        let value = unsafe { CStr::from_ptr(cvalue as *const c_char) };

        let result = value.to_str().unwrap().to_owned();
        unsafe { sqlite3ext_finalize(stmt) };
        return Ok(Some(result));
    }
    unsafe { sqlite3ext_finalize(stmt) };
    Ok(None)
}
*/

#[cfg(feature = "s3_support")]
use http0::http::HttpCall;

#[cfg(feature = "s3_support")]
pub fn sqlite_s3_http(db: *mut sqlite3, key: &str) -> Result<Box<HttpCall>, QueryRowError> {
    let lookup = "select http_call(s3_object_get_presigned_url(?, 60))";
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
    let clookup = CString::new(lookup).unwrap();
    let code =
        unsafe { sqlite3ext_prepare_v2(db, clookup.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    if code != 0 {
        return Err(QueryRowError::Prepare(code));
    }
    if stmt.is_null() {
        return Err(QueryRowError::NullQuery);
    }
    let ckey = CString::new(key.as_bytes()).unwrap();
    unsafe {
        sqlite3ext_bind_text(stmt, 1, ckey.as_ptr(), -1);
    }
    let step = unsafe { sqlite3ext_step(stmt) };

    if step == 100 {
        unsafe {
            let value = sqlite3ext_column_value(stmt, 0);
            let p = value_pointer(value, "http_call");
            sqlite3ext_finalize(stmt);
            return Ok(Box::from_raw(p.cast::<HttpCall>()));
        }
    }
    unsafe { sqlite3ext_finalize(stmt) };
    Err(QueryRowError::NoRow(step))
} //*/
#[derive(Debug)]
pub enum QueryRowError {
    Prepare(i32),
    NullQuery,
    #[cfg(feature = "http_support")]
    NoRow(i32),
}
#[cfg(feature = "http_support")]
pub fn sqlite_http_call(db: *mut sqlite3, key: &str) -> Result<Box<HttpCall>, QueryRowError> {
    let lookup = "select http_call(?)";
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
    let clookup = CString::new(lookup).unwrap();
    let code =
        unsafe { sqlite3ext_prepare_v2(db, clookup.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    if code != 0 {
        return Err(QueryRowError::Prepare(code));
    }
    if stmt.is_null() {
        return Err(QueryRowError::NullQuery);
    }
    let ckey = CString::new(key.as_bytes()).unwrap();
    unsafe {
        sqlite3ext_bind_text(stmt, 1, ckey.as_ptr(), -1);
    }
    let step = unsafe { sqlite3ext_step(stmt) };
    if step == 100 {
        unsafe {
            let value = sqlite3ext_column_value(stmt, 0);
            let p = value_pointer(value, "http_call");
            sqlite3ext_finalize(stmt);
            return Ok(Box::from_raw(p.cast::<HttpCall>()));
        }
    }
    unsafe { sqlite3ext_finalize(stmt) };
    Err(QueryRowError::NoRow(step))
}

/// Parse the `delimiter="|"` config option argument.
/// Only quoted, single-character values are allowed.
pub fn parse_delimiter_config_value(value: ConfigOptionValue) -> Result<u8> {
    if let ConfigOptionValue::Quoted(value) = value {
        let mut bytes = value.bytes();
        let result = bytes
            .next()
            .ok_or_else(|| Error::new_message("delimiter must have at least 1 character"))?;
        if bytes.next().is_some() {
            return Err(Error::new_message("delimiter can only be 1 character long"));
        }
        Ok(result)
    } else {
        Err(Error::new_message(
            "'delimiter' value must be string, wrap in single or double quotes.",
        ))
    }
}

/// Parse the `quote="'"` config option argument.
/// Only quoted, single-character values are allowed.
pub fn parse_quote_config_value(value: ConfigOptionValue) -> Result<u8> {
    if let ConfigOptionValue::Quoted(value) = value {
        let mut bytes = value.bytes();
        let result = bytes
            .next()
            .ok_or_else(|| Error::new_message("quote must have at least 1 character"))?;
        if bytes.next().is_some() {
            return Err(Error::new_message("quote can only be 1 character long`"));
        }
        Ok(result)
    } else {
        Err(Error::new_message(
            "'quote' value must be string, wrap in single or double quotes.",
        ))
    }
}

/// Parse the `file="path/to.csv"` config option argument.
/// Value can either be quoted strings or sqlite_parameter name values.
pub fn parse_filename_config_value(db: *mut sqlite3, value: ConfigOptionValue) -> Result<String> {
    match value {
        ConfigOptionValue::Quoted(value) => Ok(value),
        /*ConfigOptionValue::SqliteParameter(value) => {
            match sqlite_parameter_value(db, value.as_str()) {
                Ok(result) => match result {
                    Some(path) => Ok(path),
                    None => Err(Error::new_message(
                        format!("{value} is not defined in temp.sqlite_parameters table").as_str(),
                    )),
                },
                Err(_) => Err(Error::new_message(
                    "temp.sqlite_parameters is not defined, can't use sqlite parameters as value",
                )),
            }
        }*/
        _ => Err(Error::new_message(
            "'filename' value must be string, wrap in single or double quotes.",
        )),
    }
}

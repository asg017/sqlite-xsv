#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use flate2::read::GzDecoder;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{vtab_argparse::ConfigOptionValue, Error, Result};

/// given a "path" (which can be a filepath or URL, if http_support or
/// s3_support is enabled), return an std::io::Reader that can be passed
/// into rust-csv ReadBuilder. Determines whether or not to use gzip
/// decompressing  (based on file extension only for now), or whether
/// to use sqlite-http/sqlite-s3 if a URL is supplied.
pub fn get_csv_source_reader(path: &str) -> Result<Box<dyn Read>> {
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

/// Parse the `delimiter="|"` config option argument.
/// Only quoted, single-character values are allowed.
pub fn parse_delimiter_config_value(value: ConfigOptionValue) -> Result<u8> {
    if let ConfigOptionValue::Quoted(value) = value {
        let mut bytes = value.bytes();
        let result = bytes
            .next()
            .ok_or_else(|| Error::new_message("quote must have at least 1 character"))?;
        match bytes.next() {
            Some(c) => {
                if result != b'\\' {
                    return Err(Error::new_message("quote can only be 1 character long`"));
                }

                match c {
                    b'0' => Ok(b'\0'),
                    b't' => Ok(b'\t'),
                    b'n' => Ok(b'\n'),
                    _ => Err(Error::new_message("unrecognized slash value")),
                }
            }
            _ => Ok(result),
        }
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
        match bytes.next() {
            Some(c) => {
                if result != b'\\' {
                    return Err(Error::new_message("quote can only be 1 character long`"));
                }

                match c {
                    b'0' => Ok(b'\0'),
                    b't' => Ok(b'\t'),
                    b'n' => Ok(b'\n'),
                    _ => Err(Error::new_message("unrecognized slash value")),
                }
            }
            _ => Ok(result),
        }
    } else {
        Err(Error::new_message(
            "'quote' value must be string, wrap in single or double quotes.",
        ))
    }
}

/// Parse the `file="path/to.csv"` config option argument.
/// Value can either be quoted strings or sqlite_parameter name values.
pub fn parse_filename_config_value(_db: *mut sqlite3, value: ConfigOptionValue) -> Result<String> {
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

pub fn parse_header_config_value(value: ConfigOptionValue) -> Result<bool> {
    match value {
        ConfigOptionValue::Quoted(value) | ConfigOptionValue::Bareword(value) => {
            match value.to_lowercase().as_str() {
                "yes" | "y" | "on" | "true" | "1" => Ok(true),
                "no" | "n" | "off" | "false" | "0" => Ok(false),
                _ => Err(Error::new_message(
                    format!("Unknown header value '{}'", value).as_str(),
                )),
            }
        }

        _ => Err(Error::new_message("Unknown header value")),
    }
}

use sqlite3_loadable::ext::{
    sqlite3ext_bind_text, sqlite3ext_column_text, sqlite3ext_finalize, sqlite3ext_prepare_v2,
    sqlite3ext_step,
};
use std::{
    ffi::{CStr, CString},
    os::raw::c_char,
};

use sqlite3ext_sys::{sqlite3, sqlite3_stmt};

pub fn sqlite_parameter_value(db: *mut sqlite3, key: &str) -> Result<Option<String>, ()> {
    let lookup = "select value from temp.sqlite_parameters where key = ?";
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
    let clookup = CString::new(lookup).unwrap();
    let code =
        unsafe { sqlite3ext_prepare_v2(db, clookup.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    if code != 0 || stmt.is_null() {
        return Err(());
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

/// Given a column definition,
pub fn parse_column_definition(definition: &str) -> Result<(&str, Option<&str>), ()> {
    let mut split = definition.split(" ");
    let name = split.next().ok_or(())?;
    let declared_type = split.next();
    return Ok((name, declared_type));
}

/// A columns "affinity",
/* TODO maybe include extra affinities?
- JSON - parse as text, see if it's JSON, if so then set subtype
- boolean - 1 or 0, then 1 or 0. What about YES/NO or TRUE/FALSE or T/F?
- datetime - idk man
- interval - idk man
 */
pub enum SqliteColumnAffinity {
    /// "char", "clob", or "text"
    Text,
    /// "int"
    Integer,
    /// "real", "floa", or "doub"
    Real,
    /// "blob" or empty
    Blob,
    /// else, no other matches
    Numeric,
}
// https://www.sqlite.org/datatype3.html#determination_of_column_affinity
pub fn column_affinity(declared_type: &str) -> SqliteColumnAffinity {
    let lower = declared_type.to_lowercase();
    // "If the declared type contains the string "INT" then it is assigned INTEGER affinity."
    if lower.contains("int") {
        return SqliteColumnAffinity::Integer;
    };

    // "If the declared type of the column contains any of the strings "CHAR",
    // "CLOB", or "TEXT" then that column has TEXT affinity.
    // Notice that the type VARCHAR contains the string "CHAR" and is
    // thus assigned TEXT affinity."

    if lower.contains("char") || lower.contains("clob") || lower.contains("text") {
        return SqliteColumnAffinity::Text;
    };

    // "If the declared type for a column contains the string "BLOB" or if no
    // type is specified then the column has affinity BLOB."

    if lower.contains("blob") || lower.is_empty() {
        return SqliteColumnAffinity::Blob;
    };

    // "If the declared type for a column contains any of the strings "REAL",
    // "FLOA", or "DOUB" then the column has REAL affinity."
    if lower.contains("real") || lower.contains("floa") || lower.contains("doub") {
        return SqliteColumnAffinity::Real;
    };

    // "Otherwise, the affinity is NUMERIC"
    return SqliteColumnAffinity::Numeric;
}

// TODO renamed "parameter" to "named argument"
pub fn arg_is_parameter(arg: &str) -> Option<(&str, &str)> {
    let mut split = arg.split('=');
    let key = match split.next() {
        Some(k) => k,
        None => return None,
    };
    let value = match split.next() {
        Some(k) => k,
        None => return None,
    };
    Some((key, value))
}

/// determine is the value of a named argument is quoted or not.
/// "Quoted" means surrounded in single or double quotes.
/// Should be used for filepaths/URLs values, ex `filename="foo.csv"`
// TODO what if quotes appear inside the string, ex `"name: "alex ""`?
//
pub fn quoted_value(value: &str) -> Option<&str> {
    if (value.starts_with('"') && value.ends_with('"'))
        || value.starts_with('\'') && value.ends_with('\'')
    {
        let mut chars = value.chars();
        chars.next();
        chars.next_back();
        Some(chars.as_str())
    } else {
        None
    }
}

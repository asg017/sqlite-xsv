# sqlite-xsv

A fast and performant SQLite extension for CSVs, TSVs, and other-SVs, written in Rust!

> **Note**
> Nothing to do with [xsv](https://github.com/BurntSushi/xsv), but is based on the same [Rust CSV crate](https://github.com/BurntSushi/rust-csv). This is named `sqlite-xsv` to distinguish between the official [SQLite CSV Virtual table](https://www.sqlite.org/csv.html) and the [`sqlean` vsv extension](https://github.com/nalgeon/sqlean/blob/main/docs/vsv.md).

```
gcc \
  -DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION=1 -DSQLITE_EXTRA_INIT=core_init \
  -I./ -I./sqlite  -I../sqlite-http-rs \
  tmp.c sqlite/shell.c ../sqlite-http-rs/target/release/libhttp0.a target/release/libxsv0.a \
  -framework CoreFoundation -framework Security \
  -Os -o test

./test :memory: 'create virtual table http_lat_place_totals using csv(filename="https://github.com/datadesk/california-coronavirus-data/raw/master/latimes-place-totals.csv");' 'select * from http_lat_place_totals limit 10;'
```

gcc csv.c -fPIC -shared -O3 -o csv.dylib -I /Users/alex/projects/sqlite-lines/sqlite

# http/s3 testing

```
mc ls local/boop
```

mc mb play/asg
mc cp hello.txt play/asg/hello.txt
mc cat play/asg/hello.txt
mc --help
echo ayoo | mc cp - play/asg/ayoo.txt
echo ayoo | mc pipe play/asg/ayoo.txt
mc cat play/asg/ayoo.txt
time mc cat play/asg/ayoo.txt
mc cp yellow_202104.csv local/boop/
history | grep mc

# TODO

- [ ] `\0` or `\t` in delimiter/quote
- [ ] `quote='|'`
- [ ] s3/http
  - [ ] on startup, see if `http_version`/`s3_version` is defined and lines up
  - [ ] `xsv_reader` support for urls/ `http_request()` objects
  - [ ] can we remove `sqlite-http` as rust dependency? compiled output goes from 2.1MB -> 5MB
- [ ] when CSV more headers than column limit, give descriptive error message
- [ ] `data="..."` support
- [ ] `data=:param`
- [ ] more reader parameters
  - [ ] `sniff="filename.csv"`
  - [ ] `schema="table_or_view"`
- [ ] reader affinity fix
- [ ] `zstd` support?
- [ ] `header=no` support
- [ ] other csv utils
  - [ ] `csv_record(field1, field2, field3)`
- [ ] reader constructor: `columns(regex)` support, `* exclude (,,,)`, `* replace (,,,,)` (?)

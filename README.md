# sqlite-xsv

A fast and performant SQLite extension for CSV files, written in Rust! Based on [`sqlite-loadable-rs`](https://github.com/asg017/sqlite-loadable-rs) and the wonderful [csv crate](https://github.com/BurntSushi/rust-csv).

- Query CSVs, TSVs, and other-SVs as SQLite virtual tables
- The "reader" interface lets you query CSVs from other data sources (URLs with [`sqlite-xsv`](https://github.com/asg017/sqlite-xsv))
- Builtin support for querying CSVs with gzip or zstd compression

> **Note**
> Nothing to do with [xsv](https://github.com/BurntSushi/xsv), but is based on the same csv crate. This is named `sqlite-xsv` to distinguish between the official [SQLite CSV Virtual table](https://www.sqlite.org/csv.html) and the [`sqlean` vsv extension](https://github.com/nalgeon/sqlean/blob/main/docs/vsv.md).

## Usage

```sql
.load ./xsv0

create virtual table temp.students using csv(
  filename="students.csv"
);
```

Query TSVs or other

Provide a schema for CSVs that lack headers.

```sql
create virtual table xxx using csv(
  filename="",
  id text,
  name text,
  age int,

);
```

Query CSVs from HTTP endpoints, with the reader API and [`sqlite-xsv`](https://github.com/asg017/sqlite-xsv). Note: Only works for CSVs that work in memory, for now.

```sql
.load ./xsv0
-- Reading a CSV from the wonderful LA Times COVID proejct
-- https://github.com/datadesk/california-coronavirus-data


create virtual table temp.cdph_age_reader using csv(
  date,
  age text,
  confirmed_cases_total int,
  confirmed_cases_percent float,
  deaths_total int,
  deaths_percent float
);

create table cdph_age as
  select *
  from temp.cdph_age_reader(
    http_get_body(
      'https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-age.csv'
    )
  );

select *
from cdph_age
limit 5;

/*

*/
```

## Documentation

See [`docs.md`](./docs.md) for a full API reference.

## Installing

The [Releases page](https://github.com/asg017/sqlite-xsv/releases) contains pre-built binaries for Linux amd64, MacOS amd64 (no arm yet), and Windows.

### As a loadable extension

If you want to use `sqlite-xsv` as a [Runtime-loadable extension](https://www.sqlite.org/loadext.html), Download the `xsv0.dylib` (for MacOS), `xsv0.so` (Linux), or `xsv0.dll` (Windows) file from a release and load it into your SQLite environment.

> **Note:**
> The `0` in the filename (`xsv0.dylib`/ `xsv0.so`/`xsv0.dll`) denotes the major version of `sqlite-xsv`. Currently `sqlite-xsv` is pre v1, so expect breaking changes in future versions.

For example, if you are using the [SQLite CLI](https://www.sqlite.org/cli.html), you can load the library like so:

```sql
.load ./xsv0
select xsv_version();
-- v0.0.1
```

Or in Python, using the builtin [sqlite3 module](https://docs.python.org/3/library/sqlite3.html):

```python
import sqlite3

con = sqlite3.connect(":memory:")

con.enable_load_extension(True)
con.load_extension("./xsv0")

print(con.execute("select xsv_version()").fetchone())
# ('v0.0.1',)
```

Or in Node.js using [better-sqlite3](https://github.com/WiseLibs/better-sqlite3):

```javascript
const Database = require("better-sqlite3");
const db = new Database(":memory:");

db.loadExtension("./xsv0");

console.log(db.prepare("select xsv_version()").get());
// { 'xsv_version()': 'v0.0.1' }
```

For [Datasette](https://datasette.io/), it is currently NOT recommended to load `sqlite-xsv` in public Datasette instances. This is because the SQL API

### 1. The fastest SQLite CSV extension

A benchmark that shows how fast `sqlite-xsv` is compared to other CSV tools, **for counting rows**

![](./benchmarks/count.png)

All while keeping a familiar SQL API!

```sql
create virtual table flights using csv(filename="benchmarks/_data/flights.csv");
select year, quarter, month from flights limit 20;
```

`sqlite-xsv` and SQLite is still much slower at _analytical queries_ on top of CSVs, however.

### 2. Several CSV utilities beyond single-file reads

```sql
select
  csv_field_at(record, 0) as id,
  csv_field_at(record, 1) as name
from csv_records(readfile('students.csv'));
```

### 3. A CSV "reader" API

Rarely do you ever have only 1 CSV with all your data. Often times you'll have an entire directory of CSVs with all the same schema. The `csv_reader` virtual table can handle this with ease!

```sql
create virtual table students_reader using csv_reader(id integer, name text, age integer, progess real);

with files as (
   select name as path
   from fsdir('tests/data/student_files')

)
select
  files.path,
  students.*
from files
join students_reader(files.path) as students
where files.path like '%.csv';
/*
┌────────────────────────────────┬────┬───────────┬─────┬─────────┐
│              path              │ id │   name    │ age │ progess │
├────────────────────────────────┼────┼───────────┼─────┼─────────┤
│ tests/data/student_files/a.csv │ 1  │ alex      │ 10  │ 0.9     │
│ tests/data/student_files/a.csv │ 2  │ adrian    │ 20  │ 0.8     │
│ tests/data/student_files/a.csv │ 3  │ andres    │ 30  │ 0.7     │
│ tests/data/student_files/c.csv │ 1  │ craig     │ 70  │ 0.4     │
│ tests/data/student_files/c.csv │ 2  │ catherine │ 90  │ 0.5     │
│ tests/data/student_files/c.csv │ 3  │ coin      │ 80  │ 0.6     │
│ tests/data/student_files/b.csv │ 1  │ brian     │ 60  │ 0.1     │
│ tests/data/student_files/b.csv │ 2  │ beto      │ 50  │ 0.2     │
│ tests/data/student_files/b.csv │ 3  │ brandy    │ 40  │ 0.3     │
└────────────────────────────────┴────┴───────────┴─────┴─────────┘
*/
```

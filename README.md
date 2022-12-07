# sqlite-xsv

A fast and performant SQLite extension for CSVs, TSVs, and other-SVs, written in Rust! See [`sqlite-loadable-rs`](https://github.com/asg017/sqlite-loadable-rs), the framework that makes this extension possible.

> **Note**
> Nothing to do with [xsv](https://github.com/BurntSushi/xsv), but is based on the same [Rust CSV crate](https://github.com/BurntSushi/rust-csv). This is named `sqlite-xsv` to distinguish between the official [SQLite CSV Virtual table](https://www.sqlite.org/csv.html) and the [`sqlean` vsv extension](https://github.com/nalgeon/sqlean/blob/main/docs/vsv.md).

## WORK IN PROGRESS

This extension isn't 100% complete yet, but hoping to release in the next 1-2 weeks! A sneak peek at what to expect:

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

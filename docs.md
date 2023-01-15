## `sqlite-xsv` Documentation

## A note on the `temp.table` convention

Examples in this page will prefix new virtual tables with `temp.`, such as:

```sql
create virtual table temp.students using csv(filename="students.csv");
```

This is because it is recommended, though not required, to create CSV virtual tables in the `TEMP` schema. That way, the table only lasts for the lifetime of your database connection, and not persisted into any connected database.

If you want to persistent _data_ from a CSV file, you can simply create a new traditional table with the same contents as the temp CSV virtual table, like so.

```sql
create table students as
  select * from temp.students;
```

If you were to create a persistent CSV virtual table by omitting `temp.`, it will still work just fine. But, you'll need to ensure that other future clients and connections that use that database and query the virtual table have both the `sqlite-xsv` extension loaded, as well as the seperate CSV file in the same location as referenced in the `filename=` argument. If you're alright with that, then creating non-temp CSV virtual tables would work just fine!

## API Reference

<h3 name="xsv_version"> <pre>xsv_version()</pre></h3>

Results a version string of the current version of `sqlite-xsv`.

```sql
select xsv_version();
-- "v0.1.0"
```

<h3 name="xsv_debug"> <pre>xsv_debug()</pre></h3>

Returns a string of various debug information for xsv.

```sql
select xsv_debug();
-- ""
```

<h3 name="xsv"> <pre>xsv(filename)</pre></h3>

A virtual table for reading data from

Possible arguments in the constructor include:

- `filename` - Required string, path of the CSV file to read data from.
- `delimiter` - Required character, which delimiter to use to seperate fields (see [`csv`](#csv) and [`tsv`](#tsv)).
- `header` - Optional boolean, whether the 1st row in the file contains column names.
- `quote` - Optional character, a different quote character to use to escape fields, default's to `"` (double quote).

```sql
create virtual table students using xsv(
  filename="students.psv",
  delimiter="|",
  quote="'",
  header=false
);
```

By default, `xsv` will read the first row in the given file and use those as column names, with values defaulted to `TEXT`.

To use different column names, or to specify different types for different values, you can provide column declarations directly in the constructor. `sqlite-xsv` will apply [type affinity](https://www.sqlite.org/datatype3.html#type_affinity) to the values

```sql
create virtual table temp.students using xsv(
  filename="students.csv",
  delimiter=",",
  Name text,
  Age integer,
  Zipcode text,
  Progress real
);

select * from temp.students;
```

If your CSV lacks headers, be sure to pass in `header=false` so `sqlite-xsv` won't skip the first row.

<h3 name="csv"> <pre>csv(filename, [])</pre></h3>

Same as the [xsv virtual table](#xsv), but defaulted with a comma delimiter (`delimiter=","`). `filename` is still required, other parameters and column declarations are optional.

```sql
create virtual table temp.students using csv(
  filename="students.csv"
);

select * from temp.students;
```

<h3 name="tsv"> <pre>tsv()</pre></h3>

Same as the [xsv virtual table](#xsv), but defaulted with a comma delimiter (`delimiter="\t"`). `filename` is still required, other parameters and column declarations are optional.

```sql
create virtual table temp.students using tsv(
  filename="students.tsv"
);

select * from temp.students;
```

<h3 name="xsv_reader"> <pre>xsv_reader(schema)</pre></h3>

Similar to the `xsv` virtual table, but does not take in a `filename` parameter. Instead, column declarations are required, and the data source (filename, BLOBs, etc.) is provided at runtime.

This offers a more flexible API, say when you want to query multiple CSV files with all the same schema, or when using other SQL extensions like [`sqlite-http`](https://github.com/asg017/sqlite-http) to query CSVs from other places.

- `delimiter` - Required character, .
- `header` - Optional boolean, .
- `quote` - Option character, .

```sql
create virtual table temp.students_reader using xsv_reader(
  delimiter="|",
  id text,
  name text,
  age int,
  progress real
);

select * from temp.reader('file1.psv');
select * from temp.reader('file2.psv');
select * from temp.reader(func_returning_blob());
```

<h3 name="csv_reader"> <pre>csv_reader(filename, [])</pre></h3>

Same as the [`xsv_reader` virtual table](#xsv_reader), but defaulted with a comma delimiter (`delimiter=","`).

```sql
create virtual table temp.students_reader using csv_reader(
  id text,
  name text,
  age int,
  progress real
);

select * from temp.students_reader('file1.csv');
select * from temp.students_reader('file2.csv');
```

<h3 name="tsv_reader"> <pre>tsv_reader()</pre></h3>

Same as the [`xsv_reader` virtual table](#xsv_reader), but defaulted with a tab delimiter (`delimiter="\t"`).

```sql
create virtual table temp.students_reader using tsv_reader(
  id text,
  name text,
  age int,
  progress real
);

select * from temp.students_reader('file1.tsv');
select * from temp.students_reader('file2.tsv');
```

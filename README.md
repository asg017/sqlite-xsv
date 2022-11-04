gcc csv.c -fPIC -shared -O3 -o csv.dylib -I /Users/alex/projects/sqlite-lines/sqlite

# TODO

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

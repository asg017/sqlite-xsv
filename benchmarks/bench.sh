#!/bin/bash
hyperfine --warmup 3 \
  'duckdb :memory: "select count(*) from \"/Users/alex/projects/sqlitex-projects/lat/latimes-place-totals.csv\""' \
  'sqlite3x :memory: ".load target/release/libxsv0" "create virtual table s using csv(filename=\"/Users/alex/projects/sqlitex-projects/lat/latimes-place-totals.csv\");" "select count(*) from s;"' \
  'sqlite3x :memory: ".load target/release/libxsv0" "create virtual table s using csv_reader(id text,name text,date text,county text,fips text,confirmed_cases int,note text,population int);" "select count(*) from s(\"/Users/alex/projects/sqlitex-projects/lat/latimes-place-totals.csv\");"' \
  'sqlite3x :memory: ".load benchmarks/csv" "create virtual table s using csv(filename=\"/Users/alex/projects/sqlitex-projects/lat/latimes-place-totals.csv\");" "select count(*) from s;"'
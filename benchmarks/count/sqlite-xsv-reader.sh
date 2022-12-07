#!/bin/bash
sqlite3x :memory: \
  '.load ../../target/release/libxsv0' \
  'create virtual table s using csv_reader(id text,name text,date text,county text,fips text,confirmed_cases int,note text,population int);' \
  'select count(*) from s("../_data/totals.csv");'
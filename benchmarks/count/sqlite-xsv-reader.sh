#!/bin/bash
sqlite3 :memory: \
  '.load ../../dist/release/xsv0' \
  'create virtual table s using csv_reader(id text,name text,date text,county text,fips text,confirmed_cases int,note text,population int);' \
  'select count(*) from s("../_data/totals.csv");'
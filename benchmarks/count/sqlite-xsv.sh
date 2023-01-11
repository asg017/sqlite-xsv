#!/bin/bash
sqlite3 :memory: \
  '.load ../../target/release/libxsv0' \
  'create virtual table s using csv(filename="../_data/totals.csv");' \
  'select count(*) from s;'
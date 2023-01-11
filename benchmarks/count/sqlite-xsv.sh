#!/bin/bash
sqlite3 :memory: \
  '.load ../../dist/release/xsv0' \
  'create virtual table s using csv(filename="../_data/totals.csv");' \
  'select count(*) from s;'
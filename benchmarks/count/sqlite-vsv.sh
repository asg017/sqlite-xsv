#!/bin/bash
sqlite3 :memory: \
  '.load ../vsv' \
  'create virtual table s using vsv(filename="../_data/totals.csv");' \
  'select count(*) from s;'
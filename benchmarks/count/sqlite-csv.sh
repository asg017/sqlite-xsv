#!/bin/bash
sqlite3 :memory: \
  '.load ../csv' \
  'create virtual table s using csv(filename="../_data/totals.csv");' \
  'select count(*) from s;'
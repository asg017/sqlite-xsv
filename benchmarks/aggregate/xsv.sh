#!/bin/bash
sqlite3 :memory: \
  '.load ../../target/release/libxsv0' \
  'create virtual table s using csv(filename="../_data/yellow_202104.csv");' \
  'SELECT passenger_count, COUNT(*), AVG(total_amount) from s group by 1;'
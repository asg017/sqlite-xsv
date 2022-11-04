#!/bin/bash
hyperfine --warmup 3 \
  'dsq benchmarks/_data/yellow_202104.csv "SELECT passenger_count, COUNT(*), AVG(total_amount) FROM {} GROUP BY passenger_count"'
  'duckdb :memory: "select passenger_count, COUNT(*), AVG(total_amount) from \"benchmarks/_data/yellow_202104.csv\" group by passenger_count"' \
  'sqlite3x :memory: ".load target/release/libcsv0" "create virtual table s using xsv(filename=benchmarks/_data/yellow_202104.csv);" "select passenger_count, COUNT(*), AVG(total_amount) from s group by passenger_count" ' \
  'sqlite3x :memory: ".load benchmarks/csv" "create virtual table s using csv(filename=\"benchmarks/_data/yellow_202104.csv\", header=1);" "select passenger_count, COUNT(*), AVG(total_amount) from s group by passenger_count;"'

#hyperfine --warmup 3 \
#  'duckdb :memory: "select count(*) from \"benchmarks/_data/yellow_202104.csv\""' \
#  'sqlite3x :memory: ".load target/release/libcsv0" "create virtual table s using xsv(filename=benchmarks/_data/yellow_202104.csv);" "select count(*) from s;"' \
#  'sqlite3x :memory: ".load benchmarks/csv" "create virtual table s using csv(filename=\"benchmarks/_data/yellow_202104.csv\");" "select count(*) from s;"'
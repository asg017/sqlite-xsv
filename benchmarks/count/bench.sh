#!/bin/bash
hyperfine --warmup 10 --export-json=results.json \
  './rust-xsv.sh' \
  './sqlite-xsv.sh' \
  './sqlite-xsv-reader.sh' \
  './datafusion.sh' \
  './duckdb.sh' \
  './duckdb_parallel.sh' \
  './sqlite-csv.sh' \
  './octosql.sh' \
  './sqlite-cli-import.sh' \
  './dsq.sh' \
  './pandas.sh' \
  './sqlite-utils.sh'
  
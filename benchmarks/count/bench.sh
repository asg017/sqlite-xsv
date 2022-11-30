#!/bin/bash
hyperfine --warmup 10 --export-json=results.json \
  './rust-xsv.sh' \
  './xsv.sh' \
  './xsv_reader.sh' \
  './datafusion.sh' \
  './duck.sh' \
  './duck_parallel.sh' \
  './csv.sh' \
  './octosql.sh' \
  './sqlite_cli.sh' \
  './dsq.sh' \
  './pandas.sh' \
  './sqlite-utils.sh'
  
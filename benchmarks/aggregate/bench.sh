#!/bin/bash
hyperfine --warmup 10 --export-json=results.json \
  './xsv.sh' \
  './datafusion.sh' \
  './duck_parallel.sh' \
  './octosql.sh' \
  './dsq.sh' 
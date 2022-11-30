#!/bin/bash
hyperfine --warmup 10 --export-json=results-sqlite.json \
  './xsv.sh' \
  './xsv_reader.sh' \
  './csv.sh' \
  './vsv.sh' \
  './sqlite_cli.sh';
#!/bin/bash
duckdb :memory: \
  'SET experimental_parallel_csv=true;' \
  'select count(*) from "../_data/totals.csv"'
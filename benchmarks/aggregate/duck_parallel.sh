#!/bin/bash
duckdb :memory: \
  'SET experimental_parallel_csv=true;' \
  'SELECT passenger_count, COUNT(*), AVG(total_amount) from "../_data/yellow_202104.csv" group by 1'
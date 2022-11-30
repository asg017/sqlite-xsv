#!/bin/bash
duckdb :memory: \
'select count(*) from "../_data/totals.csv"'
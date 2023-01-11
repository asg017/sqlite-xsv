#!/bin/bash
duckdb :memory: \
"select count(*) from read_csv('../_data/totals.csv', header=true, columns={'id': 'text','name': 'text','date': 'date','county': 'text','fips': 'text','confirmed_cases': 'int64','note': 'text','population': 'int'});"
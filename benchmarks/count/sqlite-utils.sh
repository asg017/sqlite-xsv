#!/bin/bash
sqlite-utils memory ../_data/totals.csv \
  'select count(*) from totals'
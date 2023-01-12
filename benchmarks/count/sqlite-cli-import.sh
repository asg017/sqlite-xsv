#!/bin/bash
sqlite3 :memory: \
  '.import -csv ../_data/totals.csv s' \
  'select count(*) from s;'
  
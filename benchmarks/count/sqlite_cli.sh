#!/bin/bash
sqlite3x :memory: \
  '.import -csv ../_data/totals.csv s' \
  'select count(*) from s;'
  
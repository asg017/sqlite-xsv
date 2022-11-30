#!/bin/bash
OCTOSQL_NO_TELEMETRY=1  octosql 'SELECT passenger_count, COUNT(*), AVG(total_amount) from yellow_202104.csv group by passenger_count'
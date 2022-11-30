#!/bin/bash
dsq ../_data/yellow_202104.csv 'SELECT passenger_count, COUNT(*), AVG(total_amount) FROM {} group by 1;'
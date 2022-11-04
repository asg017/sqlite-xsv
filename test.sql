.load target/debug/libcsv0

.param set :t students.csv

create virtual table x using xsv(filename=:t);
select * from x;

create virtual table xx using xsv(filename=:not_exist);

.header on
.mode box
create virtual table flights using xsv(filename="benchmarks/_data/flights.csv");
select year, quarter, month from flights limit 20;

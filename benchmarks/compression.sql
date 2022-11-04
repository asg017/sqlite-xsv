.load target/release/libcsv0


.header on
.mode box
.timer on

create virtual table flights_comp using xsv(filename="benchmarks/_data/flights.csv.gz");
select count(*) from flights_comp;


create virtual table flights using xsv(filename="benchmarks/_data/flights.csv");
select count(*) from flights;

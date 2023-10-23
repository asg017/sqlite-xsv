.load dist/debug/xsv0
.mode box
.header on
.bail on


--.exit

select
  row ->> 0,
  row ->> 1,
  row ->> 'name',
  row ->> 'age',
  row ->> 'whoops',
  headers ->> 0,
  headers ->> 1,
  headers ->> 2
from csv_rows(cast('name,age
alex,10
brian,20
craig,30' as blob))
limit 5;

select
  row ->> 0,
  contents
from csv_rows(cast('name,age
alex,10
brian,20
craig,30' as blob)) as rows
join xsv_fields(rows.row);

select
  contents
from xsv_fields((select headers from csv_rows(cast('name,age
alex,10
brian,20
craig,30' as blob))));

.exit

-- testing sqlite-loadable
select
  rowid,
  line,
  byte,
  xsv_at(row, 0)
  --xsv_at(row, 1),
  length
from tsv_rows(cast('name\tage
alex\t10
brian\t20
craig\t30' as blob))
limit 5;

select name from pragma_module_list where name like '%row%';
.exit

select
  row,
  xsv_at(row, 0),
  xsv_at(row, 1),
  iif(
    xsv_record_length(row) > 2,
    xsv_at(row, 2),
    "NOPE"
  )
from xsv_rows(
  cast('name,age,maybe
  alex,10,yes
  brian,20,yas
  craig,30' as blob)
);



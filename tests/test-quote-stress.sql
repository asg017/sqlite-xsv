.load dist/debug/xsv0
.bail on 
.mode box

create virtual table students_tsv_with_xsv using xsv(
  filename='tests/data/students.tsv', 
  delimiter='\t'
);

select rowid, * from students_tsv_with_xsv;


create virtual table quote_stress using xsv(
  filename='tests/data/quote_stress.csv', 
  delimiter=',',
  quote='\0'
);


select * from quote_stress;
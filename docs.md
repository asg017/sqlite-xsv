<h3 name="xsv_version"> <pre>xsv_version()</pre><h3>

```sql
select xsv_version();
-- "v0.1.0"
```

<h3 name="csv_field_at"> <pre>csv_field_at(record, index)</pre><h3>

```sql
select csv_field_at('a,b,c', 0); -- 'a'
select csv_field_at('a,b,c', 1); -- 'b'
select csv_field_at('a,b,c', 2); -- 'c'
select csv_field_at('a,b,c', 3); -- NULL

select csv_field_at('a,b,c', -1); -- 'c'
select csv_field_at('a,b,c', -2); -- 'b'
select csv_field_at('a,b,c', -3); -- 'a'
select csv_field_at('a,b,c', -4); -- NULL
```

<h3 name="tsv_field_at"> <pre>tsv_field_at()</pre><h3>

```sql
select tsv_field_at;
```

<h3 name="xsv_field_at"> <pre>xsv_field_at()</pre><h3>

```sql
select xsv_field_at;
```

<h3 name="csv_records"> <pre>csv_records()</pre><h3>

```sql
select csv_records;
```

<h3 name="tsv_records"> <pre>tsv_records()</pre><h3>

```sql
select tsv_records;
```

<h3 name="xsv_records"> <pre>xsv_records()</pre><h3>

```sql
select xsv_records;
```

<h3 name="xsv"> <pre>xsv()</pre><h3>

```sql
select xsv;
```

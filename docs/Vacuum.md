## Vacuum multiple tables

It is recommended to regularly [vacuum](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) all tables that are subject to optimisations, updates or deletes.

Vacuum will ensure that your tables only keep the necessary files, and remove the unused ones.

With DiscoverX you can vacuum all the tables at once with the command:

```
dx.from_tables("*.*.*")\
  .apply_sql("VACUUM {full_table_name}")\
  .execute()
```

You can schedule [this example notebook](https://raw.githubusercontent.com/databrickslabs/discoverx/master/examples/vacuum_multiple_tables.py) in your Databricks workflows to run vacuum periodically.

Use the job parameter `from_tables` to specify the pattern of table names you want to vacuum using `*` as a wildcard. 
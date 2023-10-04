
## Arbitrary SQL template execution

You can run arbitrary SQL operations on multiple tables. In order for this to work, the dataset of each SQL command should return the same schema. You can align the schemas by:
* Running schema-agnostic commands (Eg. Vacuum, Optimize, History, Describe, etc.)
* Specifying the common column names to be selected
* Converting the columns into a JSON ([example](#select-entire-rows-as-json))

For example, to vacuum all the tables in "default" catalog:

```
dx.from_tables("default.*.*")\
  .with_sql("VACUUM {full_table_name}")\
  .display()
```

That will apply the SQL template `VACUUM {full_table_name}` to all tables matched by the pattern `default.*.*`.

The SQL template has the following variables available:
* `full_table_name` - The table name in format `catalog.schama.table`
* `table_catalog` - Name of the catalog
* `table_schema` - Name of the schema
* `table_name` - Name of the table

You can use the `explain()` command to see the SQL that would be executed.

```
dx.from_tables("default.*.*")\
  .with_sql("VACUUM {full_table_name}")\
  .explain()
```

You can also filter tables that have a specific column name. 

```
dx.from_tables("default.*.*")\
  .having_columns("device_id")\
  .with_sql("OPTIMIZE {full_table_name} ZORDER BY (`device_id`)")\
  .display()
```

## Select entire rows as json

```
dx.from_tables("default.*.*")\
  .with_sql("SELECT to_json(struct(*)) AS json_row FROM {full_table_name}")\
  .display()
```
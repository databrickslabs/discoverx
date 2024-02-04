## GDPR Right of Erasure (Delete)

Any EU citizen/resident has the right to ask for his data to be deleted. If the user data is spread across multiple tables, erasing all the user data can be a daunting task.

If all your tables share the same column name for a user identifier, then the operation is quite straightforward. You can select all the tables that have that column, and run a delete statement on all tables at once.

For example, if you want to delete users `1`, `2`, and `3` from all tables that have a column name `user_id`, then you can execute:

```
dx.from_tables("*.*.*")\
  .having_columns("user_id")\
  .with_sql("DELETE FROM {full_table_name} WHERE `user_id` IN (1, 2, 3)"")\
  .display() 
  # You can use .explain() instead of .display() to preview the generated SQL 
```

## Vaccum

You need to regularly [vacuum](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) all your delta tables to remove all traces of your deleted rows. 

Check out how to [vacuum all your tables at once with DiscoverX](Vacuum.md).

NOTE: Delta Lake latest features enable users to soft delete data. For example, 

* Dropping columns with [column mapping](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-column-mapping) enabled.
* Deleting rows with [deletion vectors](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-column-mapping) enabled.

For detailed instructions on executing VACUUM with these features, please refer to the documentation.

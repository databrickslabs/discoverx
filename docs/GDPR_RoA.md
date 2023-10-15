## GDPR Right of Access

Any European citizen has the right to ask for their data to be accessed. If the user data is spread across multiple tables, gathering all the user data can be a daunting task.

If all your tables share the same column name for a user identifier, then the operation is quite straightforward. You can select all the tables that have that column, and run a select statement on all tables at once.

For example, if you want to get all data for user `1` from all tables that have a column name `user_id`, then you can execute:

```
df = dx.from_tables("*.*.*")\
  .having_columns("user_id")\
  .with_sql("SELECT `user_id`, to_json(struct(*)) AS row_content FROM {full_table_name} WHERE `user_id` = 1")\
  .apply()
```

### Limitations

The current approach only selects tables that contain the specified column, and does not recursively follow the relationships with other tables.
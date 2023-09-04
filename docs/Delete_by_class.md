
### Delete by class

You can delete using a column class instead of the column name. For instance if you want to delete all rows that contain the email address `example_email@databricks.com` from any table that contains a column classified as `email`, then you can run 

```
dx.delete_by_class(from_tables="*.*.*", by_class="email", values=['example_email@databricks.com'], yes_i_am_sure=True, min_score=0.95)
```

If you want to preview the delete statements before applying them, then use `yes_i_am_sure=False`:

```
dx.delete_by_class(from_tables="*.*.*", by_class="email", values=['example_email@databricks.com'], yes_i_am_sure=False, min_score=0.95)
```


## Select by semantic class

After a `scan` you can leverage the classified columns to select all columns of specified classes from multiple tables.

```
dx.select_by_classes(from_tables="*.*.*", by_classes=["dx_iso_date", "dx_email"], min_score=None)
```

You can apply further transformations to build your summary tables. 
Eg. Count the occurrence of each IP address per day across multiple tables and columns

```
df = (dx.select_by_classes(from_tables="*.*.*", by_classes=["dx_iso_date", "dx_ip_v4"])
    .groupby(["table_catalog", "table_schema", "table_name", "classified_columns.dx_iso_date.column", "classified_columns.dx_iso_date.value", "classified_columns.dx_ip_v4.column"])
    .agg(func.count("classified_columns.dx_ip_v4.value").alias("count"))
)
```
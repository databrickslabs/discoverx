## Search

!!! NOTE: Search requires you to [scan for semantic classes](Semantic_classification.md) before running a search.

Search for a specific value across multiple tables.

```
dx.search("example_email@databricks.com", from_tables="*.*.*")
```

The search will automatically try to classify the search term and restrict the search to columns that match that rule classes.

You can also specify the classes where the search should be performed explicitly:

```
dx.search("example_email@databricks.com", from_tables="*.*.*", by_classes=["dx_email"])
```

If you want to limit the search to columns with a specific classification score
you need to provide it as parameter, i.e.

```
dx.search("example_email@databricks.com", from_tables="*.*.*", min_score=0.95)
```

The score refers to the frequency of matching rules during the scan for
the respective column.
# DiscoverX

Multi-table operations over the lakehouse.

Run a single command to execute operations across many tables. 
Eg.

* [Maintenance operations](#maintenance-operations) (vacuum, optimize, cleanup, etc.)
* [Scan & classify](#scan--classify) content for patterns (phone numbers, emails, etc.)
* Data transformations based on semantic types

## Getting started

Install DiscoverX, in Databricks notebook type

```
%pip install dbl-discoverx
```

Get started

```
from discoverx import DX
dx = DX(locale="US")
```

## Maintenance operations

You can run arbitrary SQL operations on multiple tables. 
For example, to vacuum all the tables in "default" catalog:

```
dx.from_tables("default.*.*")\
  .apply_sql("VACUUM {full_table_name}")\
  .execute()
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
  .apply_sql("VACUUM {full_table_name}")\
  .explain()
```

You can also filter tables that have a specific column name. 
Eg.

```
dx.from_tables("default.*.*")\
  .having_columns("device_id")\
  .apply_sql("OPTIMIZE {full_table_name} ZORDER BY (`device_id`)")\
  .execute()
```

## Scan & classify

You can scan a sample of 10k rows from each table with

```
dx.scan(from_tables="*.*.*")
```

Check out the [scan parameters](#scan-parameters) for more details.

The scan result is a dataset with a `score` column, which defines the fraction of matched records against the total records scanned for each rule.

### Available classes

The supported classes are:
- IP v4 address
- IP v6 address
- Email address
- URL
- fqdn (Fully qualified domain name)
- Credit card number
- Credit card expiration date
- Iso date
- Iso date time
- Mac address
- Integer number as string
- Decimal number as string

US locale specific classes
- us_mailing_address
- us_phone_number
- us_social_security_number
- us_state
- us_state_abbreviation
- us_zip_code

See the list of available classification rules with 

```
dx.display_rules()
```

You can also provide your [custom matching rules](#custom-rules).


## Save & Load the Scan Results

After a `scan` you can save the scan results in a delta table of your choice.

```
dx.save(full_table_name=<your-table-name>)
```

To load the saved results at a later time or in a different session use

```
dx.load(full_table_name=<your-table-name>)
```

## Cross-table queries

After a `scan` you can leverage the classified column classes to run cross-table `search`, `delete_by_class` and `select_by_classes` actions.


### Search

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

### Delete

Delete 

Preview delete statements
```
dx.delete_by_class(from_tables="*.*.*", by_class="dx_email", values=['example_email@databricks.com'], yes_i_am_sure=False, min_score=0.95)
```

Execute delete statements
```
dx.delete_by_class(from_tables="*.*.*", by_class="dx_email", values=['example_email@databricks.com'], yes_i_am_sure=True, min_score=0.95)
```

Note: You need to regularly [vacuum](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) all your delta tables to remove all traces of your deleted rows. 

### Select

Select all columns classified with specified classes from multiple tables

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


## Configuration

### Scan parameters

You can define 

```
dx.scan(
    from_tables="*.*.*", # Table pattern in form of <catalog>.<schema>.<table> ('*' is a wildcard)
    rules="*",           # Rule filter ('*' is a wildcard)
    sample_size=10000,   # Number of rows to sample, use None for a full table scan
    what_if=False        # If `True` it prints the SQL that would be executed
)
```

### Custom rules

You can provide your custom scanning rules based on regex expressions.

```
from discoverx.rules import RegexRule
from discoverx import DX

custom_rules = [
  RegexRule(
    name = "resource_request_id",
    description = "Resource request ID",
    definition = r"^AR-\d{9}$",
    match_example = ["AR-123456789"],
    nomatch_example = ["R-123"],
  )
]

dx = DX(custom_rules=custom_rules)
```

You should now see your rules added to the default ones with

```
dx.display_rules()
```

## Requirements

* A [Databricks workspace](https://www.databricks.com/try-databricks#account)
* [Unity Catalog](https://www.databricks.com/product/unity-catalog)

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.


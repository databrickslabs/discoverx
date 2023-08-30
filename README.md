# DiscoverX

Multi-table operations over the lakehouse.

![Multi-table operations](docs/images/DiscoverX_Multi-table_operations.png)

Run a single command to execute operations across many tables. 

## Operations examples

Operations are applied concurrently across multiple tables

* Manitenance operations
  * [VACUUM all tables](docs/Vacuum.md) ([example notebook](examples/vacuum_multiple_tables.py))
  * OPTIMIZE with z-order on tables having specified columns
  * Visualise quantity of data written per table per period
* Governance operations
  * PII detection with Presidio ([example notebook](examples/pii_detection_presidio.py))
  * [GDPR right of access: extract user data from all tables at once](docs/GDPR_RoA.md)
  * [GDPR right of erasure: delete user data from all tables at once](docs/GDPR_RoE.md)
  * [Search in any column](docs/Search.md)
* Semantic classification operations
  * [Semantic classification of columns by semantic type](docs/Semantic_classification.md): email, phone number, IP address, etc.
  * [Select data based on semantic types](docs/Select_by_class.md)
  * [Delete data based on semantic types](docs/Delete_by_class.md)
* Custom operations
  * [Arbitrary SQL template execution across multiple tables](docs/Arbitrary_multi-table_SQL.md)

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

You can now run operations across multiple tables. 

For example you can select one row from all tables which name contains `sample` in a catalog starting with `dev_` with

```
dx.from_tables("dev_*.*.*sample*")\
  .apply_sql("SELECT to_json(struct(*)) AS row FROM {full_table_name} LIMIT 1")\
  .execute()
```

## Available functionality

The available `dx` functions are

* `from_tables("<catalog>.<schema>.<table>")` selects tables based on the specified pattern (use `*` as a wildcard). Returns a [DataExplorer](#dataexplorer-object) object. 
* `intro` gives an introduction to the library
* `scan` scans the lakehouse with regex expressions defined by the rules and to power the semantic classification. [Documentation](docs/Semantic_classification.md)
* `display_rules` shows the rules available for semantic classification
* `search` searches the lakehouse content for by leveraging the semantic classes identified with scan (eg. email, ip address, etc.). [Documentaiton](docs/Search.md)
* `select_by_class` selects data from the lakehouse content by semantic class. [Documentation](docs/Select_by_class.md)
* `delete_by_class` deletes from the lakehouse by semantic class. [Documentation](docs/Delete_by_class.md)

### DataExplorer Object

The functions available from the `DataExplorer` Object are:

* `having_columns` restricts the selection to tables that have the specified columns
* `with_concurrency` defines how many queries are executed concurrently (10 by defailt)
* `apply_sql` applies a SQL template to all tables and returns a [DataExplorerActions](#dataexploreractions-object) object
* `unpivot_string_columns` returns a melted (unpivoted) dataframe with all string columns from the selected tables and returns a [DataExplorerActions](#dataexploreractions-object) object

### DataExplorerActions Object

The functions available from the `DataExplorerActions` Object are:

* `explain` explains the queries that would be executed
* `execute` executes the queries and shows the result in a unioned dataframe
* `to_union_df` unions all the dataframes that result from the queries



## Requirements

* A [Databricks workspace](https://www.databricks.com/try-databricks#account)
* [Unity Catalog](https://www.databricks.com/product/unity-catalog)

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.


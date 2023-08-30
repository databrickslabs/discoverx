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

## Requirements

* A [Databricks workspace](https://www.databricks.com/try-databricks#account)
* [Unity Catalog](https://www.databricks.com/product/unity-catalog)

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.


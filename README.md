# DiscoverX

Scan, Tag, and Discover the content of your Lakehouse

## Requirements

This tool requires
* A Databricks workspace 
* Unity Catalog

## Getting started

Install DiscoverX, in Databricks notebook type

```
%pip install discoverx 
```

Get started

```
from discoverx import DX
dx = DX()
```

## Scan

You can now scan the content of any set of tables.

Examples:

```
# Scan all (sample 10k rows from each table)
dx.scan()

# Scan parameters
dx.scan(
    catalogs="*",      # Catalog filter ('*' is a wildcard)
    databases="*",     # Database filter ('*' is a wildcard)
    tables="*",        # Table filter ('*' is a wildcard)
    rules="*",         # Rule filter ('*' is a wildcard) or list[string]
    sample_size=10000, # Number of rows to sample, use None for a full scan
    what_if=False      # Set this to true to see the SQL that would be executed
)

```

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.


# Changelog

## v0.0.9
* Added Delta housekeeping
* Fixed serverless compatibility

## v0.0.8
* Fixed bug for tables containing `-` character
* Added example for cloning all catalog/schema content
* Added filtering for table format (exclude views from queries by default)
* Added support for PII detection on non-string columns
* Updated LICENSE file

## v0.0.7
* Added filtering for speedup intro message checks
* Added tags metadata in table info
* Added map function for arbitrary python code table processing support
* Added AI example notebooks

## v0.0.6
* Refactored scan() in order to be chainable with from_tables()
* Improved metadata fetching speed for table information
* Refactored to remove duplicated SQL code from scanner class
* Updated intro messages and documentation
* Added example for detecting tables with many small files

## v0.0.5
* Added support for multi-table SQL execution dx.from_tables(...).apply_sql(...)
* Added example of VACUUM command to multiple tables
* Added example of PII detection using Presidio over multiple tables

## v0.0.4
* Removed pydantic dependency
* Fixed issues with special characters in column names
* Fixed readme docs
* Added integer and decimal rules
* Fixed case insensitive regex expressions 

## v0.0.3
* Upgraded pydantic dependency to 2.0
* Added support for special characters in column names
* Updated readme

## v0.0.2
* Improved Readme and examples
* Added System tables permissions check with friendly message
* Refactored save and load methods after customer feedback

## v0.0.1
* Lakehouse scanning with REGEX rules on string columns for 16 class types (email, IP v4, IP v6, URLs, MAC address, FQDNs, credit card numbers, credit card expiry date, ISO date, ISO datetime, US mailing address, US phone number, US social security number, US state, US state abbreviation, US zip code
* Save and load scan result
* Cross-table query based on semantic types of columns (rather than column names)
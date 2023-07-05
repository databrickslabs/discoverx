# Changelog

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
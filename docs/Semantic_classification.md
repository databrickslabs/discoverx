## Semantic column classification

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


### Save & Load the Scan Results

After a `scan` you can save the scan results in a delta table of your choice.

```
dx.save(full_table_name=<your-table-name>)
```

To load the saved results at a later time or in a different session use

```
dx.load(full_table_name=<your-table-name>)
```


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


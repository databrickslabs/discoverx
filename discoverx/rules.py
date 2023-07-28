"""
This module implements rules. Both the structure of rules as well as
actual built-in rules are defined.
"""

from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
import re
from typing import Union, Optional, List


class RuleTypes(str, Enum):
    """Allowed types/choices for Rules"""

    REGEX = "regex"


class Rule:
    """Parent class for rules
    Attributes:
        type (RuleTypes): Defines the type of Rule, e.g. Regex
    """

    type: RuleTypes


class RegexRule(Rule):
    """Definition of Regex Rule that applies to a column content
    Attributes:
        name (str): Name of rule
        description (str): Description of the rule
        definition (str): The actual definition. I.e. for regex-type
            rules this would contain the actual regular expressions as
            a RAW string, i.e. r'...'
        match_example (str, optional): It is possible to provide an example of
            a string/expression to be matched by the Rule. This example
            is used to validate the Rule upon instantiation.
        class_name (str, optional): Name of the class used to select columns in unity
            catalog
    """

    def __init__(self, name, description, definition, match_example=[], nomatch_example=[], class_name=None):
        self.name = name
        self.description = description
        self.definition = definition
        self.match_example = match_example
        self.nomatch_example = nomatch_example
        self.class_name = class_name
        self.type = RuleTypes.REGEX

        self.validate_rule(self.match_example, self.definition, self.name, False)
        self.validate_rule(self.nomatch_example, self.definition, self.name, True)

    type: RuleTypes = RuleTypes.REGEX

    name: str
    description: str
    definition: str
    match_example: Optional[Union[str, List[str]]] = None
    nomatch_example: Optional[Union[str, List[str]]] = None
    class_name: Optional[str] = None

    @staticmethod
    def validate_rule(example, definition, name, fail_match: bool):
        """Validates that given example is matched by defined pattern"""
        if not isinstance(example, list):
            validation_example = [example]
        else:
            validation_example = example

        for ex in validation_example:
            if (not re.match(definition, ex)) != fail_match:
                raise ValueError(f"The definition of the rule {name} does not match the provided example {ex}")
        return example


@dataclass
class RulesList:
    """A list of rules with added properties
    Attributes:
    -----------
    rules (List(Rule), optional): A list of rules, could be either
        built-in or custom-defined. Defaults to None
    """

    rules: Optional[List[Rule]] = None

    @property
    def rules_info(self) -> str:
        """Get a HTML-formatted description of all the rules"""
        if self.rules:
            text = [f"<li>{rule.name} - {rule.description}</li>" for rule in self.rules]
            return "\n              ".join(text)

        return ""

    @property
    def number_of_rules(self) -> int:
        """Return the number of rules in the list."""
        if self.rules:
            return len(self.rules)
        return 0

    def test_match(self, input_string: str):
        """Return a list of rules which match the given input string"""
        if self.rules is not None:
            return [rule.name for rule in self.rules if re.match(rule.definition, input_string)]

        return []


# define builtin rules
global_rules = [
    RegexRule(
        name="credit_card_expiration_date",
        description="Credit Card Expiration Date",
        definition=r"^\d{2}/\d{2}$",
        match_example=["01/20", "12/25"],
        nomatch_example=["1/20", "01/2020", "01/2", "01/200"],
    ),
    RegexRule(
        name="credit_card_number",
        description="Credit Card Number",
        definition=r"^\d{4}-\d{4}-\d{4}-\d{4}$",
        match_example=["1234-5678-9012-3456", "9876-5432-1098-7654"],
        nomatch_example=["1234-5678-9012-345", "1234-5678-9012-34567", "1234-5678-9012-3456-7890"],
    ),
    RegexRule(
        name="decimal_number",
        description="Decimal Number",
        definition=r"^-?\d+(?:[.,]\d*)?[eE]?-?\d{0,3}$",
        match_example=[
            "123.45",
            "-123.45",
            "1.1E2",
            "1.1E-1",
            "123,45",
            "-123,45",
            "123,0123",
            "-123,0",
            "123.",
            "123,",
            "-123.",
            "-123,",
        ],
        nomatch_example=["", "123,456,789", "1$", "123,456.789"],
    ),
    RegexRule(
        name="email",
        description="Email address",
        definition=r"^.+@[^\.].*\.[a-z]{2,}$",
        match_example=[
            "whatever@somewhere.museum",
            "foreignchars@myforeigncharsdomain.nu",
            "me+mysomething@mydomain.com",
        ],
        nomatch_example=["a@b.c", "me@.my.com", "a@b.comFOREIGNCHAR"],
    ),
    RegexRule(
        name="fqdn",
        description="Fully Qualified Domain Names",
        definition=r"^([-a-zA-Z0-9:%._\+~#=]{1,63}\.){1,8}[a-zA-Z]{1,12}\.?$",
        match_example=[
            "ec2-35-160-210-253.us-west-2-.compute.amazonaws.com",
            "ec2-35-160-210-253.us-west-2-.compute.amazonaws.com.mx.gmail.com.",
            "1.2.3.4.com",
            "xn--kxae4bafwg.xn--pxaix.gr",
        ],
        nomatch_example=[
            "so-me.na-me.567",
            "label.name.321",
            "1234567890-1234567890-1234567890-1234567890-12345678901234567890.123.com",
            "abc.cdf@mydoamain.com",
            "Some text abc.cdf.com",
        ],
    ),
    RegexRule(
        name="integer_number",
        description="Integer Number",
        definition=r"^-?\d+$",
        match_example=["123", "-123", "0"],
        nomatch_example=["", "123.45", "123,45", "123,0", "123.0", "123,456,789", "1$"],
    ),
    RegexRule(
        name="ip_v4",
        description="IP address v4",
        definition=r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
        match_example=["192.1.1.1", "0.0.0.0"],
        nomatch_example=["192"],
    ),
    RegexRule(
        name="ip_v6",
        description="IP address v6",
        definition=r"^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$",
        match_example=["2001:db8:3333:4444:5555:6666:7777:8888", "::1234:5678", "2001:db8::", "::"],
        nomatch_example=["2001.0000"],
    ),
    RegexRule(
        name="iso_date",
        description="ISO Date",
        definition=r"^\d{4}-\d{2}-\d{2}$",
        match_example=["2020-01-01", "2020-12-31"],
        nomatch_example=["2020-01", "2020-01-01-01", "2020-01-01T01:01:01"],
    ),
    RegexRule(
        name="iso_date_time",
        description="ISO Date Time",
        definition=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",
        match_example=["2020-01-01T01:01:01", "2020-12-31T23:59:59"],
        nomatch_example=["2020-01", "2020-01-01", "2020-01-01-01"],
    ),
    RegexRule(
        name="mac_address",
        description="MAC Addresses",
        definition=r"^(?=[-:\w]*[a-fA-F]+[-:\w]*)(([0-9A-Fa-f]{2}[:-]?){5}([0-9A-Fa-f]{2}))$",
        match_example=["01:02:03:04:ab:cd", "01-02-03-04-ab-cd", "0102-0304-abcd", "01020304abcd"],
        nomatch_example=[
            "01:02:03:04:ab",
            "01.02.03.04.ab.cd",
            "01:02:03:04:05:06",  # There must be at least one letter
        ],
    ),
    RegexRule(
        name="url",
        description="URL",
        definition=r"^(https?|ftp|file|mailto):\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)$",
        match_example=[
            "http://www.domain.com",
            "http://domain.com",
            "http://domain.com",
            "https://domain.com",
            "https://sub.domain-name.com:8080",
            "http://domain.com/dir%201/dir_2/program.ext?var1=x&var2=my%20value",
            "ftp://domain.com/index.html#bookmark",
            "file://domain.com/abc.txt",
        ],
        nomatch_example=["Some text http://domain.com", "http://domain.com some text", "my@email.com"],
    ),
]

localized_rules = {
    "us": [
        RegexRule(
            name="us_mailing_address",
            description="US Mailing Address",
            definition=r"^\d+\s[A-z]+\s[A-z]+",
            match_example=["123 Main St", "456 Elm St", "789 Pine St"],
            nomatch_example=["123 Main", "456 Elm", "789 Pine"],
        ),
        RegexRule(
            name="us_phone_number",
            description="US Phone Number",
            definition=r"^\+?1?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})$",
            match_example=["+1 (123) 456-7890", "123-456-7890", "123.456.7890", "1234567890", "(123)456-7890"],
            nomatch_example=["123-45-6789", "987-65-4321"],
        ),
        RegexRule(
            name="us_social_security_number",
            description="US Social Security Number",
            definition=r"^(?!000|666|9)\d{3}-(?!00)\d{2}-(?!0000)\d{4}$",
            match_example=["123-45-6789"],
            nomatch_example=["123-45-678", "123-456-7890", "123-45-67890", "123-456-789"],
        ),
        RegexRule(
            name="us_state",
            description="US State",
            definition=r"(?i)^(Alabama|Alaska|American Samoa|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|District of Columbia|Federated States of Micronesia|Florida|Georgia|Guam|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Marshall Islands|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Northern Mariana Islands|Ohio|Oklahoma|Oregon|Palau|Pennsylvania|Puerto Rico|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virgin Islands|Virginia|Washington|West Virginia|Wisconsin|Wyoming)$",
            match_example=[
                "Alabama",
                "Alaska",
                "Arizona",
                "Arkansas",
                "California",
                "Colorado",
                "Connecticut",
                "Delaware",
                "Florida",
                "Georgia",
                "Hawaii",
                "Idaho",
                "Illinois",
                "Indiana",
                "Iowa",
                "Kansas",
                "Kentucky",
                "Louisiana",
                "Maine",
                "Maryland",
                "Massachusetts",
                "Michigan",
                "Minnesota",
                "Mississippi",
                "Missouri",
                "Montana",
                "Nebraska",
                "Nevada",
                "New Hampshire",
                "New Jersey",
                "New Mexico",
                "New York",
                "North Carolina",
                "North Dakota",
                "Ohio",
                "Oklahoma",
                "Oregon",
                "Pennsylvania",
                "Rhode Island",
                "South Carolina",
                "South Dakota",
                "Tennessee",
                "Texas",
                "Utah",
                "Vermont",
                "Virginia",
                "Washington",
                "West Virginia",
                "Wisconsin",
                "Wyoming",
            ],
            nomatch_example=[
                "AL",
                "AK",
                "AS",
                "AZ",
                "AR",
                "CA",
                "CO",
                "CT",
                "DE",
                "DC",
                "FM",
                "FL",
                "GA",
                "GU",
                "HI",
                "ID",
                "IL",
                "IN",
                "IA",
                "KS",
                "KY",
                "LA",
                "ME",
                "MH",
                "MD",
                "MA",
                "MI",
                "MN",
                "MS",
                "MO",
                "MT",
                "NE",
                "NV",
                "NH",
                "NJ",
                "NM",
                "NY",
                "NC",
                "ND",
                "MP",
                "OH",
                "OK",
                "OR",
                "PW",
                "PA",
                "PR",
                "RI",
                "SC",
                "SD",
                "TN",
                "TX",
                "UT",
                "VT",
                "VI",
                "VA",
                "WA",
                "WV",
                "WI",
                "WY",
            ],
        ),
        RegexRule(
            name="us_state_abbreviation",
            description="US State Abbreviation",
            definition=r"(?i)^(AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|MP|OH|OK|OR|PW|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WV|WI|WY)$",
            match_example=[
                "AL",
                "AK",
                "AS",
                "AZ",
                "AR",
                "CA",
                "CO",
                "CT",
                "DE",
                "DC",
                "FM",
                "FL",
                "GA",
                "GU",
                "HI",
                "ID",
                "IL",
                "IN",
                "IA",
                "KS",
                "KY",
                "LA",
                "ME",
                "MH",
                "MD",
                "MA",
                "MI",
                "MN",
                "MS",
                "MO",
                "MT",
                "NE",
                "NV",
                "NH",
                "NJ",
                "NM",
                "NY",
                "NC",
                "ND",
                "MP",
                "OH",
                "OK",
                "OR",
                "PW",
                "PA",
                "PR",
                "RI",
                "SC",
                "SD",
                "TN",
                "TX",
                "UT",
                "VT",
                "VI",
                "VA",
                "WA",
                "WV",
                "WI",
                "WY",
            ],
            nomatch_example=[
                "XY",
                "Alabama",
                "Alaska",
                "Arizona",
                "Arkansas",
                "California",
                "Colorado",
                "Connecticut",
                "Delaware",
                "Florida",
                "Georgia",
                "Hawaii",
                "Idaho",
                "Illinois",
                "Indiana",
                "Iowa",
                "Kansas",
                "Kentucky",
                "Louisiana",
                "Maine",
                "Maryland",
                "Massachusetts",
                "Michigan",
                "Minnesota",
                "Mississippi",
                "Missouri",
                "Montana",
                "Nebraska",
                "Nevada",
                "New Hampshire",
                "New Jersey",
                "New Mexico",
                "New York",
                "North Carolina",
                "North Dakota",
                "Ohio",
                "Oklahoma",
                "Oregon",
                "Pennsylvania",
                "Rhode Island",
                "South Carolina",
                "South Dakota",
                "Tennessee",
                "Texas",
                "Utah",
                "Vermont",
                "Virginia",
                "Washington",
                "West Virginia",
                "Wisconsin",
                "Wyoming",
            ],
        ),
        RegexRule(
            name="us_zip_code",
            description="US Zip Code",
            definition=r"^\d{5}(?:[-\s]\d{4})?$",
            match_example=["12345", "12345-6789"],
            nomatch_example=["1234", "123456"],
        ),
    ]
}


class Rules:
    """This class represents built-in and custom rules

    Attributes:
        builtin_rules (RulesList): Represents all built-in rules
        custom_rules (RulesList): Represents all custom-defined rules

    """

    def __init__(self, locale: Optional[str] = None, custom_rules: Optional[List[Rule]] = None):
        """
        Args:
            locale (str, optional): The locale to use. Defaults to None.
            custom_rules (List[Rule], optional): A list of
                custom-defined rules. Defaults to None.
        """
        rules = global_rules.copy()
        if locale is not None:
            if locale.lower() not in localized_rules:
                raise ValueError(f"Unsupported locale: {locale}. Use one of {localized_rules.keys()}")
            rules += localized_rules[locale.lower()]
        self.builtin_rules: RulesList = RulesList(rules)
        self.custom_rules = RulesList(custom_rules)

    def get_rules_info(self):
        """Returns info about the available rules
        Both built-in and custom rules will be presented

        Returns:
            HTML-formatted string
        """
        text = f"""
                <h2>Matching rules</h2>
                <p>
                  Here are the {self.builtin_rules.number_of_rules} built-in rules that are available to you:
                </p>
                <ul>
                  {self.builtin_rules.rules_info}
                </ul>
                <p>
                  You've also defined {self.custom_rules.number_of_rules} custom rule(s) which is/are:
                </p>
                <ul>
                  {self.custom_rules.rules_info}
                </ul>
                """
        return text

    def get_rules(self, rule_filter: str) -> List[Rule]:
        """Return a list of specified rules
        The rules to be returned are specified using unix wildcards.
        Both built-in and custom rules are returned in one single list.

        Args:
            rule_filter (str): Unix wildcard string specifying the
                rule(s) to be returned. E.g. '*' for all rules while
                '*v4' would return all rules with names matching the
                wildcard. This would include the 'ip_v4' built-in rule.

        Returns:
            A list of rules matching the wildcard
        """
        filtered_builtin_rules = self._filter_rules(self.builtin_rules.rules, rule_filter)
        filtered_custom_rules = self._filter_rules(self.custom_rules.rules, rule_filter)
        return filtered_builtin_rules + filtered_custom_rules

    @staticmethod
    def _filter_rules(rule_set: List[Rule], rule_filter: str) -> List[Rule]:
        """Filter a given set of rules

        Args:
            rule_set (List[Rule]): The set of rules to be filtered, e.g.
                the built-in or custom rules.
            rule_filter (str): Unix wildcard string specifying the
                rule(s) to be returned. E.g. '*' for all rules while
                '*v4' would return all rules with names matching the
                wildcard. This would include the 'ip_v4' built-in rule.

        Returns:
            A list of rules from the specified rule set matching the
            wildcard.
        """
        return [rule for rule in (rule_set if rule_set is not None else []) if fnmatch(rule.name, rule_filter)]

    def match_search_term(self, search_term: str):
        """Return list of rules matching the search term

        Args:
            search_term: A string to be matched by the available rules

        Returns:
            A list of matching rules, both custom and built-in
        """
        return self.builtin_rules.test_match(search_term) + self.custom_rules.test_match(search_term)

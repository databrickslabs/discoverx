"""
This module implements rules. Both the structure of rules as well as
actual built-in rules are defined.
"""

from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
import re
from typing import Union, Optional, List
from pydantic import BaseModel, validator


class RuleTypes(str, Enum):
    """Allowed types/choices for Rules"""

    REGEX = "regex"


class Rule(BaseModel):
    """Definition of Rule
    Attributes:
        name (str): Name of rule
        type (RuleTypes): The type of the rule. Currently, only 'regex'
            is supported
        description (str): Description of the rule
        definition (str): The actual definition. I.e. for regex-type
            rules this would contain the actual regular expressions as
            a RAW string, i.e. r'...'
        example (str, optional): It is possible to provide an example of
            a string/expression to be matched by the Rule. This example
            is used to validate the Rule upon instantiation.
        tag (str, optional): Name used to tag matched columns in unity
            catalog
    """

    name: str
    type: RuleTypes
    description: str
    definition: str
    match_example: Optional[Union[str, List[str]]] = None
    nomatch_example: Optional[Union[str, List[str]]] = None
    tag: Optional[str] = None

    # pylint: disable=no-self-argument
    @validator("match_example")
    def validate_match_example(cls, match_example, values):
        return cls.validate_rule(match_example, values, False)

    # pylint: disable=no-self-argument
    @validator("nomatch_example")
    def validate_nomatch_example(cls, nomatch_example, values):
        return cls.validate_rule(nomatch_example, values, True)

    @staticmethod
    def validate_rule(example, values, fail_match: bool):
        """Validates that given example is matched by defined pattern"""
        if not isinstance(example, list):
            validation_example = [example]
        else:
            validation_example = example

        for ex in validation_example:
            if ((values["type"] == RuleTypes.REGEX) and not re.match(values["definition"], ex)) != fail_match:
                raise ValueError(
                    f"The definition of the rule {values['name']} does not match the provided example {ex}"
                )
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


# define builtin rules
ip_v4_rule = Rule(
    name="ip_v4",
    type="regex",
    description="IP address v4",
    definition=r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
    match_example=["192.1.1.1", "0.0.0.0"],
    nomatch_example=["192"],
)
ip_v6_rule = Rule(
    name="ip_v6",
    type="regex",
    description="IP address v6",
    definition=r"^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$",
    match_example=["2001:db8:3333:4444:5555:6666:7777:8888", "::1234:5678", "2001:db8::", "::"],
    nomatch_example=["2001.0000"],
)
email_rule = Rule(
    name="email",
    type="regex",
    description="Email address",
    definition=r"^.+@[^\.].*\.[a-z]{2,}$",
    match_example=["whatever@somewhere.museum", "foreignchars@myforeigncharsdomain.nu", "me+mysomething@mydomain.com"],
    nomatch_example=["a@b.c", "me@.my.com", "a@b.comFOREIGNCHAR"],
)
mac_rule = Rule(
    name="mac",
    type="regex",
    description="MAC Addresses",
    definition=r"^([0-9A-Fa-f]{2}[:-]?){5}([0-9A-Fa-f]{2})$",
    match_example=["01:02:03:04:ab:cd", "01-02-03-04-ab-cd", "0102-0304-abcd", "01020304abcd"],
    nomatch_example=["01:02:03:04:ab", "01.02.03.04.ab.cd"],
)

# Regular Expression from https://www.regexlib.com/REDetails.aspx?regexp_id=1374
# TODO: http://www.domain-.com should not be matched according to above link but is currently
url_rule = Rule(
    name="url",
    type="regex",
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
    nomatch_example=[
        "Some text http://domain.com",
        "http://domain.com some text",
    ],
)

fqdn_rule = Rule(
    name="fqdn",
    type="regex",
    description="Fully Qualified Domain Names",
    definition=r"^(?!:\/\/)(?=.{1,255}$)((.{1,63}\.){1,127}(?![0-9]*$)[a-z0-9-]+\.?)$",
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
    ],
)


class Rules:
    """This class represents built-in and custom rules

    Attributes:
        builtin_rules (RulesList): Represents all built-in rules
        custom_rules (RulesList): Represents all custom-defined rules

    """

    def __init__(self, custom_rules: Optional[List[Rule]] = None):
        """
        Args:
            custom_rules (List[Rule], optional): A list of
                custom-defined rules. Defaults to None.
        """
        self.builtin_rules: RulesList = RulesList([ip_v4_rule, ip_v6_rule, email_rule, mac_rule, url_rule, fqdn_rule])
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

"""
This module implements rules. Both the structure of rules as well as
actual built-in rules are defined.
"""

from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
import re
from typing import Optional, List
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
    example: Optional[str] = None
    tag: Optional[str] = None

    # pylint: disable=no-self-argument
    @validator("example")
    def validate_rule(cls, example, values):
        """Validates that given example is matched by defined pattern"""
        if (values["type"] == RuleTypes.REGEX) and not re.match(values["definition"], example):
            raise ValueError(f"The definition of the rule {values['name']} does not match the provided example")
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
    definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
)
ip_v6_rule = Rule(
    name="ip_v6",
    type="regex",
    description="IP address v6",
    definition=r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))",
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
        self.builtin_rules: RulesList = RulesList([ip_v4_rule, ip_v6_rule])
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

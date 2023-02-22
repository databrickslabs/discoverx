from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
from typing import Optional, List
from pydantic import BaseModel


class RuleTypes(str, Enum):
    regex = "regex"


class Rule(BaseModel):
    """
    Use pydantic for rules to ensure user input is validated
    """

    name: str
    type: RuleTypes
    description: str
    definition: str
    example: Optional[str] = None
    tag: Optional[str] = None

@dataclass
class RulesList:
    rules: Optional[List[Rule]] = None

    @property
    def rules_info(self) -> str:
        if self.rules:
            text = [f"<li>{rule.name} - {rule.description}</li>" for rule in self.rules]
            return "\n              ".join(text)

        return ""

    @property
    def number_of_rules(self) -> int:
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
    def __init__(self, custom_rules: Optional[List[Rule]] = None):
        self.builtin_rules: RulesList = RulesList([ip_v4_rule, ip_v6_rule])
        self.custom_rules = RulesList(custom_rules)

    def get_rules_info(self):

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
        filtered_builtin_rules = self._filter_rules(self.builtin_rules.rules, rule_filter)
        filtered_custom_rules = self._filter_rules(self.custom_rules.rules, rule_filter)
        return filtered_builtin_rules + filtered_custom_rules

    @staticmethod
    def _filter_rules(rule_set: List[Rule], rule_filter: str) -> List[Rule]:
        return [rule for rule in (rule_set if rule_set is not None else []) if fnmatch(rule.name, rule_filter)]

import pytest
from discoverx.rules import Rule, RegexRule, Rules, RulesList, builtin_rules


def test_ruleslist():

    none_rules_list = RulesList()
    assert none_rules_list.rules_info == ""

    rules_list = RulesList(builtin_rules)
    assert rules_list.rules_info.startswith("<li>ip_v4 - IP address v4</li>\n              <li>ip_v6 - IP address v6</li>")


def test_rules():
    # test builtin rules first
    rules = Rules()
    # check that info string is returned without error
    assert isinstance(rules.get_rules_info(), str)

    # check that we can filter with unix wildcards
    rules_ip = Rules()
    rules_ip.builtin_rules = RulesList(builtin_rules)
    assert len(rules.get_rules(rule_filter="*")) == 16
    assert [rule.name for rule in rules.get_rules(rule_filter="*v4")] == ["ip_v4"]

    # add some custom rules
    device_rule_def = {
        "name": "custom_device_id",
        "description": "Custom device ID XX-XXXX-XXXXXXXX",
        "definition": "\d{2}[-]\d{4}[-]\d{8}",
        "match_example": "00-1111-22222222",
        "tag": "device_id",
    }
    cust_rule = RegexRule(**device_rule_def)
    cust_rules = Rules(custom_rules=[cust_rule])
    cust_rules.builtin_rules = RulesList(builtin_rules)

    assert "custom_device_id" in cust_rules.get_rules_info()
    assert len(cust_rules.get_rules(rule_filter="*")) == 17


def test_rule_validation():

    # The definition of the following rule should match the match_example
    try:
        ip_v4_rule_test = RegexRule(
            name="ip_v4_test",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example=["192.0.2.1", "0.0.0.0"],
        )
    except ValueError:
        pytest.fail("The example does not match the rule definition")

    # The definition of the following rule should not match the nomatch-example
    try:
        ip_v4_rule_test = RegexRule(
            name="ip_v4_test",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example=["192.0.2.1", "0.0.0.0"],
            nomatch_example="192",
        )
    except ValueError:
        pytest.fail("The example does not match the rule definition")

    # The next tests should fail and result in a ValueError
    with pytest.raises(ValueError):
        ip_v4_rule_fail = RegexRule(
            name="ip_v4",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example="192",
        )

    with pytest.raises(ValueError):
        ip_v4_rule_fail = RegexRule(
            name="ip_v4",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            nomatch_example=["192.1.1.1", "192"],
        )

    with pytest.raises(ValueError):
        ip_v4_rule_fail = RegexRule(
            name="ip_v4",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example=["0.0.0.0", "192"],
        )


def test_standard_rules():
    """Test the standard rules defined in rules.py

    The rules are automatically tested using a pydantic-validator
    if match/no-match examples are provided. Here, we therefore simply
    test that those examples
    """
    for rule in builtin_rules:
        assert (rule.match_example is not None) and (rule.nomatch_example is not None)

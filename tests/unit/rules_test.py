import pytest
from discoverx.rules import Rule, Rules, RulesList, ip_v4_rule, ip_v6_rule, email_rule, mac_rule, url_rule, fqdn_rule


def test_ruleslist():

    none_rules_list = RulesList()
    assert none_rules_list.rules_info == ""

    rules_list = RulesList([ip_v4_rule, ip_v6_rule])
    assert rules_list.rules_info == "<li>ip_v4 - IP address v4</li>\n              <li>ip_v6 - IP address v6</li>"


def test_rules():
    # test builtin rules first
    rules = Rules()
    # check that info string is returned without error
    assert isinstance(rules.get_rules_info(), str)

    # check that we can filter with unix wildcards
    rules_ip = Rules()
    rules_ip.builtin_rules = RulesList([ip_v4_rule, ip_v6_rule])
    assert len(rules.get_rules(rule_filter="*")) == 6
    assert [rule.name for rule in rules.get_rules(rule_filter="*v4")] == ["ip_v4"]

    # add some custom rules
    device_rule_def = {
        "name": "custom_device_id",
        "type": "regex",
        "description": "Custom device ID XX-XXXX-XXXXXXXX",
        "definition": "\d{2}[-]\d{4}[-]\d{8}",
        "match_example": "00-1111-22222222",
        "tag": "device_id",
    }
    cust_rule = Rule(**device_rule_def)
    cust_rules = Rules(custom_rules=[cust_rule])
    cust_rules.builtin_rules = RulesList([ip_v4_rule, ip_v6_rule])

    assert "custom_device_id" in cust_rules.get_rules_info()
    assert len(cust_rules.get_rules(rule_filter="*")) == 3


def test_rule_validation():

    # The definition of the following rule should match the match_example
    try:
        ip_v4_rule_test = Rule(
            name="ip_v4_test",
            type="regex",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example=["192.0.2.1", "0.0.0.0"],
        )
    except ValueError:
        pytest.fail("The example does not match the rule definition")

    # The definition of the following rule should not match the nomatch-example
    try:
        ip_v4_rule_test = Rule(
            name="ip_v4_test",
            type="regex",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example=["192.0.2.1", "0.0.0.0"],
            nomatch_example="192",
        )
    except ValueError:
        pytest.fail("The example does not match the rule definition")

    # The next tests should fail and result in a ValueError
    with pytest.raises(ValueError):
        ip_v4_rule_fail = Rule(
            name="ip_v4",
            type="regex",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            match_example="192",
        )

    with pytest.raises(ValueError):
        ip_v4_rule_fail = Rule(
            name="ip_v4",
            type="regex",
            description="IP address v4",
            definition=r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
            nomatch_example=["192.1.1.1", "192"],
        )

    with pytest.raises(ValueError):
        ip_v4_rule_fail = Rule(
            name="ip_v4",
            type="regex",
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

    assert (email_rule.match_example is not None) and (email_rule.nomatch_example is not None)
    assert (ip_v4_rule.match_example is not None) and (ip_v4_rule.nomatch_example is not None)
    assert (ip_v6_rule.match_example is not None) and (ip_v6_rule.nomatch_example is not None)
    assert (mac_rule.match_example is not None) and (mac_rule.nomatch_example is not None)
    assert (url_rule.match_example is not None) and (url_rule.nomatch_example is not None)
    assert (fqdn_rule.match_example is not None) and (fqdn_rule.nomatch_example is not None)

import pytest

from discoverx.dx import DX
from discoverx.dx import Scanner


def test_dx_instantiation(spark):

    dx = DX(spark=spark)
    assert dx.column_type_classification_threshold == 0.95

    # The validation should fail if the database does not exist
    with pytest.raises(ValueError) as e_error:
        dx = DX(spark=spark)
        dx.database = "testdb"
        dx._validate_database()
    assert e_error.value.args[0] == "The given database testdb does not exist."

    # The validation should fail if threshold is outside of [0,1]
    with pytest.raises(ValueError) as e_threshold_error_plus:
        dx = DX(column_type_classification_threshold=1.4, spark=spark)

    with pytest.raises(ValueError) as e_threshold_error_minus:
        dx = DX(column_type_classification_threshold=-1.0, spark=spark)

    # simple test for displaying rules
    try:
        dx.display_rules()
    except Exception as display_rules_error:
        pytest.fail(f"Displaying rules failed with {display_rules_error}")


def test_msql(spark, monkeypatch):

    # apply the monkeypatch for the columns_table_name
    monkeypatch.setattr(Scanner, "COLUMNS_TABLE_NAME", "default.columns_mock")

    dx = DX(spark=spark)
    dx.scan(tables="tb_1", rules="ip_*")
    result = dx.msql("SELECT [ip_v4] as ip FROM *.*.*").collect()
    ips = [row.ip for row in result]
    ips.sort()

    assert ips == ["1.2.3.4", "3.4.5.60"]

import pytest
from discoverx.data_model import DataModel

from discoverx.dx import DX
from discoverx.config import ColumnInfo, TableInfo
from discoverx.rules import Rule
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import pandas as pd

from discoverx.sql_builder import SqlBuilder

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

def test_execute_scan(spark: SparkSession):
    
    expected = pd.DataFrame([
        ["None", "default", "tb_1", "ip", "any_word", 0.0],
        ["None", "default", "tb_1", "ip", "any_number", 0.0],
        ["None", "default", "tb_1", "description", "any_word", 0.5],
        ["None", "default", "tb_1", "description", "any_number", 0.0]
    ], columns = ["catalog", "database", "table", "column", "rule_name", "frequency"])
    
    columns = [
        ColumnInfo("id", "number", False),
        ColumnInfo("ip", "string", False),
        ColumnInfo("description", "string", False),
    ]
    table_list = [
        TableInfo(None, "default", "tb_1", columns)
    ]
    rules = [
        Rule(name="any_word", type="regex", description="Any word", definition=r"^\w*$"),
        Rule(name="any_number", type="regex", description="Any number", definition=r"^\d*$"),
    ]
    dx = DX(spark=spark)
    actual = dx._execute_scan(table_list, rules, 100)

    logging.info("Scan result is: \n%s", actual)
    
    assert actual.equals(expected)


def test_scan(spark: SparkSession):
    
    expected = pd.DataFrame([
        ["None", "default", "tb_1", "ip", "ip_v4", 1.0],
        ["None", "default", "tb_1", "ip", "ip_v6", 0.0],
        ["None", "default", "tb_1", "description", "ip_v4", 0.0],
        ["None", "default", "tb_1", "description", "ip_v6", 0.0]
    ], columns = ["catalog", "database", "table", "column", "rule_name", "frequency"])

    sql_builder = SqlBuilder()
    sql_builder.columns_table_name = "default.columns_mock"
    data_model = DataModel(sql_builder=sql_builder, spark=spark)

    dx = DX(data_model=data_model, sql_builder=sql_builder, spark=spark)
    dx.scan(tables="tb_1", rules="ip_*")
    
    assert dx.scan_result.equals(expected)

from discoverx.config import ColumnInfo, Rule, TableInfo
from discoverx.explorer import Explorer
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import pandas as pd

def test_scan(spark: SparkSession, tmp_path: Path):
    logging.info("Testing the ETL job")
    expected = pd.DataFrame([
        ["hive_metastore", "default", "tb_1", "ip", "any_word", 0.0],
        ["hive_metastore", "default", "tb_1", "ip", "any_number", 0.0],
        ["hive_metastore", "default", "tb_1", "description", "any_word", 0.5],
        ["hive_metastore", "default", "tb_1", "description", "any_number", 0.0]
    ], columns = ["catalog", "database", "table", "column", "rule_name", "frequency"])
    
    columns = [
        ColumnInfo("id", "number", False),
        ColumnInfo("ip", "string", False),
        ColumnInfo("description", "string", False),
    ]
    table_list = [
        TableInfo("hive_metastore", "default", "tb_1", columns)
    ]
    rules = [
        Rule("any_word", "regex", "Any word", "^\\\\w*$"),
        Rule("any_number", "regex", "Any number", "^\\\\d*$"),
    ]
    explorer = Explorer(spark)
    actual = explorer.scan(table_list, rules, 100)

    logging.info("Generated table is: \n%s", actual)
    
    assert actual.equals(expected)



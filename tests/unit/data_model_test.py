import pytest
from discoverx.sql_builder import SqlBuilder
from discoverx.data_model import DataModel
from discoverx.config import ColumnInfo, TableInfo

def test_get_table_list(spark):
    sql_builder = SqlBuilder()
    sql_builder.columns_table_name = "default.columns_mock"
    data_model = DataModel(sql_builder=sql_builder)
    expected = [
        TableInfo("hive_metastore", "default", "tb_all_types", [
            ColumnInfo("str_col", "STRING", None),
            ColumnInfo("int_col", "INT", None),
            ColumnInfo("double_col", "DOUBLE", None),
            ColumnInfo("timestamp_col", "TIMESTAMP", None),
            ColumnInfo("bool_col", "BOOLEAN", None),
            ColumnInfo("long_col", "LONG", None),
            ColumnInfo("null_col", "NULL", None),
            ColumnInfo("decimal_col", "DECIMAL", None),
            ColumnInfo("float_col", "FLOAT", None),
            ColumnInfo("map_col", "MAP", None),
            ColumnInfo("short_col", "SHORT", None),
            ColumnInfo("array_col", "ARRAY", None),
            ColumnInfo("date_col", "DATE", None),
            ColumnInfo("byte_col", "BYTE", None),
            ColumnInfo("struct_col", "STRUCT", None),
            ColumnInfo("binary_col", "BINARY", None),
            ColumnInfo("char_col", "CHAR", None),
            ColumnInfo("udt_col", "USER_DEFINED_TYPE", None),
            ColumnInfo("interval_col", "INTERVAL", None),
            ColumnInfo("str_part_col", "STRING", 1),
        ])
    ]

    actual = data_model.get_table_list('*', '*', '*_all_types')

    assert len(actual) == 1
    assert actual == expected
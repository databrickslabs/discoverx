import pandas as pd
import pytest
from discoverx.dx import DX
from discoverx import logging
from discoverx.explorer import DataExplorer

logger = logging.Logging()


# @pytest.fixture(scope="module", name="dx_ip")
# def scan_ip_in_tb1(spark, mock_uc_functionality):
#     dx = DX(spark=spark)
#     dx.scan(from_tables="*.*.tb_1", rules="ip_*")
#     return dx


def test_listing_tables(spark):
    ex = DataExplorer("*.*.*", spark, "default.columns_mock")
    assert len(ex.table_info_df) > 1


def test_filtering_tables(spark):
    ex = DataExplorer("*.*.tb_1", spark, "default.columns_mock")
    assert len(ex.table_info_df) == 1


def test_filtering_tables_raises_error(spark):
    with pytest.raises(ValueError):
        ex = DataExplorer("*.*.tb_non_existent", spark, "default.columns_mock")


def test_sql_template(spark):
    ex = DataExplorer("*.*.tb_1", spark, "default.columns_mock")
    result = ex.sql("SELECT 1 AS a FROM {full_table_name} LIMIT 1").collect()

    assert len(result) == 1

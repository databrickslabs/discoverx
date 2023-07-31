# import pandas as pd
# import pytest
# from discoverx.dx import DX
# from discoverx import logging
# from discoverx.explorer import DataExplorer, InfoFetcher

# logger = logging.Logging()


# @pytest.fixture
# def sample_data():
#     data = {
#         "table_catalog": ["db1", "db1", "db2", "db2"],
#         "table_schema": ["public", "public", "schema1", "schema1"],
#         "table_name": ["table1", "table1", "table1", "table2"],
#         "column_name": ["table1", "table1", "table1", "table2"],
#         "data_type": ["string", "string", "string", "string"],
#         "partition_index": [None, None, None, 1],
#     }
#     return pd.DataFrame(data)


# # @pytest.fixture(scope="module", name="dx_ip")
# # def scan_ip_in_tb1(spark, mock_uc_functionality):
# #     dx = DX(spark=spark)
# #     dx.scan(from_tables="*.*.tb_1", rules="ip_*")
# #     return dx


# @pytest.fixture()
# def fetcher_mock():
#     def get_info_df(a, b, c, d):
#         return pd.DataFrame(
#             [
#                 ["None", "default", "tb_1", "description", "string"],
#                 ["None", "default", "tb_1", "ip", "string"],
#                 ["None", "default", "tb_2", "ip", "string"],
#                 ["None", "default", "tb_3", "mac", "string"],
#             ],
#             columns=["table_catalog", "table_schema", "table_name", "column_name", "data_type"],
#         )

#     magic_mock = type("MagicMock", (), {"get_tables_info_df": get_info_df})
#     return magic_mock()


# @pytest.fixture()
# def fetcher(spark):
#     return InfoFetcher(spark, "default.columns_mock")


# def test_group_and_collect(sample_data):
#     result = DataExplorer._to_info_list(sample_data)
#     assert len(result) == 3


# def test_listing_tables(spark, fetcher):
#     ex = DataExplorer("*.*.*", spark, fetcher)
#     assert len(ex.table_info_df) > 1


# def test_filtering_tables(spark, fetcher):
#     ex = DataExplorer("*.*.tb_1", spark, fetcher)
#     assert len(ex.table_info_df) == 4  # 4 columns returned


# def test_filtering_tables_raises_error(spark, fetcher):
#     with pytest.raises(ValueError):
#         ex = DataExplorer("*.*.tb_non_existent", spark, fetcher)


# def test_sql_template(spark, fetcher):
#     ex = DataExplorer("*.*.tb_1", spark, fetcher)
#     result = ex.apply_sql("SELECT 1 AS a FROM {full_table_name} LIMIT 1").to_dataframe()

#     assert len(result) == 1


# def test_column_filtering(spark, fetcher):
#     ex = DataExplorer("*.*.*", spark, fetcher)
#     assert len(ex.table_info_df) > 1

#     result = ex.having_columns("interval_col")

#     assert len(result.table_info_df) == 1

import pytest
import pandas as pd
import datetime
import discoverx.delta_housekeeping as mut
from discoverx.delta_housekeeping import DeltaHousekeepingActions
from pathlib import Path


def _resolve_file_path(request, relative_path):
    module_path = Path(request.module.__file__)
    test_file_path = module_path.parent / relative_path
    return pd.read_csv(str(test_file_path.resolve()))


@pytest.fixture()
def housekeeping_stats(request):
    return _resolve_file_path(request, "data/delta_housekeeping/dhk_pandas_result.csv")


@pytest.fixture()
def expected_need_optimize(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_need_optimize.csv")


@pytest.fixture()
def expected_need_vacuum(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_need_vacuum.csv")


@pytest.fixture()
def expected_not_optimized_last_days(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_not_optimized_last_days.csv")


@pytest.fixture()
def expected_not_vacuumed_last_days(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_not_vacuumed_last_days.csv")


@pytest.fixture()
def expected_need_analysis(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_need_analysis.csv")


@pytest.fixture()
def expected_optimized_too_freq(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_optimized_too_freq.csv")


@pytest.fixture()
def expected_vacuumed_too_freq(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_vacuumed_too_freq.csv")


@pytest.fixture()
def expected_do_not_need_optimize(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_do_not_need_optimize.csv")


@pytest.fixture()
def expected_zorder_not_effective(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_zorder_not_effective.csv")


@pytest.fixture()
def patch_datetime_now(monkeypatch):
    class mydatetime:
        @classmethod
        def now(cls, tzinfo):
            return datetime.datetime(2024, 1, 28, 12, 0, 0).replace(tzinfo=tzinfo)

        @classmethod
        def today(cls):
            return datetime.datetime(2024, 1, 28, 12, 0, 0)

    monkeypatch.setattr(mut, 'datetime', mydatetime)


def test_apply_need_optimize(housekeeping_stats, expected_need_optimize, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_optimized"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_optimized"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 3
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_optimize.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_need_optimize(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_table_size_optimize=1024*1024*1024*1024
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_optimized"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_optimized"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_need_vacuum(housekeeping_stats, expected_need_vacuum, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_vacuumed"]["col_name"]
    need_vacuum_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_vacuumed"]["legend"]), :
    ]
    assert need_vacuum_df.shape[0] == 17
    pd.testing.assert_frame_equal(
        need_vacuum_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_vacuum.loc[:, ["catalog", "database", "tableName"]],
    )


def test_apply_not_optimized_last_days(housekeeping_stats, expected_not_optimized_last_days, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_optimized_last_days"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_optimized_last_days"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 2
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_not_optimized_last_days.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_not_optimized_last_days(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_days_not_optimized=1e60
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_optimized_last_days"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_optimized_last_days"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_not_vacuumed_last_days(housekeeping_stats, expected_not_vacuumed_last_days, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_vacuumed_last_days"]["col_name"]
    need_vacuum_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_vacuumed_last_days"]["legend"]), :
    ]
    assert need_vacuum_df.shape[0] == 2
    pd.testing.assert_frame_equal(
        need_vacuum_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_not_vacuumed_last_days.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_not_vacuumed_last_days(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_days_not_vacuumed=1e60
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["not_vacuumed_last_days"]["col_name"]
    need_vacuum_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["not_vacuumed_last_days"]["legend"]), :
    ]
    assert need_vacuum_df.shape[0] == 0


def test_apply_optimized_too_freq(housekeeping_stats, expected_optimized_too_freq, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["optimized_too_freq"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["optimized_too_freq"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 1
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_optimized_too_freq.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_optimized_too_freq(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        max_optimize_freq=0
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["optimized_too_freq"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["optimized_too_freq"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_vacuumed_too_freq(housekeeping_stats, expected_vacuumed_too_freq, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["vacuumed_too_freq"]["col_name"]
    need_vacuum_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["vacuumed_too_freq"]["legend"]), :
    ]
    assert need_vacuum_df.shape[0] == 2
    pd.testing.assert_frame_equal(
        need_vacuum_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_vacuumed_too_freq.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_vacuumed_too_freq(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        max_vacuum_freq=0
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["vacuumed_too_freq"]["col_name"]
    need_vacuum_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["vacuumed_too_freq"]["legend"]), :
    ]
    assert need_vacuum_df.shape[0] == 0


def test_apply_do_not_need_optimize(housekeeping_stats, expected_do_not_need_optimize, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["do_not_need_optimize"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["do_not_need_optimize"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 2
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_do_not_need_optimize.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_do_not_need_optimize(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_table_size_optimize=0
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["do_not_need_optimize"]["col_name"]
    need_optimize_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["do_not_need_optimize"]["legend"]), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_analyze_tables(housekeeping_stats, expected_need_analysis, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["to_analyze"]["col_name"]
    need_analysis_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["to_analyze"]["legend"]), :
    ]
    assert need_analysis_df.shape[0] == 1
    pd.testing.assert_frame_equal(
        need_analysis_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_analysis.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_analyze_tables(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        small_file_threshold=0
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["to_analyze"]["col_name"]
    need_analysis_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["to_analyze"]["legend"]), :
    ]
    assert need_analysis_df.shape[0] == 0


def test_apply_zorder_not_effective(housekeeping_stats, expected_zorder_not_effective, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["zorder_not_effective"]["col_name"]
    need_analysis_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["zorder_not_effective"]["legend"]), :
    ]
    assert need_analysis_df.shape[0] == 1
    pd.testing.assert_frame_equal(
        need_analysis_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_zorder_not_effective.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_zorder_not_effective(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_number_of_files_for_zorder=0
    )
    res = dha.generate_recommendations().toPandas()
    col_name = dha.recomendations_dict["zorder_not_effective"]["col_name"]
    need_analysis_df = res.loc[
        res[col_name] & res[col_name + dha.reason_col_suffix].str.contains(dha.recomendations_dict["zorder_not_effective"]["legend"]), :
    ]
    assert need_analysis_df.shape[0] == 0


def test_explain(housekeeping_stats, patch_datetime_now):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha._explain()
    assert len(res) == 9

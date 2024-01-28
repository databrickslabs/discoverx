import pytest
import pandas as pd
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


def test_apply_need_optimize(housekeeping_stats, expected_need_optimize):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_not_optimized_legend), :
    ]
    assert need_optimize_df.shape[0] == 3
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_optimize.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_need_optimize(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_table_size_optimize=1024*1024*1024*1024
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_not_optimized_legend), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_need_vacuum(housekeeping_stats, expected_need_vacuum):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_vacuum_df = res.loc[res["rec_vacuum"] & res["rec_vacuum_reason"].str.contains(dha.tables_not_vacuumed_legend), :]
    assert need_vacuum_df.shape[0] == 17
    pd.testing.assert_frame_equal(
        need_vacuum_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_vacuum.loc[:, ["catalog", "database", "tableName"]],
    )


def test_apply_not_optimized_last_days(housekeeping_stats, expected_not_optimized_last_days):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_not_optimized_last_days), :
    ]
    assert need_optimize_df.shape[0] == 2
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_not_optimized_last_days.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_not_optimized_last_days(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_days_not_optimized=1e60
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_not_optimized_last_days), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_not_vacuumed_last_days(housekeeping_stats, expected_not_vacuumed_last_days):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_vacuum_df = res.loc[
        res["rec_vacuum"] & res["rec_vacuum_reason"].str.contains(dha.tables_not_vacuumed_last_days), :
    ]
    assert need_vacuum_df.shape[0] == 2
    pd.testing.assert_frame_equal(
        need_vacuum_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_not_vacuumed_last_days.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_not_vacuumed_last_days(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_days_not_vacuumed=1e60
    )
    res = dha.generate_recommendations()
    need_vacuum_df = res.loc[
        res["rec_vacuum"] & res["rec_vacuum_reason"].str.contains(dha.tables_not_vacuumed_last_days), :
    ]
    assert need_vacuum_df.shape[0] == 0


def test_apply_optimized_too_freq(housekeeping_stats):
    # TODO add an example in the dataset?
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_optimized_too_freq), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_empty_apply_optimized_too_freq(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        max_optimize_freq=1e60
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_optimized_too_freq), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_vacuumed_too_freq(housekeeping_stats):
    # TODO add an example in the dataset?
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_vacuum_df = res.loc[
        res["rec_vacuum"] & res["rec_vacuum_reason"].str.contains(dha.tables_vacuumed_too_freq), :
    ]
    assert need_vacuum_df.shape[0] == 0


def test_empty_apply_vacuumed_too_freq(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        max_vacuum_freq=1e60
    )
    res = dha.generate_recommendations()
    need_vacuum_df = res.loc[
        res["rec_vacuum"] & res["rec_vacuum_reason"].str.contains(dha.tables_vacuumed_too_freq), :
    ]
    assert need_vacuum_df.shape[0] == 0


def test_apply_do_not_need_optimize(housekeeping_stats):
    # TODO add an example in the dataset?
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_do_not_need_optimize), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_empty_apply_do_not_need_optimize(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_table_size_optimize=1e60
    )
    res = dha.generate_recommendations()
    need_optimize_df = res.loc[
        res["rec_optimize"] & res["rec_optimize_reason"].str.contains(dha.tables_do_not_need_optimize), :
    ]
    assert need_optimize_df.shape[0] == 0


def test_apply_analyze_tables(housekeeping_stats, expected_need_analysis):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_analysis_df = res.loc[
        res["rec_misc"] & res["rec_misc_reason"].str.contains(dha.tables_to_analyze), :
    ]
    assert need_analysis_df.shape[0] == 1
    # module_path = Path(request.module.__file__)
    # test_file_path = module_path.parent / "data/delta_housekeeping/expected_need_analysis.csv"
    # need_analysis_df.to_csv(str(test_file_path.resolve()), index=False)
    pd.testing.assert_frame_equal(
        need_analysis_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_analysis.loc[:, ["catalog", "database", "tableName"]],
    )


def test_empty_apply_analyze_tables(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        small_file_threshold=0
    )
    res = dha.generate_recommendations()
    need_analysis_df = res.loc[
        res["rec_misc"] & res["rec_misc_reason"].str.contains(dha.tables_to_analyze), :
    ]
    assert need_analysis_df.shape[0] == 0


def test_apply_zorder_not_effective(request, housekeeping_stats, expected_need_analysis):
    # TODO add an example in the dataset?
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.generate_recommendations()
    need_analysis_df = res.loc[
        res["rec_misc"] & res["rec_misc_reason"].str.contains(dha.tables_zorder_not_effective), :
    ]
    assert need_analysis_df.shape[0] == 0


def test_empty_apply_zorder_not_effective(housekeeping_stats):
    # TODO add an example in the dataset?
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_number_of_files_for_zorder=0
    )
    res = dha.generate_recommendations()
    need_analysis_df = res.loc[
        res["rec_misc"] & res["rec_misc_reason"].str.contains(dha.tables_zorder_not_effective), :
    ]
    assert need_analysis_df.shape[0] == 0


def test_explain(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha._explain()
    assert len(res) == 8

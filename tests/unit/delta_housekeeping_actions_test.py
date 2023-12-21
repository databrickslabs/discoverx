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


def test_apply_output(housekeeping_stats, expected_need_optimize):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
    )
    res = dha.apply()
    assert len(res) == 7
    need_optimize = [item for item in res if (list(item.keys())[0] == dha.tables_not_optimized_legend)]
    assert len(need_optimize) == 1
    need_optimize_df = list(need_optimize[0].values())[0]
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected_need_optimize.loc[:, ["catalog", "database", "tableName"]],
    )
    # TODO complete all the tests


def test_empty_apply_output(housekeeping_stats):
    dha = DeltaHousekeepingActions(
        None,
        stats=housekeeping_stats,
        min_table_size_optimize=1024*1024*1024*1024
    )
    res = dha.apply()
    assert len(res) == 6
    need_optimize = [item for item in res if list(item.keys())[0] == dha.tables_not_optimized_legend]
    assert len(need_optimize) == 0

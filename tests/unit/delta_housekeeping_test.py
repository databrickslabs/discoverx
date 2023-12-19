import pytest
import pandas as pd
from discoverx.delta_housekeeping import DeltaHousekeepingActions
from pathlib import Path


def test_need_optimize(request):
    module_path = Path(request.module.__file__)
    test_file_path = module_path.parent / "data/delta_housekeeping/dhk_pandas_result.csv"
    stats = pd.read_csv(str(test_file_path.resolve()))
    dha = DeltaHousekeepingActions(
        None,
        stats=stats,
    )
    res = dha.apply()
    assert len(res) == 1
    need_optimize = [item for item in res if list(res[0].keys())[0] == dha.tables_not_optimized_legend]
    assert len(need_optimize) == 1
    need_optimize_df = list(need_optimize[0].values())[0]
    need_optimize_df.to_csv(module_path.parent / "data/delta_housekeeping/expected_need_optimize.csv", index=False)
    expected = pd.read_csv(module_path.parent / "data/delta_housekeeping/expected_need_optimize.csv")
    pd.testing.assert_frame_equal(
        need_optimize_df.reset_index().loc[:, ["catalog", "database", "tableName"]],
        expected.loc[:, ["catalog", "database", "tableName"]],
    )

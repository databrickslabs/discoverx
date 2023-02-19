import pytest

from discoverx.dx import DX


def test_dx_instantiation():

    dx = DX()
    assert dx.output_table == "default.discoverx_results"

    # The validation should fail if the database does not exist
    with pytest.raises(ValueError) as e_error:
        dx = DX(output_table="testdb.outputtable")
    assert e_error.value.args[0] == "The given database testdb does not exist."

    # The validation should fail if threshold is outside of [0,1]
    with pytest.raises(ValueError) as e_threshold_error_plus:
        dx = DX(column_type_classification_threshold=1.4)

    with pytest.raises(ValueError) as e_threshold_error_minus:
        dx = DX(column_type_classification_threshold=-1.0)

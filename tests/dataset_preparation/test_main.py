from unittest.mock import patch

import polars as pl
import pytest

from challenge.dataset_preparation.main import FIELDS, Config, main
from tests.conftest import DATASET


def test_main_success(mock_config: Config):
    id_field = FIELDS[0]
    expected_result_df = pl.read_csv(DATASET).sort(by=id_field)

    main(mock_config)
    result_df = pl.read_csv(mock_config.csv_output_path).sort(by=id_field)

    assert isinstance(result_df, pl.DataFrame)
    assert result_df.shape[0] == mock_config.records
    assert tuple(result_df.columns) == FIELDS
    assert expected_result_df.equals(result_df)


@patch("challenge.dataset_preparation.main.write_content", retun_value=Exception("Test Error"))
def test_main_fail(_, mock_config: Config):
    with pytest.raises(SystemExit) as exc:
        main(mock_config)
    assert exc.value.code == 1

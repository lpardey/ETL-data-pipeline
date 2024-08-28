from unittest.mock import Mock, patch

import pytest

from challenge.transform_load_to_parquet.main import main


@patch("challenge.transform_load_to_parquet.main.process_using_polars")
@patch("challenge.transform_load_to_parquet.main.process_using_pyarrow")
def test_main_success(m_process_using_pyarrow: Mock, m_process_using_polars: Mock, argv: list[str]):
    main(argv)

    if "-e" in argv:
        m_process_using_pyarrow.assert_called_once()
    else:
        m_process_using_polars.assert_called_once()


@pytest.mark.parametrize(
    "side_effect",
    [
        pytest.param(FileNotFoundError, id="File not found error"),
        pytest.param(FileExistsError, id="File exists error"),
        pytest.param(Exception, id="General exception"),
    ],
)
@patch("challenge.transform_load_to_parquet.main.process_using_polars")
@patch("challenge.transform_load_to_parquet.main.process_using_pyarrow")
def test_main_fail(m_process_using_pyarrow: Mock, m_process_using_polars: Mock, side_effect, argv):
    if "-e" in argv:
        m_process_using_pyarrow.side_effect = side_effect
    else:
        m_process_using_polars.side_effect = side_effect

    with pytest.raises(SystemExit):
        main(argv)

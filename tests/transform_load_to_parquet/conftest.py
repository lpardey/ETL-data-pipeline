from pathlib import Path

import pyarrow
import pyarrow.csv
import pyarrow.parquet
import pytest

CSV_MOCK_PATH = "tests/transform_load_to_parquet/csv_mock.csv"


@pytest.fixture
def output_temp(tmp_path: Path) -> Path:
    output_path = tmp_path / "output"
    return output_path


@pytest.fixture
def argv(output_temp: Path) -> list[str]:
    return [CSV_MOCK_PATH, "-o", str(output_temp), "-p", "region_de_venta"]


@pytest.fixture
def expected_parquet_table():
    table = pyarrow.csv.read_csv(CSV_MOCK_PATH)
    return table

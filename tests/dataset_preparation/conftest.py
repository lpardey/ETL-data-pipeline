from pathlib import Path

import pytest

from challenge.dataset_preparation.main import Config

EXPECTED_OUTPUT_FILE = Path(__file__).parent / "expected_output.csv"


@pytest.fixture
def mock_config(tmp_path: Path) -> Config:
    config = Config()
    config.records = 1_000
    config.batch_size = 100
    config.csv_output_path = tmp_path / "generated_output.csv"
    return config

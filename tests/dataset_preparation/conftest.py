import pytest

from challenge.dataset_preparation.config import Config


@pytest.fixture
def mock_config() -> Config:
    config = Config()
    config.records = 10
    config.number_of_batches = 2
    config.batch_size = 5
    return config

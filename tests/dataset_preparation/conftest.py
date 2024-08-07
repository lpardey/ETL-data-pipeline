import pytest

from challenge.dataset_preparation.main import Config


@pytest.fixture
def mock_config() -> Config:
    config = Config()
    config.records = 10
    config.batch_size = 5
    return config

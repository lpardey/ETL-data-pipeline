from unittest import mock

import pytest
from google.cloud.storage import Bucket, Client

from challenge.upload_to_google_cloud.parser import Config


@pytest.fixture
def mock_config(tmp_path):
    return Config(directory=tmp_path, bucket_name="test_bucket", blob_name=None, workers=8)


@pytest.fixture
def mock_client():
    return mock.create_autospec(Client)


@pytest.fixture
def mock_bucket():
    bucket = mock.create_autospec(Bucket)
    bucket.name = "my_bucket"
    return bucket


@pytest.fixture
def argv():
    return ["my_dir/my_file.parquet", "my_bucket"]

from unittest.mock import Mock, patch

import pytest
from google.cloud.exceptions import exceptions
from google.cloud.storage import Bucket, Client

from scripts.upload_to_google_cloud.main import get_or_create_bucket, main, transfer_manager, upload_files_to_gcs
from scripts.upload_to_google_cloud.parser import Config


def test_get_or_create_bucket_existing(mock_client: Client, mock_bucket: Bucket):
    mock_client.get_bucket.return_value = mock_bucket
    bucket_name = "my_bucket"

    bucket = get_or_create_bucket(mock_client, bucket_name)

    assert bucket == mock_bucket
    mock_client.get_bucket.assert_called_once_with(bucket_name)


def test_get_or_create_bucket_not_found(mock_client: Client, mock_bucket: Bucket):
    mock_client.get_bucket.side_effect = exceptions.NotFound("Bucket not found")
    mock_client.create_bucket.return_value = mock_bucket
    bucket_name = "my_bucket"

    bucket = get_or_create_bucket(mock_client, bucket_name)

    assert bucket == mock_bucket
    mock_client.create_bucket.assert_called_once()
    mock_client.get_bucket.assert_called_once_with(bucket_name)


def test_upload_files_to_gcs_single_file(mock_bucket: Bucket, mock_config: Config):
    filename = "test.parquet"
    file_path = mock_config.directory / filename
    file_path.touch()

    upload_files_to_gcs(mock_bucket, mock_config)

    mock_bucket.blob.assert_called_once_with(filename)
    mock_blob = mock_bucket.blob.return_value
    mock_blob.upload_from_filename.assert_called_once_with(str(file_path), if_generation_match=0)


@patch.object(transfer_manager, "upload_many_from_filenames")
def test_upload_files_to_gcs_multiple_files(
    m_upload_many_from_filenames: Mock, mock_bucket: Bucket, mock_config: Config
):
    file_paths = [mock_config.directory / f"test_{i}.parquet" for i in range(5)]
    filenames = []
    for file_path in file_paths:
        file_path.touch()
        filenames.append(file_path.name)

    upload_files_to_gcs(mock_bucket, mock_config)

    mock_bucket.blob.assert_not_called()
    upload_files_to_gcs_call_args = m_upload_many_from_filenames.call_args.kwargs
    assert upload_files_to_gcs_call_args["bucket"] == mock_bucket
    assert all(file in upload_files_to_gcs_call_args["filenames"] for file in filenames)
    assert upload_files_to_gcs_call_args["source_directory"] == str(mock_config.directory)
    assert upload_files_to_gcs_call_args["max_workers"] == mock_config.workers


@pytest.mark.parametrize(
    "expected_side_effect, func",
    [
        pytest.param(Exception, get_or_create_bucket, id="Unexpected error"),
        pytest.param(FileNotFoundError, upload_files_to_gcs, id="File not found error"),
        pytest.param(Exception, upload_files_to_gcs, id="Unexpected error"),
    ],
)
@patch("scripts.upload_to_google_cloud.main.get_or_create_bucket")
@patch("scripts.upload_to_google_cloud.main.upload_files_to_gcs")
def test_upload_files_to_gcs_fail(
    m_upload_files_to_gcs: Mock, m_get_or_create_bucket: Mock, expected_side_effect, func, argv, mock_bucket
):
    if func == get_or_create_bucket:
        m_get_or_create_bucket.side_effect = expected_side_effect
    else:
        m_get_or_create_bucket.return_value = mock_bucket
        m_upload_files_to_gcs.side_effect = expected_side_effect

    with pytest.raises(SystemExit) as exc:
        main(argv)

    if func == get_or_create_bucket:
        m_get_or_create_bucket.assert_called_once()
    else:
        m_upload_files_to_gcs.assert_called_once()
    assert exc.value.code == 1

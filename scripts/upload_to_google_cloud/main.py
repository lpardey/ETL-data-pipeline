import logging
import sys

from google.cloud.exceptions import exceptions
from google.cloud.storage import Bucket, Client, transfer_manager

from .parser import Config, get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Upload to GCS")


def main(argv: list[str] | None = None) -> None:
    """
    This script uploads Parquet files from a local directory to a specified Google Cloud Storage bucket.
    """
    config = get_config(argv)

    try:
        client = Client()
        logger.info("Getting bucket for file upload")
        bucket = get_or_create_bucket(client, config.bucket_name)
        logger.info(f"Uploading files to gcs bucket '{bucket.name}'")
        upload_files_to_gcs(bucket, config)

    except FileNotFoundError:
        logger.exception(f"Directory '{config.directory}' doesn't exist")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"An error ocurred: {e}")
        sys.exit(1)


def get_or_create_bucket(client: Client, bucket_name: str) -> Bucket:
    try:
        bucket = client.get_bucket(bucket_name)
    except exceptions.NotFound:
        bucket_constructor = client.bucket(bucket_name)
        bucket_constructor.storage_class = "STANDARD"
        bucket = client.create_bucket(bucket_constructor)
    return bucket


def upload_files_to_gcs(bucket: Bucket, config: Config) -> None:
    paths = config.directory.rglob("*")
    file_paths = [path for path in paths if path.is_file() and path.suffix == ".parquet"]

    if not file_paths:
        raise FileNotFoundError

    if len(file_paths) == 1:
        file_path_obj = file_paths[0]
        blob_name = f"{file_path_obj.name}" if config.blob_name is None else config.blob_name
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(file_path_obj), if_generation_match=0)
        logger.info(f"File '{blob_name}' uploaded successfully")
        return

    filenames = [str(path.relative_to(config.directory)) for path in file_paths]
    logger.info(f"{len(filenames)} files found for upload")
    # Uploads multiple objects concurrently
    results = transfer_manager.upload_many_from_filenames(
        bucket=bucket,
        filenames=filenames,
        source_directory=str(config.directory),
        max_workers=config.workers,
    )
    for name, result in zip(filenames, results):
        if isinstance(result, Exception):
            logger.warning(f"Error while uploading '{name}': {result}")
        else:
            logger.info(f"File '{name}' uploaded successfully")


if __name__ == "__main__":
    main()

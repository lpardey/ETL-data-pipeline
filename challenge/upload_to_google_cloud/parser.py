import argparse
from pathlib import Path

from pydantic import BaseModel


class Config(BaseModel):
    directory: Path
    bucket_name: str
    blob_name: str | None
    workers: int


def get_parser() -> argparse.ArgumentParser:
    description = "Upload Parquet files to Google Cloud Storage"
    parser = argparse.ArgumentParser(prog="Upload to GCS", description=description)
    directory_help = "Path to the directory containing Parquet files to be uploaded."
    bucket_name_help = (
        "Name of the Google Cloud Storage bucket. If the bucket doesn't exist, it will be created automatically."
    )
    blob_name_help = (
        "Optional name for the blob (file) in the bucket. If not specified, the original file names will be used."
    )
    max_workers_help = "Number of concurrent workers to use for uploading files. Default is 8."

    parser.add_argument("directory", type=Path, help=directory_help)
    parser.add_argument("bucket_name", type=str, help=bucket_name_help)
    parser.add_argument("--blob-name", type=str, help=blob_name_help)
    parser.add_argument("-w", "--workers", type=int, help=max_workers_help, default=8)
    return parser


def get_config(argv: list[str] | None = None) -> Config:
    parser = get_parser()
    args = parser.parse_args(argv)
    return Config(
        directory=args.directory,
        bucket_name=args.bucket_name,
        blob_name=args.blob_name,
        workers=args.workers,
    )

import argparse
import logging
from pathlib import Path

from pydantic import BaseModel


class Config(BaseModel):
    input: Path
    output: Path
    partition_cols: list[str] | None
    force: bool
    error_handling: bool
    log_level: int


def get_parser() -> argparse.ArgumentParser:
    prog = "Transform and load to Parquet"
    description = "Convert a plain text file into a Parquet file with optional partitioning and error handling."
    output_help = "Path to write the output Parquet file."
    partition_cols_help = "Column names by which the dataset will be partitioned. No partitions are made by default."
    error_handling_help = (
        "Errors encountered while reading the file will be captured and stored in a quarantine file. "
        "This method is slower and results in a larger output file compared to the default behavior."
    )
    log_level_help = "Set the root logger level to the specified level."
    log_level_choices = ("NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("input", type=Path, help="Plain text file to transform")
    parser.add_argument("output", type=Path, help=output_help)
    parser.add_argument("-p", "--partition-cols", help=partition_cols_help, nargs="*")
    parser.add_argument("-f", "--force", help="Overwrite output file", action="store_true")
    parser.add_argument("-e", "--error-handling", help=error_handling_help, action="store_true")
    parser.add_argument("-l", "--log-level", help=log_level_help, choices=log_level_choices, default="INFO")
    return parser


def get_config(argv: list[str] | None = None) -> Config:
    parser = get_parser()
    args = parser.parse_args(argv)
    return Config(
        input=args.input,
        output=args.output,
        partition_cols=args.partition_cols,
        force=args.force,
        error_handling=args.error_handling,
        log_level=getattr(logging, args.log_level),
    )

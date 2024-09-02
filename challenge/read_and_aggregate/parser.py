import argparse

from pydantic import BaseModel


class Config(BaseModel):
    path: str


def get_parser() -> argparse.ArgumentParser:
    description = "Script to read and query files in Google Cloud Storage"
    parser = argparse.ArgumentParser(prog="File Reader and Query Tool", description=description)
    parser.add_argument("path", type=str, help="Path to the files in Google Cloud Storage")
    return parser


def get_config(argv: list[str] | None = None) -> Config:
    parser = get_parser()
    args = parser.parse_args(argv)
    return Config(path=args.path)

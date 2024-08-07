import argparse

from pydantic import BaseModel


class Config(BaseModel):
    path: str


def get_parser() -> argparse.ArgumentParser:
    description = "Script para leer y hacer consultar de archivos en Google Cloud Storage"
    parser = argparse.ArgumentParser(prog="Lectura y consulta", description=description)
    parser.add_argument("path", type=str, help="Ruta de archivos en GSC")
    return parser


def get_config(argv: list[str] | None = None) -> Config:
    parser = get_parser()
    args = parser.parse_args(argv)
    return Config(path=args.path)

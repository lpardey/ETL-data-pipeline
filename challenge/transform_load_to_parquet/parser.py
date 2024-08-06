import argparse

from pydantic import BaseModel


class Config(BaseModel):
    input: str
    output: str
    partition_cols: list[str] | None
    force: bool


def get_parser() -> argparse.ArgumentParser:
    prog = "Carga y transformación a Parquet"
    description = "Este programa recibe archivos de texto plano y los transforma a formato Parquet"
    partition_cols_help = "Nombres de columnas mediante las cuales se hará la partición del conjunto de datos. El comportamiento por defecto no realiza particiones"

    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("input", type=str, help="Archivo a transformar")
    parser.add_argument("-o", "--output", type=str, help="Destino a escribir", default=".")
    parser.add_argument("-p", "--partition-cols", help=partition_cols_help, nargs="*")
    parser.add_argument("-f", "--force", help="Sobreescribir en destino", action="store_true", default=False)
    return parser


def get_config(argv: list[str] | None = None) -> Config:
    parser = get_parser()
    args = parser.parse_args(argv)
    return Config(input=args.input, output=args.output, partition_cols=args.partition_cols, force=args.force)

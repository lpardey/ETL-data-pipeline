import argparse

from pydantic import BaseModel


class Config(BaseModel):
    directory: str
    bucket_name: str
    blob_name: str | None
    workers: int


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Script para cargar archivos parquet a Google Cloud Storage")
    parser.add_argument("-d", "--directory", type=str, help="Ruta de directorio con archivos a cargar", required=True)
    parser.add_argument("-n", "--bucket-name", type=str, help="Nombre del bucket", required=True)
    parser.add_argument("--blob-name", type=str, help="Nombre del archivo único a cargar (opcional)")
    parser.add_argument("-w", "--workers", type=int, help="Numero de trabajadores", default=8)
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

import argparse
import logging
import sys
from pathlib import Path

from google.cloud.exceptions import exceptions
from google.cloud.storage import Bucket, Client, transfer_manager  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Carga a Google Cloud Storage")


def main() -> None:
    parser = argparse.ArgumentParser(description="Script para cargar archivos parquet a Google Cloud Storage")
    parser.add_argument("-d", "--directory", type=str, help="Ruta de directorio con archivos a cargar", required=True)
    args = parser.parse_args()
    source_directory = args.directory

    try:
        client = Client()
        logger.info("Obteniendo bucket para carga de archivos...")
        bucket = get_or_create_bucket(client, "my_celes_bucket")
        logger.info(f"Iniciando carga de archivos al gcs bucket {bucket.name}")
        upload_files_to_gcs(bucket, source_directory)
    except FileNotFoundError:
        logger.error(f"El directorio '{source_directory}' no existe")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)


def get_or_create_bucket(client: Client, bucket_name: str) -> Bucket:
    try:
        bucket = client.get_bucket(bucket_name)
    except exceptions.NotFound:
        bucket_constructor = client.bucket(bucket_name)
        bucket_constructor.storage_class = "STANDARD"
        bucket = client.create_bucket(bucket_constructor)
    return bucket


def upload_files_to_gcs(bucket: Bucket, source_directory: str, workers: int = 8) -> None:
    source_dir_path_obj = Path(source_directory)
    paths = list(source_dir_path_obj.rglob("*"))

    if not paths:
        raise FileNotFoundError

    if len(paths) == 1:
        filename = str(paths[0])
        blob = bucket.blob("my_celes_blob")
        blob.upload_from_filename(filename, if_generation_match=0)
        logger.info(f"Archivo '{filename}' cargado al bucket '{bucket.name}'.")

    else:
        file_paths = [path for path in paths if path.is_file() and path.suffix == ".parquet"]
        filenames = [str(path.relative_to(source_directory)) for path in file_paths]
        logger.info(f"Se encontraron {len(filenames)} archivos a cargar")
        # Uploads multiple objects concurrently
        results = transfer_manager.upload_many_from_filenames(
            bucket=bucket, filenames=filenames, source_directory=source_directory, max_workers=workers
        )
        for name, result in zip(filenames, results):
            if isinstance(result, Exception):
                logger.warning(f"Error al cargar archivo '{name}': {result}")
            else:
                logger.info(f"Archivo '{name}' cargado al bucket '{bucket.name}'.")


if __name__ == "__main__":
    main()

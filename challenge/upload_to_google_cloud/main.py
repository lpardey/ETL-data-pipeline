import logging
import random
import sys
from pathlib import Path

from google.cloud.exceptions import exceptions
from google.cloud.storage import Bucket, Client, transfer_manager  # type: ignore

from .parser import Config, get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Carga a Google Cloud Storage")


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)

    try:
        client = Client()
        logger.info("Obteniendo bucket para carga de archivos...")
        bucket = get_or_create_bucket(client, config.bucket_name)
        logger.info(f"Iniciando carga de archivos al gcs bucket {bucket.name}")
        upload_files_to_gcs(bucket, config.directory)
    except FileNotFoundError:
        logger.error(f"El directorio '{config.directory}' no existe")
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


def upload_files_to_gcs(bucket: Bucket, config: Config) -> None:
    source_dir_path_obj = Path(config.directory)
    paths = list(source_dir_path_obj.rglob("*"))

    if not paths:
        raise FileNotFoundError

    if len(paths) == 1:
        filename = str(paths[0])
        blob_name = f"my_blob_{random.randrange(1000, 2000)}" if config.blob_name is None else config.blob_name
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(filename, if_generation_match=0)
        logger.info(f"Archivo '{filename}' cargado al bucket '{bucket.name}'.")

    else:
        file_paths = [path for path in paths if path.is_file() and path.suffix == ".parquet"]
        filenames = [str(path.relative_to(config.directory)) for path in file_paths]
        logger.info(f"Se encontraron {len(filenames)} archivos a cargar")
        # Uploads multiple objects concurrently
        results = transfer_manager.upload_many_from_filenames(
            bucket=bucket,
            filenames=filenames,
            source_directory=config.directory,
            max_workers=config.workers,
        )
        for name, result in zip(filenames, results):
            if isinstance(result, Exception):
                logger.warning(f"Error al cargar archivo '{name}': {result}")
            else:
                logger.info(f"Archivo '{name}' cargado al bucket '{bucket.name}'.")


if __name__ == "__main__":
    main()

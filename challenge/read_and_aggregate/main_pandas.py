import logging
import sys
import time

import pandas

from .parser import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Lectura y consulta de archivos en GCS (Pandas)")


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)

    try:
        logger.info("Leyendo archivos...")
        dataframe = pandas.read_parquet(config.path)
        logger.info("Lectura terminada.")
        logger.info("Procesando información...")
        total_sales_by_category = dataframe.groupby("categoria_de_producto", observed=True)["cantidad_de_venta"].sum()
        average_sales_by_region = dataframe.groupby("region_de_venta", observed=True)["cantidad_de_venta"].mean()
        logger.info("Información procesada.")
        logger.info(f"Total de ventas por categoría de producto:\n{total_sales_by_category}")
        logger.info(f"Promedio de ventas por región:\n{average_sales_by_region}")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)


def load_time(path: str) -> float:
    start_load = time.time()
    pandas.read_parquet(path)
    end_load = time.time()
    load_time = end_load - start_load
    return load_time


def processing_time(path: str) -> float:
    dataframe = pandas.read_parquet(path)
    start_processing = time.time()
    dataframe.groupby("categoria_de_producto", observed=True)["cantidad_de_venta"].sum()
    dataframe.groupby("region_de_venta", observed=True)["cantidad_de_venta"].mean()
    end_processing = time.time()
    processing_time = end_processing - start_processing
    return processing_time


if __name__ == "__main__":
    main()

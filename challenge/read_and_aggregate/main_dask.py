import logging
import sys
import time

import dask.config
import dask.dataframe

from .parser import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Lectura y consulta de archivos en GCS (Dask)")


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)

    try:
        logger.info("Leyendo archivos...")
        dataframe: dask.dataframe.DataFrame = dask.dataframe.read_parquet(config.path, filesystem="arrow")
        logger.info("Lectura terminada.")
        persisted_dataframe: dask.dataframe.DataFrame = dataframe.persist()  # persist data in memory for reuse
        logger.info("Procesando información...")
        grouped_by_category = persisted_dataframe.groupby("categoria_de_producto", observed=True)
        total_sales_by_category = grouped_by_category["cantidad_de_venta"].sum()
        grouped_by_region = persisted_dataframe.groupby("region_de_venta", observed=True)
        average_sales_by_region = grouped_by_region["cantidad_de_venta"].mean()
        total_sales_result, average_sales_result = dask.compute(total_sales_by_category, average_sales_by_region)
        logger.info("Información procesada.")
        logger.info(f"Total de ventas por categoría de producto:\n{total_sales_result}")
        logger.info(f"Promedio de ventas por región:\n{average_sales_result}")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)


def load_time(path: str) -> float:
    start_load = time.time()
    dataframe: dask.dataframe.DataFrame = dask.dataframe.read_parquet(path=path, filesystem="arrow")
    dataframe.persist()
    end_load = time.time()
    load_time = end_load - start_load
    return load_time


def processing_time(path: str) -> float:
    dataframe: dask.dataframe.DataFrame = dask.dataframe.read_parquet(path=path, filesystem="arrow")
    persisted_dataframe: dask.dataframe.DataFrame = dataframe.persist()
    start_processing = time.time()
    grouped_by_category = persisted_dataframe.groupby("categoria_de_producto", observed=True)
    total_sales_by_category = grouped_by_category["cantidad_de_venta"].sum()
    grouped_by_region = persisted_dataframe.groupby("region_de_venta", observed=True)
    average_sales_by_region = grouped_by_region["cantidad_de_venta"].mean()
    dask.compute(total_sales_by_category, average_sales_by_region)
    end_processing = time.time()
    processing_time = end_processing - start_processing
    return processing_time


if __name__ == "__main__":
    main()

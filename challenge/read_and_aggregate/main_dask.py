import logging

import dask.config
import dask.dataframe

from .common import Processor, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DaskProcessor")


class DaskProcessor(Processor):
    def read(self) -> dask.dataframe.DataFrame:
        logger.info("Leyendo archivos...")
        dataframe: dask.dataframe.DataFrame = dask.dataframe.read_parquet(self.path, filesystem="arrow")
        logger.info("Lectura terminada.")
        persisted_dataframe: dask.dataframe.DataFrame = dataframe.persist()  # persist data in memory for reuse
        return persisted_dataframe

    def process(self, data: dask.dataframe.DataFrame) -> None:
        logger.info("Procesando información...")
        grouped_by_category = data.groupby("categoria_de_producto", observed=True)
        total_sales_by_category = grouped_by_category["cantidad_de_venta"].sum()
        grouped_by_region = data.groupby("region_de_venta", observed=True)
        average_sales_by_region = grouped_by_region["cantidad_de_venta"].mean()
        total_sales_result, average_sales_result = dask.compute(total_sales_by_category, average_sales_by_region)
        logger.info("Información procesada.")
        logger.info(f"Total de ventas por categoría de producto:\n{total_sales_result}")
        logger.info(f"Promedio de ventas por región:\n{average_sales_result}")


if __name__ == "__main__":
    main(DaskProcessor)

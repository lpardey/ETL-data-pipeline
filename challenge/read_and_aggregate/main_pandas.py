import logging

import pandas

from .common import Processor, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PandasProcessor")


class PandasProcessor(Processor):
    def read(self):
        logger.info("Leyendo archivos...")
        dataframe = pandas.read_parquet(self.path)
        logger.info("Lectura terminada.")
        return dataframe

    def process(self, data: pandas.DataFrame) -> None:
        total_sales_by_category = data.groupby("categoria_de_producto", observed=True)["cantidad_de_venta"].sum()
        average_sales_by_region = data.groupby("region_de_venta", observed=True)["cantidad_de_venta"].mean()
        logger.info("Información procesada.")
        logger.info(f"Total de ventas por categoría de producto:\n{total_sales_by_category}")
        logger.info(f"Promedio de ventas por región:\n{average_sales_by_region}")


if __name__ == "__main__":
    main(PandasProcessor)

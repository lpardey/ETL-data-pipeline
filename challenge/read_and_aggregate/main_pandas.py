import logging

import pandas as pd

from .common import Processor, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PandasProcessor")


class PandasProcessor(Processor):
    def read(self) -> pd.DataFrame:
        logger.info("Reading files...")
        df = pd.read_parquet(self.path)
        logger.info("Reading completed")
        return df

    def process(self, data: pd.DataFrame) -> None:
        total_sales_by_category = data.groupby("categoria_de_producto", observed=True)["cantidad_de_venta"].sum()
        average_sales_by_region = data.groupby("region_de_venta", observed=True)["cantidad_de_venta"].mean()
        logger.info("Information processed")
        logger.info(f"Total sales by product category:\n{total_sales_by_category}")
        logger.info(f"Average sales by region:\n{average_sales_by_region}")

    def cleanup(self) -> None:
        pass


if __name__ == "__main__":
    main(PandasProcessor)

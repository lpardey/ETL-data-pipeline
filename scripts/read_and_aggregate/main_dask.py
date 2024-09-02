import logging

import dask
import dask.dataframe as dd

from .common import Processor, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DaskProcessor")


class DaskProcessor(Processor):
    def read(self) -> dd.DataFrame:
        logger.info("Reading files...")
        df: dd.DataFrame = dd.read_parquet(self.path, filesystem="arrow")
        logger.info("Reading completed")
        # persist data in memory for reuse
        persisted_df: dd.DataFrame = df.persist()  # type: ignore
        return persisted_df

    def process(self, data: dd.DataFrame) -> None:
        logger.info("Processing information...")
        grouped_by_category = data.groupby("categoria_de_producto", observed=True)
        total_sales_by_category = grouped_by_category["cantidad_de_venta"].sum()
        grouped_by_region = data.groupby("region_de_venta", observed=True)
        average_sales_by_region = grouped_by_region["cantidad_de_venta"].mean()
        total_sales_result, average_sales_result = dask.compute(total_sales_by_category, average_sales_by_region)  # type: ignore
        logger.info("Information processed")
        logger.info(f"Total sales by product category:\n{total_sales_result}")
        logger.info(f"Average sales by region:\n{average_sales_result}")

    def cleanup(self) -> None:
        pass


if __name__ == "__main__":
    main(DaskProcessor)

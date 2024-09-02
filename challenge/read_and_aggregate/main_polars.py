import logging

import polars as pl

from .common import Processor, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PolarsProcessor")


class PolarsProcessor(Processor):
    def read(self) -> pl.DataFrame:
        logger.info("Reading files...")
        df = pl.read_parquet(self.path)
        logger.info("Reading completed")
        return df

    def process(self, data: pl.DataFrame) -> None:
        lazy_df = data.lazy()
        total_sales_by_category = (
            lazy_df.group_by("categoria_de_producto")
            .agg(pl.col("cantidad_de_venta").sum().alias("total_sales"))
            .collect()
        )
        average_sales_by_region = (
            lazy_df.group_by("region_de_venta")
            .agg(pl.col("cantidad_de_venta").mean().alias("average_sales"))
            .collect()
        )
        logger.info("Information processed")
        logger.info(f"Total sales by product category:\n{total_sales_by_category}")
        logger.info(f"Average sales by region:\n{average_sales_by_region}")

    def cleanup(self) -> None:
        pass


if __name__ == "__main__":
    main(PolarsProcessor)

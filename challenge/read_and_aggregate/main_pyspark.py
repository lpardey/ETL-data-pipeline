import logging

from pyspark.sql import DataFrame, SparkSession, functions

from .common import Processor, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkProcessor")


class SparkProcessor(Processor):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.spark: SparkSession = (
            SparkSession.builder.appName("SparkProcessor")
            .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
            .config("spark.sql.repl.eagerEval.enabled", True)
            .getOrCreate()
        )
        self.data: DataFrame | None = None

    def read(self) -> DataFrame:
        logger.info("Reading files...")
        required_cols = ["categoria_de_producto", "cantidad_de_venta", "region_de_venta"]
        df = self.spark.read.parquet(self.path).select(required_cols)
        logger.info("Reading completed")
        self.data = df
        df.cache()
        return df

    def process(self, data: DataFrame) -> None:
        logger.info("Processing information...")
        df_grouped_by_category = data.groupBy("categoria_de_producto")
        total_sales_by_category = df_grouped_by_category.agg(functions.sum("cantidad_de_venta").alias("venta_total"))
        df_grouped_by_region = data.groupBy("region_de_venta")
        average_sales_by_region = df_grouped_by_region.agg(functions.mean("cantidad_de_venta").alias("venta_promedio"))
        logger.info("Information processed")
        logger.info(f"Total sales by product category:\n{total_sales_by_category}")
        logger.info(f"Average sales by region:\n{average_sales_by_region}")

    def cleanup(self) -> None:
        if self.data is not None:
            self.data.unpersist()
        self.spark.stop()


if __name__ == "__main__":
    main(SparkProcessor)

import logging
import sys
import time

from pyspark.sql import SparkSession, functions

from .parser import get_config

SCRIPT_NAME = "Lectura y consulta de archivos en GCS (Pyspark)"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(SCRIPT_NAME)


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)

    try:
        logger.info("Leyendo archivos...")
        spark = get_spark_session()
        required_cols = ["categoria_de_producto", "cantidad_de_venta", "region_de_venta"]
        dataframe = spark.read.parquet(config.path).select(required_cols)
        logger.info("Lectura terminada.")
        dataframe.cache()
        logger.info("Procesando información...")
        df_grouped_by_category = dataframe.groupBy("categoria_de_producto")
        total_sales_by_category = df_grouped_by_category.agg(functions.sum("cantidad_de_venta").alias("venta_total"))
        df_grouped_by_region = dataframe.groupBy("region_de_venta")
        average_sales_by_region = df_grouped_by_region.agg(functions.mean("cantidad_de_venta").alias("venta_promedio"))
        logger.info("Información procesada.")
        logger.info(f"Total de ventas por categoría de producto:\n{total_sales_by_category}")
        logger.info(f"Promedio de ventas por región:\n{average_sales_by_region}")
        dataframe.unpersist()
        spark.stop()
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)


def load_time(path: str) -> float:
    start_load = time.time()
    spark = get_spark_session()
    required_cols = ["categoria_de_producto", "cantidad_de_venta", "region_de_venta"]
    dataframe = spark.read.parquet(path).select(required_cols)
    dataframe.cache()
    end_load = time.time()
    spark.stop()
    load_time = end_load - start_load
    return load_time


def processing_time(path: str) -> float:
    spark = get_spark_session()
    required_cols = ["categoria_de_producto", "cantidad_de_venta", "region_de_venta"]
    dataframe = spark.read.parquet(path).select(required_cols)
    dataframe.cache()
    start_processing = time.time()
    df_grouped_by_category = dataframe.groupBy("categoria_de_producto")
    df_grouped_by_category.agg(functions.sum("cantidad_de_venta").alias("venta_total"))
    df_grouped_by_region = dataframe.groupBy("region_de_venta")
    df_grouped_by_region.agg(functions.mean("cantidad_de_venta").alias("venta_promedio"))
    end_processing = time.time()
    dataframe.unpersist()
    spark.stop()
    processing_time = end_processing - start_processing
    return processing_time


def get_spark_session() -> SparkSession:
    spark: SparkSession = (
        SparkSession.builder.appName(SCRIPT_NAME)
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        .config("spark.sql.repl.eagerEval.enabled", True)
        .getOrCreate()
    )
    return spark


if __name__ == "__main__":
    main()

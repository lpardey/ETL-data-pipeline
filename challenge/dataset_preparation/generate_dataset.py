import logging

import numpy
import pandas

from .config import Config, DataBatch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Datos Sintéticos")


def generate_dataset() -> None:
    config = Config()
    dataframe_batches = []

    for batch_number in range(config.number_of_batches):
        start_id = batch_number * config.batch_size + 1
        end_id = start_id + config.batch_size
        dataframe_batch = generate_dataframe_batch(start_id, end_id, config)
        logger.info(f"Conjunto de datos generados {batch_number + 1 }/{config.number_of_batches}")
        dataframe_batches.append(dataframe_batch)

    final_dataframe = pandas.concat(dataframe_batches)
    logger.info("Escribiendo datos...")
    final_dataframe.to_csv("dataset_base.csv", index=False)
    logger.info("Datos generados y guardados exitosamente")


def generate_dataframe_batch(start_id: int, end_id: int, config: Config) -> pandas.DataFrame:
    ids = numpy.arange(start_id, end_id)
    dates = config.random_number_generator.choice(config.dates_range, config.batch_size)
    sales = config.random_number_generator.integers(10, 10001, config.batch_size)
    categories = config.random_number_generator.choice(config.product_categories, config.batch_size)
    regions = config.random_number_generator.choice(config.sale_regions, config.batch_size)
    data = DataBatch(
        id_cliente=ids,
        fecha_de_transaccion=dates,
        cantidad_de_venta=sales,
        categoria_de_producto=categories,
        region_de_venta=regions,
    ).model_dump()
    dataframe = pandas.DataFrame(data)
    return dataframe


if __name__ == "__main__":
    generate_dataset()

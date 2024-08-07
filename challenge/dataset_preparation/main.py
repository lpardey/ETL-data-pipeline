import logging
import math
from datetime import datetime
from functools import cached_property

import numpy
import numpy.typing
import pandas
from pydantic import BaseModel

DateArray = numpy.typing.NDArray[numpy.datetime64]
IntArray = numpy.typing.NDArray[numpy.int_]
StrArray = numpy.typing.NDArray[numpy.str_]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Datos Sintéticos")


class Config(BaseModel):
    records: int = 10_000_000
    batch_size: int = 1_000_000
    start_date: datetime = datetime(year=2023, month=1, day=1)
    end_date: datetime = datetime(year=2024, month=1, day=1)
    product_categories: list[str] = ["Moda", "Tecnología", "Belleza", "Salud", "Juguetes"]
    sale_regions: list[str] = ["Caribe", "Andina", "Pacífico", "Orinoquía", "Amazonía", "Insular"]
    rng_seed: int = 12345
    csv_output_path: str = "dataset_preparation/dataset_base.csv"

    @property
    def number_of_batches(self) -> int:
        return math.ceil(self.records / self.batch_size)

    @cached_property
    def random_number_generator(self) -> numpy.random.Generator:
        return numpy.random.default_rng(self.rng_seed)

    @property
    def dates_range(self) -> DateArray:
        start = self.start_date.strftime("%Y-%m")
        end = self.end_date.strftime("%Y-%m")
        return numpy.arange(start, end, dtype="datetime64[D]")


def main() -> None:
    config = Config()
    dataframes = []

    for batch_number in range(config.number_of_batches):
        start_id = batch_number * config.batch_size + 1
        end_id = start_id + config.batch_size
        batch_dataframe = generate_dataframe(start_id, end_id, config)
        logger.info(f"Conjunto de datos generados {batch_number + 1 }/{config.number_of_batches}")
        dataframes.append(batch_dataframe)

    final_dataframe = pandas.concat(dataframes)
    logger.info("Escribiendo datos...")
    final_dataframe.to_csv(config.csv_output_path, index=False)
    logger.info("Datos generados y guardados exitosamente")
    logger.info(f"Registros iniciales:\n{final_dataframe.head()}")


def generate_dataframe(start_id: int, end_id: int, config: Config) -> pandas.DataFrame:
    ids = numpy.arange(start_id, end_id)
    dates = config.random_number_generator.choice(config.dates_range, config.batch_size)
    sales = config.random_number_generator.integers(10, 10001, config.batch_size)
    categories = config.random_number_generator.choice(config.product_categories, config.batch_size)
    regions = config.random_number_generator.choice(config.sale_regions, config.batch_size)
    data = dict(
        id_cliente=ids,
        fecha_de_transaccion=dates,
        cantidad_de_venta=sales,
        categoria_de_producto=categories,
        region_de_venta=regions,
    )
    dataframe = pandas.DataFrame(data)
    return dataframe


if __name__ == "__main__":
    main()

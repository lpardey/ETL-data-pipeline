import logging
import math
import sys
from concurrent import futures
from functools import cached_property
from typing import Any, TextIO

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger("Synthetic Data")

FIELDS = ("id_cliente", "fecha_de_transaccion", "cantidad_de_venta", "categoria_de_producto", "region_de_venta")
PRODUCT_CATEGORIES = ["Moda", "Tecnología", "Belleza", "Salud", "Juguetes"]
SALE_REGIONS = ["Caribe", "Andina", "Pacífico", "Orinoquía", "Amazonía", "Insular"]
DATES_RANGE = np.arange("2023-01-01", "2024-01-01", dtype="datetime64[D]")


class Config(BaseModel):
    records: int = 10_000_000
    batch_size: int = 1_000_000
    workers: int = 3
    rng_seed: int = 12345
    csv_output_path: str = "challenge/dataset_preparation/dataset_improved6.csv"
    log_level: int = logging.INFO

    @cached_property
    def number_of_batches(self) -> int:
        return math.ceil(self.records / self.batch_size)

    @cached_property
    def random_number_generator(self) -> np.random.Generator:
        return np.random.default_rng(self.rng_seed)


class DataframeConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    start_id: int
    end_id: int
    dates_range: npt.NDArray[np.datetime64]
    batch_size: int
    seed: int


def main() -> None:
    """
    Generates and saves synthetic data to a CSV file.
    This function uses multiprocessing to generate data in parallel, which may result in the saved data being unordered.
    The generated data is written to the specified CSV file path.
    """
    config = Config()
    logging.basicConfig(level=config.log_level)

    with (
        open(config.csv_output_path, "w") as file,
        futures.ProcessPoolExecutor(max_workers=config.workers) as executor,
    ):
        write_header(file)
        df_configs = (generate_df_config(batch_num, config) for batch_num in range(config.number_of_batches))
        logger.info("Generating data")
        future_dfs = [executor.submit(_generate_df, df_config) for df_config in df_configs]
        logger.info("Writing data")
        results = [write_content(file, future_df) for future_df in futures.as_completed(future_dfs)]

    exceptions = [e for e in results if e is not None]
    if not exceptions:
        logger.info(f"Data generated and saved successfully to {config.csv_output_path}")
        return

    logger.error("The following errors occurred during the execution:")
    for e in exceptions:
        logger.error(e)

    logger.warning(f"{len(exceptions)}/{config.number_of_batches} batches failed")
    sys.exit(1)


def write_header(file: TextIO) -> None:
    file.write(",".join(FIELDS) + "\n")


def generate_df_config(batch_number: int, config: Config) -> DataframeConfig:
    start_id = batch_number * config.batch_size + 1
    end_id = start_id + config.batch_size
    next_seed = config.random_number_generator.integers(0, 2**63 - 1)
    df_config = DataframeConfig(
        start_id=start_id,
        end_id=end_id,
        dates_range=DATES_RANGE,
        batch_size=config.batch_size,
        seed=next_seed,
    )
    return df_config


def _generate_df(df_config: DataframeConfig) -> pl.DataFrame:
    data = generate_data_batch(**df_config.model_dump())
    df = pl.DataFrame(data)
    return df


def generate_data_batch(
    start_id: int, end_id: int, dates_range: npt.NDArray[np.datetime64], batch_size: int, seed: int
) -> dict[str, Any]:
    # Column oriented generator
    rng = np.random.default_rng(seed)
    ids = np.arange(start_id, end_id)
    dates = rng.choice(dates_range, batch_size)
    sales = rng.integers(10, 10001, batch_size)
    categories = rng.choice(PRODUCT_CATEGORIES, batch_size)
    regions = rng.choice(SALE_REGIONS, batch_size)
    data = dict(zip(FIELDS, (ids, dates, sales, categories, regions)))
    return data


def write_content(file: TextIO, future: futures.Future[pl.DataFrame]) -> Exception | None:
    try:
        df = future.result()
        df.write_csv(file, include_header=False)
        return None
    except Exception as e:
        logger.exception(f"An error ocurred while trying to write the data: {e}")
        return e


if __name__ == "__main__":
    main()

import logging
import sys
from pathlib import Path
from typing import Literal

import polars as pl
import pyarrow.csv
import pyarrow.parquet

from .parser import get_config

logger = logging.getLogger("Transform and load to Parquet")


class ErrorHandler:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.records: list[str] = []

    def register_error(self, record: str) -> Literal["skip"]:
        self.records.append(record)
        return "skip"

    def save(self) -> None:
        if self.records:
            logger.warning(f"{len(self.records)} invalid records. Saving records to '{self.path}'")
            with open(self.path, "a") as f:
                f.writelines(self.records)


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)
    logging.basicConfig(level=config.log_level)

    try:
        if not config.input.exists():
            raise FileNotFoundError(f"Input file '{config.input.name}' doesn't exist.")

        if not config.force and config.output.exists():
            raise FileExistsError("The file already exists. Add '-f' flag to overwrite it, or choose another name.")

        config.output.parent.mkdir(exist_ok=True)

        if not config.error_handling:
            logger.info("Reading file (ignore errors)")
            df = pl.read_csv(config.input, ignore_errors=True)

            logger.info("Generating Parquet file")
            file = config.output if config.partition_cols is None else config.output.parent
            # 'snappy' guarantees more backwards compatibility when dealing with older parquet readers
            # https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_parquet.html
            df.write_parquet(file, compression="snappy", partition_by=config.partition_cols)
        else:
            logger.info("Reading file")
            quarantine = ErrorHandler(config.output.parent / "quarantine.csv")
            parse_options = pyarrow.csv.ParseOptions(invalid_row_handler=quarantine.register_error)
            table = pyarrow.csv.read_csv(config.input, parse_options=parse_options)
            quarantine.save()

            logger.info("Generating Parquet file")
            pyarrow.parquet.write_to_dataset(
                table=table, root_path=config.output.parent, partition_cols=config.partition_cols
            )

        logger.info(f"Successfully transform and load '{config.input.name}' to Parquet")

    except FileNotFoundError as e:
        logger.exception(e)
        sys.exit(1)
    except FileExistsError as e:
        logger.exception(e)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

import logging
import sys
from pathlib import Path
from typing import Literal

import pyarrow
import pyarrow.csv
import pyarrow.parquet

from .parser import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Carga y transformación a Parquet")


class ErrorHandler:
    def __init__(self, path: str) -> None:
        self.path = path
        self.records: list[str] = []

    def register_error(self, record) -> Literal["skip"]:
        self.records.append(record)
        return "skip"

    def save(self) -> None:
        if self.records:
            logger.warning(f"Encontrados {len(self.records)} inválidos. Enviando registros a archivo '{self.path}'")
        with open(self.path, "a") as f:
            for row in self.records:
                f.write(row + "\n")


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)

    try:
        if not Path(config.input).exists():
            raise FileNotFoundError(f"El archivo de entrada {config.input} no existe.")
        if not config.force and Path(config.output).exists():
            raise FileExistsError(f"El destino {config.output} ya existe.")
        Path(config.output).mkdir(exist_ok=True)

        logger.info("Leyendo archivo a transformar")
        quarantine = ErrorHandler(config.output + "/quarantine.csv")
        parse_options = pyarrow.csv.ParseOptions(invalid_row_handler=quarantine.register_error)
        table = pyarrow.csv.read_csv(config.input, parse_options=parse_options)
        quarantine.save()

        logger.info("Transformando a formato Parquet")
        pyarrow.parquet.write_to_dataset(
            table=table,
            root_path=config.output,
            partition_cols=config.partition_cols,
        )

        logger.info("Lectura y transformación de datos completada")

    except FileNotFoundError:
        logger.exception("El archivo de entrada no existe")
    except FileExistsError:
        logger.exception("El destino a escribir ya existe.")
        sys.exit(1)
    except pyarrow.ArrowInvalid as e:
        logger.exception(f"Error al leer el archivo: {e}")
        sys.exit(1)
    except pyarrow.ArrowException as e:
        logger.exception(f"Error al escribir el archivo: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

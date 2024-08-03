import logging
import sys
from pathlib import Path

import pyarrow  # type: ignore
import pyarrow.csv  # type: ignore
import pyarrow.parquet  # type: ignore

from .parser import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Carga y transformación a Parquet")


def main(argv: list[str] | None = None) -> None:
    config = get_config(argv)

    try:
        if not Path(config.input).exists():
            raise FileNotFoundError(f"El archivo de entrada {config.input} no existe.")
        if not config.force and Path(config.output).exists():
            raise FileExistsError(f"El destino {config.output} ya existe.")

        logger.info("Leyendo archivo a transformar")
        table = pyarrow.csv.read_csv(config.input)

        logger.info("Transformando a formato Parquet")
        pyarrow.parquet.write_to_dataset(
            table=table,
            root_path=config.output,
            partition_cols=config.partition_cols,
        )

        logger.info("Lectura y transformación de datos completada")

    except FileNotFoundError:
        logger.error("El archivo de entrada no existe")
    except FileExistsError:
        logger.error("El destino a escribir ya existe.")
        sys.exit(1)
    except pyarrow.ArrowInvalid as e:
        logger.error(f"Error al leer el archivo: {e}")
        sys.exit(1)
    except pyarrow.ArrowException as e:
        logger.error(f"Error al escribir el archivo: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

import numpy
import pandas
from pydantic import BaseModel, ConfigDict

from .types import DateArray, IntArray, StrArray


class Config(BaseModel):
    num_records: int = 10_000_000
    batch_size: int = 1_000_000
    number_of_batches: int = num_records // batch_size
    dates_range: DateArray = pandas.date_range(start="2023-01-01", end="2023-12-31").to_numpy()
    product_categories: list[str] = ["Moda", "Tecnología", "Belleza", "Salud", "Juguetes"]
    sale_regions: list[str] = ["Caribe", "Andina", "Pacífico", "Orinoquía", "Amazonía", "Insular"]
    random_number_generator: numpy.random.Generator = numpy.random.default_rng(12345)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class DataBatch(BaseModel):
    id_cliente: IntArray
    fecha_de_transaccion: DateArray
    cantidad_de_venta: IntArray
    categoria_de_producto: StrArray
    region_de_venta: StrArray

    model_config = ConfigDict(arbitrary_types_allowed=True)

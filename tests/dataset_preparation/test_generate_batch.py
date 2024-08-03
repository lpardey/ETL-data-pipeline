import numpy
import pandas

from challenge.dataset_preparation.config import Config
from challenge.dataset_preparation.generate_dataset import generate_dataframe_batch


def test_generate_dataframe_batch(mock_config: Config):
    start_id = 1
    end_id = 6
    expected_ids = numpy.arange(start_id, end_id)

    dataframe = generate_dataframe_batch(start_id, end_id, mock_config)

    assert isinstance(dataframe, pandas.DataFrame)
    assert dataframe.shape[0] == 5
    assert (dataframe["id_cliente"] == expected_ids).all()
    assert dataframe["categoria_de_producto"].isin(mock_config.product_categories).all()
    assert dataframe["region_de_venta"].isin(mock_config.sale_regions).all()

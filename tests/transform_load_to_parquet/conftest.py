import pytest

from tests.conftest import DATASET


@pytest.fixture(
    params=[
        pytest.param([], id="Default behavior"),
        pytest.param(["-p", "region_de_venta"], id="Partition"),
        pytest.param(["-e"], id="With error handling"),
        pytest.param(["-p", "region_de_venta", "-e"], id="Partition with error handling"),
    ],
)
def argv(request: pytest.FixtureRequest, tmp_path):
    return [str(DATASET), str(tmp_path), "-f"] + request.param

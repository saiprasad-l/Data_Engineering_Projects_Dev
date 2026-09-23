import pytest

from src import load


@pytest.fixture(scope="session")
def spark():
    from src.spark import get_spark
    return get_spark()


@pytest.fixture(scope="session")
def loans():
    if not load.SAMPLE.exists():
        pytest.skip("sample not built - run: python -m src.sample --input <kaggle csv>")
    return load.loans()

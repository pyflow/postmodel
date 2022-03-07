
import pytest

@pytest.fixture()
def db_url():
    return 'postgres://postgres:postgres@localhost:5432/test_db?min_size=10&max_size=30'


@pytest.fixture()
def db_url2():
    return 'postgres://postgres:postgres@localhost:5432/test_db2?min_size=10&max_size=30'
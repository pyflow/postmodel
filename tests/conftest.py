
import pytest

@pytest.fixture()
def db_url():
    return 'postgres://postgres@localhost:54320/test_db?min_size=10&max_size=30'

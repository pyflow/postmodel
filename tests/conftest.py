
import pytest

@pytest.fixture()
def db_url():
    return 'postgres://postgres@localhost:54320/test_db'


from postmodel import Postmodel
import pytest
from postmodel import models
import sys
from postmodel.exceptions import ConfigurationError, DBConnectionError
from tests.testmodels import Foo, Book


class Foo2(Foo):
    class Meta:
        table = "foo_mapper"
        db_name = "db2"

class Book2(Book):
    class Meta:
        table = "book"
        db_name = "db2"

@pytest.mark.asyncio
async def test_multi_db_1(db_url, db_url2):
    await Postmodel.init(db_url, extra_db_urls={"db2": db_url2}, modules=[__name__])
    assert len(Postmodel._databases) == 2
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    mapper = Postmodel.get_mapper(Foo2)
    await mapper.delete_table()
    mapper = Postmodel.get_mapper(Book2)
    await mapper.delete_table()
    await Postmodel.close()


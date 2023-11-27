
from postmodel import Postmodel
import pytest
from postmodel import models
import sys
from postmodel.exceptions import ConfigurationError, DBConnectionError

class Foo(models.Model):
    foo_id = models.IntField(pk=True)

    class Meta:
        table = "foo_init"

@pytest.mark.asyncio
async def test_init_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    await Postmodel.close()

@pytest.mark.asyncio
async def test_init_2(db_url):
    with pytest.raises(ConfigurationError):
        await Postmodel.generate_schemas()
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    with pytest.raises(Exception):
        await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    await Postmodel.close()

    with pytest.raises(ConfigurationError):
        await Postmodel.init(db_url.replace('postgres://', 'mysql://'), modules=[__name__])
    with pytest.raises(ConfigurationError):
        Postmodel.DATABASE_CLASS['redisql'] = ('postmodel.sqldb.postgres', 'RedisqlEngine')
        await Postmodel.init(db_url.replace('postgres://', 'redisql://'), modules=[__name__])
    with pytest.raises(ConfigurationError):
        await Postmodel.init(db_url.replace('5432', 'abcd'), modules=[__name__])

    with pytest.raises(DBConnectionError):
        await Postmodel.init('postgres://postgres@127.0.0.1/test_db', modules=[__name__])
    await Postmodel.close()

    current_module = sys.modules[__name__]
    setattr(current_module, '__models__', 'Foo')

    with pytest.raises(ConfigurationError):
        await Postmodel.init(db_url, modules=[__name__])
    await Postmodel.close()

    setattr(current_module, '__models__', ["Foo"])
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1

    del current_module.__models__
    await Postmodel.close()
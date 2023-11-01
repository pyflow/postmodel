
from postmodel import Postmodel
import pytest
from postmodel import models
import sys
from postmodel.exceptions import ConfigurationError, DBConnectionError
from tests.testmodels import FooJsonModel


@pytest.mark.asyncio
async def test_json_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()

    await FooJsonModel.all().delete()

    m = await FooJsonModel.create(foo_id=1, value={"name": "tom", "age": 32})
    await FooJsonModel.bulk_create([
        FooJsonModel(foo_id=2, value={"name": "duck"}),
        FooJsonModel(foo_id=3, value={"nickname": "tom", "items2": []}),
        FooJsonModel(foo_id=4, value={"name": "jerry", "age": 22, "items":["book", "pen"]}),
        FooJsonModel(foo_id=5, value={"age": 24}),
        FooJsonModel(foo_id=6, value=["book", "pen"])
    ])

    await FooJsonModel.bulk_create([
        FooJsonModel(foo_id=11, value={"info": {"name": "tom", "age": 32} }),
        FooJsonModel(foo_id=12, value={"info": {"name": "duck"}}),
        FooJsonModel(foo_id=13, value={"info": {"nickname": "tom", "items2": []}}),
        FooJsonModel(foo_id=14, value={"info": {"name": "jerry", "age": 22, "items":["book", "pen"]}}),
        FooJsonModel(foo_id=15, value={"info": {"age": 24}}),
        FooJsonModel(foo_id=16, value={"info": ["book", "pen"]})
    ])

    m = await FooJsonModel.filter(**{"value.name": "jerry"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 4

    m = await FooJsonModel.filter(**{"value.age": 24}).all()
    assert len(m) == 1
    assert m[0].foo_id == 5

    m = await FooJsonModel.filter(**{"value.age__gt": 30}).all()
    assert len(m) == 1
    assert m[0].foo_id == 1

    m = await FooJsonModel.filter(**{"value.age__gte": 30}).all()
    assert len(m) == 1
    assert m[0].foo_id == 1

    m = await FooJsonModel.filter(**{"value.age__lt": 23}).all()
    assert len(m) == 1
    assert m[0].foo_id == 4

    m = await FooJsonModel.filter(**{"value.age__lte": 22}).all()
    assert len(m) == 1
    assert m[0].foo_id == 4

    m = await FooJsonModel.filter(**{"value.name__startswith": "d"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 2

    m = await FooJsonModel.filter(**{"value.name__endswith": "y"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 4

    m = await FooJsonModel.filter(**{"value__has_key": "items"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 4

    m = await FooJsonModel.filter(**{"value__has_keys": ["book", "pen"]}).all()
    assert len(m) == 1
    assert m[0].foo_id == 6

    m = await FooJsonModel.filter(**{"value__has_keys": ["nickname", "items2"]}).all()
    assert len(m) == 1
    assert m[0].foo_id == 3

    m = await FooJsonModel.filter(**{"value__has_anykeys": ["nickname", "items2"]}).all()
    assert len(m) == 1
    assert m[0].foo_id == 3

    m = await FooJsonModel.filter(**{"value__has_anykeys": ["name", "age"]}).all()
    assert len(m) == 4

    m = await FooJsonModel.filter(**{"value__contains": {"name": "tom"}}).all()
    assert len(m) == 1
    assert m[0].foo_id == 1

    m = await FooJsonModel.filter(**{"value.info.name": "jerry"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 14

    m = await FooJsonModel.filter(**{"value.info.age": 24}).all()
    assert len(m) == 1
    assert m[0].foo_id == 15

    m = await FooJsonModel.filter(**{"value.info.age__gt": 30}).all()
    assert len(m) == 1
    assert m[0].foo_id == 11

    m = await FooJsonModel.filter(**{"value.info.age__gte": 30}).all()
    assert len(m) == 1
    assert m[0].foo_id == 11

    m = await FooJsonModel.filter(**{"value.info.age__lt": 23}).all()
    assert len(m) == 1
    assert m[0].foo_id == 14

    m = await FooJsonModel.filter(**{"value.info.age__lte": 22}).all()
    assert len(m) == 1
    assert m[0].foo_id == 14

    m = await FooJsonModel.filter(**{"value.info.name__startswith": "d"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 12

    m = await FooJsonModel.filter(**{"value.info.name__endswith": "y"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 14


    m = await FooJsonModel.filter(**{"value.info__has_key": "items"}).all()
    assert len(m) == 1
    assert m[0].foo_id == 14

    m = await FooJsonModel.filter(**{"value.info__has_keys": ["book", "pen"]}).all()
    assert len(m) == 1
    assert m[0].foo_id == 16

    m = await FooJsonModel.filter(**{"value.info__has_keys": ["nickname", "items2"]}).all()
    assert len(m) == 1
    assert m[0].foo_id == 13

    m = await FooJsonModel.filter(**{"value.info__has_anykeys": ["nickname", "items2"]}).all()
    assert len(m) == 1
    assert m[0].foo_id == 13

    m = await FooJsonModel.filter(**{"value.info__has_anykeys": ["name", "age"]}).all()
    assert len(m) == 4

    m = await FooJsonModel.filter(**{"value.info__contains": {"name": "tom"}}).all()
    assert len(m) == 1
    assert m[0].foo_id == 11

    await FooJsonModel.all().delete()
    await Postmodel.close()


from postmodel import Postmodel, models
import pytest
from postmodel.exceptions import (
    IntegrityError,
    OperationalError,
    DoesNotExist,
    MultipleObjectsReturned)
import asyncio
from postmodel.models import QueryExpression, Q
from postmodel.models import functions as fn
from tests.testmodels import (Foo, Book,
    CharFieldsModel)

@pytest.mark.asyncio
async def test_api_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    
    await CharFieldsModel.all().delete()

    m = await CharFieldsModel.create(id=1, char="hello", char_null="hi")
    await CharFieldsModel.bulk_create([
        CharFieldsModel(id=2, char="hello2"),
        CharFieldsModel(id=3, char="hello3", char_null="null3"),
        CharFieldsModel(id=4, char="hello4", char_null="null4"),
        CharFieldsModel(id=5, char="hello5")
    ])
    
    mlist = await CharFieldsModel.exclude(id = 3)
    assert len(mlist) == 4

    mlist = await CharFieldsModel.filter(id__in = [2, 4])
    assert len(mlist) == 2
    mlist = await CharFieldsModel.exclude(id__not_in = [2, 4])
    assert len(mlist) == 2

    mlist = await CharFieldsModel.exclude(id__in = [2, 4])
    assert len(mlist) == 3

    mlist = await CharFieldsModel.filter(id__not_in = [2, 4])
    assert len(mlist) == 3

    await CharFieldsModel.all().delete()
    await Postmodel.close()


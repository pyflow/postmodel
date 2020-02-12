
from postmodel import Postmodel
import pytest
from postmodel import models

class Foo(models.Model):
    foo_id = models.IntField(pk=True)

@pytest.mark.asyncio
async def test_init_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    await Postmodel.close()
    print('test_init_1 end.')
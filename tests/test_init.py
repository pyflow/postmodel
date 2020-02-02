
from postmodel import Postmodel
import pytest
from postmodel import Model, fields

class Foo(Model):
    foo_id = fields.IntField(pk=True)

@pytest.mark.asyncio
async def test_init_1():
    await Postmodel.init('postgres://user:pass@localhost:54320/test_db', modules=[__name__])
    assert len(Postmodel._engines) == 1
    assert Postmodel._inited == True